# Ejecución: python EV_Central.py 9090 localhost:9092 (python EV_Central.py <puerto_socket> <broker_kafka>)
#
# Este script implementa "EV_Central", el componente principal del sistema.
# Es el cerebro que gestiona la lógica de negocio, monitoriza los CPs y
# se comunica con los Drivers (vía Kafka) y los Monitors de los CPs (vía Sockets).
#

import sys
import socket
import threading
import json
import time
import os
from collections import deque
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Constantes ---
# El enunciado permite usar "simples ficheros" como base de datos.
# Este fichero simula la BD de CPs, almacenando su ID, ubicación y precio.
DATA_FILE = "data.json"

# Temas de Kafka (el "Queues and event Manager" del diagrama)
TOPIC_DRIVER_REQUESTS = "driver_requests"      # Driver -> Central (Peticiones de recarga)
TOPIC_DRIVER_NOTIFY = "driver_notifications"   # Central -> Driver (Autorizaciones, denegaciones, tickets)
TOPIC_CP_STATUS = "cp_status_updates"        # CP_E -> Central (Telemetría en tiempo real durante la carga)
TOPIC_CP_TRANSACTIONS = "cp_transactions"    # CP_E -> Central (Notificación de fin de suministro/ticket)
# Los topics de autorización son dinámicos: "cp_auth_{cp_id}" (Central -> CP_E específico)

# --- Colores ANSI para el Panel ---
# Clase para definir los colores de fondo requeridos en el panel de monitorización
class Color:
    GREEN_BG = '\033[42m\033[30m'  # Fondo Verde (Activado, Suministrando)
    RED_BG = '\033[41m'           # Fondo Rojo (Averiado)
    ORANGE_BG = '\033[43m\033[30m' # Fondo Naranja (Parado / "Out of Order")
    GRAY_BG = '\033[47m\033[30m'  # Fondo Gris (Desconectado)
    BLUE_BG = '\033[44m'          # Fondo Azul (Para el título del panel)
    RESET = '\033[0m'
    BOLD = '\033[1m'
    WHITE = '\033[97m'

# --- Variables Globales ---
kafka_producer = None            # Objeto productor de Kafka (para enviar mensajes)
kafka_bootstrap_servers = None # IP:Puerto del broker Kafka
socket_port = None             # Puerto TCP donde CENTRAL escuchará a los Monitors
panel_refresh_event = threading.Event() # Evento para detener el hilo de refresco de la UI

# Estructuras de datos (protegidas por cerrojo)
# cp_states es la "BD en memoria" que mantiene el estado en tiempo real de todos los CPs.
# Es la fuente de verdad para el panel de monitorización.
cp_states = {}
# cp_states_lock es crucial porque múltiples hilos (Socket, Kafka, UI) acceden a cp_states simultáneamente.
cp_states_lock = threading.Lock()
on_going_requests = [] # Almacena las últimas peticiones para el panel
application_messages = deque(maxlen=5) # Almacena los últimos mensajes de log para el panel

# --- Funciones de Persistencia y Log ---

def log_message(message):
    """Añade un mensaje al log de la aplicación para el panel (*** APLICATION MESSAGES ***)."""
    with cp_states_lock:
        timestamp = time.strftime('%H:%M:%S')
        application_messages.append(f"[{timestamp}] {message}")
        
def load_initial_data():
    """
    Carga la configuración de CPs y Drivers desde data.json.
    Implementa el Paso 1 de la "Mecánica de la solución".
    Inicializa los CPs en estado DESCONECTADO.
    """
    global cp_states
    try:
        with open(DATA_FILE, 'r') as f:
            data = json.load(f)
            
        with cp_states_lock:
            # Lee la configuración de cada CP del fichero
            for cp_id, config in data.get("charge_points", {}).items():
                cp_states[cp_id] = {
                    "location": config.get("location", "N/A"),     # Ubicación
                    "price_kwh": config.get("price_kwh", 0.50),  # Precio
                    "status": "DISCONNECTED", # Estado inicial
                    # Datos de sesión (se rellenan durante la carga)
                    "driver_id": None,       # ID del conductor
                    "session_kwh": 0.0,    # Consumo en Kw
                    "session_cost": 0.0    # Importe en €
                }
            # (Opcional) Cargar drivers si es necesario
            # drivers = data.get("drivers", {})
        log_message(f"Cargados {len(cp_states)} CPs desde {DATA_FILE}")

    except FileNotFoundError:
        log_message(f"[Error] No se encontró {DATA_FILE}. Iniciando con 0 CPs.")
    except json.JSONDecodeError:
        log_message(f"[Error] {DATA_FILE} está mal formateado.")
    except Exception as e:
        log_message(f"[Error] Cargando {DATA_FILE}: {e}")

# --- Funciones de Kafka ---

def send_kafka_message(topic, message):
    """Función helper para enviar un mensaje JSON a un topic de Kafka."""
    global kafka_producer
    try:
        # El productor se inicializa en main()
        kafka_producer.send(topic, value=message)
        kafka_producer.flush() # Asegura que el mensaje se envíe inmediatamente
    except Exception as e:
        log_message(f"[Error Kafka] No se pudo enviar a {topic}: {e}")

# --- Hilos Consumidores de Kafka ---

def kafka_consumer_driver_requests():
    """
    Este hilo escucha peticiones de carga de los EV_Driver (Punto 2.b y 3 de la Mecánica).
    Se suscribe al topic 'driver_requests'.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_DRIVER_REQUESTS,
            bootstrap_servers=kafka_bootstrap_servers,
            auto_offset_reset='earliest', # Lee desde el principio si es un consumidor nuevo
            group_id='central-driver-requests-group', # ID de grupo para Kafka
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializa JSON
        )
    except Exception as e:
        log_message(f"[Error Kafka] No se pudo iniciar consumidor de Drivers: {e}")
        return

    log_message("Consumidor Kafka (Driver Requests) iniciado.")
    # Bucle infinito para procesar mensajes
    for message in consumer:
        try:
            msg = message.value
            cp_id = msg.get('cp_id')
            driver_id = msg.get('driver_id')

            # --- Variables para enviar FUERA del lock ---
            auth_topic = None
            auth_msg = None
            notify_topic = TOPIC_DRIVER_NOTIFY
            notify_msg = None
            log_msg = f"Petición de {driver_id} para {cp_id}"
            
            # --- Sección Crítica ---
            # Bloqueamos el estado para tomar una decisión de autorización
            with cp_states_lock:
                # Añadir al panel de "ON GOING DRIVERS REQUESTS"
                on_going_requests.append({
                    "date": time.strftime('%d/%m/%y'),
                    "time": time.strftime('%H:%M:%S'),
                    "user_id": driver_id,
                    "cp_id": cp_id
                })
                
                # Limitar historial de peticiones (mostramos solo las 5 últimas)
                if len(on_going_requests) > 5:
                    on_going_requests.pop(0)

                cp = cp_states.get(cp_id)
                
                # Lógica de autorización (Punto 4 de la Mecánica)
                # Comprobar que el CP existe Y su estado es 'IDLE' ('Activado')
                if cp and cp['status'] == 'IDLE':
                    # AUTORIZAR
                    cp['status'] = 'AUTHORIZED' # Estado interno (esperando plug-in)
                    price = cp['price_kwh']
                    
                    # 1. Preparar mensaje para el EV_CP_E (Engine)
                    # Se envía a un topic específico de ese CP
                    auth_topic = f"cp_auth_{cp_id}"
                    auth_msg = {
                        "action": "AUTHORIZE",
                        "driver_id": driver_id,
                        "price_kwh": price
                    }
                    # 2. Preparar notificación para el EV_Driver
                    notify_msg = {
                        "driver_id": driver_id,
                        "status": "AUTHORIZED",
                        "cp_id": cp_id,
                        "info": f"Autorizado. Precio: {price}€/kWh. Conecte el vehículo."
                    }
                    log_msg = f"Autorizada carga de {driver_id} en {cp_id}"
                
                else:
                    # DENEGAR
                    status = cp['status'] if cp else 'UNKNOWN'
                    # 1. Preparar notificación de denegación para el EV_Driver
                    notify_msg = {
                        "driver_id": driver_id,
                        "status": "DENIED",
                        "cp_id": cp_id,
                        "info": f"CP no disponible. Estado actual: {status}"
                    }
                    log_msg = f"Denegada carga de {driver_id} en {cp_id} (Estado: {status})"
            # --- Fin Sección Crítica ---

            # --- Enviar mensajes FUERA del lock ---
            # Es importante enviar los mensajes fuera del cerrojo para no bloquear
            # el procesamiento mientras se realizan las (lentas) operaciones de red/Kafka.
            log_message(log_msg)
            if auth_msg:
                # Envía la autorización al Engine (EV_CP_E)
                send_kafka_message(auth_topic, auth_msg)
            if notify_msg:
                # Envía la notificación (AUTORIZADO o DENEGADO) al Driver
                send_kafka_message(notify_topic, notify_msg)

        except Exception as e:
            log_message(f"[Error Proc. Petición] {e}")

def kafka_consumer_cp_updates():
    """
    Escucha telemetría (TOPIC_CP_STATUS) y tickets (TOPIC_CP_TRANSACTIONS)
    provenientes de los EV_CP_E.
    Asegura que los tickets (incluido el STOP_BY_CENTRAL) sean reenviados al Driver.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_CP_STATUS,        # Para telemetría (Punto 8)
            TOPIC_CP_TRANSACTIONS,  # Para tickets finales (Punto 9)
            bootstrap_servers=kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='central-cp-updates-group', # ID de grupo distinto
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        log_message(f"[Error Kafka] No se pudo iniciar consumidor de CPs: {e}")
        return

    log_message("Consumidor Kafka (CP Updates) iniciado.")
    for message in consumer:
        try:
            msg = message.value
            cp_id = msg.get('cp_id')
            
            # --- Variables para enviar FUERA del lock ---
            notify_topic = None
            notify_msg = None 
            log_msg = None
            
            # --- Sección Crítica ---
            with cp_states_lock:
                if cp_id not in cp_states:
                    continue # Ignorar mensajes de CPs desconocidos

                if message.topic == TOPIC_CP_STATUS:
                    # Es un mensaje de telemetría (Punto 8 de la Mecánica)
                    # El EV_CP_E envía esto cada segundo durante la carga.
                    # Lógica de telemetría STATUS sin cambios
                    status = msg.get('status')
                    cp_states[cp_id]['status'] = status
                    
                    if status == 'CHARGING':
                        # Actualizar datos de sesión para el panel en tiempo real
                        cp_states[cp_id]['driver_id'] = msg.get('driver_id')
                        cp_states[cp_id]['session_kwh'] = msg.get('session_kwh')
                        cp_states[cp_id]['session_cost'] = msg.get('session_cost')
                    elif status == 'FAULTED':
                        # El Engine también puede reportar averías (ej. simulación menú)
                        log_msg = f"¡AVERÍA reportada por Engine {cp_id}!"
                    

                elif message.topic == TOPIC_CP_TRANSACTIONS:
                    # Es un ticket final (Punto 9 de la Mecánica)
                    # (CHARGE_COMPLETE, CHARGE_FAILED, CHARGE_STOPPED_BY_CENTRAL)
                    event = msg.get('event') 
                    driver_id = msg.get('driver_id')
                    
                    # Resetear datos de sesión
                    # Limpiar datos de sesión del CP en el panel
                    cp_states[cp_id]['driver_id'] = None
                    cp_states[cp_id]['session_kwh'] = 0.0
                    cp_states[cp_id]['session_cost'] = 0.0
                    
                    # Actualizar estado si fue COMPLETO 
                    # El CP vuelve a estado IDLE (disponible)
                    if event == "CHARGE_COMPLETE":
                         cp_states[cp_id]['status'] = 'IDLE' # "El CP volverá al estado de reposo"
                    
                    # --- ASIGNACIÓN DE VARIABLES DE ENVÍO (Resuelve el problema de ámbito) ---
                    # Preparar el reenvío del ticket al Driver
                    # El Driver está escuchando en TOPIC_DRIVER_NOTIFY
                    notify_topic = TOPIC_DRIVER_NOTIFY
                    notify_msg = msg # <-- AHORA SÍ ASIGNA EL TICKET AL VALOR CORRECTO
                    
                    if event == "CHARGE_STOPPED_BY_CENTRAL":
                        # Caso especial: Parada forzada (Punto 13.a)
                        log_msg = f"Parada Forzada: Ticket parcial enviado a {driver_id} de CP {cp_id}"
                    else:
                        log_msg = f"Ticket final ({event}) enviado a {driver_id} de CP {cp_id}"
            # --- Fin Sección Crítica ---
            
            # --- Enviar mensajes FUERA del lock ---
            if log_msg:
                log_message(log_msg)
            
            # 2. Reenvío del Ticket al Driver (Desbloqueo)
            if notify_msg:
                send_kafka_message(notify_topic, notify_msg)

        except Exception as e:
            log_message(f"[Error Proc. Update CP] {e}")

# --- Hilos del Servidor de Sockets (para Monitors) ---
#
# Esta sección gestiona la comunicación directa con EV_CP_M (Monitor)
# como se ve en el diagrama (Sockets (Status & Autentication)).
#

def handle_monitor_client(conn, addr):
    """
    Gestiona la conexión de un EV_CP_M (Monitor).
    Se ejecuta en un hilo separado por cada Monitor conectado.
    
    Tiene 2 fases:
    1. Registro: Autentica al Monitor (Punto 5 y 10).
    2. Escucha: Recibe actualizaciones de estado (OK/FAULTED) (Punto 10).
    """
    cp_id = None
    ack_msg = None
    log_msg = None
    try:
        # 1. Proceso de Registro
        data = conn.recv(1024).decode('utf-8')
        if not data:
            print(f"[Socket] Cliente {addr} desconectado antes de registrarse.")
            return

        msg = json.loads(data)
        
        # El Monitor debe enviar un mensaje 'REGISTER_MONITOR' para identificarse
        if msg.get('type') == 'REGISTER_MONITOR':
            cp_id = msg.get('cp_id')
            # --- Sección Crítica ---
            with cp_states_lock:
                # Comprobar si el CP_ID existe en nuestra "BD" (cargada de data.json)
                if cp_id in cp_states:
                    # Si estaba desconectado, ahora está Activado (IDLE)
                    # Si estaba DESCONECTADO, ahora que el Monitor se ha conectado,
                    # pasa a estar disponible (IDLE / 'Activado').
                    if cp_states[cp_id]['status'] == 'DISCONNECTED':
                         cp_states[cp_id]['status'] = 'IDLE'
                    ack_msg = b'ACK_REGISTER' # Confirmar registro
                    log_msg = f"Monitor {cp_id} conectado desde {addr}. Estado -> IDLE"
                else:
                    # CP_ID no encontrado en data.json
                    ack_msg = b'NACK_UNKNOWN_CP'
                    log_msg = f"Monitor {cp_id} ({addr}) RECHAZADO (CP desconocido)"
                    cp_id = None # Marcar para desconexión
            # --- Fin Sección Crítica ---
        else:
            ack_msg = b'NACK_EXPECTED_REGISTER'
            cp_id = None # Marcar para desconexión

        # --- Enviar respuesta y log FUERA del lock ---
        if ack_msg:
            conn.sendall(ack_msg)
        if log_msg:
            log_message(log_msg)

        if cp_id is None:
            return # Cerrar conexión si el registro falló

        # 2. Bucle de escucha de estado (Averías)
        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                # Conexión perdida
                raise ConnectionResetError("Monitor disconnected")
            
            msg = json.loads(data)
            
            # Esperar mensajes de estado del Monitor
            if msg.get('type') == 'MONITOR_STATUS':
                status = msg.get('status') # "OK" o "FAULTED"
                info = msg.get('info', '')
                
                # Este lock es rápido (solo actualiza estado y log), está bien.
                # --- Sección Crítica ---
                with cp_states_lock:
                    if status == 'FAULTED':
                        # El monitor reporta una avería
                        if cp_states[cp_id]['status'] != 'FAULTED':
                            cp_states[cp_id]['status'] = 'FAULTED' # El panel lo pondrá en ROJO
                            log_message(f"¡AVERÍA de Monitor {cp_id}! ({info})")
                    
                    elif status == 'OK':
                        # El monitor reporta recuperación de avería
                        if cp_states[cp_id]['status'] == 'FAULTED':
                            cp_states[cp_id]['status'] = 'IDLE' # Vuelve a disponible (VERDE)
                            log_message(f"Avería RESUELTA en {cp_id}. Estado -> IDLE")
                # --- Fin Sección Crítica ---
                            
    except (ConnectionResetError, BrokenPipeError, json.JSONDecodeError) as e:
        log_message(f"[Socket] Conexión perdida con Monitor {cp_id if cp_id else addr}: {e}")
    except Exception as e:
        log_message(f"[Error Socket Handler] {e}")
    finally:
        if cp_id:
            # Si un monitor se desconecta (por error o cierre), el CP pasa a estado DESCONECTADO
            # Esto implementa el "fallo de red".
            with cp_states_lock:
                if cp_id in cp_states:
                    cp_states[cp_id]['status'] = 'DISCONNECTED' # El panel lo pondrá en GRIS
                    log_message(f"Monitor {cp_id} desconectado. Estado -> DISCONNECTED")
        conn.close()

def start_socket_server():
    """
    Inicia el servidor de sockets principal.
    Acepta conexiones entrantes de los EV_CP_M y crea un hilo (handle_monitor_client) para cada uno.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Permite reusar el puerto
    try:
        # Escucha en el puerto especificado en los argumentos
        server_socket.bind(('0.0.0.0', socket_port))
        server_socket.listen()
        log_message(f"Servidor Socket escuchando en puerto {socket_port}...")

        while True:
            conn, addr = server_socket.accept()
            # Inicia un nuevo hilo para gestionar este monitor
            threading.Thread(target=handle_monitor_client, args=(conn, addr), daemon=True).start()
            
    except OSError as e:
        log_message(f"[Error Fatal Socket] {e}. ¿Puerto {socket_port} en uso?")
        os._exit(1) # Salida forzada si no puede bindear el puerto
    except Exception as e:
        log_message(f"[Error Socket Server] {e}")
    finally:
        server_socket.close()

# --- Panel de Monitorización (UI) ---
#
# Esta sección implementa el panel de control descrito en las páginas 4 y 5 del PDF.
#

def draw_panel():
    """
    Dibuja el panel de monitorización en la consola.
    Lee el estado actual de `cp_states` (protegido por lock) y lo formatea
    según los requisitos de estado y color.
    """
    os.system('cls' if os.name == 'nt' else 'clear') # Limpiar la consola
    
    # Título del Panel (similar a Figura 1)
    print(f"{Color.BLUE_BG}{Color.BOLD}{Color.WHITE}" + 
          " *** SD EV CHARGING SOLUTION. MONITORIZATION PANEL *** " +
          f"{Color.RESET}")
    print("-" * 54)
    
    cp_blocks = []
    # --- Sección Crítica ---
    with cp_states_lock: # Acceso seguro a la estructura de datos
        # Ordenamos los CPs por ID para una visualización consistente
        for cp_id, data in sorted(cp_states.items()):
            status = data['status']
            color = Color.GRAY_BG # Color por defecto (Desconectado)
            lines = [
                f"{cp_id}",                       # ID del CP
                data['location'],                 # Ubicación
                f"{data['price_kwh']:.2f}€/kWh"   # Precio
            ]
            
            # Mapeo de estados a colores y texto según el enunciado
            if status == 'IDLE' or status == 'AUTHORIZED':
                color = Color.GREEN_BG # Activado (VERDE)
            elif status == 'CHARGING':
                color = Color.GREEN_BG # Suministrando (VERDE)
                # Añadir datos de sesión en tiempo real
                lines.append(f"Driver {data['driver_id']}") # Id del conductor
                lines.append(f"{data['session_kwh']:.3f} kWh") # Consumo en Kw
                lines.append(f"{data['session_cost']:.2f} €")  # Importe en €
            elif status == 'STOPPED':
                color = Color.ORANGE_BG # Parado (NARANJA)
                lines.append("Out of Order") # Leyenda "Out of Order"
            elif status == 'FAULTED':
                color = Color.RED_BG # Averiado (ROJO)
                lines.append("AVERIADO")
            elif status == 'DISCONNECTED':
                color = Color.GRAY_BG # Desconectado (GRIS)
                lines.append("DESCONECTADO")
                
            cp_blocks.append((color, lines))
    # --- Fin Sección Crítica ---

    # Lógica para dibujar los CPs en filas (5 por fila, como en Figura 1)
    max_lines = max(len(b[1]) for b in cp_blocks) if cp_blocks else 0
    for i in range(0, len(cp_blocks), 5):
        row_blocks = cp_blocks[i:i+5]
        for line_idx in range(max_lines):
            line_str = ""
            for color, lines in row_blocks:
                if line_idx < len(lines):
                    line_str += f"{color}{lines[line_idx]:<15}{Color.RESET} "
                else:
                    line_str += f"{color}{' ':<15}{Color.RESET} "
            print(line_str.strip())
        print(f"{' ' * (16 * len(row_blocks) - 1)}") # Espacio entre filas

def panel_refresh_loop():
    """
    Hilo dedicado a redibujar el panel y los logs cada 2 segundos.
    Esto permite que la UI se actualice mientras el hilo principal
    espera el input del administrador (en el menú).
    """
    while not panel_refresh_event.is_set():
        # 1. Dibujar los bloques de CPs
        draw_panel() 

        # 2. Dibujar peticiones ("ON GOING DRIVERS REQUESTS")
        print(f"\n{Color.BOLD}*** ON GOING DRIVERS REQUESTS ***{Color.RESET}")
        print(f"{'DATE':<10} {'START TIME':<10} {'User ID':<10} {'CP':<10}")
        with cp_states_lock:
            for req in reversed(on_going_requests[-3:]): # Mostrar solo las 3 últimas
                print(f"{req['date']:<10} {req['time']:<10} {req['user_id']:<10} {req['cp_id']:<10}")

        # 3. Dibujar mensajes ("APLICATION MESSAGES")
        print(f"\n{Color.BOLD}*** APLICATION MESSAGES ***{Color.RESET}")
        # log_message("CENTRAL system status OK") # El latido se genera por otros mensajes
        with cp_states_lock:
            for msg in application_messages:
                print(msg)

        # 4. Imprimir el menú y el prompt
        print("-" * 54)
        print_admin_menu()
        # Imprime el prompt sin saltar línea, esperando el input() del hilo main
        print("Seleccione una opción: ", end='', flush=True) 

        time.sleep(2) # Refrescar cada 2 segundos
    print("Hilo de refresco del panel detenido.")

def print_admin_menu():
    """
    Muestra el menú de administrador (Punto 13 de la Mecánica).
    """
    print(f"\n{Color.BOLD}--- MENÚ DE ADMINISTRADOR ---{Color.RESET}")
    print("1. Parar CP (Poner 'Out of Order')") # Punto 13.a
    print("2. Reanudar CP (Quitar 'Out of Order')") # Punto 13.b
    print("3. Salir")

# --- Función Principal ---

def main():
    global kafka_bootstrap_servers, socket_port, kafka_producer
    
    # 1. Validar argumentos de entrada
    if len(sys.argv) != 3:
        print("Uso: python EV_Central.py <puerto_escucha_socket> <ip_broker_kafka:puerto>")
        sys.exit(1)
        
    try:
        # Argumento 1: Puerto de escucha para Sockets (Monitors)
        socket_port = int(sys.argv[1])
    except ValueError:
        print("Error: El <puerto_escucha_socket> debe ser un número.")
        sys.exit(1)
        
    # Argumento 2: IP y puerto del Broker Kafka
    kafka_bootstrap_servers = sys.argv[2]
    log_message(f"Iniciando EV_Central...")
    log_message(f"Kafka Broker: {kafka_bootstrap_servers}")

    # 2. Cargar datos iniciales (Paso 1 de la Mecánica)
    load_initial_data()

    # 3. Inicializar Kafka Producer (para enviar autorizaciones y comandos)
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8') # Serializa a JSON
        )
    except NoBrokersAvailable:
        log_message(f"[Error Fatal] No se puede conectar a Kafka en {kafka_bootstrap_servers}")
        sys.exit(1)
    
    # 4. Iniciar hilos de red (Socket y Consumidores Kafka)
    # Todos los hilos se marcan como 'daemon=True' para que se
    # cierren automáticamente cuando el hilo principal (menú) termine.
    
    # Hilo para aceptar conexiones de Monitors (EV_CP_M)
    threading.Thread(target=start_socket_server, daemon=True).start()
    # Hilo para escuchar peticiones de Drivers (EV_Driver)
    threading.Thread(target=kafka_consumer_driver_requests, daemon=True).start()
    # Hilo para escuchar telemetría y tickets de los CPs (EV_CP_E)
    threading.Thread(target=kafka_consumer_cp_updates, daemon=True).start()
    
    # 5. Iniciar hilo de refresco del panel
    # Este hilo se encarga solo de la UI
    threading.Thread(target=panel_refresh_loop, daemon=True).start()

    # 6. Bucle principal (Gestión del Menú de Administrador)
    # El hilo principal se queda aquí, gestionando la entrada del usuario
    # para las opciones del Punto 13.
    try:
        while True:
            choice = input() # Espera el input del admin (el prompt lo pone el hilo del panel)

            if choice == '1': # Parar CP (Punto 13.a)
                cp_id = input("  > Introduzca ID del CP a PARAR: ").strip().upper()
                if cp_id in cp_states:
                    # 1. Enviar comando STOP al Engine (EV_CP_E) vía Kafka
                    auth_topic = f"cp_auth_{cp_id}"
                    send_kafka_message(auth_topic, {"action": "STOP_COMMAND"})
                    log_message(f"Comando PARAR enviado a {cp_id}")
                    # 2. Actualizar estado local para que el panel lo muestre (NARANJA)
                    # El CP pondrá su estado en "ROJO" (en la simulación es NARANJA)
                    with cp_states_lock:
                        cp_states[cp_id]['status'] = 'STOPPED' 
                else:
                    log_message(f"Comando PARAR fallido. CP {cp_id} no existe.")

            elif choice == '2': # Reanudar CP (Punto 13.b)
                cp_id = input("  > Introduzca ID del CP a REANUDAR: ").strip().upper()
                if cp_id in cp_states:
                    # 1. Enviar comando RESUME al Engine (EV_CP_E) vía Kafka
                    auth_topic = f"cp_auth_{cp_id}"
                    send_kafka_message(auth_topic, {"action": "RESUME_COMMAND"})
                    log_message(f"Comando REANUDAR enviado a {cp_id}")
                    # Nota: El CP (Engine) recibirá el comando y reportará su nuevo estado 'IDLE'
                    # vía telemetría (TOPIC_CP_STATUS), actualizando el panel a VERDE.
                else:
                    log_message(f"Comando REANUDAR fallido. CP {cp_id} no existe.")

            elif choice == '3': # Salir
                log_message("Apagando EV_Central...")
                panel_refresh_event.set() # Detener el hilo del panel
                time.sleep(1.1) # Esperar a que el hilo del panel se detenga
                break # Salir del bucle while

            else:
                log_message(f"Opción '{choice}' no válida.")

    except KeyboardInterrupt:
        log_message("Cierre solicitado (Ctrl+C).")
        panel_refresh_event.set() # Detener el hilo del panel
        time.sleep(1.1)
    finally:
        # Limpieza final
        if kafka_producer:
            kafka_producer.close()
        print("\nEV_Central apagado.")
        # Salida forzada para detener todos los hilos daemon
        os._exit(0)

if __name__ == "__main__":
    # Punto de entrada del script
    main()