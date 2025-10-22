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
DATA_FILE = "data.json"

# Temas de Kafka
TOPIC_DRIVER_REQUESTS = "driver_requests"      # Driver -> Central
TOPIC_DRIVER_NOTIFY = "driver_notifications"   # Central -> Driver
TOPIC_CP_STATUS = "cp_status_updates"        # CP_E -> Central (Telemetría)
TOPIC_CP_TRANSACTIONS = "cp_transactions"    # CP_E -> Central (Tickets)
# Los topics de autorización son dinámicos: "cp_auth_{cp_id}"

# --- Colores ANSI para el Panel ---
class Color:
    GREEN_BG = '\033[42m\033[30m'  # Fondo Verde, Texto Negro
    RED_BG = '\033[41m'           # Fondo Rojo
    ORANGE_BG = '\033[43m\033[30m' # Fondo Naranja, Texto Negro
    GRAY_BG = '\033[47m\033[30m'  # Fondo Gris, Texto Negro
    BLUE_BG = '\033[44m'          # Fondo Azul
    RESET = '\033[0m'
    BOLD = '\033[1m'
    WHITE = '\033[97m'

# --- Variables Globales ---
kafka_producer = None
kafka_bootstrap_servers = None
socket_port = None

# Estructuras de datos (protegidas por cerrojo)
cp_states = {}  # El estado en tiempo real de todos los CPs
cp_states_lock = threading.Lock()
on_going_requests = [] # Para el panel [cite: 120]
application_messages = deque(maxlen=5) # Para el panel [cite: 138]

# --- Funciones de Persistencia y Log ---

def log_message(message):
    """Añade un mensaje al log de la aplicación para el panel."""
    with cp_states_lock:
        timestamp = time.strftime('%H:%M:%S')
        application_messages.append(f"[{timestamp}] {message}")
        
def load_initial_data():
    """
    Carga la configuración de CPs y Drivers desde data.json.
    Inicializa los CPs en estado DESCONECTADO.
    """
    global cp_states
    try:
        with open(DATA_FILE, 'r') as f:
            data = json.load(f)
            
        with cp_states_lock:
            for cp_id, config in data.get("charge_points", {}).items():
                cp_states[cp_id] = {
                    "location": config.get("location", "N/A"),
                    "price_kwh": config.get("price_kwh", 0.50),
                    "status": "DISCONNECTED", # Estado inicial [cite: 168]
                    # Datos de sesión (se rellenan al cargar)
                    "driver_id": None,
                    "session_kwh": 0.0,
                    "session_cost": 0.0
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
    """Envía un mensaje JSON a un topic de Kafka."""
    global kafka_producer
    try:
        kafka_producer.send(topic, value=message)
        kafka_producer.flush()
    except Exception as e:
        log_message(f"[Error Kafka] No se pudo enviar a {topic}: {e}")

# --- Hilos Consumidores de Kafka ---

def kafka_consumer_driver_requests():
    """
    Escucha peticiones de carga de los EV_Driver.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_DRIVER_REQUESTS,
            bootstrap_servers=kafka_bootstrap_servers,
            auto_offset_reset='latest',
            group_id='central-driver-requests-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        log_message(f"[Error Kafka] No se pudo iniciar consumidor de Drivers: {e}")
        return

    log_message("Consumidor Kafka (Driver Requests) iniciado.")
    for message in consumer:
        try:
            msg = message.value
            if msg.get('action') == 'REQUEST_CHARGE':
                cp_id = msg.get('cp_id')
                driver_id = msg.get('driver_id')
                log_message(f"Petición de {driver_id} para {cp_id}")

                with cp_states_lock:
                    on_going_requests.append({
                        "date": time.strftime('%d/%m/%y'),
                        "time": time.strftime('%H:%M:%S'),
                        "user_id": driver_id,
                        "cp_id": cp_id
                    })
                    
                    # Limitar historial de peticiones
                    if len(on_going_requests) > 5:
                        on_going_requests.pop(0)

                    cp = cp_states.get(cp_id)
                    
                    if cp and cp['status'] == 'IDLE': # 'IDLE' es 'Activado' [cite: 82]
                        # AUTORIZAR
                        cp['status'] = 'AUTHORIZED' # Estado interno
                        price = cp['price_kwh']
                        auth_topic = f"cp_auth_{cp_id}"
                        
                        # Enviar autorización al CP_Engine [cite: 175]
                        send_kafka_message(auth_topic, {
                            "action": "AUTHORIZE",
                            "driver_id": driver_id,
                            "price_kwh": price
                        })
                        
                        # Notificar al conductor [cite: 176]
                        send_kafka_message(TOPIC_DRIVER_NOTIFY, {
                            "driver_id": driver_id,
                            "status": "AUTHORIZED",
                            "cp_id": cp_id,
                            "info": f"Autorizado. Precio: {price}€/kWh. Conecte el vehículo."
                        })
                        log_message(f"Autorizada carga de {driver_id} en {cp_id}")
                    
                    else:
                        # DENEGAR
                        status = cp['status'] if cp else 'UNKNOWN'
                        send_kafka_message(TOPIC_DRIVER_NOTIFY, {
                            "driver_id": driver_id,
                            "status": "DENIED",
                            "cp_id": cp_id,
                            "info": f"CP no disponible. Estado actual: {status}"
                        })
                        log_message(f"Denegada carga de {driver_id} en {cp_id} (Estado: {status})")

        except Exception as e:
            log_message(f"[Error Proc. Petición] {e}")

def kafka_consumer_cp_updates():
    """
    Escucha telemetría y tickets de los EV_CP_E.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_CP_STATUS,
            TOPIC_CP_TRANSACTIONS,
            bootstrap_servers=kafka_bootstrap_servers,
            auto_offset_reset='latest',
            group_id='central-cp-updates-group',
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
            
            with cp_states_lock:
                if cp_id not in cp_states:
                    continue # Ignorar mensaje de un CP desconocido

                if message.topic == TOPIC_CP_STATUS:
                    # Es un update de telemetría o estado [cite: 182]
                    status = msg.get('status')
                    cp_states[cp_id]['status'] = status
                    
                    if status == 'CHARGING':
                        cp_states[cp_id]['driver_id'] = msg.get('driver_id')
                        cp_states[cp_id]['session_kwh'] = msg.get('session_kwh')
                        cp_states[cp_id]['session_cost'] = msg.get('session_cost')
                    elif status == 'FAULTED':
                        log_message(f"¡AVERÍA reportada por Engine {cp_id}!")
                    elif status == 'IDLE':
                        # Limpiar datos de sesión si vuelve a IDLE
                        cp_states[cp_id]['driver_id'] = None
                        cp_states[cp_id]['session_kwh'] = 0.0
                        cp_states[cp_id]['session_cost'] = 0.0

                elif message.topic == TOPIC_CP_TRANSACTIONS:
                    # Es un ticket final [cite: 187]
                    event = msg.get('event') # CHARGE_COMPLETE o CHARGE_FAILED
                    driver_id = msg.get('driver_id')
                    
                    # Resetear estado del CP
                    cp_states[cp_id]['status'] = 'IDLE' # Vuelve a estar disponible
                    cp_states[cp_id]['driver_id'] = None
                    cp_states[cp_id]['session_kwh'] = 0.0
                    cp_states[cp_id]['session_cost'] = 0.0

                    # Enviar ticket final al conductor [cite: 187]
                    send_kafka_message(TOPIC_DRIVER_NOTIFY, msg)
                    log_message(f"Ticket final ({event}) enviado a {driver_id} de CP {cp_id}")

        except Exception as e:
            log_message(f"[Error Proc. Update CP] {e}")


# --- Hilos del Servidor de Sockets (para Monitors) ---

def handle_monitor_client(conn, addr):
    """
    Gestiona la conexión de un EV_CP_M (Monitor).
    Recibe el registro y luego los updates de estado (avería/OK).
    """
    cp_id = None
    try:
        # 1. Proceso de Registro
        data = conn.recv(1024).decode('utf-8')
        if not data:
            print(f"[Socket] Cliente {addr} desconectado antes de registrarse.")
            return

        msg = json.loads(data)
        
        if msg.get('type') == 'REGISTER_MONITOR':
            cp_id = msg.get('cp_id')
            with cp_states_lock:
                if cp_id in cp_states:
                    # Si estaba desconectado, ahora está Activado (IDLE)
                    if cp_states[cp_id]['status'] == 'DISCONNECTED':
                         cp_states[cp_id]['status'] = 'IDLE'
                    conn.sendall(b'ACK_REGISTER')
                    log_message(f"Monitor {cp_id} conectado desde {addr}. Estado -> IDLE")
                else:
                    conn.sendall(b'NACK_UNKNOWN_CP')
                    log_message(f"Monitor {cp_id} ({addr}) RECHAZADO (CP desconocido)")
                    cp_id = None # Marcar para desconexión
        else:
            conn.sendall(b'NACK_EXPECTED_REGISTER')
            cp_id = None # Marcar para desconexión

        if cp_id is None:
            return # Cerrar conexión si el registro falló

        # 2. Bucle de escucha de estado (Averías)
        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                # Conexión perdida
                raise ConnectionResetError("Monitor disconnected")
            
            msg = json.loads(data)
            
            if msg.get('type') == 'MONITOR_STATUS':
                status = msg.get('status') # "OK" o "FAULTED"
                info = msg.get('info', '')
                
                with cp_states_lock:
                    if status == 'FAULTED':
                        # El monitor reporta una avería [cite: 190]
                        if cp_states[cp_id]['status'] != 'FAULTED':
                            cp_states[cp_id]['status'] = 'FAULTED'
                            log_message(f"¡AVERÍA de Monitor {cp_id}! ({info})")
                    
                    elif status == 'OK':
                        # El monitor reporta recuperación [cite: 190]
                        if cp_states[cp_id]['status'] == 'FAULTED':
                            cp_states[cp_id]['status'] = 'IDLE' # Vuelve a disponible
                            log_message(f"Avería RESUELTA en {cp_id}. Estado -> IDLE")
                            
    except (ConnectionResetError, BrokenPipeError, json.JSONDecodeError) as e:
        log_message(f"[Socket] Conexión perdida con Monitor {cp_id if cp_id else addr}: {e}")
    except Exception as e:
        log_message(f"[Error Socket Handler] {e}")
    finally:
        if cp_id:
            # Si un monitor se desconecta, el CP pasa a estado DESCONECTADO [cite: 100]
            with cp_states_lock:
                if cp_id in cp_states:
                    cp_states[cp_id]['status'] = 'DISCONNECTED'
                    log_message(f"Monitor {cp_id} desconectado. Estado -> DISCONNECTED")
        conn.close()

def start_socket_server():
    """Inicia el servidor de sockets para escuchar a los Monitors."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server_socket.bind(('0.0.0.0', socket_port))
        server_socket.listen()
        log_message(f"Servidor Socket escuchando en puerto {socket_port}...")

        while True:
            conn, addr = server_socket.accept()
            threading.Thread(target=handle_monitor_client, args=(conn, addr), daemon=True).start()
            
    except OSError as e:
        log_message(f"[Error Fatal Socket] {e}. ¿Puerto {socket_port} en uso?")
        os._exit(1) # Salida forzada
    except Exception as e:
        log_message(f"[Error Socket Server] {e}")
    finally:
        server_socket.close()

# --- Panel de Monitorización (UI) ---

def draw_panel():
    """Dibuja el panel de monitorización en la consola."""
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print(f"{Color.BLUE_BG}{Color.BOLD}{Color.WHITE}" + 
          " *** SD EV CHARGING SOLUTION. MONITORIZATION PANEL *** " +
          f"{Color.RESET}")
    print("-" * 54)
    
    cp_blocks = []
    with cp_states_lock:
        for cp_id, data in sorted(cp_states.items()):
            status = data['status']
            color = Color.GRAY_BG
            lines = [
                f"{Color.BOLD}{cp_id}{Color.RESET}",
                data['location'],
                f"{data['price_kwh']:.2f}€/kWh"
            ]
            
            if status == 'IDLE' or status == 'AUTHORIZED':
                color = Color.GREEN_BG # Activado [cite: 82]
            elif status == 'CHARGING':
                color = Color.GREEN_BG # Suministrando [cite: 86]
                lines.append(f"Driver {data['driver_id']}")
                lines.append(f"{data['session_kwh']:.3f} kWh")
                lines.append(f"{data['session_cost']:.2f} €")
            elif status == 'STOPPED':
                color = Color.ORANGE_BG # Parado [cite: 84]
                lines.append("Out of Order")
            elif status == 'FAULTED':
                color = Color.RED_BG # Averiado [cite: 98]
                lines.append("AVERIADO")
            elif status == 'DISCONNECTED':
                color = Color.GRAY_BG # Desconectado [cite: 100]
                lines.append("DESCONECTADO")
                
            cp_blocks.append((color, lines))

    # Dibujar bloques de CPs (simplificado, 5 por fila)
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
        print(f"{Color.GRAY_BG}{' ' * (16 * len(row_blocks) - 1)}{Color.RESET}")

    # Dibujar peticiones y mensajes
    print("\n" + f"{Color.BOLD}*** ON GOING DRIVERS REQUESTS ***{Color.RESET}")
    print(f"{'DATE':<10} {'START TIME':<10} {'User ID':<10} {'CP':<10}")
    with cp_states_lock:
        for req in reversed(on_going_requests[-3:]):
            print(f"{req['date']:<10} {req['time']:<10} {req['user_id']:<10} {req['cp_id']:<10}")
            
    print("\n" + f"{Color.BOLD}*** APLICATION MESSAGES ***{Color.RESET}")
    with cp_states_lock:
        for msg in application_messages:
            print(msg)
    
    print("-" * 54)
    log_message("CENTRAL system status OK") # Latido del sistema

def print_admin_menu():
    """Muestra el menú de administrador."""
    print(f"\n{Color.BOLD}--- MENÚ DE ADMINISTRADOR ---{Color.RESET}")
    print("1. Parar CP (Poner 'Out of Order')")
    print("2. Reanudar CP (Quitar 'Out of Order')")
    print("3. Salir")
    return input("Seleccione una opción: ")

# --- Función Principal ---

def main():
    global kafka_bootstrap_servers, socket_port, kafka_producer
    
    # 1. Validar argumentos [cite: 253, 254]
    if len(sys.argv) != 3:
        print("Uso: python EV_Central.py <puerto_escucha_socket> <ip_broker_kafka:puerto>")
        sys.exit(1)
        
    try:
        socket_port = int(sys.argv[1])
    except ValueError:
        print("Error: El <puerto_escucha_socket> debe ser un número.")
        sys.exit(1)
        
    kafka_bootstrap_servers = sys.argv[2]
    log_message(f"Iniciando EV_Central...")
    log_message(f"Kafka Broker: {kafka_bootstrap_servers}")

    # 2. Cargar datos iniciales
    load_initial_data()

    # 3. Inicializar Kafka Producer
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except NoBrokersAvailable:
        log_message(f"[Error Fatal] No se puede conectar a Kafka en {kafka_bootstrap_servers}")
        sys.exit(1)
    
    # 4. Iniciar hilos de red
    threading.Thread(target=start_socket_server, daemon=True).start()
    threading.Thread(target=kafka_consumer_driver_requests, daemon=True).start()
    threading.Thread(target=kafka_consumer_cp_updates, daemon=True).start()
    
    # 5. Bucle principal (Panel y Menú Admin)
    try:
        while True:
            draw_panel()
            choice = print_admin_menu()
            
            if choice == '1': # Parar CP [cite: 198]
                cp_id = input("  > Introduzca ID del CP a PARAR: ").strip().upper()
                if cp_id in cp_states:
                    auth_topic = f"cp_auth_{cp_id}"
                    send_kafka_message(auth_topic, {"action": "STOP_COMMAND"})
                    log_message(f"Comando PARAR enviado a {cp_id}")
                    with cp_states_lock:
                        cp_states[cp_id]['status'] = 'STOPPED' # [cite: 84]
                else:
                    log_message(f"Comando PARAR fallido. CP {cp_id} no existe.")

            elif choice == '2': # Reanudar CP [cite: 199]
                cp_id = input("  > Introduzca ID del CP a REANUDAR: ").strip().upper()
                if cp_id in cp_states:
                    auth_topic = f"cp_auth_{cp_id}"
                    send_kafka_message(auth_topic, {"action": "RESUME_COMMAND"})
                    log_message(f"Comando REANUDAR enviado a {cp_id}")
                    # El estado cambiará a IDLE cuando el CP lo confirme
                else:
                    log_message(f"Comando REANUDAR fallido. CP {cp_id} no existe.")

            elif choice == '3': # Salir
                log_message("Apagando EV_Central...")
                break
            
            time.sleep(1) # Pequeña pausa para ver el resultado del comando

    except KeyboardInterrupt:
        log_message("Cierre solicitado (Ctrl+C).")
    finally:
        if kafka_producer:
            kafka_producer.close()
        print("EV_Central apagado.")
        os._exit(0) # Salida forzada para detener hilos daemon

if __name__ == "__main__":
    main()