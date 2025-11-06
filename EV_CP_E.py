# Ejecución: python EV_CP_E.py localhost:9092 100001 (python EV_CP_E.py <broker_kafka> <puerto monitor>)
#
# Este script implementa "EV_CP_E" (Engine), la lógica principal del punto de recarga.
#
# Funcionalidades principales:
# 1. Servidor Socket: Escucha una conexión del EV_CP_M (Monitor) para recibir
#    su ID y responder a sus comprobaciones de salud ("health checks").
# 2. Cliente/Productor Kafka: Envía telemetría (estado, consumo) y tickets
#    (transacciones finalizadas) a EV_Central.
# 3. Consumidor Kafka: Escucha autorizaciones de carga y comandos
#    administrativos (Parar/Reanudar) de EV_Central.
# 4. Simulación: Implementa la lógica de suministro de energía (telemetría por segundo)
#    y un menú para simular acciones físicas (enchufar, desenchufar, averías).
#

import sys
import socket
import threading
import time
import json
import random
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Constantes ---
ENGINE_HOST = '0.0.0.0'  # Escuchar en todas las interfaces
# ENGINE_PORT se pasa como argumento, no es una constante fija.

# Temas de Kafka
TOPIC_STATUS_UPDATES = "cp_status_updates"  # CP -> Central (para telemetría, Punto 8)
TOPIC_TRANSACTIONS = "cp_transactions"      # CP -> Central (para tickets/fin de carga, Punto 9)
# El tema de autorizaciones (cp_auth_CP001) se construye dinámicamente

# --- Variables de Estado Global ---
cp_id = None                   # ID de este punto de recarga, se recibe del Monitor
kafka_bootstrap_servers = None
kafka_producer = None
# Evento para sincronizar: el Monitor espera a que Kafka esté listo
kafka_consumer_ready = threading.Event() 

# Estados del CP (Máquina de estados interna)
class State:
    IDLE = "IDLE"                 # Disponible (VERDE)
    AUTHORIZED = "AUTHORIZED"     # Autorizado, esperando plug-in (Punto 6)
    CHARGING = "CHARGING"         # Suministrando (VERDE + datos)
    FAULTED = "FAULTED"           # Averiado (ROJO)
    STOPPED = "STOPPED"           # Parado por Central (NARANJA/ROJO, Punto 13.a)

# Variables protegidas por cerrojo
lock = threading.Lock() # Protege todas las variables de estado
state = State.IDLE
health_status = "OK"  # 'OK' o 'KO'. 'KO' simula una avería (Punto 10)
current_driver_id = None
current_price_kwh = 0.5  # Precio por defecto, se actualiza con la autorización

# Evento para detener el hilo de carga (simula 'unplug' o avería)
stop_charging_event = threading.Event()

# --- Funciones Auxiliares ---

def send_kafka_message(topic, message):
    """Envía un mensaje JSON al topic de Kafka especificado."""
    global kafka_producer
    try:
        if not kafka_producer:
            print("[Error] Kafka Producer no inicializado.")
            return
        
        # print(f"[KAFKA->] Enviando a {topic}: {message}") # Descomentar para debug
        kafka_producer.send(topic, message)
        kafka_producer.flush()
    except Exception as e:
        print(f"[Error Kafka] No se pudo enviar mensaje a {topic}: {e}")

# --- Hilos de Red ---

def handle_monitor_connection(conn, addr):
    """
    Gestiona la conexión de un EV_CP_M (Monitor).
    Se ejecuta en un hilo por cada conexión de Monitor.
    Recibe el ID del CP y luego atiende peticiones de 'health check'.
    """
    global cp_id, health_status, lock
    print(f"[Socket] Monitor conectado desde {addr}")
    
    try:
        # 1. Esperar mensaje de registro del Monitor con el ID del CP
        reg_data = conn.recv(1024).decode('utf-8')
        if reg_data.startswith("REGISTER_ID|"):
            received_id = reg_data.split('|')[1]
            # --- Sección Crítica ---
            with lock:
                if cp_id is None:
                    cp_id = received_id # Establece el ID de este Engine
                    print(f"[Socket] CP Engine registrado con ID: {cp_id}")
                elif cp_id != received_id:
                    print(f"[Error] Monitor intentó registrar ID {received_id} pero ya somos {cp_id}")
                    conn.sendall(b"NACK_ID_MISMATCH")
                    return
            # --- Fin Sección Crítica ---
                
            kafka_consumer_ready.clear() # Limpiar evento

            # Iniciar hilo de Kafka (Consumidor)
            print(f"[Sync] Iniciando hilo consumidor de Kafka para {received_id}...")
            # Ahora que tenemos ID, podemos suscribirnos al topic de autorización
            threading.Thread(target=kafka_consumer_thread, args=(received_id,), daemon=True).start()

            # Esperar a que el hilo de Kafka esté listo (máx 10s)
            if not kafka_consumer_ready.wait(timeout=10.0):
                print("[Error Fatal] Timeout. El consumidor de Kafka no se inició (¿Broker caído?).")
                conn.sendall(b"NACK_KAFKA_TIMEOUT")
                return

            print("[Sync] Hilo de Kafka listo.")            
            conn.sendall(b"ACK_REGISTER") # Confirmar al Monitor que todo está OK
        else:
            print("[Error] Primer mensaje del Monitor no fue REGISTER_ID. Desconectando.")
            conn.sendall(b"NACK_EXPECTED_REGISTER")
            return

        # 2. Bucle de Health Check
        # El Monitor enviará "HEALTH_CHECK" cada segundo (Punto 10)
        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                print(f"[Socket] Monitor {addr} desconectado.")
                break
            
            if data == "HEALTH_CHECK":
                # --- Sección Crítica ---
                with lock:
                    response = health_status  # Responder 'OK' o 'KO' (Punto 10 / 280)
                # --- Fin Sección Crítica ---
                conn.sendall(response.encode('utf-8'))
            else:
                print(f"[Socket] Mensaje no reconocido del Monitor: {data}")
                conn.sendall(b"NACK_UNKNOWN_COMMAND")

    except ConnectionResetError:
        print(f"[Socket] Conexión con Monitor {addr} perdida abruptamente.")
    except Exception as e:
        print(f"[Error Socket] Error en handle_monitor_connection: {e}")
    finally:
        conn.close()

def start_socket_server(port):
    """Inicia el servidor de sockets para escuchar al Monitor."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.bind((ENGINE_HOST, port))
        server_socket.listen()
        print(f"[Socket] Engine escuchando conexiones del Monitor en {ENGINE_HOST}:{port}...")

        while True:
            conn, addr = server_socket.accept()
            # Iniciar un nuevo hilo para gestionar esta conexión de monitor
            threading.Thread(target=handle_monitor_connection, args=(conn, addr), daemon=True).start()
            
    except OSError as e:
        print(f"[Error Fatal] No se pudo bindear el puerto {port}. ¿Ya está en uso? {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[Error Socket] Servidor principal falló: {e}")
    finally:
        server_socket.close()

def kafka_consumer_thread(cp_id_arg):
    """
    Escucha mensajes de EV_Central (autorizaciones, comandos stop/resume).
    Se suscribe al topic dinámico 'cp_auth_{cp_id}'.
    """
    global state, current_driver_id, current_price_kwh, lock
        
    authorization_topic = f"cp_auth_{cp_id_arg}"
    consumer = None
    
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                authorization_topic,
                bootstrap_servers=kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                group_id=f"engine-group-{cp_id_arg}", # Grupo único para este CP
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            # Forzar conexión para detectar errores
            consumer.poll(timeout_ms=1000)

            print(f"[KAFKA] Escuchando autorizaciones en topic: {authorization_topic}")
            kafka_consumer_ready.set() # Indicar que el consumidor está listo

        except NoBrokersAvailable:
            print("[Error Kafka] No se puede conectar al broker. Reintentando en 5s...")
            time.sleep(5)
            kafka_consumer_ready.set() # Señalizar (con fallo) para que el Monitor no se bloquee
            return
        except Exception as e:
            print(f"[Error Kafka] Error al crear consumidor: {e}. Reintentando en 5s...")
            time.sleep(5)
            kafka_consumer_ready.set() # Señalizar (con fallo)
            return

    # Bucle principal del consumidor Kafka
    for message in consumer:
        try:
            msg = message.value
            action = msg.get('action')
            print(f"\n[KAFKA<-] Mensaje recibido de Central: {msg}")

            # --- Sección Crítica ---
            with lock:
                # Caso 1: Autorización de carga (Punto 4, 5, 6)
                if action == 'AUTHORIZE' and state == State.IDLE:
                    state = State.AUTHORIZED
                    current_driver_id = msg.get('driver_id')
                    current_price_kwh = msg.get('price_kwh', 0.5) # Usar precio de central
                    print(f"  > AUTORIZADO. Driver: {current_driver_id}. Esperando plug-in...")
                
                # Caso 2: Comando de parada de Central (Punto 13.a)
                elif action == 'STOP_COMMAND':
                    if state == State.CHARGING:
                        stop_charging_event.set() # Detener el suministro
                        print("  > COMANDO STOP RECIBIDO. Deteniendo suministro...")
                    state = State.STOPPED # Poner en estado "Parado"
                    print("  > CP puesto en 'Fuera de Servicio' por Central.")
                    # Notificar a central del cambio de estado (Panel -> NARANJA)
                    send_kafka_message(TOPIC_STATUS_UPDATES, {
                        "cp_id": cp_id,
                        "timestamp": time.time(),
                        "status": State.STOPPED,
                        "info": "Parado por Central"
                    })

                # Caso 3: Comando de reanudación de Central (Punto 13.b)
                elif action == 'RESUME_COMMAND': 
                    if state == State.STOPPED:
                        state = State.IDLE # Volver a "Activado"
                        print("  > COMANDO RESUME RECIBIDO. CP vuelve a estado 'Activado'.")
                        # Notificar a central del cambio de estado (Panel -> VERDE)
                        send_kafka_message(TOPIC_STATUS_UPDATES, {
                            "cp_id": cp_id,
                            "timestamp": time.time(),
                            "status": State.IDLE,
                            "info": "Reanudado por Central"
                        })
            # --- Fin Sección Crítica ---
        except json.JSONDecodeError:
            print(f"[Error Kafka] Mensaje malformado recibido: {message.value}")
        except Exception as e:
            print(f"[Error Kafka] Error procesando mensaje: {e}")

# --- Lógica de Simulación ---

def charging_simulation_thread():
    """
    Simula el proceso de carga (Punto 8).
    Se ejecuta en un hilo separado mientras state == CHARGING.
    Envía telemetría a Central cada segundo.
    """
    global state, health_status, lock, current_driver_id, current_price_kwh
    
    total_kwh = 0.0
    total_cost = 0.0
    start_time = time.time()
    # Capturar el driver_id de la sesión para evitar problemas si se limpia
    driver_on_session = current_driver_id 
    
    print(f"[Carga] Iniciando suministro para Driver {driver_on_session}...")
    
    # Bucle de telemetría (cada segundo)
    while not stop_charging_event.is_set():
        time.sleep(1) 
        
        status_msg = None
        kwh_this_loop = 0.0
        cost_this_loop = 0.0
        
        # --- Sección Crítica ---
        with lock:
            # Comprobar interrupciones: Avería (Punto 10/11) o Parada Central (Punto 13.a)
            if health_status == "KO" or state == State.FAULTED or state == State.STOPPED:
                # Si el estado es STOPPED, la parada viene de Central.
                if state == State.STOPPED:
                    reason = "Stopped by Central Command"
                    event_type = "CHARGE_STOPPED_BY_CENTRAL"
                    print("[Carga] ¡PARADA FORZADA! Deteniendo suministro inmediatamente.")
                else: # FAULTED o KO
                    reason = "CP Fault"
                    event_type = "CHARGE_FAILED"
                    print("[Carga] ¡AVERÍA DETECTADA! Deteniendo suministro inmediatamente.")
                
                stop_charging_event.set() # Forzar salida del bucle
                
                # Enviar notificación/ticket de finalización forzada/avería
                send_kafka_message(TOPIC_TRANSACTIONS, {
                    "cp_id": cp_id,
                    "timestamp": time.time(),
                    "event": event_type,
                    "driver_id": driver_on_session, # Usamos el driver_on_session capturado
                    "reason": reason,
                    "duration_sec": time.time() - start_time,
                    "total_kwh": total_kwh,
                    "total_cost": total_cost
                })
                break # Salir del bucle while
            
            # Simular consumo
            kwh_this_second = random.uniform(0.01, 0.05)
            cost_this_second = kwh_this_second * current_price_kwh
            
            total_kwh += kwh_this_second
            total_cost += cost_this_second
            
            # Enviar telemetría a Central (Punto 8)
            status_msg = {
                "cp_id": cp_id,
                "timestamp": time.time(),
                "status": State.CHARGING,
                "driver_id": driver_on_session,
                "session_kwh": total_kwh,  # Consumo en Kw en tiempo real
                "session_cost": total_cost # Importe en € en tiempo real
            }
            # Copia los valores para el print (fuera del lock)
            kwh_this_loop = total_kwh
            cost_this_loop = total_cost
        # --- Fin Sección Crítica ---

        if status_msg:
            send_kafka_message(TOPIC_STATUS_UPDATES, status_msg)

        # Mostrar progreso en la consola local del CP
        print(f"  ...Suministrando: {kwh_this_loop:.3f} kWh, {cost_this_loop:.2f} €", end='\r')
            
    print("\n[Carga] Bucle de simulación detenido.") 

    # --- Bucle de carga finalizado (por unplug, avería o stop) ---
    
    # --- Sección Crítica ---
    with lock:
        # Si NO fue una avería y NO fue un STOPPED, fue un 'unplug' normal (Punto 9)
        if state != State.FAULTED and state != State.STOPPED:
            print(f"[Carga] Suministro finalizado para Driver {driver_on_session}.")
            
            # Enviar "ticket" final a Central (CHARGE_COMPLETE)
            send_kafka_message(TOPIC_TRANSACTIONS, {
                "cp_id": cp_id,
                "timestamp": time.time(),
                "event": "CHARGE_COMPLETE",
                "driver_id": driver_on_session,
                "duration_sec": time.time() - start_time,
                "total_kwh": total_kwh,
                "total_cost": total_cost
            })
        
        # Resetear variables de sesión
        current_driver_id = None
        stop_charging_event.clear()
        
        # El CP vuelve al estado de reposo (IDLE)
        # a menos que esté PARADO por Central o AVERIADO
        if state != State.STOPPED and state != State.FAULTED:
            state = State.IDLE 
        elif state == State.STOPPED:
            print("[Carga] CP permanece en estado STOPPED (Parado por Central).")
        else: # state == State.FAULTED
             print("[Carga] CP permanece en estado FAULTED (Averiado).")
    # --- Fin Sección Crítica ---

def print_menu():
    """Muestra el menú de simulación del CP."""
    global state, health_status, cp_id, current_driver_id
    
    # 1. Copia las variables de estado de forma atómica
    with lock:
        _id = cp_id
        _state = state
        _health = health_status
        _driver = current_driver_id
    
    # 2. Imprime el menú (ya sin tener el cerrojo)
    print("\n--- EV Charging Point ENGINE Menu ---")
    print(f"      ID: {_id} | Estado: {_state} | Salud: {_health}")
    if _state == State.CHARGING:
        print(f"      Suministrando a: {_driver}")
    print("-----------------------------------")
    print("1. Iniciar Carga MANUAL (Simula pago en terminal, Punto 158)")
    print("2. Simular 'Plug-in' (Tras autorización de Central, Punto 7)")
    print("3. Simular 'Unplug' (Desenchufr vehículo, Punto 9)")
    print("4. Simular AVERÍA (Reportar KO al Monitor, Punto 10/280)")
    print("5. Resolver AVERÍA (Reportar OK al Monitor, Punto 10)")
    print("6. Salir")
    print("-----------------------------------")
    print("Esperando acciones (Menú) o eventos (Red)...")
    
    # 3. Pide la entrada del usuario (sin tener el cerrojo)
    return input("Seleccione una opción: ")

# --- Función Principal ---

def main():
    global kafka_bootstrap_servers, kafka_producer, state, health_status, lock
    
    # 1. Validar argumentos de línea de comandos
    if len(sys.argv) != 3:
        print("Uso: python EV_CP_E.py <ip_broker_kafka:puerto> <puerto_engine>")
        sys.exit(1)
        
    # Argumento 1: Broker Kafka
    kafka_bootstrap_servers = sys.argv[1]

    try:
        # Argumento 2: Puerto donde escuchar al Monitor
        engine_port = int(sys.argv[2])
    except ValueError:
        print("Error: El <puerto_engine> debe ser un número.")
        sys.exit(1)

    print(f"[Info] Conectando a Kafka Broker en: {kafka_bootstrap_servers}")

    # 2. Inicializar Kafka Producer (para telemetría y tickets)
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except NoBrokersAvailable:
        print(f"[Error Fatal] No se puede conectar a Kafka Broker en {kafka_bootstrap_servers}. Abortando.")
        sys.exit(1)
    except Exception as e:
        print(f"[Error Fatal] Inicializando Kafka Producer: {e}")
        sys.exit(1)

    # 3. Iniciar hilo del servidor de sockets para el Monitor
    threading.Thread(target=start_socket_server, args=(engine_port,), daemon=True).start()

    # 4. Esperar a que el Monitor se conecte y registre el ID
    print("[Info] Esperando que EV_CP_M (Monitor) se conecte y registre el ID...")
    while cp_id is None:
        time.sleep(1)
    print(f"[Info] ¡Motor del CP {cp_id} listo!")

    # 5. Bucle principal (Menú de simulación)
    while True:
        choice = print_menu()

        if choice == '1': # Carga Manual (Punto 158)
            with lock:
                if state == State.IDLE:
                    print("[Menu] Iniciando carga manual...")
                    state = State.CHARGING
                    current_driver_id = "MANUAL" # Identificador para carga local
                    print(f"[Menu] Usando precio: {current_price_kwh} €/kWh") 
                    
                    stop_charging_event.clear()
                    # Iniciar hilo de simulación de carga
                    threading.Thread(target=charging_simulation_thread, daemon=True).start()
                else:
                    print(f"[Menu] No se puede iniciar carga manual. Estado actual: {state}")
        
        elif choice == '2': # Simular Plug-in (Punto 7)
            with lock:
                if state == State.AUTHORIZED:
                    state = State.CHARGING
                    stop_charging_event.clear()
                    # Iniciar hilo de simulación de carga
                    threading.Thread(target=charging_simulation_thread, daemon=True).start()
                elif state == State.IDLE:
                    print("[Menu] No se puede enchufar. El vehículo no ha sido autorizado por Central.")
                else:
                    print(f"[Menu] No se puede enchufar. Estado actual: {state}")
                    
        elif choice == '3': # Simular Unplug (Punto 9)
            with lock:
                if state == State.CHARGING:
                    print("[Menu] Simulación de 'Unplug'. Deteniendo suministro...")
                    stop_charging_event.set() # 1. Enviar señal de parada al hilo
                else:
                    print("[Menu] No se puede desenchufar. No hay ninguna carga activa.")
                    continue # Saltar el resto del bucle
            
            print("[Menu] Esperando a que el hilo de carga finalice...")
            
            # Esperar a que el hilo 'charging_simulation_thread' termine
            # (limpiará el evento stop_charging_event)
            timeout_counter = 0
            while stop_charging_event.is_set() and timeout_counter < 30:
                time.sleep(0.1) # Esperar 100ms
                timeout_counter += 1

            if stop_charging_event.is_set():
                 print("[Error] El hilo de carga no finalizó. Forzando reseteo.")
                 stop_charging_event.clear()
            else:
                 print("[Menu] Hilo de carga detenido. CP listo.")

        elif choice == '4': # Simular Avería (Punto 10 / 280)
            with lock:
                if health_status == "KO":
                    print("[Menu] La avería ya está simulada.")
                else:
                    health_status = "KO" # El Monitor leerá esto
                    state = State.FAULTED # Cambiar estado interno
                    print("[Menu] ¡AVERÍA SIMULADA! Se reportará 'KO' al Monitor.")

                    # Enviar notificación de estado a Central (Panel -> ROJO)
                    send_kafka_message(TOPIC_STATUS_UPDATES, {
                        "cp_id": cp_id,
                        "timestamp": time.time(),
                        "status": State.FAULTED,
                        "info": "Fault simulated by user"
                    })

        elif choice == '5': # Resolver Avería (Punto 10)
            with lock:
                if health_status == "OK":
                    print("[Menu] El CP no está en estado de avería.")
                else:
                    health_status = "OK" # El Monitor leerá esto
                    state = State.IDLE # Vuelve a estar disponible
                    print("[Menu] Avería resuelta. Se reportará 'OK' al Monitor.")

                    # Enviar notificación de estado a Central (Panel -> VERDE)
                    send_kafka_message(TOPIC_STATUS_UPDATES, {
                        "cp_id": cp_id,
                        "timestamp": time.time(),
                        "status": State.IDLE,
                        "info": "Fault resolved by user"
                    })
        
        elif choice == '6': # Salir
            print("[Info] Apagando EV_CP_E...")
            if kafka_producer:
                kafka_producer.close()
            break
            
        else:
            print("[Error] Opción no válida.")

if __name__ == "__main__":
    main()