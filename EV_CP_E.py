# Ejecución: python EV_CP_E.py localhost:9092 (python EV_CP_E.py <broker_kafka>)

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
ENGINE_PORT = 10001      # Puerto para que se conecte el EV_CP_M (Monitor)

# Temas de Kafka
TOPIC_STATUS_UPDATES = "cp_status_updates"  # CP -> Central (para telemetría)
TOPIC_TRANSACTIONS = "cp_transactions"      # CP -> Central (para inicio/fin de carga)
# El tema de autorizaciones será específico del CP, p.ej., "cp_auth_CP001"

# --- Variables de Estado Global ---
cp_id = None
kafka_bootstrap_servers = None
kafka_producer = None
kafka_consumer_ready = threading.Event() # Para sincronizar con el Monitor

# Estados del CP
class State:
    IDLE = "IDLE"                 # Disponible (VERDE)
    AUTHORIZED = "AUTHORIZED"     # Autorizado, esperando plug-in
    CHARGING = "CHARGING"         # Suministrando (VERDE + datos)
    FAULTED = "FAULTED"           # Averiado (ROJO)
    STOPPED = "STOPPED"           # Parado por Central (NARANJA/ROJO)

# Variables protegidas por cerrojo
lock = threading.Lock()
state = State.IDLE
health_status = "OK"  # 'OK' o 'KO'
current_driver_id = None
current_price_kwh = 0.5  # Precio por defecto, debería venir de Central

# Evento para detener el hilo de carga
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
    Recibe el ID del CP y luego atiende peticiones de 'health check'.
   
    """
    global cp_id, health_status, lock
    print(f"[Socket] Monitor conectado desde {addr}")
    
    try:
        # 1. Esperar mensaje de registro del Monitor con el ID del CP
        reg_data = conn.recv(1024).decode('utf-8')
        if reg_data.startswith("REGISTER_ID|"):
            received_id = reg_data.split('|')[1]
            with lock:
                if cp_id is None:
                    cp_id = received_id
                    print(f"[Socket] CP Engine registrado con ID: {cp_id}")
                elif cp_id != received_id:
                    print(f"[Error] Monitor intentó registrar ID {received_id} pero ya somos {cp_id}")
                    conn.sendall(b"NACK_ID_MISMATCH")
                    return
                
            kafka_consumer_ready.clear() # Limpiar evento

            # Iniciar hilo de Kafka
            print(f"[Sync] Iniciando hilo consumidor de Kafka para {received_id}...")
            threading.Thread(target=kafka_consumer_thread, args=(received_id,), daemon=True).start()

            # Esperar a que el hilo de Kafka esté listo (máx 10s)
            if not kafka_consumer_ready.wait(timeout=10.0):
                print("[Error Fatal] Timeout. El consumidor de Kafka no se inició (¿Broker caído?).")
                conn.sendall(b"NACK_KAFKA_TIMEOUT")
                return

            print("[Sync] Hilo de Kafka listo.")            
            conn.sendall(b"ACK_REGISTER")
        else:
            print("[Error] Primer mensaje del Monitor no fue REGISTER_ID. Desconectando.")
            conn.sendall(b"NACK_EXPECTED_REGISTER")
            return

        # 2. Bucle de Health Check
        while True:
            # Esperar health check del monitor
            data = conn.recv(1024).decode('utf-8')
            if not data:
                print(f"[Socket] Monitor {addr} desconectado.")
                break
            
            if data == "HEALTH_CHECK":
                with lock:
                    response = health_status  # Responder 'OK' o 'KO'
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

def start_socket_server():
    """Inicia el servidor de sockets para escuchar al Monitor."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.bind((ENGINE_HOST, ENGINE_PORT))
        server_socket.listen()
        print(f"[Socket] Engine escuchando conexiones del Monitor en {ENGINE_HOST}:{ENGINE_PORT}...")

        while True:
            conn, addr = server_socket.accept()
            # Iniciar un nuevo hilo para gestionar esta conexión de monitor
            threading.Thread(target=handle_monitor_connection, args=(conn, addr), daemon=True).start()
            
    except OSError as e:
        print(f"[Error Fatal] No se pudo bindear el puerto {ENGINE_PORT}. ¿Ya está en uso? {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[Error Socket] Servidor principal falló: {e}")
    finally:
        server_socket.close()

def kafka_consumer_thread(cp_id_arg):
    """
    Escucha mensajes de EV_Central (autorizaciones, comandos stop/resume).
   
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

            # Forzar conexión
            consumer.poll(timeout_ms=1000)

            print(f"[KAFKA] Escuchando autorizaciones en topic: {authorization_topic}")
            kafka_consumer_ready.set() # Indicar que el consumidor está listo

        except NoBrokersAvailable:
            print("[Error Kafka] No se puede conectar al broker. Reintentando en 5s...")
            time.sleep(5)
            kafka_consumer_ready.set() 
            return
        except Exception as e:
            print(f"[Error Kafka] Error al crear consumidor: {e}. Reintentando en 5s...")
            time.sleep(5)
            kafka_consumer_ready.set()
            return

    for message in consumer:
        try:
            msg = message.value
            action = msg.get('action')
            print(f"\n[KAFKA<-] Mensaje recibido de Central: {msg}")

            with lock:
                if action == 'AUTHORIZE' and state == State.IDLE:
                    state = State.AUTHORIZED
                    current_driver_id = msg.get('driver_id')
                    current_price_kwh = msg.get('price_kwh', 0.5) # Usar precio de central
                    print(f"  > AUTORIZADO. Driver: {current_driver_id}. Esperando plug-in...")
                
                elif action == 'STOP_COMMAND': # Punto 13.a
                    if state == State.CHARGING:
                        stop_charging_event.set() # Detener el suministro
                        print("  > COMANDO STOP RECIBIDO. Deteniendo suministro...")
                    state = State.STOPPED
                    print("  > CP puesto en 'Fuera de Servicio' por Central.")
                    # Notificar a central del cambio de estado
                    send_kafka_message(TOPIC_STATUS_UPDATES, {
                        "cp_id": cp_id,
                        "timestamp": time.time(),
                        "status": State.STOPPED,
                        "info": "Parado por Central"
                    })

                elif action == 'RESUME_COMMAND': 
                    if state == State.STOPPED:
                        state = State.IDLE
                        print("  > COMANDO RESUME RECIBIDO. CP vuelve a estado 'Activado'.")
                        # Notificar a central del cambio de estado
                        send_kafka_message(TOPIC_STATUS_UPDATES, {
                            "cp_id": cp_id,
                            "timestamp": time.time(),
                            "status": State.IDLE,
                            "info": "Reanudado por Central"
                        })
        except json.JSONDecodeError:
            print(f"[Error Kafka] Mensaje malformado recibido: {message.value}")
        except Exception as e:
            print(f"[Error Kafka] Error procesando mensaje: {e}")

# --- Lógica de Simulación ---

def charging_simulation_thread():
    """
    Simula el proceso de carga, enviando telemetría cada segundo.
   
    """
    global state, health_status, lock, current_driver_id, current_price_kwh
    
    total_kwh = 0.0
    total_cost = 0.0
    start_time = time.time()
    
    print(f"[Carga] Iniciando suministro para Driver {current_driver_id}...")
    
    while not stop_charging_event.is_set():
        time.sleep(1) # Enviar información cada segundo

        status_msg = None
        kwh_this_loop = 0.0
        cost_this_loop = 0.0
        
        with lock:
            # Comprobar si hay una avería durante el suministro
            if health_status == "KO" or state == State.FAULTED:
                print("[Carga] ¡AVERÍA DETECTADA! Deteniendo suministro inmediatamente.")
                state = State.FAULTED
                stop_charging_event.set() # Forzar salida del bucle
                
                # Enviar notificación de finalización por avería
                send_kafka_message(TOPIC_TRANSACTIONS, {
                    "cp_id": cp_id,
                    "timestamp": time.time(),
                    "event": "CHARGE_FAILED",
                    "driver_id": current_driver_id,
                    "reason": "CP Fault",
                    "duration_sec": time.time() - start_time,
                    "total_kwh": total_kwh,
                    "total_cost": total_cost
                })
                break # Salir del bucle while
            
            # Simular consumo
            kwh_this_second = random.uniform(0.01, 0.05) # Entre 10 y 50 Wh por segundo
            cost_this_second = kwh_this_second * current_price_kwh
            
            total_kwh += kwh_this_second
            total_cost += cost_this_second
            
            # Enviar telemetría a Central
            status_msg = {
                "cp_id": cp_id,
                "timestamp": time.time(),
                "status": State.CHARGING,
                "driver_id": current_driver_id,
                "session_kwh": total_kwh,
                "session_cost": total_cost
            }
            # Copia los valores para el print
            kwh_this_loop = total_kwh
            cost_this_loop = total_cost

        if status_msg:
            send_kafka_message(TOPIC_STATUS_UPDATES, status_msg)

        print(f"  ...Suministrando: {kwh_this_loop:.3f} kWh, {cost_this_loop:.2f} €", end='\r')
            
    print("\n[Carga] Bucle de simulación detenido.") # Añadimos \n para limpiar

    # --- Bucle de carga finalizado (por unplug, avería o stop) ---
    print("\n[Carga] Bucle de simulación detenido.")
    
    with lock:
        # Si no fue una avería, fue un 'unplug' normal
        if state != State.FAULTED and state != State.STOPPED:
            print(f"[Carga] Suministro finalizado para Driver {current_driver_id}.")
            state = State.IDLE
            
            # Enviar "ticket" final a Central
            send_kafka_message(TOPIC_TRANSACTIONS, {
                "cp_id": cp_id,
                "timestamp": time.time(),
                "event": "CHARGE_COMPLETE",
                "driver_id": current_driver_id,
                "duration_sec": time.time() - start_time,
                "total_kwh": total_kwh,
                "total_cost": total_cost
            })
        
        # Resetear variables de sesión
        current_driver_id = None
        stop_charging_event.clear()
        if state != State.STOPPED and state != State.FAULTED:
            state = State.IDLE # Vuelve a estar disponible
        elif state == State.STOPPED:
            print("[Carga] CP permanece en estado STOPPED (Parado por Central).")
        else: # state == State.FAULTED
             print("[Carga] CP permanece en estado FAULTED (Averiado).")

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
    print("1. Simular 'Plug-in' (Enchufar vehículo)")
    print("2. Simular 'Unplug' (Desenchufr vehículo)")
    print("3. Simular AVERÍA (Reportar KO al Monitor)")
    print("4. Resolver AVERÍA (Reportar OK al Monitor)")
    print("5. Salir")
    print("-----------------------------------")
    print("Esperando acciones (Menú) o eventos (Red)...")
    
    # 3. Pide la entrada del usuario (sin tener el cerrojo)
    return input("Seleccione una opción: ")

# --- Función Principal ---

def main():
    global kafka_bootstrap_servers, kafka_producer, state, health_status, lock
    
    # 1. Validar argumentos de línea de comandos
    if len(sys.argv) != 2:
        print("Uso: python EV_CP_E.py <ip_broker_kafka:puerto>")
        sys.exit(1)
        
    kafka_bootstrap_servers = sys.argv[1]
    print(f"[Info] Conectando a Kafka Broker en: {kafka_bootstrap_servers}")

    # 2. Inicializar Kafka Producer
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
    threading.Thread(target=start_socket_server, daemon=True).start()

    # 4. Esperar a que el Monitor se conecte y registre el ID
    print("[Info] Esperando que EV_CP_M (Monitor) se conecte y registre el ID...")
    while cp_id is None:
        time.sleep(1)
    print(f"[Info] ¡Motor del CP {cp_id} listo!")

    # 5. Bucle principal (Menú de simulación)
    while True:
        choice = print_menu()
        
        if choice == '1': # Simular Plug-in
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
                    
        elif choice == '2': # Simular Unplug
            with lock:
                if state == State.CHARGING:
                    print("[Menu] Simulación de 'Unplug'. Deteniendo suministro...")
                    stop_charging_event.set() # 1. Enviar señal de parada
                else:
                    print("[Menu] No se puede desenchufar. No hay ninguna carga activa.")
                    continue # Saltar el resto del bucle
            
            print("[Menu] Esperando a que el hilo de carga finalice...")
            
            # Esperamos MÁXIMO 3 segundos a que el evento sea limpiado
            timeout_counter = 0
            while stop_charging_event.is_set() and timeout_counter < 30:
                time.sleep(0.1) # Esperar 100ms
                timeout_counter += 1

            if stop_charging_event.is_set():
                 print("[Error] El hilo de carga no finalizó. Forzando reseteo.")
                 stop_charging_event.clear()
            else:
                 print("[Menu] Hilo de carga detenido. CP listo.")

        elif choice == '3': # Simular Avería
            with lock:
                if health_status == "KO":
                    print("[Menu] La avería ya está simulada.")
                else:
                    health_status = "KO"
                    state = State.FAULTED # Poner en estado averiado
                    print("[Menu] ¡AVERÍA SIMULADA! Se reportará 'KO' al Monitor.")

                    # Enviar notificación de estado a Central
                    send_kafka_message(TOPIC_STATUS_UPDATES, {
                        "cp_id": cp_id,
                        "timestamp": time.time(),
                        "status": State.FAULTED,
                        "info": "Fault simulated by user"
                    })

        elif choice == '4': # Resolver Avería
            with lock:
                if health_status == "OK":
                    print("[Menu] El CP no está en estado de avería.")
                else:
                    health_status = "OK"
                    state = State.IDLE # Vuelve a estar disponible
                    print("[Menu] Avería resuelta. Se reportará 'OK' al Monitor.")

                    send_kafka_message(TOPIC_STATUS_UPDATES, {
                        "cp_id": cp_id,
                        "timestamp": time.time(),
                        "status": State.IDLE,
                        "info": "Fault resolved by user"
                    })
        
        elif choice == '5': # Salir
            print("[Info] Apagando EV_CP_E...")
            if kafka_producer:
                kafka_producer.close()
            break
            
        else:
            print("[Error] Opción no válida.")

if __name__ == "__main__":
    main()