# EV_CP_E.py

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
        kafka_producer.send(topic, json.dumps(message).encode('utf-8'))
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

def kafka_consumer_thread():
    """
    Escucha mensajes de EV_Central (autorizaciones, comandos stop/resume).
   
    """
    global state, current_driver_id, current_price_kwh, lock
    
    # Esperar hasta que el CP_ID haya sido establecido por el Monitor
    while cp_id is None:
        time.sleep(1)
        
    authorization_topic = f"cp_auth_{cp_id}"
    consumer = None
    
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                authorization_topic,
                bootstrap_servers=kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                group_id=f"engine-group-{cp_id}", # Grupo único para este CP
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"[KAFKA] Escuchando autorizaciones en topic: {authorization_topic}")
        except NoBrokersAvailable:
            print("[Error Kafka] No se puede conectar al broker. Reintentando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"[Error Kafka] Error al crear consumidor: {e}. Reintentando en 5s...")
            time.sleep(5)

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

                elif action == 'RESUME_COMMAND': # Punto 13.b
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
            kwh_this_second = random.uniform(0.01, 0.05) # Simulación de ~36-180kW
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
            send_kafka_message(TOPIC_STATUS_UPDATES, status_msg)
            print(f"  ...Suministrando: {total_kwh:.3f} kWh, {total_cost:.2f} €", end='\r')

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
    
    with lock:
        print("\n--- EV Charging Point ENGINE Menu ---")
        print(f"      ID: {cp_id} | Estado: {state} | Salud: {health_status}")
        if state == State.CHARGING:
            print(f"      Suministrando a: {current_driver_id}")
        print("-----------------------------------")
        print("1. Simular 'Plug-in' (Enchufar vehículo)")
        print("2. Simular 'Unplug' (Desenchufr vehículo)")
        print("3. Simular AVERÍA (Reportar KO al Monitor)")
        print("4. Resolver AVERÍA (Reportar OK al Monitor)")
        print("5. Salir")
        print("-----------------------------------")
        print("Esperando acciones (Menú) o eventos (Red)...")
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
    
    # 5. Iniciar hilo del consumidor de Kafka (ahora que tenemos ID)
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()

    # 6. Bucle principal (Menú de simulación)
    while True:
        choice = print_menu()
        
        if choice == '1': # Simular Plug-in