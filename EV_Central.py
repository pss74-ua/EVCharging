# Ejecución: python EV_Central.py 9090 localhost:9092
# Descripción: Backend Central. Gestiona lógica, seguridad y actualiza la BD para la vista web.

import sys
import socket
import threading
import json
import time
import os
import mysql.connector
import webbrowser
from collections import deque
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import base64
import hashlib
from cryptography.fernet import Fernet


# --- CONFIGURACIÓN BD ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'psusana', 
    'database': 'ev_charging'
}

# --- TEMAS KAFKA ---
TOPIC_DRIVER_REQUESTS = "driver_requests"
TOPIC_DRIVER_NOTIFY = "driver_notifications"
TOPIC_CP_STATUS = "cp_status_updates"
TOPIC_CP_TRANSACTIONS = "cp_transactions"

# --- VARIABLES GLOBALES ---
kafka_producer = None
kafka_bootstrap_servers = None
socket_port = None
stop_event = threading.Event()
local_weather_alerts = {}

# Estado en memoria (se sincroniza con BD)
cp_states = {} 
cp_states_lock = threading.Lock()

from collections import deque
application_messages = deque(maxlen=10)
_initial_sync_complete = False # Bandera para controlar la primera ejecución

# --- Funciones de Cifrado ---

def get_cipher(hex_key):
    """
    Convierte la clave hexadecimal del Registry en un objeto Fernet válido para cifrar/descifrar.
    """
    # 1. Hasheamos la clave para asegurar que tenga 32 bytes exactos
    key_bytes = hashlib.sha256(hex_key.encode('utf-8')).digest()
    # 2. Codificamos en Base64 URL-safe (requisito de Fernet)
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    return Fernet(fernet_key)

# --- FUNCIONES DE BASE DE DATOS ---

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Exception as e:
        print(f"[Error BD] {e}")
        return None

def update_cp_status_in_db(cp_id, status, driver_id=None):
    """
    Actualiza el estado en la BD para que el Front (HTML) lo vea.
    """
    conn = get_db_connection()
    if not conn: return
    try:
        cursor = conn.cursor()
        # Actualizamos estado y driver (si hay carga)
        query = "UPDATE charge_points SET status = %s WHERE cp_id = %s"
        cursor.execute(query, (status, cp_id))
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"[BD Error] Al actualizar status de {cp_id}: {e}")
    finally:
        conn.close()

def cp_state_synchronization_thread():
    """
    Hilo que realiza la sincronización completa de cp_states desde la BD
    y gestiona la lógica de comandos por alerta climática (weather_alert).
    """
    global local_weather_alerts, _initial_sync_complete
    print("[Sincronización] Hilo de sincronización de estado y alerta climática iniciado.")
    
    while not stop_event.is_set():
        time.sleep(5) # Pollear la BD cada 5 segundos

        db_connection = None
        try:
            db_connection = mysql.connector.connect(**DB_CONFIG)
            cursor = db_connection.cursor(dictionary=True) 
            
            query = "SELECT cp_id, location, price_kwh, status, symmetric_key, is_registered, weather_alert FROM charge_points"
            cursor.execute(query)
            cp_data_from_db = cursor.fetchall()
            cursor.close()
            
            is_first_run = not _initial_sync_complete # Comprueba si es la primera vez
            
            with cp_states_lock:
                for row in cp_data_from_db:
                    cp_id = row['cp_id']
                    
                    if cp_id not in cp_states:
                         cp_states[cp_id] = {}
                         
                    # 1. SINCRONIZACIÓN COMPLETA DE cp_states
                    # ... [Sincronización de campos de estado (location, price, key, etc.)] ...
                    
                    is_alert_active = row['weather_alert']
                    
                    # SI ES LA PRIMERA EJECUCIÓN: SOLO INICIALIZAR ESTADO LOCAL Y SALTAR COMANDO
                    if is_first_run:
                        local_weather_alerts[cp_id] = is_alert_active
                        continue # Pasa al siguiente CP
                        
                    # 3. DETECCIÓN DE TRANSICIÓN DE ESTADO (Solo después de la primera corrida)
                    
                    # Detectar TRANSICIÓN de estado de alerta
                    if is_alert_active != local_weather_alerts.get(cp_id):
                        
                        command = ""
                        if is_alert_active:
                            # TRANSICIÓN: OK -> ALERTA (Enviar STOP)
                            print(f"[ALERTA CLIMA] Detectada alerta en DB para {cp_id}. Enviando STOP.")
                            command = "STOP_COMMAND"
                            cp_states[cp_id]['status'] = 'STOPPED' 
                        else:
                            # TRANSICIÓN: ALERTA -> OK (Enviar RESUME)
                            print(f"[ALERTA CLIMA] Detectada cancelación para {cp_id}. Enviando RESUME.")
                            command = "RESUME_COMMAND"
                            # Si no está cargando, lo devuelve a IDLE
                            if cp_states[cp_id]['status'] != 'CHARGING':
                                cp_states[cp_id]['status'] = 'IDLE' 
                            
                        # Enviar comando Kafka
                        auth_topic = f"cp_auth_{cp_id}"
                        send_kafka_message(auth_topic, {"action": command})

                        # Actualizar el estado local
                        local_weather_alerts[cp_id] = is_alert_active
            
            # Marcar la sincronización inicial como completa
            if is_first_run:
                _initial_sync_complete = True
                #print("[Sincronización] Inicial completada. Empezando a detectar transiciones.")
                            
        except mysql.connector.Error as err:
            print(f"[Error BD Polling] {err}")
        except Exception as e:
            print(f"[Error Polling] {e}")
        finally:
            if db_connection and db_connection.is_connected():
                db_connection.close()

def verify_cp_registration(cp_id):
    """
    Verifica si el CP está registrado y obtiene su CLAVE SIMÉTRICA.
    Requisito de Seguridad Release 2.
    """
    conn = get_db_connection()
    if not conn: return False, None
    try:
        cursor = conn.cursor(dictionary=True)
        query = "SELECT is_registered, symmetric_key, price_kwh FROM charge_points WHERE cp_id = %s"
        cursor.execute(query, (cp_id,))
        row = cursor.fetchone()
        cursor.close()
        
        if row and row['is_registered'] == 1:
            return True, row # Devuelve datos incluyendo la clave
        return False, None
    except Exception as e:
        print(f"[BD Error] Verificando registro: {e}")
        return False, None
    finally:
        conn.close()

def load_db_data():
    """
    Carga el estado de los CPs desde la BD.
    Permite que Central 'recuerde' los CPs y sus claves tras un reinicio.
    """
    # print("[Arranque] Cargando datos desde Base de Datos...")
    conn = get_db_connection()
    if not conn:
        print("[Error Fatal] No se pudo conectar a la BD.")
        return

    try:
        cursor = conn.cursor(dictionary=True)
        # Recuperamos todo lo necesario para reconstruir la memoria
        query = "SELECT cp_id, price_kwh, status, symmetric_key, is_registered FROM charge_points"
        cursor.execute(query)
        rows = cursor.fetchall()
        
        count = 0
        with cp_states_lock:
            for row in rows:
                cp_id = row['cp_id']
                is_reg = row['is_registered']
                
                # Si no existe en memoria, lo inicializamos
                if cp_id not in cp_states:
                    cp_states[cp_id] = {}

                # ACTUALIZAMOS SIEMPRE EL FLAG DE REGISTRO
                cp_states[cp_id]['is_registered'] = is_reg

                # Si está registrado, actualizamos el resto de datos estáticos
                if is_reg == 1:
                    # Preservamos datos dinámicos si ya existen (coste, kwh)
                    cp_states[cp_id]['status'] = row['status'] or 'DISCONNECTED'
                    
                    cp_states[cp_id]['price_kwh'] = float(row['price_kwh'])
                    cp_states[cp_id]['key'] = row['symmetric_key']
        
        # print(f"[Arranque] Recuperación completada: {count} CPs cargados en memoria.")
        
    except Exception as e:
        print(f"[Error BD] Fallo en carga inicial: {e}")
    finally:
        if conn: conn.close()

def log_audit_event(source_ip, entity_id, event_type, details):
    """
    Registra un evento en la tabla de auditoría.
    """
    conn = get_db_connection()
    if not conn: return

    try:
        cursor = conn.cursor()
        query = """
            INSERT INTO audit_logs (source_ip, entity_id, event_type, details)
            VALUES (%s, %s, %s, %s)
        """
        # Si details es un diccionario, lo convertimos a string JSON para guardarlo ordenado
        if isinstance(details, dict):
            import json
            details_str = json.dumps(details)
        else:
            details_str = str(details)

        cursor.execute(query, (source_ip, entity_id, event_type, details_str))
        conn.commit()
        cursor.close()
        # Opcional: Imprimir en consola también para debug
        # print(f"[AUDIT] {event_type} - {entity_id}: {details_str}")
    except Exception as e:
        print(f"[BD Error] Fallo al escribir audit log: {e}")
    finally:
        conn.close()

# --- UTILIDADES KAFKA ---

def send_kafka_message(topic, message):
    try:
        kafka_producer.send(topic, value=message)
        kafka_producer.flush()
    except Exception as e:
        print(f"[Kafka Error] En envío a {topic}: {e}")

# --- CONSUMIDORES KAFKA ---

def kafka_consumer_driver_requests():
    """Escucha peticiones de carga de Drivers."""
    try:
        consumer = KafkaConsumer(
            TOPIC_DRIVER_REQUESTS,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id='central-logic-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("[Kafka] Escuchando Peticiones de Drivers...")
        for message in consumer:
            if stop_event.is_set(): break
            
            msg = message.value
            cp_id = msg.get('cp_id')
            driver_id = msg.get('driver_id')
            source_ip = msg.get('source_ip', 'Kafka/Unknown')
            
            # Lógica de autorización
            auth_success = False
            price = 0.50
            
            with cp_states_lock:
                # Verificamos estado en memoria
                state_info = cp_states.get(cp_id, {})
                if state_info.get('status') == 'IDLE':
                    auth_success = True
                    price = state_info.get('price_kwh', 0.50)
                    # Cambio de estado provisional
                    cp_states[cp_id]['status'] = 'AUTHORIZED'
                    update_cp_status_in_db(cp_id, 'AUTHORIZED')

            if auth_success:
                print(f"[Lógica] AUTORIZADO Driver {driver_id} en {cp_id}")
                # 1. Avisar al Engine (CP)
                send_kafka_message(f"cp_auth_{cp_id}", {
                    "action": "AUTHORIZE", 
                    "driver_id": driver_id,
                    "price_kwh": price
                })
                # 2. Avisar al Driver
                send_kafka_message(TOPIC_DRIVER_NOTIFY, {
                    "driver_id": driver_id, "status": "AUTHORIZED", 
                    "cp_id": cp_id, "info": "Puede conectar el vehículo"
                })
                log_audit_event(source_ip, driver_id, "DRIVER_AUTH_OK", f"Autorizado en {cp_id}")

            else:
                print(f"[Lógica] DENEGADO Driver {driver_id} en {cp_id}")
                send_kafka_message(TOPIC_DRIVER_NOTIFY, {
                    "driver_id": driver_id, "status": "DENIED", 
                    "cp_id": cp_id, "info": "CP no disponible"
                })
                log_audit_event(source_ip, driver_id, "DRIVER_AUTH_DENIED", f"Denegado en {cp_id} (CP no IDLE)")

    except Exception as e:
        print(f"[Kafka Error] Consumer Requests: {e}")

def kafka_consumer_cp_updates():
    """Escucha telemetría y tickets de los CPs."""
    try:
        consumer = KafkaConsumer(
            TOPIC_CP_STATUS, TOPIC_CP_TRANSACTIONS,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id='central-updates-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("[Kafka] Escuchando Updates de CPs...")
        for message in consumer:
            if stop_event.is_set(): break
            msg = message.value
            cp_id = msg.get('cp_id')
            source_ip = msg.get('source_ip', 'Kafka/Unknown')

            with cp_states_lock:
                if cp_id not in cp_states: continue # Ignorar desconocidos

                if message.topic == TOPIC_CP_STATUS:
                    # Telemetría
                    new_status = msg.get('status')
                    old_status = cp_states[cp_id].get('status')

                    cp_states[cp_id]['status'] = new_status
                    cp_states[cp_id]['session_cost'] = msg.get('session_cost', 0.0)
                    cp_states[cp_id]['session_kwh'] = msg.get('session_kwh', 0.0)
                    
                    # Solo actualizamos BD si cambia el estado (para no saturar)
                    if new_status != old_status:
                        update_cp_status_in_db(cp_id, new_status)
                        log_audit_event(source_ip=source_ip, entity_id=cp_id, event_type="STATUS_CHANGE", details={"old": old_status, "new": new_status})

                elif message.topic == TOPIC_CP_TRANSACTIONS:
                    # Fin de carga / Ticket
                    event = msg.get('event')
                    print(f"[Transacción] Ticket recibido: {event} en {cp_id}")
                    
                    # Restaurar estado a IDLE si acabó
                    if event == "CHARGE_COMPLETE":
                        cp_states[cp_id]['status'] = 'IDLE'
                        cp_states[cp_id]['session_cost'] = 0.0
                        cp_states[cp_id]['session_kwh'] = 0.0
                        update_cp_status_in_db(cp_id, 'IDLE')
                    
                    # Reenviar ticket al driver
                    send_kafka_message(TOPIC_DRIVER_NOTIFY, msg)

    except Exception as e:
        print(f"[Kafka Error] Consumer Updates: {e}")

# --- SERVIDOR SOCKETS (Comunicación Monitor) ---

def handle_monitor(conn, addr):
    """
    Gestiona conexión con Monitor. Implementa seguridad Release 2.
    """
    cp_id = None
    cipher = None

    try:
        # 1. Esperar mensaje de registro
        data = conn.recv(1024).decode('utf-8')
        msg = json.loads(data)
        
        if msg.get('type') == 'REGISTER_MONITOR':
            requested_id = msg.get('cp_id')
            
            # --- SEGURIDAD RELEASE 2 ---
            # Verificar en BD si está registrado
            is_valid, cp_data = verify_cp_registration(requested_id)
            
            if is_valid:
                cp_id = requested_id
                symmetric_key = cp_data['symmetric_key'] # Esta clave se usa para descifrar futuros mensajes
                cipher = get_cipher(symmetric_key)

                with cp_states_lock:
                    cp_states[cp_id] = {
                        'status': 'IDLE', 
                        'price_kwh': float(cp_data['price_kwh']),
                        'key': symmetric_key
                    }
                
                update_cp_status_in_db(cp_id, 'IDLE')
                conn.sendall(b"ACK_REGISTER")
                print(f"[Socket] Monitor {cp_id} Conectado y Autenticado.")
                log_audit_event(addr[0], cp_id, "AUTH_SUCCESS", "Monitor conectado y autenticado correctamente.")
            else:
                print(f"[Socket] Rechazado {requested_id}. No registrado en BD.")
                log_audit_event(addr[0], cp_id, "AUTH_FAILED", "Intento de conexión con credenciales inválidas o CP no registrado.")
                conn.sendall(b"NACK_NOT_REGISTERED")
                return
        else:
            return

        # 2. Bucle de latido (Estado Monitor)
        while not stop_event.is_set():
            encrypted_data = conn.recv(2048) # Recibimos bytes (no decode todavía)
            if not encrypted_data: break
            
            try:
                # --- AQUÍ OCURRE EL DESCIFRADO ---
                decrypted_bytes = cipher.decrypt(encrypted_data)
                json_str = decrypted_bytes.decode('utf-8')
                msg = json.loads(json_str)

                if msg.get('type') == 'MONITOR_STATUS':
                    status = msg.get('status') # OK / FAULTED / DISCONNECTED
                    if status == 'FAULTED':
                        with cp_states_lock:
                            cp_states[cp_id]['status'] = 'FAULTED'
                        update_cp_status_in_db(cp_id, 'FAULTED')
                        print(f"[ALERTA] Monitor {cp_id} reporta AVERÍA.")
                        log_audit_event(addr[0], cp_id, "FAULT_REPORTED", "El monitor reportó una avería.")
                    elif status == 'DISCONNECTED':
                        with cp_states_lock:
                            cp_states[cp_id]['status'] = 'DISCONNECTED'
                        update_cp_status_in_db(cp_id, 'DISCONNECTED')
                        print(f"[ALERTA] Monitor {cp_id} reporta Engine APAGADO/DESCONECTADO.")
                        log_audit_event(addr[0], cp_id, "CONNECTION_LOST", "El monitor cerró la conexión inesperadamente.")
                    elif status == 'OK':
                        # Si estaba averiado y vuelve a OK -> IDLE
                        with cp_states_lock:
                            if cp_states[cp_id]['status'] == 'FAULTED':
                                cp_states[cp_id]['status'] = 'IDLE'
                                update_cp_status_in_db(cp_id, 'IDLE')
                                log_audit_event(addr[0], cp_id, "RECOVERY", "El monitor reportó recuperación del estado de avería.")
            except Exception as e:
                print(f"[Error Cifrado] No se pudo descifrar mensaje de {cp_id}: {e}")
                break

    except Exception as e:
        print(f"[Socket] Error con {addr}: {e}")
    finally:
        if cp_id:
            with cp_states_lock:
                if cp_id in cp_states:
                    cp_states[cp_id]['status'] = 'DISCONNECTED'
            update_cp_status_in_db(cp_id, 'DISCONNECTED')
            print(f"[Socket] Monitor {cp_id} desconectado.")
        conn.close()

def start_socket_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(('0.0.0.0', socket_port))
        server.listen()
        print(f"[Socket] Escuchando Monitores en puerto {socket_port}")
        while not stop_event.is_set():
            conn, addr = server.accept()
            t = threading.Thread(target=handle_monitor, args=(conn, addr), daemon=True)
            t.start()
    except Exception as e:
        print(f"[Socket Fatal] {e}")

def revoke_cp_credentials(cp_id):
    """
    Simula una vulnerabilidad: Borra la clave simétrica y desregistra el CP.
    """
    print(f"[Seguridad] Revocando claves de {cp_id}...")
    
    # 1. Actualizar Base de Datos (Clave a NULL y is_registered a 0)
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # Borramos clave, quitamos registro y ponemos status DISCONNECTED
            query = """
                UPDATE charge_points 
                SET symmetric_key = NULL, is_registered = 0, status = 'DISCONNECTED' 
                WHERE cp_id = %s
            """
            cursor.execute(query, (cp_id,))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"[Error BD] Al revocar: {e}")
            return

    # 2. Actualizar Memoria (cp_states)
    with cp_states_lock:
        if cp_id in cp_states:
            cp_states[cp_id]['key'] = None
            cp_states[cp_id]['is_registered'] = 0
            cp_states[cp_id]['status'] = 'DISCONNECTED'
    
    # 3. Auditoría
    log_audit_event("Central/Admin", cp_id, "KEY_REVOKED", "Claves revocadas por vulnerabilidad. CP fuera de servicio.")
    print(f"[Seguridad] {cp_id} ha sido expulsado del sistema.")

# --- MENÚ DE CONTROL (TERMINAL) ---

def run_control_menu():
    """Solo muestra el menú de control, sin gráficos."""
    time.sleep(1)
    print("\n" + "="*30)
    print(" EV CENTRAL - CONTROL CONSOLE")
    print("="*30)
    print(" (La monitorización gráfica está disponible en la Web)")
    
    while not stop_event.is_set():
        print("\n--- COMANDOS ADMIN ---")
        print("1. Parar un CP (Stop)")
        print("2. Reanudar un CP (Resume)")
        print("3. Monitorizar CPs")
        print("4. Revocar claves")
        print("5. Salir")
        
        choice = input("Seleccione opción: ")
        
        if choice == '1':
            tid = input("ID del CP a detener: ")
            send_kafka_message(f"cp_auth_{tid}", {"action": "STOP_COMMAND"})
            print("Comando enviado.")
            update_cp_status_in_db(tid, 'STOPPED')
            
        elif choice == '2':
            tid = input("ID del CP a reanudar: ")
            send_kafka_message(f"cp_auth_{tid}", {"action": "RESUME_COMMAND"})
            print("Comando enviado.")
            update_cp_status_in_db(tid, 'IDLE')
            
        elif choice == '3':
            # print(json.dumps(cp_states, indent=2))
            # Imprimimos una tabla de texto simple pero útil
            # load_db_data()  # Refrescar datos desde BD antes de mostrar
            print("\nIniciando monitorización en tiempo real...")
            print("PULSA CTRL+C PARA VOLVER AL MENÚ PRINCIPAL\n")
            time.sleep(1)
            
            try:
                # Bucle de refresco infinito
                while True:
                    os.system('cls' if os.name == 'nt' else 'clear') 
                    
                    print(f"*** MONITORIZACIÓN EN VIVO ({time.strftime('%H:%M:%S')}) ***")
                    print("-" * 85)
                    print(f"{'CP ID':<10} | {'STATUS':<15} | {'COSTE (€)':<12} | {'KWH':<10} | {'PRECIO T.':<10} | {'ALERTA CLIMA'}")
                    print("-" * 85)
                    
                    with cp_states_lock:
                        if not cp_states:
                            print(" (Esperando conexión de CPs...)")
                        
                        for cid, d in sorted(cp_states.items()):
                            status = d.get('status', 'UNKNOWN')
                            cost = d.get('session_cost', 0.0)
                            kwh = d.get('session_kwh', 0.0)
                            price = d.get('price_kwh', 0.0)
                            # Usamos el estado de alerta que el hilo de sincronización mantiene
                            alert = "SI" if local_weather_alerts.get(cid) else "NO" 
                            
                            marker = ">>" if status == 'CHARGING' else "  "
                            
                            print(f"{marker} {cid:<7} | {status:<15} | {cost:06.3f} €     | {kwh:06.3f}     | {price:<10} | {alert}")
                            
                    print("-" * 85)
                    print("(Ctrl+C para salir)")
                    
                    time.sleep(1) # Refrescar cada 1 segundo
                    
            except KeyboardInterrupt:
                print("\n\nDeteniendo monitorización... Volviendo al menú.")
                # No hacemos break aquí para que vuelva al while del menú principal

        elif choice == '4':
            tid = input("ID del CP a revocar: ")
            revoke_cp_credentials(tid)
            
        elif choice == '5':
            print("Apagando sistema...")
            stop_event.set()
            # Enviar señal de apagado a CPs
            for cid in cp_states:
                send_kafka_message(f"cp_auth_{cid}", {"action": "CENTRAL_SHUTDOWN"})
            break

# --- MAIN ---

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python EV_Central.py <puerto_socket> <broker_kafka>")
        sys.exit(1)
        
    socket_port = int(sys.argv[1])
    kafka_bootstrap_servers = sys.argv[2]

    # load_initial_data()
    
    # Iniciar Productor Kafka
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except:
        print("Error conectando a Kafka.")
        sys.exit(1)

    # Iniciar Hilos
    threading.Thread(target=start_socket_server, daemon=True).start()
    threading.Thread(target=kafka_consumer_driver_requests, daemon=True).start()
    threading.Thread(target=kafka_consumer_cp_updates, daemon=True).start()
    threading.Thread(target=cp_state_synchronization_thread, daemon=True).start()
    print("Hilo de Sincronización de estado y Alerta Climática iniciado.")

    # --- ABRIR DASHBOARD AUTOMÁTICAMENTE ---
    print("Abriendo Dashboard en el navegador...")
    webbrowser.open("http://localhost:5000")  # Abre la URL servida por la API
    
    # Iniciar Menú
    try:
        run_control_menu()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        if kafka_producer: kafka_producer.close()