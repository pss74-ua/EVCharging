# Ejecución: python EV_Driver.py localhost:9092 23 requests.txt (python EV_Driver.py <broker_kafka> <id_cliente> <archivo_peticiones>)
#
# Este script implementa "EV_Driver", la aplicación que simula al conductor
# (Punto 143, 241).
#
# Funcionalidades principales:
# 1. Cliente/Productor Kafka: Envía peticiones de carga a EV_Central (Punto 145, 171).
# 2. Consumidor Kafka: Escucha notificaciones de EV_Central (Autorizado, Denegado,
#    Ticket final) (Punto 176, 187).
# 3. Modo Batch: Puede leer un fichero de peticiones y ejecutarlas
#    secuencialmente (Punto 173, 287).
# 4. Modo Interactivo: Permite solicitar cargas manualmente (Punto 287).
#

import sys
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import threading

# --- Constantes ---
TOPIC_DRIVER_REQUESTS = "driver_requests" # Driver -> Central
TOPIC_DRIVER_NOTIFY = "driver_notifications" # Central -> Driver

# --- Variables Globales ---
kafka_bootstrap_servers = None
driver_id = None # ID de este conductor
request_list = [] # Lista de CPs a solicitar (en modo batch)
kafka_producer = None
stop_event = threading.Event() # Evento para detener hilos de forma limpia

# --- Funciones Auxiliares ---

def send_kafka_message(topic, message):
    """Envía un mensaje JSON al topic de Kafka especificado."""
    global kafka_producer
    try:
        kafka_producer.send(topic, value=message)
        kafka_producer.flush() # Asegurar envío inmediato
    except Exception as e:
        print(f"[Error Kafka] No se pudo enviar mensaje: {e}")

def load_requests_from_file(filename):
    """
    Carga la lista de CPs a solicitar desde un archivo de texto (Modo Batch).
    Implementa la lectura del fichero (Punto 173, 287).
    """
    try:
        with open(filename, 'r') as f:
            # Lee todas las líneas y quita espacios en blanco/saltos de línea
            requests = [line.strip() for line in f if line.strip()]
        
        if not requests:
            print(f"[Error] El archivo {filename} está vacío o no contiene IDs válidos.")
            return None
        
        print(f"[Info] {len(requests)} peticiones de carga leídas de {filename}.")
        return requests
    except FileNotFoundError:
        print(f"[Error Fatal] No se encontró el archivo de peticiones: {filename}")
        return None
    except Exception as e:
        print(f"[Error] No se pudo leer {filename}: {e}")
        return None

# --- Hilo Consumidor ---

def kafka_consumer_thread():
    """
    Escucha notificaciones de EV_Central (Autorizado, Denegado, Ticket).
    Este hilo corre en segundo plano y muestra los mensajes al conductor (Punto 176, 186, 187).
    """
    global driver_id, kafka_bootstrap_servers
    consumer = None

    while not stop_event.is_set():
        try:
            consumer = KafkaConsumer(
                TOPIC_DRIVER_NOTIFY,
                bootstrap_servers=kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                group_id=f"driver-group-{driver_id}", # Grupo único para este driver
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("[KAFKA] Escuchando notificaciones de Central...")
            
            for message in consumer:
                if stop_event.is_set():
                    break
                
                msg = message.value
                
                # Filtrar mensajes solo para este conductor
                # (Central envía todo a este topic)
                if msg.get('driver_id') != driver_id:
                    continue
                
                status = msg.get('status') # Autorización/Denegación
                cp_id = msg.get('cp_id')
                info = msg.get('info', '')
                event = msg.get('event')   # Ticket (fin de carga)
                
                print(f"\n*** NOTIFICACIÓN DE CENTRAL ***")
                
                # --- Lógica de Autorización/Denegación (STATUS) ---
                if status == "AUTHORIZED":
                    # (Punto 176)
                    print(f"  > Estado: ¡AUTORIZADO!")
                    print(f"  > CP: {cp_id}")
                    print(f"  > Info: {info}")
                    print(f"  > (Esperando a que CP_E confirme inicio de carga...)")
                
                elif status == "DENIED":
                    # (Punto 176)
                    print(f"  > Estado: DENEGADO ")
                    print(f"  > CP: {cp_id}")
                    print(f"  > Info: {info}")
                    # Desbloquear el hilo principal/batch para que envíe la siguiente petición
                    time_to_next_request.set() 
                
                # --- Lógica de Transacción/Ticket (EVENT) ---
                # (Punto 187)
                elif event:
                    
                    if event == "CHARGE_COMPLETE":
                        print(f"  > ¡CARGA FINALIZADA! (Ticket)")
                    elif event == "CHARGE_FAILED":
                        print(f"  > ¡CARGA FALLIDA!")
                        print(f"  > Razón: {msg.get('reason', 'N/A')}")
                    # Manejo del ticket parcial por parada forzada (Punto 13.a)
                    elif event == "CHARGE_STOPPED_BY_CENTRAL":
                        print(f"  > ¡CARGA DETENIDA POR CENTRAL! (Ticket Parcial)")
                    else:
                        print(f"  > Evento Desconocido: {event}")
                        
                    # Imprimir detalles del ticket
                    print(f"  > CP: {cp_id}")
                    print(f"  > Duración: {msg.get('duration_sec', 0):.2f} seg")
                    print(f"  > Consumo: {msg.get('total_kwh', 0):.3f} kWh")
                    print(f"  > Coste Total: {msg.get('total_cost', 0):.2f} €")
                    
                    # Desbloquear el hilo principal/batch para el siguiente paso (Punto 288)
                    time_to_next_request.set() 

                print("*******************************\n")

        except NoBrokersAvailable:
            print("[Error Kafka] No se puede conectar al broker. Reintentando en 5s...")
            time.sleep(5)
        except Exception as e:
            if not stop_event.is_set():
                print(f"[Error Kafka] Error en consumidor: {e}. Reiniciando...")
                time.sleep(1)
        finally:
            if consumer:
                consumer.close()

# --- Hilo de Peticiones ---

# Evento para coordinar cuándo se debe enviar la siguiente petición
# El hilo productor (batch o interactivo) "wait()" en este evento.
# El hilo consumidor (kafka_consumer_thread) "set()" este evento
# cuando recibe un DENIED, CHARGE_COMPLETE, o CHARGE_FAILED.
time_to_next_request = threading.Event()

def request_batch_thread():
    """
    Gestiona el envío de peticiones de carga una por una desde el archivo (Modo Batch).
    (Punto 173, 287, 288)
    """
    global request_list, driver_id
    
    # Esperar un poco a que el consumidor (kafka_consumer_thread) esté listo
    time.sleep(2) 
    
    for i, cp_id in enumerate(request_list):
        if stop_event.is_set():
            break
            
        print(f"\n[Petición {i+1}/{len(request_list)}] Solicitando carga en CP: {cp_id}...")
        
        # Limpiar el evento antes de enviar la petición
        time_to_next_request.clear()
        
        # Enviar petición a Central
        request_msg = {
            "action": "REQUEST_CHARGE",
            "driver_id": driver_id,
            "cp_id": cp_id
        }
        send_kafka_message(TOPIC_DRIVER_REQUESTS, request_msg)
        
        # Esperar a que el consumidor reciba 'CHARGE_COMPLETE', 'CHARGE_FAILED' o 'DENIED'
        print(f"[Info] Petición enviada. Esperando finalización de la carga...")
        time_to_next_request.wait() # Bloquea hasta que el evento se dispare
        
        # Esperar 4 segundos antes de la siguiente petición (Punto 195, 288)
        if i < len(request_list) - 1: # Si no es la última petición
            print("\n[Info] Siguiente carga en 4 segundos...")
            time.sleep(4)
            
    print("\n[Info] Todas las peticiones del archivo han sido procesadas.")
    stop_event.set() # Indicar al hilo principal que termine

def run_interactive_loop():
    """
    Gestiona el envío de peticiones una por una de forma INTERACTIVA.
    Se ejecuta en el hilo principal. (Punto 287)
    """
    global driver_id
    
    while not stop_event.is_set():
        print("\n--- Modo Interactivo EV Driver ---")
        print("1. Solicitar Carga")
        print("2. Salir")
        choice = input("Seleccione una opción: ")
        
        if choice == '1':
            cp_id = input("  > Introduzca el ID del CP: ").strip().upper()
            if not cp_id:
                print("[Error] ID de CP no puede estar vacío.")
                continue

            print(f"\n[Petición Manual] Solicitando carga en CP: {cp_id}...")
            
            time_to_next_request.clear() # Limpiar evento
            
            request_msg = {
                "action": "REQUEST_CHARGE",
                "driver_id": driver_id,
                "cp_id": cp_id
            }
            send_kafka_message(TOPIC_DRIVER_REQUESTS, request_msg)
            
            print(f"[Info] Petición enviada. Esperando finalización de la carga...")
            # Bloquear hasta que el consumidor reciba respuesta
            time_to_next_request.wait() 
            
            print("\n[Info] Operación finalizada. Volviendo al menú.")
            time.sleep(1) # Pausa breve

        elif choice == '2':
            print("[Info] Saliendo de modo interactivo.")
            stop_event.set()
        
        else:
            print("[Error] Opción no válida.")

# --- Función Principal ---

def main():
    global kafka_bootstrap_servers, driver_id, request_list, kafka_producer
    
    # 1. Validar argumentos (Punto 283)
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Uso: python EV_Driver.py <ip_broker_kafka:puerto> <id_cliente> [archivo_peticiones]")
        sys.exit(1)
        
    # Arg 1: Broker Kafka (Punto 285)
    kafka_bootstrap_servers = sys.argv[1]
    # Arg 2: ID del Cliente/Driver (Punto 286)
    driver_id = sys.argv[2]
    
    # Comprobar modo (Batch o Interactivo)
    # Si se proporciona un 4º argumento, es modo Batch
    is_batch_mode = len(sys.argv) == 4
    
    print(f"--- Iniciando EV Driver ---")
    print(f"  Driver ID:  {driver_id}")
    print(f"  Broker:     {kafka_bootstrap_servers}")
    
    if is_batch_mode:
        # Arg 3: Archivo de peticiones (Punto 287)
        requests_file = sys.argv[3]
        print(f"  Modo:       Batch (Archivo: {requests_file})")
        print("---------------------------")
        # 2. Cargar peticiones (solo en modo batch)
        request_list = load_requests_from_file(requests_file)
        if not request_list:
            sys.exit(1)
    else:
        print(f"  Modo:       Interactivo")
        print("---------------------------")
        
    # 3. Inicializar Kafka Producer (para enviar peticiones)
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except NoBrokersAvailable:
        print(f"[Error Fatal] No se puede conectar a Kafka Broker. Abortando.")
        sys.exit(1)
        
    # 4. Iniciar hilos
    # Hilo consumidor para escuchar notificaciones de Central
    consumer = threading.Thread(target=kafka_consumer_thread, daemon=True)    
    consumer.start()

    # 5. Ejecutar modo (Batch o Interactivo)
    try:
        if is_batch_mode:
            # Iniciar hilo de peticiones batch
            manager = threading.Thread(target=request_batch_thread, daemon=True)
            manager.start()
            # Esperar a que el hilo batch ponga el stop_event (o Ctrl+C)
            while not stop_event.is_set():
                time.sleep(1)
        else:
            # Ejecutar el bucle interactivo en el hilo principal
            run_interactive_loop()
            
    except KeyboardInterrupt:
        print("\n[Info] Cierre solicitado (Ctrl+C).")
        stop_event.set() # Señalizar a los hilos que terminen
    finally:
        if kafka_producer:
            kafka_producer.close()
        print("EV_Driver apagado.")

if __name__ == "__main__":
    main()