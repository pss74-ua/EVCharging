# Ejecución: python EV_Driver.py localhost:9092 23 requests.txt (python EV_Driver.py <broker_kafka> <id_cliente> <archivo_peticiones>)

import sys
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import threading

# --- Constantes ---
TOPIC_DRIVER_REQUESTS = "driver_requests"
TOPIC_DRIVER_NOTIFY = "driver_notifications"

# --- Variables Globales ---
kafka_bootstrap_servers = None
driver_id = None
request_list = []
kafka_producer = None
stop_event = threading.Event()

# --- Funciones Auxiliares ---

def send_kafka_message(topic, message):
    """Envía un mensaje JSON al topic de Kafka especificado."""
    global kafka_producer
    try:
        kafka_producer.send(topic, value=message)
        kafka_producer.flush()
    except Exception as e:
        print(f"[Error Kafka] No se pudo enviar mensaje: {e}")

def load_requests_from_file(filename):
    """Carga la lista de CPs a solicitar desde un archivo de texto."""
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
    """
    global driver_id, kafka_bootstrap_servers
    consumer = None

    while not stop_event.is_set():
        try:
            consumer = KafkaConsumer(
                TOPIC_DRIVER_NOTIFY,
                bootstrap_servers=kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                group_id=f"driver-group-{driver_id}", # Grupo único
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("[KAFKA] Escuchando notificaciones de Central...")
            
            for message in consumer:
                if stop_event.is_set():
                    break
                
                msg = message.value
                
                # Filtrar mensajes solo para este conductor
                if msg.get('driver_id') != driver_id:
                    continue
                
                status = msg.get('status')
                cp_id = msg.get('cp_id')
                info = msg.get('info', '')
                event = msg.get('event')
                
                print(f"\n*** NOTIFICACIÓN DE CENTRAL ***")
                if status == "AUTHORIZED":
                    print(f"  > Estado: ¡AUTORIZADO! [cite: 176]")
                    print(f"  > CP: {cp_id}")
                    print(f"  > Info: {info}")
                    print(f"  > (Esperando a que CP_E confirme inicio de carga...)")
                
                elif status == "DENIED":
                    print(f"  > Estado: DENEGADO [cite: 176]")
                    print(f"  > CP: {cp_id}")
                    print(f"  > Info: {info}")
                    # Como fue denegado, disparamos el evento para pedir el siguiente
                    time_to_next_request.set() 
                
                elif event == "CHARGE_COMPLETE":
                    print(f"  > ¡CARGA FINALIZADA! (Ticket) [cite: 187]")
                    print(f"  > CP: {cp_id}")
                    print(f"  > Duración: {msg.get('duration_sec', 0):.2f} seg")
                    print(f"  > Consumo: {msg.get('total_kwh', 0):.3f} kWh")
                    print(f"  > Coste Total: {msg.get('total_cost', 0):.2f} €")
                    time_to_next_request.set() # Disparar evento para siguiente carga

                elif event == "CHARGE_FAILED":
                    print(f"  > ¡CARGA FALLIDA! [cite: 191]")
                    print(f"  > CP: {cp_id}")
                    print(f"  > Razón: {msg.get('reason', 'N/A')}")
                    time_to_next_request.set() # Disparar evento para siguiente carga

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
time_to_next_request = threading.Event()

def request_manager_thread():
    """
    Gestiona el envío de peticiones de carga una por una.
    """
    global request_list, driver_id
    
    # Esperar un poco a que el consumidor esté listo
    time.sleep(2) 
    
    for i, cp_id in enumerate(request_list):
        if stop_event.is_set():
            break
            
        print(f"\n[Petición {i+1}/{len(request_list)}] Solicitando carga en CP: {cp_id}...")
        
        # Limpiar el evento
        time_to_next_request.clear()
        
        # Enviar petición a Central
        request_msg = {
            "action": "REQUEST_CHARGE",
            "driver_id": driver_id,
            "cp_id": cp_id
        }
        send_kafka_message(TOPIC_DRIVER_REQUESTS, request_msg)
        
        # Esperar a que el consumidor reciba 'CHARGE_COMPLETE' o 'CHARGE_FAILED'
        # o 'DENIED' [cite: 195, 288]
        print(f"[Info] Petición enviada. Esperando finalización de la carga...")
        time_to_next_request.wait() # Bloquea hasta que el evento se dispare
        
        # Esperar 4 segundos antes de la siguiente petición [cite: 195]
        if i < len(request_list) - 1: # Si no es la última petición
            print("\n[Info] Siguiente carga en 4 segundos...")
            time.sleep(4)
            
    print("\n[Info] Todas las peticiones del archivo han sido procesadas.")
    stop_event.set() # Indicar a otros hilos que terminen

# --- Función Principal ---

def main():
    global kafka_bootstrap_servers, driver_id, request_list, kafka_producer
    
    # 1. Validar argumentos
    if len(sys.argv) != 4:
        print("Uso: python EV_Driver.py <ip_broker_kafka:puerto> <id_cliente> <archivo_peticiones>")
        sys.exit(1)
        
    kafka_bootstrap_servers = sys.argv[1]
    driver_id = sys.argv[2]
    requests_file = sys.argv[3]
    
    print(f"--- Iniciando EV Driver ---")
    print(f"  Driver ID:  {driver_id}")
    print(f"  Broker:     {kafka_bootstrap_servers}")
    print(f"  Archivo:    {requests_file}")
    print("---------------------------")

    # 2. Cargar peticiones
    request_list = load_requests_from_file(requests_file)
    if not request_list:
        sys.exit(1)
        
    # 3. Inicializar Kafka Producer
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except NoBrokersAvailable:
        print(f"[Error Fatal] No se puede conectar a Kafka Broker. Abortando.")
        sys.exit(1)
        
    # 4. Iniciar hilos
    consumer = threading.Thread(target=kafka_consumer_thread, daemon=True)
    manager = threading.Thread(target=request_manager_thread, daemon=True)
    
    consumer.start()
    manager.start()

    # 5. Esperar a que todo termine
    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Info] Cierre solicitado (Ctrl+C).")
        stop_event.set()
    finally:
        if kafka_producer:
            kafka_producer.close()
        print("EV_Driver apagado.")

if __name__ == "__main__":
    main()