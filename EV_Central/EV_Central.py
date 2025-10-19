"""
import sys
from confluent_kafka import Consumer, KafkaException
import json

# El primer argumento es la dirección del Broker de Kafka (kafka:29092)
BOOTSTRAP_SERVERS = sys.argv[1] 
TOPIC_NAME = 'TEST_CONNECTIVITY'

def run_consumer():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'test_confluent_group',
        # Leer desde el principio del tópico para asegurar que vemos el mensaje de prueba
        'auto.offset.reset': 'earliest' 
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe([TOPIC_NAME])
        print(f"✅ Consumidor Confluent conectado a {BOOTSTRAP_SERVERS} y escuchando en {TOPIC_NAME}...")

        # Bucle de sondeo (polling) para mensajes
        msg = consumer.poll(timeout=60.0) # Espera 5 segundos por un mensaje

        if msg is None:
            print("Tiempo de espera agotado. No se recibieron mensajes de prueba.")
        elif msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # Caso especial para el final de la partición (no es un error real)
                print("Fin de la partición alcanzado. No hay más mensajes.")
            else:
                print(f"❌ ERROR de Consumidor: {msg.error()}")
        else:
            # Mensaje recibido con éxito
            try:
                message_data = json.loads(msg.value().decode('utf-8'))
                print(f"Mensaje recibido de {msg.topic()} @ offset {msg.offset()}:")
                print(f"Contenido: {message_data}")
            except json.JSONDecodeError:
                print(f"Mensaje recibido con éxito, pero fallo al decodificar JSON.")
                
    except Exception as e:
        print(f"❌ ERROR: Fallo al conectar/consumir de Kafka.")
        print(f"Detalle del error: {e}")
        
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python confluent_consumer_test.py <IP_Puerto_Broker>")
        sys.exit(1)
    run_consumer()
"""
# Ejecución: docker exec -it ev_central python3 EV_Central.py kafka:29092 8080
import threading
import time
import sys
import logging
from confluent_kafka import Consumer, KafkaException, error, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
# ... otras importaciones (sockets, etc.)

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')

# --- FUNCIONES DE ESCUCHA (THREADS) ---

def kafka_consumer_thread(topic_name, bootstrap_servers):
    """
    Función que ejecuta el bucle de escucha para un tópico específico de Kafka.
    """
    # 1. DEFINICIÓN DE CONFIGURACIÓN (MOVIDA FUERA DEL BUCLE)
    # ----------------------------------------------------
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': f'central_consumer_{topic_name}',
        'auto.offset.reset': 'earliest'
    }
    # ----------------------------------------------------
    
    try:
        # 2. CREACIÓN DEL CONSUMER
        consumer = Consumer(conf) # Aquí es donde 'conf' debe estar definida
        consumer.subscribe([topic_name])
        logging.info(f"Consumer iniciado, escuchando en el tópico: {topic_name}")
        
        while True:
            # 3. Bucle de polling
            msg = consumer.poll(timeout=1.0) 

            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == -191:  # KafkaError._PARTITION_EOF
                    logging.debug(f"[{topic_name}] Fin de la partición alcanzado.")
                    continue
                else:
                    logging.error(f"Error de consumidor en {topic_name}: {msg.error()}")
                    continue

            # --- LÓGICA DE PROCESAMIENTO DEL MENSAJE ---
            data = msg.value().decode('utf-8')
            logging.warning(f"MENSAJE RECIBIDO de {topic_name}: {data}")
            
    except Exception as e:
        # Si la conexión falla aquí, el hilo reporta el error y muere
        logging.error(f"Fallo crítico en el consumidor de {topic_name}: {e}")
        # El hilo muere, pero el MainThread y otros hilos siguen vivos
            
    finally:
        # Cerrar el consumer si se pudo crear
        if 'consumer' in locals():
            consumer.close()


def socket_server_thread(port):
    """
    Función que inicia el servidor de sockets y maneja las conexiones.
    (Debe usar un bucle while True para aceptar nuevas conexiones)
    """
    logging.info(f"Servidor de Sockets iniciado en el puerto {port}...")
    # Implementación del bucle while True s.accept()...
    
    # Dentro de este bucle, cada conexión aceptada DEBE ser manejada por un 
    # HILO ADICIONAL para que el servidor principal no se bloquee.
    while True:
        # Aquí iría la lógica de escucha del socket (siempre viva)
        time.sleep(5) # Placeholder para la lógica del socket


def create_topics(bootstrap_servers, topic_list):
    """Crea los tópicos si no existen."""
    
    # Configuración del AdminClient
    admin_conf = {'bootstrap.servers': bootstrap_servers}
    admin_client = AdminClient(admin_conf)
    
    # Crear una lista de objetos NewTopic
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) 
                  for topic in topic_list]
    
    # Lanza la creación de tópicos de forma asíncrona
    fs = admin_client.create_topics(new_topics, request_timeout=15.0)

    # Espera a que las operaciones terminen
    for topic, f in fs.items():
        try:
            f.result()  # Si no hay excepción, el tópico se creó o ya existía
            logging.info(f"✅ Tópico '{topic}' creado o verificado con éxito.")
        except Exception as e:
            # Si el error es TOPIC_ALREADY_EXISTS, simplemente continuamos
            if 'Topic already exists' not in str(e):
                logging.error(f"❌ Fallo al crear el tópico '{topic}': {e}")
            else:
                logging.info(f"✅ Tópico '{topic}' verificado (ya existía).")

# --- FUNCIÓN PRINCIPAL DE ARRANQUE ---

if __name__ == "__main__":
    if len(sys.argv) < 3:
        logging.error("Uso: python EV_Central.py <IP_Puerto_Broker> <Puerto_Escucha>")
        sys.exit(1)
        
    KAFKA_BOOTSTRAP_SERVERS = sys.argv[1] # kafka:29092
    SOCKET_PORT = int(sys.argv[2])        # 8080
    
    # 1. Tópicos de Kafka 
    # --- EJECUCIÓN CRÍTICA DE CREACIÓN DE TÓPICOS ---
    TOPICS_TO_CREATE = [
        'EV_REQUESTS', 
        'EV_NOTIFICATIONS', 
        'CP_TELEMETRY', 
        'CP_ALARMS'
    ]
    create_topics(KAFKA_BOOTSTRAP_SERVERS, TOPICS_TO_CREATE)
    # ------------------------------------------------
    
    # 2. Inicializar y Lanzar Hilos Consumidores
    
    # Hilo para manejar las peticiones del Driver
    t_requests = threading.Thread(
        target=kafka_consumer_thread, 
        args=('EV_REQUESTS', KAFKA_BOOTSTRAP_SERVERS),
        name="Consumer-Requests"
    )
    t_requests.daemon = True
    t_requests.start()
    
    # Hilo para manejar la telemetría del CP Engine
    t_telemetry = threading.Thread(
        target=kafka_consumer_thread, 
        args=('CP_TELEMETRY', KAFKA_BOOTSTRAP_SERVERS),
        name="Consumer-Telemetry"
    )
    t_telemetry.daemon = True
    t_telemetry.start()
    
    # Hilo para manejar las notificaciones de avería
    t_alarms = threading.Thread(
        target=kafka_consumer_thread, 
        args=('CP_ALARMS', KAFKA_BOOTSTRAP_SERVERS),
        name="Consumer-Alarms"
    )
    t_alarms.daemon = True
    t_alarms.start()

    # Hilo para el Servidor de Sockets
    t_sockets = threading.Thread(
        target=socket_server_thread, 
        args=(SOCKET_PORT,),
        name="Socket-Server"
    )
    t_sockets.daemon = True
    t_sockets.start()
    
    logging.info("Sistema Central EVCharging completamente inicializado y concurrente.")
    
    # 3. Mantener el Hilo Principal vivo
    try:
        while True:
            # Aquí podrías poner la lógica de la interfaz de usuario (panel de control)
            # o simplemente dejarlo dormido.
            time.sleep(0.1) 
    except KeyboardInterrupt:
        logging.critical("Central detenida por el usuario.")
        sys.exit(0)