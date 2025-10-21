# Ejecución: docker exec -it ev_central python3 EV_Central.py kafka:29092 8080
import threading
import time
import sys
import logging
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError

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


# --- FUNCIÓN PRINCIPAL DE ARRANQUE ---

if __name__ == "__main__":
    if len(sys.argv) < 3:
        logging.error("Uso: python EV_Central.py <IP_Puerto_Broker> <Puerto_Escucha>")
        sys.exit(1)
        
    KAFKA_BOOTSTRAP_SERVERS = sys.argv[1] # kafka:29092
    SOCKET_PORT = int(sys.argv[2])        # 8080
    
    # 1. Tópicos de Kafka 

    # ------------------------------------------------
    
    # 2. Inicializar y Lanzar Hilos Consumidores

    
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