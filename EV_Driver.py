# Ejecución: docker exec -it ev_driver_1 python3 EV_Driver.py kafka:29092 101
import sys
from confluent_kafka import Producer
import json
import time

# El primer argumento es la dirección del Broker de Kafka (kafka:29092)
BOOTSTRAP_SERVERS = sys.argv[1] 
TOPIC_NAME = 'EV_REQUESTS'

def delivery_report(err, msg):
    """Callback para confirmar el envío del mensaje."""
    if err is not None:
        print(f"❌ ERROR: Fallo en la entrega del mensaje: {err}")
    else:
        print(f"✅ Mensaje enviado a {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def run_producer():
    conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}

    try:
        producer = Producer(conf)
        print(f"✅ Productor Confluent conectado a {BOOTSTRAP_SERVERS}")
        
        # Enviar un mensaje de prueba
        test_message = {'timestamp': time.time(), 'message': 'Prueba Confluent OK'}
        message_json = json.dumps(test_message).encode('utf-8')
        
        # Envío asíncrono
        producer.produce(
            TOPIC_NAME, 
            value=message_json, 
            callback=delivery_report
        )
        
        producer.poll(0) # Permite que se procesen los callbacks
        
        # Esperar a que todos los mensajes pendientes sean enviados (muy importante)
        producer.flush(timeout=10) 
        
    except Exception as e:
        print(f"❌ ERROR: Fallo al conectar/enviar mensaje a Kafka.")
        print(f"Detalle del error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python confluent_producer_test.py <IP_Puerto_Broker>")
        sys.exit(1)
    run_producer()