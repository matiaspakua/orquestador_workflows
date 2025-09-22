import os
import json
import time
import uuid
import logging
import psycopg2
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pythonjsonlogger.json import JsonFormatter

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

class EventProducer:
    def __init__(self):
        self.fake = Faker()
        self.kafka_producer = None
        self.db_connection = None
        self.setup_kafka()
        self.setup_database()
        
    def setup_kafka(self):
        """Configura la conexión a Kafka"""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None,
                retries=5,
                retry_backoff_ms=1000,
                acks='all'
            )
            logger.info("Kafka producer configurado correctamente")
        except Exception as e:
            logger.error(f"Error configurando Kafka: {e}")
            raise

    def setup_database(self):
        """Configura la conexión a PostgreSQL"""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.db_connection = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'postgres'),
                    port=os.getenv('POSTGRES_PORT', 5432),
                    database=os.getenv('POSTGRES_DB', 'eventstore'),
                    user=os.getenv('POSTGRES_USER', 'eventuser'),
                    password=os.getenv('POSTGRES_PASSWORD', 'eventpass')
                )
                self.db_connection.autocommit = True
                logger.info("Conexión a PostgreSQL establecida")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Intento {attempt + 1} de conexión a PostgreSQL falló, reintentando...")
                    time.sleep(5)
                else:
                    logger.error(f"Error conectando a PostgreSQL después de {max_retries} intentos: {e}")
                    raise

    def store_data_in_db(self, data_type: str, payload: dict) -> str:
        """Almacena los datos en PostgreSQL y retorna el ID de referencia"""
        try:
            with self.db_connection.cursor() as cursor:
                data_id = str(uuid.uuid4())
                cursor.execute(
                    "INSERT INTO event_data (id, data_type, payload, status) VALUES (%s, %s, %s, %s)",
                    (data_id, data_type, json.dumps(payload), 'PENDING')
                )
                logger.info(f"Datos almacenados en DB con ID: {data_id}")
                return data_id
        except Exception as e:
            logger.error(f"Error almacenando datos en DB: {e}")
            raise

    def generate_event_data(self) -> dict:
        """Genera datos de evento aleatorios"""
        event_types = ['order', 'user_registration', 'payment', 'product_update', 'inventory_change']
        event_type = self.fake.random_element(event_types)
        
        payloads = {
            'order': {
                'customer_id': self.fake.random_int(1000, 9999),
                'order_id': f"ORD-{self.fake.uuid4()[:8]}",
                'amount': round(self.fake.random.uniform(10.0, 1000.0), 2),
                'items': [f"item_{i}" for i in range(self.fake.random_int(1, 5))],
                'shipping_address': self.fake.address(),
                'timestamp': datetime.now().isoformat()
            },
            'user_registration': {
                'user_id': str(uuid.uuid4()),
                'email': self.fake.email(),
                'username': self.fake.user_name(),
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'country': self.fake.country(),
                'timestamp': datetime.now().isoformat()
            },
            'payment': {
                'transaction_id': f"TXN-{self.fake.uuid4()[:8]}",
                'amount': round(self.fake.random.uniform(5.0, 500.0), 2),
                'currency': self.fake.random_element(['USD', 'EUR', 'GBP']),
                'payment_method': self.fake.random_element(['credit_card', 'debit_card', 'paypal']),
                'merchant_id': self.fake.random_int(100, 999),
                'timestamp': datetime.now().isoformat()
            },
            'product_update': {
                'product_id': f"PROD-{self.fake.random_int(1000, 9999)}",
                'name': self.fake.catch_phrase(),
                'price': round(self.fake.random.uniform(10.0, 200.0), 2),
                'category': self.fake.random_element(['electronics', 'clothing', 'books', 'home']),
                'in_stock': self.fake.boolean(),
                'timestamp': datetime.now().isoformat()
            },
            'inventory_change': {
                'product_id': f"PROD-{self.fake.random_int(1000, 9999)}",
                'old_quantity': self.fake.random_int(0, 100),
                'new_quantity': self.fake.random_int(0, 100),
                'warehouse': f"WH-{self.fake.random_int(1, 5)}",
                'reason': self.fake.random_element(['sale', 'restock', 'damaged', 'returned']),
                'timestamp': datetime.now().isoformat()
            }
        }
        
        return event_type, payloads[event_type]

    def publish_event(self):
        """Genera y publica un evento"""
        try:
            # Generar datos del evento
            data_type, payload = self.generate_event_data()
            
            # Almacenar datos en PostgreSQL
            data_reference_id = self.store_data_in_db(data_type, payload)
            
            # Crear mensaje del evento (solo referencia, no el payload completo)
            event_message = {
                'event_id': str(uuid.uuid4()),
                'event_type': data_type,
                'data_reference_id': data_reference_id,
                'timestamp': datetime.now().isoformat(),
                'producer_id': 'producer-1',
                'metadata': {
                    'data_size_bytes': len(json.dumps(payload)),
                    'data_location': 'postgresql://postgres/eventstore'
                }
            }
            
            # Publicar en Kafka
            future = self.kafka_producer.send(
                topic=os.getenv('KAFKA_TOPIC', 'data-events'),
                key=data_reference_id,
                value=event_message
            )
            
            # Esperar confirmación
            record_metadata = future.get(timeout=10)
            
            logger.info({
                'action': 'event_published',
                'event_id': event_message['event_id'],
                'event_type': data_type,
                'data_reference_id': data_reference_id,
                'kafka_topic': record_metadata.topic,
                'kafka_partition': record_metadata.partition,
                'kafka_offset': record_metadata.offset
            })
            
        except KafkaError as e:
            logger.error(f"Error publicando evento en Kafka: {e}")
        except Exception as e:
            logger.error(f"Error general publicando evento: {e}")

    def run(self):
        """Ejecuta el productor de eventos"""
        interval = int(os.getenv('PRODUCER_INTERVAL', 5))
        logger.info(f"Iniciando productor de eventos (intervalo: {interval}s)")
        
        event_counter = 0
        try:
            while True:
                event_counter += 1
                logger.info(f"Generando evento #{event_counter}")
                
                # Simular ocasionales fallos de conectividad
                if event_counter % 20 == 0:
                    logger.warning("Simulando timeout/error de conexión")
                    time.sleep(2)
                
                self.publish_event()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Deteniendo productor...")
        finally:
            if self.kafka_producer:
                self.kafka_producer.close()
            if self.db_connection:
                self.db_connection.close()

if __name__ == "__main__":
    producer = EventProducer()
    producer.run()