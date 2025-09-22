import os
import json
import time
import logging
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pythonjsonlogger.json import JsonFormatter

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

class EventConsumer:
    def __init__(self):
        self.consumer_id = os.getenv('CONSUMER_ID', 'consumer-default')
        self.kafka_consumer = None
        self.db_connection = None
        self.setup_kafka()
        self.setup_database()
        
    def setup_kafka(self):
        """Configura la conexión a Kafka"""
        try:
            self.kafka_consumer = KafkaConsumer(
                os.getenv('KAFKA_TOPIC', 'data-events'),
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
                group_id=os.getenv('KAFKA_GROUP_ID', 'data-processors'),
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=int(os.getenv('CONSUMER_TIMEOUT', 30000))
            )
            logger.info(f"Kafka consumer {self.consumer_id} configurado correctamente")
        except Exception as e:
            logger.error(f"Error configurando Kafka consumer: {e}")
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
                logger.info(f"Conexión a PostgreSQL establecida para {self.consumer_id}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Intento {attempt + 1} de conexión a PostgreSQL falló, reintentando...")
                    time.sleep(5)
                else:
                    logger.error(f"Error conectando a PostgreSQL después de {max_retries} intentos: {e}")
                    raise

    def get_data_from_db(self, data_reference_id: str) -> dict:
        """Recupera los datos reales desde PostgreSQL usando la referencia"""
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, data_type, payload, created_at, status FROM event_data WHERE id = %s",
                    (data_reference_id,)
                )
                result = cursor.fetchone()
                
                if result:
                    return {
                        'id': result[0],
                        'data_type': result[1],
                        'payload': json.loads(result[2]),
                        'created_at': result[3],
                        'status': result[4]
                    }
                else:
                    logger.warning(f"No se encontraron datos para la referencia: {data_reference_id}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error obteniendo datos de DB: {e}")
            return None

    def update_data_status(self, data_reference_id: str, status: str):
        """Actualiza el estado de los datos en PostgreSQL"""
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE event_data SET status = %s, processed_at = %s, processed_by = %s WHERE id = %s",
                    (status, datetime.now(), self.consumer_id, data_reference_id)
                )
        except Exception as e:
            logger.error(f"Error actualizando estado en DB: {e}")

    def log_event_processing(self, event_id: str, data_reference_id: str, status: str, 
                           processing_time_ms: int, error_message: str = None):
        """Registra el procesamiento del evento en los logs"""
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO event_logs (event_id, consumer_id, data_reference_id, 
                       status, processing_time_ms, error_message) 
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (event_id, self.consumer_id, data_reference_id, status, 
                     processing_time_ms, error_message)
                )
        except Exception as e:
            logger.error(f"Error registrando log de evento: {e}")

    def process_event_data(self, event_type: str, payload: dict) -> bool:
        """Procesa los datos del evento según su tipo"""
        try:
            # Simulación de procesamiento específico por tipo de evento
            processing_rules = {
                'order': self.process_order,
                'user_registration': self.process_user_registration,
                'payment': self.process_payment,
                'product_update': self.process_product_update,
                'inventory_change': self.process_inventory_change
            }
            
            if event_type in processing_rules:
                return processing_rules[event_type](payload)
            else:
                logger.warning(f"Tipo de evento no reconocido: {event_type}")
                return False
                
        except Exception as e:
            logger.error(f"Error procesando evento tipo {event_type}: {e}")
            return False

    def process_order(self, payload: dict) -> bool:
        """Procesa eventos de orden"""
        logger.info(f"Procesando orden: {payload.get('order_id', 'N/A')}")
        
        # Simulación de validaciones y procesamiento
        if payload.get('amount', 0) > 0:
            # Simular tiempo de procesamiento
            time.sleep(0.5)
            
            # Simular ocasionales errores de procesamiento (5%)
            if time.time() % 20 < 1:
                raise Exception("Error simulado en procesamiento de orden")
                
            return True
        return False

    def process_user_registration(self, payload: dict) -> bool:
        """Procesa eventos de registro de usuario"""
        logger.info(f"Procesando registro de usuario: {payload.get('username', 'N/A')}")
        time.sleep(0.3)
        return payload.get('email') is not None

    def process_payment(self, payload: dict) -> bool:
        """Procesa eventos de pago"""
        logger.info(f"Procesando pago: {payload.get('transaction_id', 'N/A')}")
        time.sleep(0.4)
        return payload.get('amount', 0) > 0

    def process_product_update(self, payload: dict) -> bool:
        """Procesa eventos de actualización de producto"""
        logger.info(f"Procesando actualización de producto: {payload.get('product_id', 'N/A')}")
        time.sleep(0.2)
        return payload.get('product_id') is not None

    def process_inventory_change(self, payload: dict) -> bool:
        """Procesa eventos de cambio de inventario"""
        logger.info(f"Procesando cambio de inventario: {payload.get('product_id', 'N/A')}")
        time.sleep(0.3)
        return payload.get('new_quantity', 0) >= 0

    def process_message(self, message):
        """Procesa un mensaje de Kafka"""
        start_time = time.time()
        event_data = message.value
        
        event_id = event_data.get('event_id')
        event_type = event_data.get('event_type')
        data_reference_id = event_data.get('data_reference_id')
        
        logger.info({
            'action': 'message_received',
            'consumer_id': self.consumer_id,
            'event_id': event_id,
            'event_type': event_type,
            'data_reference_id': data_reference_id,
            'kafka_partition': message.partition,
            'kafka_offset': message.offset
        })
        
        try:
            # Obtener los datos reales desde PostgreSQL
            data = self.get_data_from_db(data_reference_id)
            
            if not data:
                raise Exception(f"No se pudieron obtener datos para la referencia: {data_reference_id}")
            
            # Procesar los datos
            success = self.process_event_data(event_type, data['payload'])
            
            if success:
                self.update_data_status(data_reference_id, 'PROCESSED')
                status = 'SUCCESS'
                error_message = None
            else:
                self.update_data_status(data_reference_id, 'FAILED')
                status = 'FAILED'
                error_message = "Procesamiento falló"
                
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            self.update_data_status(data_reference_id, 'ERROR')
            status = 'ERROR'
            error_message = str(e)
        
        # Calcular tiempo de procesamiento y registrar log
        processing_time_ms = int((time.time() - start_time) * 1000)
        self.log_event_processing(event_id, data_reference_id, status, processing_time_ms, error_message)
        
        logger.info({
            'action': 'message_processed',
            'consumer_id': self.consumer_id,
            'event_id': event_id,
            'status': status,
            'processing_time_ms': processing_time_ms
        })
        
        return status == 'SUCCESS'

    def run(self):
        """Ejecuta el consumidor de eventos"""
        logger.info(f"Iniciando consumidor de eventos: {self.consumer_id}")
        
        messages_processed = 0
        timeout_count = 0
        
        try:
            while True:
                try:
                    # Poll por mensajes con timeout
                    message_batch = self.kafka_consumer.poll(timeout_ms=10000)
                    
                    if not message_batch:
                        timeout_count += 1
                        logger.info({
                            'action': 'poll_timeout',
                            'consumer_id': self.consumer_id,
                            'timeout_count': timeout_count,
                            'messages_processed': messages_processed
                        })
                        continue
                    
                    # Procesar mensajes
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            messages_processed += 1
                            success = self.process_message(message)
                            
                            if success:
                                # Confirmar procesamiento exitoso
                                self.kafka_consumer.commit_async()
                            else:
                                logger.warning(f"Mensaje no procesado correctamente: {message.offset}")
                            
                except Exception as e:
                    logger.error(f"Error en el loop principal del consumidor: {e}")
                    time.sleep(5)
                    
        except KeyboardInterrupt:
            logger.info(f"Deteniendo consumidor {self.consumer_id}...")
        finally:
            if self.kafka_consumer:
                self.kafka_consumer.close()
            if self.db_connection:
                self.db_connection.close()

if __name__ == "__main__":
    consumer = EventConsumer()
    consumer.run()