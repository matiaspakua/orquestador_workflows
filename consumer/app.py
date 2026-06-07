import os
import json
import time
import logging
import psycopg2
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pythonjsonlogger.jsonlogger import JsonFormatter
from prometheus_client import (
    start_http_server, Counter, Gauge, Info,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

MESSAGES_CONSUMED = Counter("messages_consumed_total", "Total Kafka messages consumed", ["topic", "consumer_id"])
CONSUME_ERRORS = Counter("errors_total", "Total consume errors", ["type", "consumer_id"])
HEALTH_STATUS = Gauge("health_status", "1=healthy 0=unhealthy", ["component"])
COMPONENT_INFO = Info("component", "Static component metadata")
CONSUMER_LAG = Gauge("consumer_lag", "Consumer lag per topic/partition", ["topic", "partition", "consumer_id"])
MESSAGES_CONSUME_RATE = Gauge("messages_consume_rate", "Recent consume rate (msg/s)", ["topic", "consumer_id"])


def get_db():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                database=os.getenv("POSTGRES_DB", "eventdb"),
                user=os.getenv("POSTGRES_USER", "eventuser"),
                password=os.getenv("POSTGRES_PASSWORD", "eventpass"),
            )
            conn.autocommit = True
            return conn
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"DB retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(5)
            else:
                raise
    return None


def check_processed(db, event_id: str) -> bool:
    with db.cursor() as cursor:
        cursor.execute("SELECT 1 FROM processed_events WHERE event_id = %s", (event_id,))
        return cursor.fetchone() is not None


def mark_processed(db, event_id: str, consumer_id: str):
    with db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO processed_events (event_id, consumer_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (event_id, consumer_id),
        )


def log_to_dlq(db, event_id: str, worker: str, step_name: str, error: str, payload: dict):
    with db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO dead_letter_events (event_id, worker, step_name, error, payload) VALUES (%s, %s, %s, %s, %s)",
            (event_id, worker, step_name, error, json.dumps(payload)),
        )


class EventConsumer:
    def __init__(self):
        self.consumer_id = os.getenv("CONSUMER_ID", "consumer-default")
        self.kafka_consumer = None
        self.db = None
        self._consumed_in_window = 0
        self._window_start = time.monotonic()
        COMPONENT_INFO.info({"name": self.consumer_id, "version": "1.0.0"})
        HEALTH_STATUS.labels(component=self.consumer_id).set(0)

        for attempt in range(15):
            try:
                self.kafka_consumer = KafkaConsumer(
                    os.getenv("KAFKA_TOPIC", "data-events"),
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                    group_id=os.getenv("KAFKA_GROUP_ID", "data-processors"),
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    key_deserializer=lambda m: m.decode("utf-8") if m else None,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    consumer_timeout_ms=int(os.getenv("CONSUMER_TIMEOUT", "30000")),
                )
                logger.info(f"Kafka consumer {self.consumer_id} configured")
                break
            except Exception as e:
                if attempt < 14:
                    logger.warning(f"Kafka retry {attempt + 1}/15: {e}")
                    time.sleep(5)
                else:
                    raise

        self.db = get_db()
        HEALTH_STATUS.labels(component=self.consumer_id).set(1)

    def get_data_from_db(self, data_reference_id: str) -> dict | None:
        try:
            with self.db.cursor() as cursor:
                cursor.execute(
                    "SELECT id, data_type, payload, created_at, status FROM event_data WHERE id = %s",
                    (data_reference_id,),
                )
                result = cursor.fetchone()
                if result:
                    return {
                        "id": result[0],
                        "data_type": result[1],
                        "payload": json.loads(result[2]) if isinstance(result[2], str) else result[2],
                        "created_at": result[3].isoformat() if result[3] else None,
                        "status": result[4],
                    }
                return None
        except Exception as e:
            logger.error(f"DB fetch error: {e}")
            return None

    def update_data_status(self, data_reference_id: str, status: str):
        try:
            with self.db.cursor() as cursor:
                cursor.execute(
                    "UPDATE event_data SET status = %s, processed_at = %s, processed_by = %s WHERE id = %s",
                    (status, datetime.now(timezone.utc), self.consumer_id, data_reference_id),
                )
        except Exception as e:
            logger.error(f"DB update error: {e}")

    def log_event_processing(self, event_id: str, data_reference_id: str, status: str,
                             processing_time_ms: int, error_message: str = None):
        try:
            with self.db.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO event_logs (event_id, consumer_id, data_reference_id, status, processing_time_ms, error_message) VALUES (%s, %s, %s, %s, %s, %s)",
                    (event_id, self.consumer_id, data_reference_id, status, processing_time_ms, error_message),
                )
        except Exception as e:
            logger.error(f"DB log error: {e}")

    def process_event_data(self, event_type: str, payload: dict) -> bool:
        try:
            processing_rules = {
                "order": self.process_order,
                "user_registration": self.process_user_registration,
                "payment": self.process_payment,
                "product_update": self.process_product_update,
                "inventory_change": self.process_inventory_change,
            }
            handler = processing_rules.get(event_type)
            if handler:
                return handler(payload)
            logger.warning(f"Unknown event type: {event_type}")
            return False
        except Exception as e:
            logger.error(f"Processing error for {event_type}: {e}")
            return False

    def process_order(self, payload: dict) -> bool:
        logger.info(f"Processing order: {payload.get('order_id', 'N/A')}")
        if payload.get("amount", 0) > 0:
            time.sleep(0.5)
            if time.time() % 20 < 1:
                raise Exception("Simulated order processing error")
            return True
        return False

    def process_user_registration(self, payload: dict) -> bool:
        logger.info(f"Processing registration: {payload.get('username', 'N/A')}")
        time.sleep(0.3)
        return payload.get("email") is not None

    def process_payment(self, payload: dict) -> bool:
        logger.info(f"Processing payment: {payload.get('transaction_id', 'N/A')}")
        time.sleep(0.4)
        return payload.get("amount", 0) > 0

    def process_product_update(self, payload: dict) -> bool:
        logger.info(f"Processing product update: {payload.get('product_id', 'N/A')}")
        time.sleep(0.2)
        return payload.get("product_id") is not None

    def process_inventory_change(self, payload: dict) -> bool:
        logger.info(f"Processing inventory change: {payload.get('product_id', 'N/A')}")
        time.sleep(0.3)
        return payload.get("new_quantity", 0) >= 0

    def process_message(self, message):
        start_time = time.time()
        event_data = message.value

        event_id = event_data.get("event_id")
        event_type = event_data.get("event_type")
        data_reference_id = event_data.get("data_reference_id")

        logger.info({
            "action": "message_received",
            "consumer_id": self.consumer_id,
            "event_id": event_id,
            "event_type": event_type,
            "data_reference_id": data_reference_id,
            "kafka_partition": message.partition,
            "kafka_offset": message.offset,
        })

        if check_processed(self.db, event_id):
            logger.info(f"Duplicate event {event_id} skipped (idempotency)")
            return True

        try:
            data = self.get_data_from_db(data_reference_id)
            if not data:
                raise Exception(f"No data for reference: {data_reference_id}")

            success = self.process_event_data(event_type, data["payload"])

            if success:
                self.update_data_status(data_reference_id, "PROCESSED")
                status = "SUCCESS"
                error_message = None
            else:
                self.update_data_status(data_reference_id, "FAILED")
                status = "FAILED"
                error_message = "Processing failed"
        except Exception as e:
            CONSUME_ERRORS.labels(type="processing", consumer_id=self.consumer_id).inc()
            logger.error(f"Processing error: {e}")
            self.update_data_status(data_reference_id, "ERROR")
            status = "ERROR"
            error_message = str(e)

        processing_time_ms = int((time.time() - start_time) * 1000)
        self.log_event_processing(event_id, data_reference_id, status, processing_time_ms, error_message)
        mark_processed(self.db, event_id, self.consumer_id)

        logger.info({
            "action": "message_processed",
            "consumer_id": self.consumer_id,
            "event_id": event_id,
            "status": status,
            "processing_time_ms": processing_time_ms,
        })

        return status == "SUCCESS"

    def run(self):
        logger.info(f"Starting consumer: {self.consumer_id}")
        messages_processed = 0
        timeout_count = 0

        try:
            while True:
                try:
                    message_batch = self.kafka_consumer.poll(timeout_ms=10000)

                    if not message_batch:
                        timeout_count += 1
                        logger.info({
                            "action": "poll_timeout",
                            "consumer_id": self.consumer_id,
                            "timeout_count": timeout_count,
                            "messages_processed": messages_processed,
                        })
                        continue

                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            messages_processed += 1
                            topic = topic_partition.topic
                            partition = str(topic_partition.partition)
                            MESSAGES_CONSUMED.labels(topic=topic, consumer_id=self.consumer_id).inc()
                            self._consumed_in_window += 1
                            elapsed = time.monotonic() - self._window_start
                            if elapsed >= 10 or self._consumed_in_window >= 10:
                                rate = self._consumed_in_window / max(elapsed, 1)
                                MESSAGES_CONSUME_RATE.labels(topic=topic, consumer_id=self.consumer_id).set(rate)
                                self._consumed_in_window = 0
                                self._window_start = time.monotonic()

                            success = self.process_message(message)
                            if success:
                                self.kafka_consumer.commit_async()
                            else:
                                logger.warning(f"Message not processed: offset {message.offset}")

                except Exception as e:
                    logger.error(f"Consumer loop error: {e}")
                    time.sleep(5)

        except KeyboardInterrupt:
            logger.info(f"Stopping consumer {self.consumer_id}...")
        finally:
            if self.kafka_consumer:
                self.kafka_consumer.close()
            if self.db:
                self.db.close()


if __name__ == "__main__":
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics server started on port {metrics_port}")
    consumer = EventConsumer()
    consumer.run()
