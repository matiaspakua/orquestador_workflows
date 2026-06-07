import os
import json
import time
import uuid
import logging
import random
import psycopg2
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pythonjsonlogger.json import JsonFormatter
from prometheus_client import (
    start_http_server, Counter, Gauge, Histogram, Info,
)

MESSAGES_PUBLISHED = Counter("messages_published_total", "Total Kafka messages published", ["topic"])
PUBLISH_ERRORS = Counter("errors_total", "Total publish errors", ["type"])
HEALTH_STATUS = Gauge("health_status", "1=healthy 0=unhealthy", ["component"])
COMPONENT_INFO = Info("component", "Static component metadata")
WORKFLOW_EXECUTIONS = Counter("workflow_executions_total", "Workflow executions by name and status", ["workflow_name", "status"])
WORKFLOW_RUNNING = Gauge("workflow_running", "Currently running workflow executions")
WORKFLOW_DURATION = Histogram("workflow_duration_seconds", "Workflow execution duration in seconds",
                              buckets=[1, 5, 10, 30, 60, 120, 300, 600])
MESSAGES_PUBLISH_RATE = Gauge("messages_publish_rate", "Recent publish rate (msg/s)", ["topic"])

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)


def get_db():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "eventdb"),
        user=os.getenv("POSTGRES_USER", "eventuser"),
        password=os.getenv("POSTGRES_PASSWORD", "eventpass"),
    )
    conn.autocommit = True
    return conn


def wait_for_db(max_retries=15, delay=3):
    for attempt in range(max_retries):
        try:
            conn = get_db()
            conn.close()
            logger.info("Database connection established")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"DB retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(delay)
            else:
                logger.error(f"DB unavailable after {max_retries} attempts: {e}")
                raise
    return False


class EventProducer:
    def __init__(self):
        self.fake = Faker()
        self.kafka_producer = None
        self.db = None
        self._published_in_window = 0
        self._window_start = time.monotonic()
        COMPONENT_INFO.info({"name": "producer", "version": "1.0.0"})
        HEALTH_STATUS.labels(component="producer").set(0)
        wait_for_db()
        self.db = get_db()
        self.kafka_producer = self._create_kafka_producer()
        HEALTH_STATUS.labels(component="producer").set(1)

    def _create_kafka_producer(self):
        for attempt in range(15):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    key_serializer=lambda v: v.encode("utf-8") if v else None,
                    retries=5,
                    retry_backoff_ms=1000,
                    acks="all",
                )
                logger.info("Kafka producer configured")
                return producer
            except Exception as e:
                if attempt < 14:
                    logger.warning(f"Kafka retry {attempt + 1}/15: {e}")
                    time.sleep(5)
                else:
                    raise

    def store_data_in_db(self, data_type: str, payload: dict) -> str:
        data_id = str(uuid.uuid4())
        with self.db.cursor() as cursor:
            cursor.execute(
                "INSERT INTO event_data (id, data_type, payload, status) VALUES (%s, %s, %s, %s)",
                (data_id, data_type, json.dumps(payload), "PENDING"),
            )
        return data_id

    def generate_event_data(self):
        event_types = ["order", "user_registration", "payment", "product_update", "inventory_change"]
        event_type = self.fake.random_element(event_types)

        payloads = {
            "order": {
                "customer_id": self.fake.random_int(1000, 9999),
                "order_id": f"ORD-{self.fake.uuid4()[:8]}",
                "amount": round(self.fake.random.uniform(10.0, 1000.0), 2),
                "items": [f"item_{i}" for i in range(self.fake.random_int(1, 5))],
                "shipping_address": self.fake.address(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "user_registration": {
                "user_id": str(uuid.uuid4()),
                "email": self.fake.email(),
                "username": self.fake.user_name(),
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "country": self.fake.country(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "payment": {
                "transaction_id": f"TXN-{self.fake.uuid4()[:8]}",
                "amount": round(self.fake.random.uniform(5.0, 500.0), 2),
                "currency": self.fake.random_element(["USD", "EUR", "GBP"]),
                "payment_method": self.fake.random_element(["credit_card", "debit_card", "paypal"]),
                "merchant_id": self.fake.random_int(100, 999),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "product_update": {
                "product_id": f"PROD-{self.fake.random_int(1000, 9999)}",
                "name": self.fake.catch_phrase(),
                "price": round(self.fake.random.uniform(10.0, 200.0), 2),
                "category": self.fake.random_element(["electronics", "clothing", "books", "home"]),
                "in_stock": self.fake.boolean(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "inventory_change": {
                "product_id": f"PROD-{self.fake.random_int(1000, 9999)}",
                "old_quantity": self.fake.random_int(0, 100),
                "new_quantity": self.fake.random_int(0, 100),
                "warehouse": f"WH-{self.fake.random_int(1, 5)}",
                "reason": self.fake.random_element(["sale", "restock", "damaged", "returned"]),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }

        return event_type, payloads[event_type]

    def publish_event(self):
        try:
            data_type, payload = self.generate_event_data()
            data_reference_id = self.store_data_in_db(data_type, payload)

            event_message = {
                "event_id": str(uuid.uuid4()),
                "event_type": data_type,
                "data_reference_id": data_reference_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "producer_id": "producer-1",
                "metadata": {
                    "data_size_bytes": len(json.dumps(payload)),
                    "data_location": "postgresql://postgres/eventdb",
                },
            }

            future = self.kafka_producer.send(
                topic=os.getenv("KAFKA_TOPIC", "data-events"),
                key=data_reference_id,
                value=event_message,
            )

            record_metadata = future.get(timeout=10)
            topic = record_metadata.topic
            MESSAGES_PUBLISHED.labels(topic=topic).inc()
            self._published_in_window += 1

            elapsed = time.monotonic() - self._window_start
            if elapsed >= 10 or self._published_in_window >= 10:
                rate = self._published_in_window / max(elapsed, 1)
                MESSAGES_PUBLISH_RATE.labels(topic=topic).set(rate)
                self._published_in_window = 0
                self._window_start = time.monotonic()

            logger.info({
                "action": "event_published",
                "event_id": event_message["event_id"],
                "event_type": data_type,
                "data_reference_id": data_reference_id,
                "kafka_topic": topic,
                "kafka_partition": record_metadata.partition,
                "kafka_offset": record_metadata.offset,
            })

        except KafkaError as e:
            PUBLISH_ERRORS.labels(type="kafka").inc()
            HEALTH_STATUS.labels(component="producer").set(0)
            logger.error(f"Kafka publish error: {e}")
        except Exception as e:
            PUBLISH_ERRORS.labels(type="general").inc()
            logger.error(f"General publish error: {e}")

    def run(self):
        interval = int(os.getenv("PRODUCER_INTERVAL", "5"))
        logger.info(f"Producer started (interval: {interval}s)")

        event_counter = 0
        try:
            while True:
                event_counter += 1
                if event_counter % 20 == 0:
                    logger.warning("Simulating connection delay")
                    time.sleep(2)
                self.publish_event()
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Stopping producer...")
        finally:
            if self.kafka_producer:
                self.kafka_producer.close()
            if self.db:
                self.db.close()


if __name__ == "__main__":
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics server started on port {metrics_port}")
    producer = EventProducer()
    producer.run()
