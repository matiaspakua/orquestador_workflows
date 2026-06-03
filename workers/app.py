"""
4-service workers for the Order Processing workflow:
  - OrderValidator    : validates order fields and amount limits
  - FraudChecker      : detects suspicious orders (amount > $5,000)
  - InventoryChecker  : confirms items are in stock (10% chance of stockout)
  - NotificationSender: sends order confirmation (always succeeds)

Each worker runs in its own thread, consuming from its dedicated Kafka topic.
Results are published to `orchestration-results` for the orchestrator to pick up.
"""
import os
import json
import time
import logging
import threading
import random
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from pythonjsonlogger.json import JsonFormatter
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger('workers')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# ── Metrics ───────────────────────────────────────────────────────────────────
TASKS_PROCESSED  = Counter('worker_tasks_processed_total',    'Tasks processed', ['worker', 'status'])
PROCESSING_TIME  = Histogram('worker_processing_seconds',     'Task duration',   ['worker'],
                              buckets=[0.1, 0.5, 1, 2, 5, 10])
HEALTH_STATUS    = Gauge(    'health_status', '1=healthy 0=unhealthy', ['component'])

BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')


def make_results_producer() -> KafkaProducer:
    for attempt in range(15):
        try:
            return KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
                key_serializer=lambda k: k.encode() if k else None,
                retries=5, acks='all',
            )
        except Exception as e:
            if attempt < 14:
                logger.warning(f"Producer retry {attempt+1}/15: {e}")
                time.sleep(5)
            else:
                raise


def make_consumer(topic: str, group_id: str) -> KafkaConsumer:
    for attempt in range(15):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=BROKER,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode()),
                auto_offset_reset='latest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
            )
        except Exception as e:
            if attempt < 14:
                logger.warning(f"Consumer({topic}) retry {attempt+1}/15: {e}")
                time.sleep(5)
            else:
                raise


def publish_result(producer: KafkaProducer, task: dict,
                   success: bool, detail: str | None = None) -> None:
    result = {
        "workflow_execution_id": task.get("workflow_execution_id"),
        "step_id":               task.get("step_id"),
        "step_name":             task.get("step_name"),
        "success":               success,
        "error":                 detail if not success else None,
        "detail":                detail if success else None,
    }
    producer.send('orchestration-results',
                  key=task.get("workflow_execution_id"), value=result)
    producer.flush()


# ── Worker functions ──────────────────────────────────────────────────────────

def order_validator(task: dict) -> tuple[bool, str]:
    """Validates order fields and rejects zero-amount orders."""
    time.sleep(random.uniform(0.5, 1.5))
    order = task.get("order_data", {})
    if not order.get("order_id"):
        return False, "Missing order_id"
    if order.get("amount", 0) <= 0:
        return False, "Order amount must be positive"
    if order.get("items", 0) < 1:
        return False, "Order must contain at least 1 item"
    return True, f"Order {order['order_id']} validated OK"


def fraud_checker(task: dict) -> tuple[bool, str]:
    """Flags orders over $5,000 as high-risk (requires manual review)."""
    time.sleep(random.uniform(1.0, 3.0))
    order = task.get("order_data", {})
    amount = order.get("amount", 0)
    if amount > 5000:
        return False, f"High-risk order: amount ${amount:.2f} exceeds fraud threshold $5,000"
    return True, f"Fraud check passed for ${amount:.2f}"


def inventory_checker(task: dict) -> tuple[bool, str]:
    """Checks inventory availability — 10% chance of stockout."""
    time.sleep(random.uniform(0.5, 2.0))
    order = task.get("order_data", {})
    if random.random() < 0.10:
        return False, f"Stockout: {order.get('items', 1)} items not available"
    return True, f"{order.get('items', 1)} items confirmed in stock"


def notification_sender(task: dict) -> tuple[bool, str]:
    """Sends order confirmation — always succeeds."""
    time.sleep(random.uniform(0.2, 0.8))
    order = task.get("order_data", {})
    customer = order.get("customer_id", "unknown")
    return True, f"Confirmation sent to {customer} for order {order.get('order_id')}"


# ── Worker loop ───────────────────────────────────────────────────────────────

WORKERS = [
    ("order-validator",     "order-validation",  "workers-order-validation",  order_validator),
    ("fraud-checker",       "fraud-check",        "workers-fraud-check",       fraud_checker),
    ("inventory-checker",   "inventory-check",    "workers-inventory-check",   inventory_checker),
    ("notification-sender", "notification-send",  "workers-notification-send", notification_sender),
]


def worker_loop(worker_name: str, topic: str,
                group_id: str, handler_fn) -> None:
    HEALTH_STATUS.labels(component=worker_name).set(0)
    consumer = make_consumer(topic, group_id)
    producer = make_results_producer()
    HEALTH_STATUS.labels(component=worker_name).set(1)
    logger.info({"action": "worker_started", "worker": worker_name, "topic": topic})

    while True:
        try:
            batch = consumer.poll(timeout_ms=1000)
            for _, msgs in batch.items():
                for msg in msgs:
                    task = msg.value
                    start = time.monotonic()
                    logger.info({"action": "task_received", "worker": worker_name,
                                 "execution_id": task.get("workflow_execution_id"),
                                 "step": task.get("step_name")})
                    try:
                        success, detail = handler_fn(task)
                    except Exception as e:
                        success, detail = False, str(e)

                    elapsed = time.monotonic() - start
                    PROCESSING_TIME.labels(worker=worker_name).observe(elapsed)
                    status_label = 'success' if success else 'failure'
                    TASKS_PROCESSED.labels(worker=worker_name, status=status_label).inc()

                    publish_result(producer, task, success, detail)
                    consumer.commit()

                    logger.info({"action": "task_completed", "worker": worker_name,
                                 "success": success, "detail": detail,
                                 "duration_ms": int(elapsed * 1000)})

                    # Route failures to DLQ
                    if not success:
                        dlq_msg = {**task, "error": detail, "worker": worker_name}
                        producer.send('orchestration-dlq', key=task.get("workflow_execution_id"),
                                      value=json.dumps(dlq_msg, default=str).encode())
                        producer.flush()

        except Exception as e:
            HEALTH_STATUS.labels(component=worker_name).set(0)
            logger.error({"action": "worker_error", "worker": worker_name, "error": str(e)})
            time.sleep(5)
            HEALTH_STATUS.labels(component=worker_name).set(1)


if __name__ == '__main__':
    start_http_server(int(os.getenv('METRICS_PORT', 8000)))
    threads = []
    for name, topic, group, fn in WORKERS:
        t = threading.Thread(target=worker_loop, args=(name, topic, group, fn), daemon=True)
        t.start()
        threads.append(t)
        logger.info(f"Started worker: {name}")

    for t in threads:
        t.join()
