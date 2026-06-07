"""
Workflow Orchestrator — coordinates 4-service Order Processing workflow.

Lifecycle:
  1. Every WORKFLOW_INTERVAL seconds, create a new workflow execution in DB.
  2. Execute steps sequentially: publish task → wait for result → advance.
  3. Write step-level progress to workflow_steps (UI reads this in real time).
  4. Publish orchestration events to Kafka for traceability.
"""
import os
import json
import time
import uuid
import logging
import threading
import random
import psycopg2
import psycopg2.extras
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from pythonjsonlogger.jsonlogger import JsonFormatter
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger('orchestrator')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# ── Prometheus metrics ────────────────────────────────────────────────────────
WORKFLOWS_STARTED   = Counter('orchestrator_workflows_started_total',   'Workflows started')
WORKFLOWS_COMPLETED = Counter('orchestrator_workflows_completed_total', 'Workflows completed', ['status'])
STEPS_EXECUTED      = Counter('orchestrator_steps_executed_total',      'Steps executed',      ['step_name', 'status'])
ACTIVE_WORKFLOWS    = Gauge(  'orchestrator_active_workflows',          'Currently running workflow executions')
WORKFLOW_DURATION   = Histogram('orchestrator_workflow_duration_seconds','Workflow duration',
                                buckets=[5, 10, 20, 30, 60, 120])
HEALTH_STATUS       = Gauge(  'health_status', '1=healthy 0=unhealthy', ['component'])

# ── Workflow definition ───────────────────────────────────────────────────────
WORKFLOW_DEF = {
    "name": "order-processing",
    "version": "1.0.0",
    "description": "4-step order pipeline: validate → fraud-check → inventory → notify",
    "steps": [
        {"id": "step-validate-order",    "name": "Validate Order",       "step_type": "Task",
         "sequence_order": 1, "topic": "order-validation",  "timeout_seconds": 30},
        {"id": "step-check-fraud",       "name": "Check Fraud",          "step_type": "Task",
         "sequence_order": 2, "topic": "fraud-check",       "timeout_seconds": 30},
        {"id": "step-check-inventory",   "name": "Check Inventory",      "step_type": "Task",
         "sequence_order": 3, "topic": "inventory-check",   "timeout_seconds": 30},
        {"id": "step-send-notification", "name": "Send Notification",    "step_type": "Task",
         "sequence_order": 4, "topic": "notification-send", "timeout_seconds": 15},
    ]
}


def get_db():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        database=os.getenv('POSTGRES_DB', 'eventdb'),
        user=os.getenv('POSTGRES_USER', 'eventuser'),
        password=os.getenv('POSTGRES_PASSWORD', 'eventpass'),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


class Orchestrator:
    def __init__(self):
        self._lock = threading.Lock()
        self._pending_results: dict[str, threading.Event] = {}
        self._results: dict[str, dict] = {}
        self.db = None
        self.producer = None
        self.results_consumer = None
        HEALTH_STATUS.labels(component='orchestrator').set(0)
        self._setup()
        HEALTH_STATUS.labels(component='orchestrator').set(1)

    def _setup(self):
        # DB with retry
        for attempt in range(15):
            try:
                self.db = get_db()
                self.db.autocommit = True
                logger.info("DB connected")
                break
            except Exception as e:
                if attempt < 14:
                    logger.warning(f"DB retry {attempt+1}/15: {e}")
                    time.sleep(5)
                else:
                    raise

        broker = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        # Kafka producer
        for attempt in range(15):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=broker,
                    value_serializer=lambda v: json.dumps(v, default=str).encode(),
                    key_serializer=lambda k: k.encode() if k else None,
                    retries=5, acks='all',
                )
                logger.info("Kafka producer ready")
                break
            except Exception as e:
                if attempt < 14:
                    logger.warning(f"Kafka retry {attempt+1}/15: {e}")
                    time.sleep(5)
                else:
                    raise

        # Results consumer
        self.results_consumer = KafkaConsumer(
            'orchestration-results',
            bootstrap_servers=broker,
            group_id='orchestrator-results',
            value_deserializer=lambda m: json.loads(m.decode()),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            consumer_timeout_ms=500,
        )

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _create_execution(self, execution_id: str, order_data: dict) -> None:
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO workflow_executions (id, name, status, started_at)
                VALUES (%s, %s, 'Running', NOW())
                """,
                (execution_id, f"order-processing / {order_data['order_id']}"),
            )
            for step in WORKFLOW_DEF['steps']:
                cur.execute(
                    """
                    INSERT INTO workflow_steps
                      (id, workflow_execution_id, name, step_type, status, sequence_order)
                    VALUES (%s, %s, %s, %s, 'Pending', %s)
                    """,
                    (str(uuid.uuid4()), execution_id,
                     step['name'], step['step_type'], step['sequence_order']),
                )

    def _start_step(self, execution_id: str, step_name: str) -> None:
        with self.db.cursor() as cur:
            cur.execute(
                """
                UPDATE workflow_steps
                SET status = 'Running', started_at = NOW()
                WHERE workflow_execution_id = %s AND name = %s
                """,
                (execution_id, step_name),
            )

    def _finish_step(self, execution_id: str, step_name: str,
                     success: bool, error_msg: str | None = None) -> None:
        status = 'Completed' if success else 'Failed'
        with self.db.cursor() as cur:
            cur.execute(
                """
                UPDATE workflow_steps
                SET status = %s, completed_at = NOW(), error_message = %s
                WHERE workflow_execution_id = %s AND name = %s
                """,
                (status, error_msg, execution_id, step_name),
            )

    def _finish_execution(self, execution_id: str, success: bool) -> None:
        status = 'Completed' if success else 'Failed'
        with self.db.cursor() as cur:
            cur.execute(
                """
                UPDATE workflow_executions
                SET status = %s, completed_at = NOW()
                WHERE id = %s
                """,
                (status, execution_id),
            )

    # ── Kafka helpers ─────────────────────────────────────────────────────────

    def _publish_event(self, event_type: str, execution_id: str,
                       step: dict | None = None, payload: dict | None = None) -> None:
        msg = {
            "event_type": event_type,
            "schema_version": 1,
            "workflow_execution_id": execution_id,
            "step_id": step['id'] if step else None,
            "step_name": step['name'] if step else None,
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "payload": payload or {},
        }
        self.producer.send('orchestration-events', key=execution_id, value=msg)

    def _dispatch_step(self, execution_id: str, step: dict, order_data: dict) -> None:
        task = {
            "workflow_execution_id": execution_id,
            "step_id": step['id'],
            "step_name": step['name'],
            "order_data": order_data,
        }
        self.producer.send(step['topic'], key=execution_id, value=task)
        self.producer.flush()

    def _wait_for_result(self, execution_id: str, step: dict,
                         timeout: int = 60) -> dict:
        """Block until the results consumer delivers the result for this step."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                batch = self.results_consumer.poll(timeout_ms=500)
                for _, msgs in batch.items():
                    for msg in msgs:
                        r = msg.value
                        if (r.get('workflow_execution_id') == execution_id
                                and r.get('step_id') == step['id']):
                            return r
            except Exception as e:
                logger.warning(f"Results poll error: {e}")
        return {"success": False, "error": "Timeout waiting for worker result"}

    # ── Workflow execution ────────────────────────────────────────────────────

    def run_workflow(self, order_data: dict) -> None:
        execution_id = str(uuid.uuid4())
        started = time.monotonic()
        ACTIVE_WORKFLOWS.inc()
        WORKFLOWS_STARTED.inc()
        logger.info({"action": "workflow_started", "execution_id": execution_id,
                     "order_id": order_data['order_id']})

        try:
            self._create_execution(execution_id, order_data)
            self._publish_event('WorkflowStarted', execution_id,
                                payload={"input": order_data,
                                         "definition_name": WORKFLOW_DEF['name'],
                                         "definition_version": WORKFLOW_DEF['version']})
            overall_success = True
            for step in WORKFLOW_DEF['steps']:
                self._start_step(execution_id, step['name'])
                self._publish_event('StepStarted', execution_id, step=step)
                self._dispatch_step(execution_id, step, order_data)

                result = self._wait_for_result(execution_id, step, step['timeout_seconds'])
                success = result.get('success', False)
                error = result.get('error') if not success else None

                self._finish_step(execution_id, step['name'], success, error)
                status_label = 'Completed' if success else 'Failed'
                self._publish_event('StepCompleted' if success else 'StepFailed',
                                    execution_id, step=step,
                                    payload={"step_id": step['id'],
                                             "error_message": error,
                                             "will_retry": False})
                STEPS_EXECUTED.labels(step_name=step['name'], status=status_label).inc()

                if not success:
                    # Mark remaining steps Skipped
                    remaining = WORKFLOW_DEF['steps'][step['sequence_order']:]
                    for rem in remaining:
                        self._finish_step(execution_id, rem['name'], False, 'Skipped — previous step failed')
                        STEPS_EXECUTED.labels(step_name=rem['name'], status='Skipped').inc()
                    overall_success = False
                    break

            duration = time.monotonic() - started
            self._finish_execution(execution_id, overall_success)
            final_event = 'WorkflowCompleted' if overall_success else 'WorkflowFailed'
            self._publish_event(final_event, execution_id,
                                payload={"total_duration_ms": int(duration * 1000)})
            WORKFLOW_DURATION.observe(duration)
            WORKFLOWS_COMPLETED.labels(status='Completed' if overall_success else 'Failed').inc()
            logger.info({"action": "workflow_finished", "execution_id": execution_id,
                         "status": "Completed" if overall_success else "Failed",
                         "duration_s": round(duration, 2)})
        except Exception as e:
            logger.error({"action": "workflow_error", "execution_id": execution_id, "error": str(e)})
            self._finish_execution(execution_id, False)
            WORKFLOWS_COMPLETED.labels(status='Failed').inc()
        finally:
            ACTIVE_WORKFLOWS.dec()

    def run(self) -> None:
        interval = int(os.getenv('WORKFLOW_INTERVAL', 60))
        logger.info(f"Orchestrator started — new workflow every {interval}s")

        order_counter = 0
        while True:
            order_counter += 1
            amount = round(random.uniform(50, 8000), 2)
            order_data = {
                "order_id": f"ORD-{order_counter:05d}",
                "customer_id": f"CUST-{random.randint(1000, 9999)}",
                "amount": amount,
                "items": random.randint(1, 10),
                "currency": "USD",
            }
            # Run in a thread so the main loop can keep scheduling
            t = threading.Thread(target=self.run_workflow, args=(order_data,), daemon=True)
            t.start()
            time.sleep(interval)


if __name__ == '__main__':
    start_http_server(int(os.getenv('METRICS_PORT', 8000)))
    orch = Orchestrator()
    orch.run()
