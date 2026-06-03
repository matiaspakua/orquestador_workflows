"""Mock orchestrator implementing the orchestration-events interface.

Per the spec clarifications, orchestrator-coordination tests (US3) use a mock
that implements the SAME interface as the real orchestrator from
``specs/003-workflow-definition/`` (see contracts/orchestration-events.md) so
behaviour matches production without depending on the not-yet-built service.

The mock:
  * exposes ``start_workflow`` / ``cancel`` / ``wait_until_done`` / ``status``;
  * publishes the workflow's data messages (via ``MessagePublisher``) on a
    background thread so a cancellation can interrupt publishing mid-flight;
  * emits ``WorkflowStarted`` / ``WorkflowCompleted`` / ``WorkflowFailed``
    envelope events to an orchestration-events test topic, keyed by
    ``workflow_execution_id``.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from kafka import KafkaProducer

from .kafka_clients import MessagePublisher

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1


class WorkflowStatus(str, Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    COMPLETED = "Completed"
    CANCELLED = "Cancelled"
    FAILED = "Failed"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MockOrchestrator:
    """Coordinate a producer/consumer workflow and emit orchestration events."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        publisher: MessagePublisher,
        messages_topic: str,
        events_topic: str,
        workflow_id: str | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.publisher = publisher
        self.messages_topic = messages_topic
        self.events_topic = events_topic
        self.workflow_id = workflow_id or str(uuid.uuid4())
        self.workflow_definition_id = str(uuid.uuid4())
        self.workflow_execution_id: str | None = None

        self.status = WorkflowStatus.PENDING
        self.published_count = 0
        self.result: dict[str, Any] | None = None
        self.emitted_events: list[dict[str, Any]] = []

        self._cancel = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at: float | None = None
        self._event_producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            acks="all",
        )

    # -- event emission -----------------------------------------------------
    def _emit(self, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        envelope = {
            "event_type": event_type,
            "schema_version": SCHEMA_VERSION,
            "workflow_id": self.workflow_id,
            "workflow_execution_id": self.workflow_execution_id,
            "workflow_definition_id": self.workflow_definition_id,
            "step_id": None,
            "step_name": None,
            "timestamp": _now_iso(),
            "payload": payload,
        }
        self._event_producer.send(
            self.events_topic, key=self.workflow_execution_id, value=envelope
        ).get(timeout=10)
        self.emitted_events.append(envelope)
        logger.info("Orchestrator emitted %s for %s", event_type, self.workflow_execution_id)
        return envelope

    # -- lifecycle ----------------------------------------------------------
    def start_workflow(
        self,
        workflow_input: dict[str, Any],
        *,
        total_messages: int = 1,
        interval_s: float = 0.0,
    ) -> str:
        """Transition Pending -> Running, emit WorkflowStarted, begin publishing.

        Returns the ``workflow_execution_id``. Publishing runs on a background
        thread so ``cancel`` can stop it mid-stream.
        """
        if self.status != WorkflowStatus.PENDING:
            raise RuntimeError(f"cannot start workflow in state {self.status}")

        self.workflow_execution_id = str(uuid.uuid4())
        self.status = WorkflowStatus.RUNNING
        self._started_at = time.time()
        self._emit(
            "WorkflowStarted",
            {
                "input": workflow_input,
                "definition_name": "producer-consumer-test",
                "definition_version": "1.0.0",
            },
        )

        self._thread = threading.Thread(
            target=self._publish_loop,
            args=(total_messages, interval_s),
            daemon=True,
        )
        self._thread.start()
        return self.workflow_execution_id

    def _publish_loop(self, total_messages: int, interval_s: float) -> None:
        for seq in range(1, total_messages + 1):
            if self._cancel.is_set():
                logger.info("Publishing cancelled after %d message(s)", self.published_count)
                return
            self.publisher.publish(
                self.messages_topic,
                scenario="orchestrator-coordination",
                payload={"seq": seq, "workflow_execution_id": self.workflow_execution_id},
                sequence=seq,
                workflow_id=self.workflow_id,
            )
            self.published_count += 1
            if interval_s:
                # Sleep in small slices so cancellation is responsive.
                slept = 0.0
                while slept < interval_s and not self._cancel.is_set():
                    time.sleep(min(0.05, interval_s - slept))
                    slept += 0.05

    def cancel(self) -> None:
        """Cancel the running workflow: stop publishing, mark Cancelled."""
        if self.status != WorkflowStatus.RUNNING:
            return
        self._cancel.set()
        if self._thread:
            self._thread.join(timeout=10)
        self.status = WorkflowStatus.CANCELLED
        self._emit(
            "WorkflowFailed",
            {
                "error": "Workflow cancelled by orchestrator",
                "error_code": "CANCELLED",
                "failed_step_id": None,
                "failed_step_name": None,
                "total_duration_ms": self._duration_ms(),
                "steps_completed": self.published_count,
            },
        )

    def wait_until_published(self, timeout_s: float = 30.0) -> None:
        """Block until the background publish loop finishes."""
        if self._thread:
            self._thread.join(timeout=timeout_s)

    def complete(self, result: dict[str, Any], *, steps_total: int | None = None) -> dict[str, Any]:
        """Mark the workflow Completed and emit WorkflowCompleted with results."""
        if self.status == WorkflowStatus.CANCELLED:
            raise RuntimeError("cannot complete a cancelled workflow")
        self.wait_until_published()
        self.status = WorkflowStatus.COMPLETED
        self.result = result
        return self._emit(
            "WorkflowCompleted",
            {
                "result": result,
                "total_duration_ms": self._duration_ms(),
                "steps_completed": self.published_count,
                "steps_total": steps_total if steps_total is not None else self.published_count,
            },
        )

    def _duration_ms(self) -> int:
        if self._started_at is None:
            return 0
        return int((time.time() - self._started_at) * 1000)

    def close(self) -> None:
        self._cancel.set()
        if self._thread:
            self._thread.join(timeout=5)
        try:
            self._event_producer.flush()
            self._event_producer.close()
        except Exception:  # pragma: no cover
            pass
