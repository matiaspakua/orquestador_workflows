"""Kafka client harnesses for the integration suite.

* ``MessagePublisher`` — producer-side harness (T008). Publishes contract
  messages with a known payload and sequence number, and implements the
  contract retry policy (3 attempts, exponential backoff 1s/2s/4s) so the
  Kafka-unavailable test (T013) can assert the producer's behaviour.
* ``MessageCollector`` — consumer-side harness (T009). Subscribes to a topic and
  captures received messages, deduplicating by ``message_id`` (at-least-once
  delivery + idempotency) and routing malformed messages aside (FR-005).
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from .contract import build_message, is_valid_message

logger = logging.getLogger(__name__)

# Contract retry policy: 3 attempts with exponential backoff.
DEFAULT_BACKOFF_SCHEDULE_S = (1.0, 2.0, 4.0)


@dataclass
class PublishResult:
    """Outcome of a publish, including retry telemetry for assertions."""

    message: dict[str, Any]
    topic: str
    partition: int | None = None
    offset: int | None = None
    attempts: int = 1
    backoffs_s: list[float] = field(default_factory=list)
    delivered: bool = False
    error: str | None = None


class MessagePublisher:
    """Producer test harness — publishes contract messages to a test topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        secret: str | None = None,
        request_timeout_ms: int = 5_000,
        max_request_size: int = 1_048_576,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.secret = secret
        # retries=0 / acks='all': we drive retries explicitly so the test can
        # observe attempt count and backoff timing (SC-002).
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            acks="all",
            retries=0,
            max_block_ms=request_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            max_request_size=max_request_size,
        )

    def publish(
        self,
        topic: str,
        *,
        scenario: str,
        payload: dict[str, Any],
        sequence: int,
        workflow_id: str | None = None,
        message_id: str | None = None,
        send_timeout_s: float = 10.0,
        backoff_schedule_s: tuple[float, ...] = DEFAULT_BACKOFF_SCHEDULE_S,
        max_attempts: int = 3,
    ) -> PublishResult:
        """Publish one contract message, retrying on failure per the contract.

        The Kafka key is ``workflow_id`` so all messages for a workflow land on
        one partition (ordering guarantee). Returns a ``PublishResult`` carrying
        attempt count and the backoff delays that were applied.
        """
        message = build_message(
            scenario=scenario,
            payload=payload,
            sequence=sequence,
            workflow_id=workflow_id,
            message_id=message_id,
            secret=self.secret,
        )
        result = PublishResult(message=message, topic=topic)

        for attempt in range(1, max_attempts + 1):
            result.attempts = attempt
            try:
                future = self._producer.send(topic, key=message["workflow_id"], value=message)
                meta = future.get(timeout=send_timeout_s)
                result.partition = meta.partition
                result.offset = meta.offset
                result.delivered = True
                result.error = None
                return result
            except KafkaError as err:
                result.error = str(err)
                logger.warning("Publish attempt %d/%d failed: %s", attempt, max_attempts, err)
                if attempt < max_attempts:
                    backoff = backoff_schedule_s[min(attempt - 1, len(backoff_schedule_s) - 1)]
                    result.backoffs_s.append(backoff)
                    time.sleep(backoff)
        return result

    def publish_raw(self, topic: str, raw_value: bytes, *, key: str | None = None) -> None:
        """Send a raw byte payload (used to inject malformed messages, T014)."""
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: v,  # already bytes
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            acks="all",
        )
        try:
            producer.send(topic, key=key, value=raw_value).get(timeout=10)
            producer.flush()
        finally:
            producer.close()

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        try:
            self._producer.flush()
            self._producer.close()
        except Exception:  # pragma: no cover
            pass


@dataclass
class CollectResult:
    """Messages captured by ``MessageCollector``."""

    valid: list[dict[str, Any]] = field(default_factory=list)
    invalid: list[Any] = field(default_factory=list)
    duplicates: int = 0

    @property
    def sequences(self) -> list[int]:
        return [m["sequence"] for m in self.valid]

    @property
    def message_ids(self) -> list[str]:
        return [m["message_id"] for m in self.valid]


class MessageCollector:
    """Consumer test harness — subscribes and captures received messages.

    Deduplicates by ``message_id`` so at-least-once redelivery does not double
    count (idempotency, EC-2). Malformed values are logged and routed to
    ``invalid`` rather than crashing the loop (FR-005).
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        *,
        group_id: str | None = None,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
    ) -> None:
        self.topic = topic
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            # Raw deserialize: we decode/validate ourselves so malformed bytes
            # surface as ``invalid`` instead of raising during fetch.
            value_deserializer=lambda m: m,
            key_deserializer=lambda m: m.decode("utf-8") if m else None,
            consumer_timeout_ms=1_000,
        )

    def collect(
        self,
        *,
        expected: int | None = None,
        timeout_s: float = 30.0,
        poll_ms: int = 500,
    ) -> CollectResult:
        """Poll until ``expected`` valid messages arrive or ``timeout_s`` elapses."""
        result = CollectResult()
        seen_ids: set[str] = set()
        deadline = time.time() + timeout_s

        while time.time() < deadline:
            batch = self._consumer.poll(timeout_ms=poll_ms)
            for _tp, records in batch.items():
                for record in records:
                    self._ingest(record.value, result, seen_ids)
            if expected is not None and len(result.valid) >= expected:
                break
        return result

    def _ingest(self, raw: bytes, result: CollectResult, seen_ids: set[str]) -> None:
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            logger.warning("Skipping non-JSON message: %s", err)
            result.invalid.append(raw)
            return

        if not is_valid_message(value):
            logger.warning("Skipping malformed message: %r", value)
            result.invalid.append(value)
            return

        message_id = value["message_id"]
        if message_id in seen_ids:
            result.duplicates += 1
            logger.info("Duplicate message_id %s ignored (idempotency)", message_id)
            return

        seen_ids.add(message_id)
        result.valid.append(value)

    def commit(self) -> None:
        self._consumer.commit()

    def position(self) -> dict[Any, int]:
        """Return current offset per assigned partition (for offset-resume tests)."""
        self._consumer.poll(timeout_ms=0)
        return {tp: self._consumer.position(tp) for tp in self._consumer.assignment()}

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception:  # pragma: no cover
            pass
