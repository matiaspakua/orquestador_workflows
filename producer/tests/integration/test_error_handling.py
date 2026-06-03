"""User Story 2 — error handling and recovery (producer side).

Covers:
  T013  Kafka unavailable: producer retries 3 times with exponential backoff
        (1s, 2s) and does not crash (FR-004 / SC-002). [docker]
  T025  large payload: a message near the Kafka size limit is processed, and an
        oversize message fails with a clear error rather than crashing (EC-3).
"""

from __future__ import annotations

import json
import os
import time

import pytest

from tests_support import docker_control
from tests_support.contract import KAFKA_MESSAGE_MAX_BYTES
from tests_support.kafka_clients import MessageCollector
from tests_support.kafka_topics import TestTopicManager

pytestmark = pytest.mark.us2

KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "test-kafka")


def _wait_for_broker(bootstrap_servers: str, timeout_s: float = 90.0) -> None:
    """Block until the Kafka broker accepts admin connections again."""
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            mgr = TestTopicManager(bootstrap_servers, connect_retries=1, connect_backoff_s=0.5)
            mgr.close()
            return
        except Exception as err:  # broker still down
            last_err = err
            time.sleep(2.0)
    raise RuntimeError(f"Kafka did not recover within {timeout_s}s") from last_err


@pytest.mark.docker
@pytest.mark.skipif(not docker_control.docker_available(), reason="requires docker socket")
def test_producer_retries_when_kafka_unavailable(publisher, topic_manager, bootstrap_servers):
    """T013: with Kafka down, the producer makes 3 attempts with exponential backoff."""
    topic = topic_manager.create("messages", partitions=1)

    # Producer is already connected (fixture built while broker was up). Now down it.
    docker_control.stop_container(KAFKA_CONTAINER)
    try:
        result = publisher.publish(
            topic,
            scenario="error-handling",
            payload={"probe": "kafka-down"},
            sequence=1,
            send_timeout_s=4.0,
            max_attempts=3,
        )
        # Did not crash; exhausted the retry budget without delivery.
        assert result.delivered is False
        assert result.attempts == 3, f"expected 3 attempts, got {result.attempts}"
        # Exponential backoff between attempts: 1s then 2s.
        assert result.backoffs_s == [1.0, 2.0], f"unexpected backoff schedule: {result.backoffs_s}"
        assert result.error is not None
    finally:
        docker_control.start_container(KAFKA_CONTAINER)
        assert docker_control.wait_until_running(KAFKA_CONTAINER, timeout_s=60)
        _wait_for_broker(bootstrap_servers)


def test_large_payload_near_limit_is_processed(publisher, topic_manager, bootstrap_servers):
    """T025 (a): a payload just under the 1 MiB limit is delivered and consumed."""
    topic = topic_manager.create("messages", partitions=1)
    collector = MessageCollector(bootstrap_servers, topic, group_id="t025-large")
    try:
        # Leave headroom for the envelope/serialization overhead.
        big_blob = "x" * (KAFKA_MESSAGE_MAX_BYTES - 50_000)
        payload = {"blob": big_blob, "size": len(big_blob)}

        result = publisher.publish(topic, scenario="error-handling", payload=payload, sequence=1)
        assert result.delivered, f"near-limit payload should be accepted: {result.error}"

        received = collector.collect(expected=1, timeout_s=30.0)
        assert len(received.valid) == 1
        assert received.valid[0]["payload"]["size"] == len(big_blob)
    finally:
        collector.close()


def test_oversize_payload_fails_with_clear_error(publisher, topic_manager):
    """T025 (b): an oversize payload is rejected with a clear error, no crash (EC-3)."""
    topic = topic_manager.create("messages", partitions=1)
    oversize = "y" * (KAFKA_MESSAGE_MAX_BYTES + 200_000)
    payload = {"blob": oversize}

    result = publisher.publish(
        topic, scenario="error-handling", payload=payload, sequence=1, max_attempts=1
    )
    assert result.delivered is False, "oversize payload must not be delivered"
    assert result.error is not None
    # The error should clearly indicate the size problem.
    assert "size" in result.error.lower() or "large" in result.error.lower(), (
        f"error not clearly about size: {result.error}"
    )
    # Sanity: the payload really did exceed the contract limit.
    assert len(json.dumps(payload).encode()) > KAFKA_MESSAGE_MAX_BYTES
