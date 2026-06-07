"""User Story 2 — error handling and recovery (consumer side).

Covers:
  T014  invalid message: malformed messages are logged/skipped and the consumer
        continues processing valid messages (FR-005).
  T015  network interruption: after a consumer restart, processing resumes from
        the last committed offset with no loss and no duplication (FR-006).
  T024  duplicate message: the same message_id delivered twice is processed once
        (EC-2, idempotency).
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests_support.kafka_clients import MessageCollector, MessagePublisher

pytestmark = pytest.mark.us2


def test_invalid_message_is_skipped_and_valid_continue(
    topic_manager, make_collector, bootstrap_servers
):
    """T014: malformed messages are routed aside; valid messages still process."""
    topic = topic_manager.create("messages", partitions=1)
    collector = make_collector(topic, group_id="t014-invalid")

    publisher = MessagePublisher(bootstrap_servers)
    try:
        # valid, then non-JSON bytes, then JSON-missing-required-fields, then valid.
        publisher.publish(topic, scenario="error-handling", payload={"n": 1}, sequence=1)
        publisher.publish_raw(topic, b"this is not valid json {{{", key=str(uuid.uuid4()))
        publisher.publish_raw(
            topic, json.dumps({"foo": "bar"}).encode("utf-8"), key=str(uuid.uuid4())
        )
        publisher.publish(topic, scenario="error-handling", payload={"n": 2}, sequence=2)
        publisher.flush()
    finally:
        publisher.close()

    received = collector.collect(expected=2, timeout_s=30.0)

    assert len(received.valid) == 2, "valid messages should still be processed"
    assert received.sequences == [1, 2]
    assert len(received.invalid) >= 2, "both malformed messages should be flagged invalid"


def test_consumer_resumes_from_committed_offset(topic_manager, bootstrap_servers):
    """T015: after a restart the consumer resumes from its committed offset (FR-006).

    The test stack's consumer is the test runner itself, so a container stop
    would kill the run. We instead model the disconnect faithfully at the offset
    level: a consumer in a fixed group commits after reading the first batch,
    disconnects, new messages arrive, and a fresh consumer in the SAME group
    resumes exactly where the first left off — no loss, no duplication.
    """
    topic = topic_manager.create("messages", partitions=1)
    group_id = "t015-resume"

    publisher = MessagePublisher(bootstrap_servers)
    try:
        for seq in range(1, 4):  # first batch: 1,2,3
            publisher.publish(topic, scenario="error-handling", payload={"n": seq}, sequence=seq)
        publisher.flush()

        # First consumer reads and commits, then "disconnects".
        first = MessageCollector(bootstrap_servers, topic, group_id=group_id, enable_auto_commit=False)
        batch1 = first.collect(expected=3, timeout_s=30.0)
        assert batch1.sequences == [1, 2, 3]
        first.commit()
        first.close()

        # New messages arrive while the consumer is gone.
        for seq in range(4, 6):  # 4,5
            publisher.publish(topic, scenario="error-handling", payload={"n": seq}, sequence=seq)
        publisher.flush()

        # Fresh consumer in the same group resumes from the committed offset.
        second = MessageCollector(bootstrap_servers, topic, group_id=group_id, enable_auto_commit=False)
        try:
            batch2 = second.collect(expected=2, timeout_s=30.0)
            # No duplication (no re-delivery of 1-3) and no loss (4,5 both arrive).
            assert batch2.sequences == [4, 5], f"resume offset wrong: {batch2.sequences}"
        finally:
            second.commit()
            second.close()
    finally:
        publisher.close()


def test_duplicate_message_processed_once(topic_manager, make_collector, bootstrap_servers):
    """T024: the same message_id delivered twice is processed exactly once (EC-2)."""
    topic = topic_manager.create("messages", partitions=1)
    collector = make_collector(topic, group_id="t024-dedup")

    fixed_id = str(uuid.uuid4())
    publisher = MessagePublisher(bootstrap_servers)
    try:
        publisher.publish(
            topic, scenario="error-handling", payload={"v": 1}, sequence=1, message_id=fixed_id
        )
        publisher.publish(
            topic, scenario="error-handling", payload={"v": 1}, sequence=1, message_id=fixed_id
        )
        publisher.flush()
    finally:
        publisher.close()

    # Wait long enough that BOTH copies have certainly arrived before asserting.
    received = collector.collect(expected=2, timeout_s=10.0)

    assert len(received.valid) == 1, "duplicate message_id must be processed only once"
    assert received.duplicates >= 1, "the duplicate delivery should have been detected"
    assert received.message_ids == [fixed_id]
