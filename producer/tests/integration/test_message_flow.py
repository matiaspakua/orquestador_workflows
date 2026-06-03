"""User Story 1 — end-to-end message flow (producer side).

Covers:
  T008  producer harness publishes a known payload + sequence number
  T010  end-to-end: publish 1 message, consumer receives it within 30s (SC-001)
  T011  payload integrity: consumed payload matches published exactly (FR-002)
  T012  ordering: 10 sequential messages to a 1-partition topic stay ordered (FR-003)
  T023  consumer late-join: message published before consumer starts is picked up (EC-1)
"""

from __future__ import annotations

import time

import pytest

from tests_support.kafka_clients import MessageCollector

pytestmark = pytest.mark.us1


def test_producer_publishes_known_message(publisher, topic_manager, make_payload):
    """T008: the producer harness publishes a message with a known payload+sequence."""
    topic = topic_manager.create("messages", partitions=1)
    payload = make_payload(seq=1, marker="known-payload")

    result = publisher.publish(topic, scenario="message-flow", payload=payload, sequence=1)

    assert result.delivered, f"publish failed: {result.error}"
    assert result.offset is not None
    assert result.message["sequence"] == 1
    assert result.message["payload"]["marker"] == "known-payload"


def test_end_to_end_delivery_within_30s(publisher, topic_manager, make_payload, bootstrap_servers):
    """T010: a published message reaches the consumer within 30 seconds (SC-001)."""
    topic = topic_manager.create("messages", partitions=1)
    collector = MessageCollector(bootstrap_servers, topic, group_id="t010-e2e")
    try:
        payload = make_payload(seq=1)
        start = time.time()
        result = publisher.publish(topic, scenario="message-flow", payload=payload, sequence=1)
        assert result.delivered

        received = collector.collect(expected=1, timeout_s=30.0)
        elapsed = time.time() - start

        assert len(received.valid) == 1, "consumer did not receive the message within 30s"
        assert elapsed < 30.0, f"end-to-end flow took {elapsed:.1f}s (budget 30s)"
        assert received.valid[0]["message_id"] == result.message["message_id"]
    finally:
        collector.close()


def test_payload_integrity(publisher, topic_manager, bootstrap_servers):
    """T011: a structured JSON payload is consumed byte-for-byte identical (FR-002)."""
    topic = topic_manager.create("messages", partitions=1)
    collector = MessageCollector(bootstrap_servers, topic, group_id="t011-integrity")
    try:
        payload = {
            "order": {"id": "ORD-42", "lines": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}]},
            "amount": 199.95,
            "flags": [True, False, None],
            "unicode": "café — déjà vu — 日本語",
            "nested": {"deep": {"deeper": {"value": 7}}},
        }
        result = publisher.publish(topic, scenario="message-flow", payload=payload, sequence=1)
        assert result.delivered

        received = collector.collect(expected=1, timeout_s=30.0)
        assert len(received.valid) == 1
        assert received.valid[0]["payload"] == payload, "consumed payload differs from published"
    finally:
        collector.close()


def test_message_ordering_preserved(publisher, topic_manager, make_payload, bootstrap_servers):
    """T012: 10 sequential messages on a single-partition topic stay ordered (FR-003)."""
    topic = topic_manager.create("messages", partitions=1)
    collector = MessageCollector(bootstrap_servers, topic, group_id="t012-ordering")
    try:
        for seq in range(1, 11):
            result = publisher.publish(
                topic, scenario="message-flow", payload=make_payload(seq=seq), sequence=seq
            )
            assert result.delivered, f"failed to publish sequence {seq}"
        publisher.flush()

        received = collector.collect(expected=10, timeout_s=30.0)
        assert len(received.valid) == 10, f"expected 10 messages, got {len(received.valid)}"
        assert received.sequences == list(range(1, 11)), (
            f"messages out of order: {received.sequences}"
        )
    finally:
        collector.close()


def test_consumer_late_join_picks_up_message(publisher, topic_manager, make_payload, bootstrap_servers):
    """T023: a message published before the consumer starts is still received (EC-1).

    Kafka retains messages, so a consumer with auto_offset_reset='earliest'
    started after publication still reads the retained message.
    """
    topic = topic_manager.create("messages", partitions=1)

    # Publish BEFORE any consumer exists.
    result = publisher.publish(
        topic, scenario="message-flow", payload=make_payload(seq=1), sequence=1
    )
    assert result.delivered
    time.sleep(1.0)  # ensure the message is durably stored before consumer joins

    # Now start the consumer (late join).
    collector = MessageCollector(bootstrap_servers, topic, group_id="t023-late-join")
    try:
        received = collector.collect(expected=1, timeout_s=30.0)
        assert len(received.valid) == 1, "late-joining consumer missed the retained message"
        assert received.valid[0]["message_id"] == result.message["message_id"]
    finally:
        collector.close()
