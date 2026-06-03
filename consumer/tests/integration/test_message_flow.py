"""User Story 1 — end-to-end message flow (consumer side).

Covers:
  T009  consumer harness subscribes to the test topic and captures received
        messages (the building block the e2e producer tests rely on).
"""

from __future__ import annotations

import pytest

from consumer.tests.models import RunStatus
from tests_support.kafka_clients import MessagePublisher

pytestmark = pytest.mark.us1


def test_consumer_harness_captures_messages(
    topic_manager, make_collector, bootstrap_servers, test_run
):
    """T009: the consumer harness subscribes and captures published messages."""
    topic = topic_manager.create("messages", partitions=1)
    collector = make_collector(topic, group_id="t009-harness")

    publisher = MessagePublisher(bootstrap_servers)
    try:
        for seq in range(1, 4):
            publisher.publish(
                topic, scenario="message-flow", payload={"seq": seq}, sequence=seq
            )
        publisher.flush()
    finally:
        publisher.close()

    received = collector.collect(expected=3, timeout_s=30.0)

    test_run.finish(
        RunStatus.PASSED if len(received.valid) == 3 else RunStatus.FAILED,
        summary=f"captured {len(received.valid)}/3 messages",
        message_count=len(received.valid),
    )

    assert len(received.valid) == 3, "consumer harness did not capture all messages"
    assert received.sequences == [1, 2, 3]
    assert test_run.status is RunStatus.PASSED
    assert test_run.duration_s is not None
