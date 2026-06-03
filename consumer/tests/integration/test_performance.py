"""Performance / throughput benchmark (T019).

Verifies the consumer processes at least 1,000 messages without data loss or
corruption in a single run (SC-003). Each message carries a deterministic
checksum so corruption (not just loss) is detected.
"""

from __future__ import annotations

import pytest

from tests_support.kafka_clients import MessageCollector, MessagePublisher

pytestmark = pytest.mark.performance

MESSAGE_COUNT = 1_000


def _checksum(seq: int) -> int:
    return (seq * 2_654_435_761) % 1_000_000_007


def test_processes_1000_messages_without_loss(topic_manager, bootstrap_servers):
    """T019: publish 1,000 messages and consume them all intact (SC-003)."""
    topic = topic_manager.create("messages", partitions=1)
    collector = MessageCollector(bootstrap_servers, topic, group_id="t019-perf")

    publisher = MessagePublisher(bootstrap_servers)
    try:
        for seq in range(1, MESSAGE_COUNT + 1):
            result = publisher.publish(
                topic,
                scenario="performance",
                payload={"seq": seq, "checksum": _checksum(seq)},
                sequence=seq,
            )
            assert result.delivered, f"failed to publish message {seq}"
        publisher.flush()
    finally:
        publisher.close()

    received = collector.collect(expected=MESSAGE_COUNT, timeout_s=120.0)
    try:
        # No loss: every sequence number 1..N present exactly once.
        seqs = sorted(received.sequences)
        assert len(seqs) == MESSAGE_COUNT, f"data loss: got {len(seqs)}/{MESSAGE_COUNT}"
        assert seqs == list(range(1, MESSAGE_COUNT + 1)), "missing or duplicated sequences"

        # No corruption: checksum matches for every message.
        for msg in received.valid:
            seq = msg["sequence"]
            assert msg["payload"]["checksum"] == _checksum(seq), f"corrupted payload at seq {seq}"
    finally:
        collector.close()
