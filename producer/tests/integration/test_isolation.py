"""Test isolation validation (T021).

Verifies that (a) all test topics are namespaced under the ``test.`` prefix so
they cannot collide with production topics, and (b) topics are actually deleted
after a run, leaving no residue (spec Edge Case 4 / quickstart troubleshooting).
"""

from __future__ import annotations

import pytest

from tests_support.kafka_topics import TEST_TOPIC_PREFIX, TestTopicManager

pytestmark = pytest.mark.us2

# Topic names used by the running system (docker-compose.yml / .env). Test topics
# must never match these.
PRODUCTION_TOPICS = {"data-events", "events", "orchestration-events"}


def test_topics_use_test_prefix(topic_manager):
    """Every created topic is namespaced under ``test.`` (production isolation)."""
    t1 = topic_manager.create("messages", partitions=1)
    t2 = topic_manager.create("results", partitions=1)

    for name in (t1, t2):
        assert name.startswith(TEST_TOPIC_PREFIX), f"{name} is not test-namespaced"
        assert name not in PRODUCTION_TOPICS

    # And the broker confirms they are present and test-prefixed.
    live = topic_manager.list_test_topics()
    assert t1 in live and t2 in live
    assert all(t.startswith(TEST_TOPIC_PREFIX) for t in live)


def test_topics_cleaned_up_after_run(bootstrap_servers):
    """After cleanup, the created topics are gone — no leftover residue."""
    mgr = TestTopicManager(bootstrap_servers)
    try:
        name = mgr.create("ephemeral", partitions=1)
        assert name in mgr.list_test_topics()
        mgr.cleanup()

        # Re-query via a fresh manager to confirm deletion propagated.
        verifier = TestTopicManager(bootstrap_servers)
        try:
            assert name not in verifier.list_test_topics(), "test topic was not cleaned up"
        finally:
            verifier.close()
    finally:
        mgr.close()
