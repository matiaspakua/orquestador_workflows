"""Kafka test topic lifecycle manager (T004).

Creates and tears down isolated topics following the contract naming pattern
``test.{scenario_id}.{suffix}`` (see contracts/message-contract.md). Guarantees
production-topic isolation (everything is prefixed ``test.``) and provides
auto-cleanup of orphaned ``test.*`` topics from aborted runs (spec Edge Case 4).
"""

from __future__ import annotations

import logging
import time
import uuid

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (
    NoBrokersAvailable,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)

logger = logging.getLogger(__name__)

TEST_TOPIC_PREFIX = "test."


class TestTopicManager:
    """Manage the lifecycle of isolated Kafka test topics.

    Typical use (handled by the ``topic_manager`` fixture)::

        mgr = TestTopicManager(bootstrap_servers)
        mgr.purge_orphans()                       # clean leftovers at run start
        topic = mgr.create("messages", partitions=1)
        ...                                       # run the scenario
        mgr.cleanup()                             # delete everything we created
    """

    # Name starts with "Test" but this is a helper, not a pytest test class.
    __test__ = False

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        scenario_id: str | None = None,
        connect_retries: int = 10,
        connect_backoff_s: float = 2.0,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        # Short, stable per-instance scenario id used in the topic name.
        self.scenario_id = scenario_id or uuid.uuid4().hex[:8]
        self._created: list[str] = []
        self._admin = self._connect(connect_retries, connect_backoff_s)

    def _connect(self, retries: int, backoff_s: float) -> KafkaAdminClient:
        """Connect to the broker, retrying while it is still coming up."""
        last_err: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                return KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id=f"test-topic-mgr-{self.scenario_id}",
                    request_timeout_ms=10_000,
                )
            except NoBrokersAvailable as err:  # broker not ready yet
                last_err = err
                logger.warning(
                    "Kafka admin not ready (attempt %d/%d); retrying in %.1fs",
                    attempt,
                    retries,
                    backoff_s,
                )
                time.sleep(backoff_s)
        raise RuntimeError(
            f"Could not connect to Kafka admin at {self.bootstrap_servers}"
        ) from last_err

    def topic_name(self, suffix: str) -> str:
        """Return the contract topic name ``test.{scenario_id}.{suffix}``."""
        return f"{TEST_TOPIC_PREFIX}{self.scenario_id}.{suffix}"

    def create(self, suffix: str, *, partitions: int = 1, replication: int = 1) -> str:
        """Create an isolated test topic and return its full name.

        Ordering tests (T012) rely on ``partitions=1`` to get a total order.
        """
        name = self.topic_name(suffix)
        topic = NewTopic(name=name, num_partitions=partitions, replication_factor=replication)
        try:
            self._admin.create_topics([topic])
            self._created.append(name)
            logger.info("Created test topic %s (partitions=%d)", name, partitions)
        except TopicAlreadyExistsError:
            # Idempotent: a prior aborted run may have left it behind.
            if name not in self._created:
                self._created.append(name)
            logger.info("Test topic %s already exists; reusing", name)
        # Give the broker a beat to propagate metadata before producers connect.
        self._await_topic(name)
        return name

    def _await_topic(self, name: str, timeout_s: float = 10.0) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if name in self._admin.list_topics():
                return
            time.sleep(0.25)
        logger.warning("Topic %s not visible in metadata after %.1fs", name, timeout_s)

    def list_test_topics(self) -> list[str]:
        """Return every topic currently on the broker that we treat as a test topic."""
        return [t for t in self._admin.list_topics() if t.startswith(TEST_TOPIC_PREFIX)]

    def purge_orphans(self) -> list[str]:
        """Delete ALL ``test.*`` topics on the broker (run-start cleanup).

        Implements the "auto-cleanup on each test run start" decision from the
        spec clarifications so aborted runs cannot interfere with new ones.
        """
        orphans = self.list_test_topics()
        if orphans:
            self._delete(orphans)
            logger.info("Purged %d orphaned test topic(s): %s", len(orphans), orphans)
        return orphans

    def cleanup(self) -> None:
        """Delete the topics created by this manager instance."""
        if self._created:
            self._delete(list(self._created))
            self._created.clear()

    def _delete(self, names: list[str]) -> None:
        try:
            self._admin.delete_topics(names)
        except UnknownTopicOrPartitionError:
            pass  # already gone — fine
        except Exception as err:  # pragma: no cover - best-effort teardown
            logger.warning("Error deleting topics %s: %s", names, err)

    def close(self) -> None:
        try:
            self._admin.close()
        except Exception:  # pragma: no cover
            pass

    def __enter__(self) -> "TestTopicManager":
        return self

    def __exit__(self, *exc: object) -> None:
        self.cleanup()
        self.close()
