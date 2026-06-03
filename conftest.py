"""Root pytest fixtures shared by both test trees (producer & consumer).

Provides the infrastructure-level fixtures (broker address, timeouts, the test
topic lifecycle manager) used across all integration tests. Component-specific
fixtures live in ``producer/tests/conftest.py`` and ``consumer/tests/conftest.py``.
"""

from __future__ import annotations

import os

import pytest

from tests_support.kafka_topics import TestTopicManager


@pytest.fixture(scope="session")
def bootstrap_servers() -> str:
    """Kafka bootstrap address (test broker inside the compose overlay)."""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-test:29092")


@pytest.fixture(scope="session")
def kafka_timeout() -> float:
    """Default wait budget for message delivery/consumption (seconds)."""
    return float(os.getenv("KAFKA_TIMEOUT", "30"))


@pytest.fixture(scope="session", autouse=True)
def _purge_orphan_topics(bootstrap_servers: str):
    """Delete leftover ``test.*`` topics once at suite start (Edge Case 4).

    Implements the run-start auto-cleanup decision so an aborted previous run
    cannot interfere with this one.
    """
    mgr = TestTopicManager(bootstrap_servers)
    try:
        mgr.purge_orphans()
    finally:
        mgr.close()
    yield


@pytest.fixture
def topic_manager(bootstrap_servers: str) -> TestTopicManager:
    """A per-test topic manager with a unique scenario id; auto-cleans topics.

    Each test gets isolated ``test.{scenario_id}.{suffix}`` topics that are
    deleted on teardown (test isolation, T021).
    """
    mgr = TestTopicManager(bootstrap_servers)
    try:
        yield mgr
    finally:
        mgr.cleanup()
        mgr.close()
