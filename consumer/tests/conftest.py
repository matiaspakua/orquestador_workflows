"""Consumer-side test fixtures (T003).

Common consumer fixtures: the collect harness factory and a TestRun tracker.
Infrastructure fixtures (``bootstrap_servers``, ``topic_manager``) come from the
repo-root conftest.
"""

from __future__ import annotations

import uuid
from typing import Callable

import pytest

from consumer.tests.models import RunStatus, TestRun
from tests_support.kafka_clients import MessageCollector


@pytest.fixture
def make_collector(bootstrap_servers: str) -> Callable[..., MessageCollector]:
    """Factory that builds (and registers for cleanup) consumer harnesses."""
    created: list[MessageCollector] = []

    def _make(topic: str, *, group_id: str | None = None, **kwargs) -> MessageCollector:
        collector = MessageCollector(
            bootstrap_servers,
            topic,
            group_id=group_id or f"test-cg-{uuid.uuid4().hex[:8]}",
            **kwargs,
        )
        created.append(collector)
        return collector

    yield _make

    for collector in created:
        collector.close()


@pytest.fixture
def test_run() -> TestRun:
    """A TestRun tracker (status/timing/result_summary) for the current test."""
    return TestRun(scenario=str(uuid.uuid4()), status=RunStatus.RUNNING)
