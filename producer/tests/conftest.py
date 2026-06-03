"""Producer-side test fixtures (T003).

Common producer fixtures: the publish harness, scenario declarations, and test
payload/data generators. Infrastructure fixtures (``bootstrap_servers``,
``topic_manager``) come from the repo-root conftest.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

import pytest

from producer.tests.models import ScenarioCategory, TestScenario
from tests_support.kafka_clients import MessagePublisher


@pytest.fixture
def publisher(bootstrap_servers: str) -> MessagePublisher:
    """A producer harness for publishing contract messages; closed on teardown."""
    pub = MessagePublisher(bootstrap_servers)
    try:
        yield pub
    finally:
        pub.close()


@pytest.fixture
def make_payload() -> Callable[..., dict[str, Any]]:
    """Factory for deterministic-but-unique structured JSON payloads."""

    def _make(seq: int = 0, **extra: Any) -> dict[str, Any]:
        payload = {
            "order_id": f"ORD-{uuid.uuid4().hex[:8]}",
            "amount": 100 + seq,
            "items": [f"item_{i}" for i in range(seq % 5 + 1)],
            "seq": seq,
        }
        payload.update(extra)
        return payload

    return _make


@pytest.fixture
def message_flow_scenario() -> TestScenario:
    """A declarative TestScenario for the end-to-end message-flow tests."""
    return TestScenario(
        name="end-to-end-message-flow",
        category=ScenarioCategory.MESSAGE_FLOW,
        setup_steps=["create isolated test topic", "start consumer harness"],
        execution_steps=["publish known message", "consume message"],
        expected_outcomes=["payload matches exactly", "delivered within 30s"],
        cleanup_steps=["delete test topic"],
        dependencies=["kafka-test"],
    )
