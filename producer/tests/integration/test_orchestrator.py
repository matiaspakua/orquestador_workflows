"""User Story 3 — orchestrator coordination (producer side).

Uses a mock orchestrator implementing the same orchestration-events interface as
the real orchestrator (specs/003-workflow-definition/contracts/).

Covers:
  T016  workflow start: orchestrator initiates a workflow, producer begins
        publishing and the consumer begins processing (FR-007).
  T018  workflow cancellation: cancelling a running workflow stops producer
        publishing and consumer processing for that workflow (FR-008).
"""

from __future__ import annotations

import time

import pytest

from tests_support.kafka_clients import MessageCollector
from tests_support.mock_orchestrator import MockOrchestrator, WorkflowStatus

pytestmark = pytest.mark.us3


def test_orchestrator_starts_producer_and_consumer(publisher, topic_manager, bootstrap_servers):
    """T016: starting a workflow makes the producer publish and consumer process."""
    messages_topic = topic_manager.create("messages", partitions=1)
    events_topic = topic_manager.create("events", partitions=1)
    orchestrator = MockOrchestrator(
        bootstrap_servers,
        publisher=publisher,
        messages_topic=messages_topic,
        events_topic=events_topic,
    )
    collector = MessageCollector(bootstrap_servers, messages_topic, group_id="t016-start")
    try:
        execution_id = orchestrator.start_workflow({"order_id": "ORD-1"}, total_messages=3)
        assert execution_id is not None
        assert orchestrator.status is WorkflowStatus.RUNNING

        # Producer begins publishing...
        orchestrator.wait_until_published(timeout_s=30)
        assert orchestrator.published_count == 3

        # ...and the consumer begins processing what was published.
        received = collector.collect(expected=3, timeout_s=30.0)
        assert len(received.valid) == 3

        # The orchestrator emitted a WorkflowStarted event.
        started = [e for e in orchestrator.emitted_events if e["event_type"] == "WorkflowStarted"]
        assert len(started) == 1
        assert started[0]["workflow_execution_id"] == execution_id
    finally:
        collector.close()
        orchestrator.close()


def test_orchestrator_cancellation_stops_activity(publisher, topic_manager, bootstrap_servers):
    """T018: cancelling a running workflow halts publishing/processing (FR-008)."""
    messages_topic = topic_manager.create("messages", partitions=1)
    events_topic = topic_manager.create("events", partitions=1)
    orchestrator = MockOrchestrator(
        bootstrap_servers,
        publisher=publisher,
        messages_topic=messages_topic,
        events_topic=events_topic,
    )
    collector = MessageCollector(bootstrap_servers, messages_topic, group_id="t018-cancel")
    try:
        # A long-running workflow we can interrupt mid-stream.
        orchestrator.start_workflow({"order_id": "ORD-2"}, total_messages=200, interval_s=0.05)
        time.sleep(0.3)  # let a few messages publish
        orchestrator.cancel()

        # Producer stopped early.
        assert orchestrator.status is WorkflowStatus.CANCELLED
        assert orchestrator.published_count < 200, "publishing did not stop on cancel"

        # Consumer only ever sees what was actually published — nothing after cancel.
        received = collector.collect(expected=orchestrator.published_count, timeout_s=15.0)
        assert len(received.valid) == orchestrator.published_count

        # A terminal (cancelled) event was emitted.
        cancelled = [
            e for e in orchestrator.emitted_events
            if e["payload"].get("error_code") == "CANCELLED"
        ]
        assert len(cancelled) == 1
    finally:
        collector.close()
        orchestrator.close()
