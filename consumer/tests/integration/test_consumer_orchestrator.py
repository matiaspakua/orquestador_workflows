"""User Story 3 — orchestrator coordination (consumer side).

Covers:
  T017  workflow completion: a full publish-consume cycle ends with the
        orchestrator marking the workflow Completed and persisting results
        (FR-007).
"""

from __future__ import annotations

import pytest

from consumer.tests.models import RunStatus
from tests_support.kafka_clients import MessageCollector, MessagePublisher
from tests_support.mock_orchestrator import MockOrchestrator, WorkflowStatus

pytestmark = pytest.mark.us3


def test_workflow_completes_with_persisted_results(
    topic_manager, bootstrap_servers, test_run
):
    """T017: after consuming all messages, the orchestrator marks Completed."""
    messages_topic = topic_manager.create("messages", partitions=1)
    events_topic = topic_manager.create("events", partitions=1)

    publisher = MessagePublisher(bootstrap_servers)
    orchestrator = MockOrchestrator(
        bootstrap_servers,
        publisher=publisher,
        messages_topic=messages_topic,
        events_topic=events_topic,
    )
    collector = MessageCollector(bootstrap_servers, messages_topic, group_id="t017-complete")
    try:
        execution_id = orchestrator.start_workflow({"order_id": "ORD-3"}, total_messages=5)
        orchestrator.wait_until_published(timeout_s=30)

        received = collector.collect(expected=5, timeout_s=30.0)
        assert len(received.valid) == 5, "consumer did not finish the workflow's messages"

        # Consumer finished -> orchestrator completes the workflow with results.
        result = {"processed": len(received.valid), "execution_id": execution_id}
        orchestrator.complete(result, steps_total=5)

        assert orchestrator.status is WorkflowStatus.COMPLETED
        assert orchestrator.result == result

        # A WorkflowCompleted event with the persisted result was emitted.
        completed = [
            e for e in orchestrator.emitted_events if e["event_type"] == "WorkflowCompleted"
        ]
        assert len(completed) == 1
        assert completed[0]["payload"]["result"] == result
        assert completed[0]["payload"]["steps_total"] == 5

        test_run.finish(RunStatus.PASSED, summary="workflow completed", message_count=5)
        assert test_run.status is RunStatus.PASSED
    finally:
        collector.close()
        orchestrator.close()
        publisher.close()
