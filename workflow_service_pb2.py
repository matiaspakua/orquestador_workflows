"""Generated protobuf definitions - manual stub for testing."""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

_grpc_import_error = None
try:
    import grpc
    from grpc import StatusCode
    from concurrent import futures
except ImportError as e:
    _grpc_import_error = e
    grpc = None
    futures = None
    StatusCode = None


class WorkflowStatusResponse:
    def __init__(self, **kwargs):
        self.workflow_execution_id = kwargs.get("workflow_execution_id", "")
        self.status = kwargs.get("status", "")
        self.name = kwargs.get("name", "")
        self.started_at = kwargs.get("started_at", "")
        self.completed_at = kwargs.get("completed_at", "")
        self.duration_seconds = kwargs.get("duration_seconds", 0)
        self.steps = kwargs.get("steps", [])


class StepStatus:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", "")
        self.name = kwargs.get("name", "")
        self.step_type = kwargs.get("step_type", "")
        self.status = kwargs.get("status", "")
        self.started_at = kwargs.get("started_at", "")
        self.completed_at = kwargs.get("completed_at", "")
        self.duration_seconds = kwargs.get("duration_seconds", 0)
        self.error_message = kwargs.get("error_message", "")
        self.sequence_order = kwargs.get("sequence_order", 0)


class WorkflowResponse:
    def __init__(self, **kwargs):
        self.success = kwargs.get("success", True)
        self.message = kwargs.get("message", "")
        self.workflow_execution_id = kwargs.get("workflow_execution_id", "")


class WorkflowStatusRequest:
    def __init__(self, **kwargs):
        self.workflow_execution_id = kwargs.get("workflow_execution_id", "")


class CancelWorkflowRequest:
    def __init__(self, **kwargs):
        self.workflow_execution_id = kwargs.get("workflow_execution_id", "")
        self.reason = kwargs.get("reason", "")


class ListWorkflowsRequest:
    def __init__(self, **kwargs):
        self.page = kwargs.get("page", 1)
        self.per_page = kwargs.get("per_page", 50)
        self.status_filter = kwargs.get("status_filter", "")
        self.date_from = kwargs.get("date_from", "")
        self.date_to = kwargs.get("date_to", "")


class ListWorkflowsResponse:
    def __init__(self, **kwargs):
        self.executions = kwargs.get("executions", [])
        self.total = kwargs.get("total", 0)
        self.page = kwargs.get("page", 1)
        self.per_page = kwargs.get("per_page", 50)


class WorkflowEvent:
    def __init__(self, **kwargs):
        self.event_type = kwargs.get("event_type", "")
        self.workflow_execution_id = kwargs.get("workflow_execution_id", "")
        self.timestamp = kwargs.get("timestamp", "")
        self.payload = kwargs.get("payload", "{}")


class StreamRequest:
    def __init__(self, **kwargs):
        self.workflow_execution_id = kwargs.get("workflow_execution_id", "")
        self.event_types = kwargs.get("event_types", "")


class StartWorkflowRequest:
    def __init__(self, **kwargs):
        self.workflow_name = kwargs.get("workflow_name", "")
        self.workflow_version = kwargs.get("workflow_version", "")
        self.input_params = kwargs.get("input_params", {})
        self.timeout_seconds = kwargs.get("timeout_seconds", 300)


class WorkflowOrchestratorServicer:
    """Stub servicer - real implementation uses the common/grpc_service module."""

    def StartWorkflow(self, request, context):
        return WorkflowResponse(success=True)

    def GetWorkflowStatus(self, request, context):
        context.set_code(getattr(StatusCode, "UNIMPLEMENTED", 12))
        return WorkflowStatusResponse()

    def CancelWorkflow(self, request, context):
        return WorkflowResponse(success=True)

    def ListWorkflows(self, request, context):
        return ListWorkflowsResponse()

    def StreamWorkflowEvents(self, request, context):
        return iter([])


class WorkflowOrchestratorStub:
    def __init__(self, channel):
        self._channel = channel

    def StartWorkflow(self, request, timeout=None):
        return WorkflowResponse()

    def GetWorkflowStatus(self, request, timeout=None):
        return WorkflowStatusResponse()

    def CancelWorkflow(self, request, timeout=None):
        return WorkflowResponse()

    def ListWorkflows(self, request, timeout=None):
        return ListWorkflowsResponse()

    def StreamWorkflowEvents(self, request, timeout=None):
        return iter([])


def add_WorkflowOrchestratorServicer_to_server(servicer, server):
    pass
