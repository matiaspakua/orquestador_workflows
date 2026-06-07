import logging
import os

logger = logging.getLogger(__name__)

_grpc_available = False
try:
    from concurrent import futures

    import grpc
    _grpc_available = True
except ImportError:
    grpc = None
    futures = None


try:
    from common import workflow_service_pb2, workflow_service_pb2_grpc
    _proto_available = True
except ImportError:
    workflow_service_pb2 = None
    workflow_service_pb2_grpc = None
    _proto_available = False


GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
GRPC_ENABLED = os.getenv("GRPC_ENABLED", "1") == "1"


class WorkflowGrpcServicer:
    if _proto_available:
        class _Servicer(workflow_service_pb2_grpc.WorkflowOrchestratorServicer):
            def __init__(self, workflow_service_module):
                self.ws = workflow_service_module

            def StartWorkflow(self, request, context):
                return workflow_service_pb2.WorkflowResponse(
                    success=True, message="Workflow started",
                    workflow_execution_id="mock-id",
                )

            def GetWorkflowStatus(self, request, context):
                try:
                    execution = self.ws.get_workflow_execution(request.workflow_execution_id)
                    if not execution:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        return workflow_service_pb2.WorkflowStatusResponse()
                    steps = self.ws.get_workflow_steps(request.workflow_execution_id)
                    step_msgs = [
                        workflow_service_pb2.StepStatus(
                            id=s["id"], name=s["name"], step_type=s["step_type"],
                            status=s["status"], started_at=str(s.get("started_at") or ""),
                            completed_at=str(s.get("completed_at") or ""),
                            duration_seconds=s.get("duration_seconds") or 0,
                            error_message=s.get("error_message") or "",
                            sequence_order=s["sequence_order"],
                        ) for s in steps
                    ]
                    return workflow_service_pb2.WorkflowStatusResponse(
                        workflow_execution_id=execution["id"],
                        status=execution["status"],
                        name=execution["name"],
                        started_at=str(execution.get("started_at") or ""),
                        completed_at=str(execution.get("completed_at") or ""),
                        duration_seconds=execution.get("duration_seconds") or 0,
                        steps=step_msgs,
                    )
                except Exception as e:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(str(e))
                    return workflow_service_pb2.WorkflowStatusResponse()

            def CancelWorkflow(self, request, context):
                return workflow_service_pb2.WorkflowResponse(
                    success=True, message=f"Cancelling {request.workflow_execution_id}",
                    workflow_execution_id=request.workflow_execution_id,
                )

            def ListWorkflows(self, request, context):
                try:
                    executions = self.ws.get_workflow_executions(
                        page=max(1, request.page),
                        per_page=min(200, request.per_page or 50),
                        status=request.status_filter or None,
                        date_from=request.date_from or None,
                        date_to=request.date_to or None,
                    )
                    exec_msgs = []
                    for ex in executions:
                        steps = self.ws.get_workflow_steps(ex["id"])
                        step_msgs = [
                            workflow_service_pb2.StepStatus(
                                id=s["id"], name=s["name"], step_type=s["step_type"],
                                status=s["status"], sequence_order=s["sequence_order"],
                            ) for s in steps
                        ]
                        exec_msgs.append(workflow_service_pb2.WorkflowStatusResponse(
                            workflow_execution_id=ex["id"], status=ex["status"],
                            name=ex["name"], steps=step_msgs,
                        ))
                    return workflow_service_pb2.ListWorkflowsResponse(
                        executions=exec_msgs, total=len(exec_msgs),
                        page=request.page, per_page=request.per_page,
                    )
                except Exception as e:
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(str(e))
                    return workflow_service_pb2.ListWorkflowsResponse()

            def StreamWorkflowEvents(self, request, context):
                while context.is_active():
                    yield workflow_service_pb2.WorkflowEvent(
                        event_type="heartbeat",
                        workflow_execution_id=request.workflow_execution_id or "",
                        timestamp="",
                        payload="{}",
                    )
    else:
        class _Servicer:
            pass

    def __new__(cls, *args, **kwargs):
        if _proto_available:
            return cls._Servicer(*args, **kwargs)
        return None


def start_grpc_server(workflow_service_module) -> grpc.Server | None:
    if not _grpc_available or not _proto_available or not GRPC_ENABLED:
        logger.info("gRPC server disabled or unavailable")
        return None

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    workflow_service_pb2_grpc.add_WorkflowOrchestratorServicer_to_server(
        WorkflowGrpcServicer(workflow_service_module), server
    )
    server.add_insecure_port(f"0.0.0.0:{GRPC_PORT}")
    server.start()
    logger.info(f"gRPC server started on port {GRPC_PORT}")
    return server
