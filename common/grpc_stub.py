"""gRPC client stub for integration testing - falls back to REST if unavailable."""

import logging

logger = logging.getLogger(__name__)

_grpc_available = False
try:
    import grpc  # noqa: F401 — used via try/except availability check
    _grpc_available = True
except ImportError:
    pass


class GrpcClientStub:
    def __init__(self, host="localhost", port=50051):
        self.address = f"{host}:{port}"
        self.channel = None
        self.stub = None
        if _grpc_available:
            self._connect()

    def _connect(self):
        try:
            import grpc

            from common import workflow_service_pb2_grpc
            self.channel = grpc.insecure_channel(self.address)
            self.stub = workflow_service_pb2_grpc.WorkflowOrchestratorStub(self.channel)
        except Exception as e:
            logger.warning(f"gRPC not available: {e}")
            self.stub = None

    def is_available(self) -> bool:
        if not self.stub:
            return False
        try:
            import grpc
            grpc.channel_ready_future(self.channel).result(timeout=2)
            return True
        except Exception:
            return False

    def get_workflow_status(self, execution_id: str) -> dict | None:
        if not self.stub:
            return None
        try:
            from common import workflow_service_pb2
            resp = self.stub.GetWorkflowStatus(
                workflow_service_pb2.WorkflowStatusRequest(
                    workflow_execution_id=execution_id
                )
            )
            return {
                "id": resp.workflow_execution_id,
                "status": resp.status,
                "name": resp.name,
                "duration_seconds": resp.duration_seconds,
                "steps": [
                    {"name": s.name, "status": s.status, "sequence_order": s.sequence_order}
                    for s in resp.steps
                ],
            }
        except Exception as e:
            logger.warning(f"gRPC GetWorkflowStatus failed: {e}")
            return None

    def close(self):
        if self.channel:
            self.channel.close()
