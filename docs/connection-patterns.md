# Step Connection Patterns

## Sequential Execution

Steps execute one after the other using `depends_on`.

```
[Step A] ──► [Step B] ──► [Step C]
```

---

## Parallel Execution (Independent Steps)

Steps with no shared `depends_on` are eligible to run concurrently.

```
          ┌─► [Step B] ─┐
[Step A] ─┤              ├─► [Step D]
          └─► [Step C] ─┘
```

---

## Conditional Branching (Decision Step)

A Decision step routes to one of two paths based on a runtime condition.

```
[Step A] ──► [Decision] ──true──►  [Step B]
                        └──false──► [Step C]
```

---

## Structured Parallelism (Parallel Step)

A Parallel step runs N isolated branch sequences concurrently.

---

## Timeout and Error Routing

Steps can route to compensating steps on failure instead of failing the entire workflow.

---

## REST API Integration

The system exposes a REST API at port 5000:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/ready` | GET | Readiness check (includes DB check) |
| `/api/workflows` | GET | List all workflow executions |
| `/api/workflows/<id>` | GET | Get workflow detail |
| `/api/workflows/stream` | GET | SSE stream for live updates |
| `/api/stats` | GET | System statistics |
| `/api/consumers` | GET | Consumer statistics |
| `/api/events/<type>` | GET | Events by type |

## gRPC API Integration

The system also exposes a gRPC API at port 50051:

| Service | Method | Description |
|---------|--------|-------------|
| `WorkflowOrchestrator` | `GetWorkflowStatus` | Get workflow execution status |
| `WorkflowOrchestrator` | `ListWorkflows` | List workflow executions |
| `WorkflowOrchestrator` | `StartWorkflow` | Start a new workflow |
| `WorkflowOrchestrator` | `CancelWorkflow` | Cancel a running workflow |
| `WorkflowOrchestrator` | `StreamWorkflowEvents` | Stream workflow events |

Both APIs provide the same capabilities — use REST for HTTP clients and gRPC for high-performance streaming scenarios.

### gRPC Client Example

```python
import grpc
from workflow_service_pb2_grpc import WorkflowOrchestratorStub
from workflow_service_pb2 import WorkflowStatusRequest

channel = grpc.insecure_channel("localhost:50051")
stub = WorkflowOrchestratorStub(channel)
resp = stub.GetWorkflowStatus(
    WorkflowStatusRequest(workflow_execution_id="abc-123")
)
print(resp.status)
channel.close()
```

---

## Sentinels

| Sentinel | Used in | Meaning |
|----------|---------|---------|
| `__end__` | `on_success`, branch targets | Terminate workflow successfully |
| `__fail__` | `on_failure` | Terminate workflow with failure |
| `__retry__` | `on_failure` | Re-execute the same step |
