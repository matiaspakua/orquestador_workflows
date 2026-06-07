# Workflow Lifecycle

## State Machine

```
                    ┌─────────┐
              ┌────►│ Pending │────────────────────┐
              │     └────┬────┘                    │
              │          │ accepted                │ cancelled
              │          ▼                         │
              │     ┌─────────┐                    ▼
              │     │ Running │───────────►  ┌───────────┐
              │     └────┬────┘  failed      │ Cancelled │
              │          │                   └───────────┘
              │          │ all steps ok
              │          ▼
              │     ┌───────────┐
              │     │ Completed │
              │     └───────────┘
              │          │
              │          │ (terminal — immutable)
              │
         ┌────┴────┐
         │ Failed  │
         └─────────┘
           (terminal)
```

### States

| State | Terminal | Description |
|-------|----------|-------------|
| Pending | No | Submitted but not yet started |
| Running | No | Actively executing steps |
| Completed | Yes | All steps executed successfully |
| Failed | Yes | Execution terminated due to error |
| Cancelled | Yes | Execution aborted by operator |

### Valid Transitions

| From | To | Trigger |
|------|----|---------|
| Pending | Running | Orchestrator accepts and starts execution |
| Pending | Cancelled | Operator cancels before execution begins |
| Running | Completed | All steps succeed |
| Running | Failed | Any step fails with no remaining retries, or timeout exceeded |
| Running | Cancelled | Operator cancels during execution |

No other transitions are valid. Terminal states are immutable.

---

## Submission Validation Rules

1. **Step ID uniqueness**: Two or more steps share the same `id`
2. **Circular dependency**: The `depends_on` graph contains a cycle
3. **Invalid step name**: A step `name` is empty or exceeds 255 characters
4. **Invalid timeout**: A `timeout_seconds` value is ≤ 0 or is not an integer
5. **Invalid retry policy**: `max_attempts` < 1 or `backoff_seconds` < 0
6. **Decision branch count**: A Decision step has fewer than 2 branches or more than 2 branches
7. **Parallel minimum branches**: A Parallel step has fewer than 2 branches
8. **Per-type config missing**: A Task step is missing `action` or `target`
9. **Minimum step count**: The `steps` array is empty
10. **Dead `depends_on` reference**: A step references a `depends_on` ID that does not exist

---

## Testing the Lifecycle

### REST API

```bash
# List all workflows
curl http://localhost:5000/api/workflows

# Get workflow detail
curl http://localhost:5000/api/workflows/<execution_id>

# Check system health
curl http://localhost:5000/health
```

### gRPC API

```python
import grpc
from workflow_service_pb2_grpc import WorkflowOrchestratorStub
from workflow_service_pb2 import ListWorkflowsRequest

channel = grpc.insecure_channel("localhost:50051")
stub = WorkflowOrchestratorStub(channel)
response = stub.ListWorkflows(ListWorkflowsRequest(page=1, per_page=10))
```
