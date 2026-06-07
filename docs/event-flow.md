# Orchestrator Event Flow

## Kafka Topics

| Topic | Purpose |
|-------|---------|
| `orchestration-events` | Lifecycle events (WorkflowStarted, StepStarted, etc.) |
| `orchestration-results` | Worker results consumed by the orchestrator |
| `orchestration-dlq` | Failed messages for dead-letter processing |
| `order-validation` | Tasks for the OrderValidator worker |
| `fraud-check` | Tasks for the FraudChecker worker |
| `inventory-check` | Tasks for the InventoryChecker worker |
| `notification-send` | Tasks for the NotificationSender worker |
| `data-events` | Domain events from the producer |

## Success Path

```
WorkflowStarted
    │
    ▼
StepStarted (step 1)
    │
    ▼
StepCompleted (step 1)
    │
    ▼
StepStarted (step 2)
    │
    ▼
StepCompleted (step 2)
    │
    ▼
  ...
    │
    ▼
WorkflowCompleted
```

## Failure Path

```
WorkflowStarted
    │
    ▼
StepStarted (step N)
    │
    ▼
StepFailed (step N)  ──► will_retry = true ──► StepStarted (retry)
    │
    └── will_retry = false
    │
    ▼
WorkflowFailed
```

---

## Event Type Reference

| Event | Trigger | Payload summary |
|-------|---------|-----------------|
| `WorkflowStarted` | Execution transitions Pending → Running | `input`, `definition_name`, `definition_version` |
| `StepStarted` | A step begins execution | `step_id`, `step_name`, `step_type`, `attempt`, `input` |
| `StepCompleted` | A step finishes successfully | `step_id`, `output`, `duration_ms`, `next_step_id` |
| `StepFailed` | A step fails | `step_id`, `error_message`, `error_code`, `attempt`, `will_retry` |
| `WorkflowCompleted` | All steps complete | `result`, `total_duration_ms`, `steps_completed`, `steps_total` |
| `WorkflowFailed` | Workflow terminates on error | `error`, `error_code`, `failed_step_id`, `total_duration_ms` |

---

## Correlation: Tracing a Single Workflow

Every event carries `workflow_execution_id` in the envelope. To trace a complete workflow execution:

### Using Kafka key

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orchestration-events \
  --from-beginning \
  | jq 'select(.workflow_execution_id == "abc-123")'
```

### Using Grafana / Loki

```logql
{container="workers"} | json | workflow_execution_id = "abc-123"
```

---

## gRPC Event Streaming

```python
import grpc
from workflow_service_pb2_grpc import WorkflowOrchestratorStub
from workflow_service_pb2 import StreamRequest

channel = grpc.insecure_channel("localhost:50051")
stub = WorkflowOrchestratorStub(channel)

for event in stub.StreamWorkflowEvents(
    StreamRequest(workflow_execution_id="abc-123")
):
    print(event.event_type, event.payload)
```
