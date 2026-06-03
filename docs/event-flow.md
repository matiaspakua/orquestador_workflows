# Orchestrator Event Flow

## Kafka Topic

All orchestration events are published to the `orchestration-events` topic.

**Kafka key**: `workflow_execution_id` (UUID) — guarantees ordering per execution instance.

---

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
| `StepFailed` | A step fails | `step_id`, `error_message`, `error_code`, `attempt`, `will_retry`, `next_retry_in_seconds` |
| `WorkflowCompleted` | All steps complete | `result`, `total_duration_ms`, `steps_completed`, `steps_total` |
| `WorkflowFailed` | Workflow terminates on error | `error`, `error_code`, `failed_step_id`, `failed_step_name`, `total_duration_ms`, `steps_completed` |

---

## Correlation: Tracing a Single Workflow

Every event carries `workflow_execution_id` in the envelope. To trace a complete workflow execution:

### Using Kafka key

All events for execution `abc-123` are written with Kafka key `abc-123`. Since they all go to the same partition, they appear in strict chronological order.

```bash
# Consume all events for a specific execution
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orchestration-events \
  --from-beginning \
  | jq 'select(.workflow_execution_id == "abc-123")'
```

### Using the database log

If events are mirrored to the `orchestration_events` PostgreSQL table:

```sql
SELECT event_type, step_name, timestamp, payload
FROM orchestration_events
WHERE workflow_execution_id = 'abc-123'
ORDER BY timestamp;
```

### Using Grafana / Loki

Filter logs by the structured field `workflow_execution_id`:

```logql
{container="producer"} | json | workflow_execution_id = "abc-123"
```

---

## Envelope Structure (all events)

```json
{
  "event_type": "WorkflowStarted",
  "schema_version": 1,
  "workflow_id": "<definition UUID>",
  "workflow_execution_id": "<execution UUID>",
  "workflow_definition_id": "<definition UUID>",
  "step_id": null,
  "step_name": null,
  "timestamp": "2026-06-03T12:00:00.000Z",
  "payload": { "...event-specific fields..." }
}
```
