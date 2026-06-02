# Orchestration Events Contract

## Overview

All orchestration events are published to the `orchestration-events` Kafka topic. The Kafka key is the `workflow_execution_id` (UUID), ensuring all events for a given execution are ordered within a partition.

## Common Envelope

Every event shares this envelope structure. Event-specific data goes in the `payload` field.

```json
{
  "event_type": "string",
  "schema_version": 1,
  "workflow_id": "UUID",
  "workflow_execution_id": "UUID",
  "workflow_definition_id": "UUID",
  "step_id": "UUID or null",
  "step_name": "string or null",
  "timestamp": "ISO8601 string"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| event_type | string | Yes | Discriminator — one of the 6 event types |
| schema_version | integer | Yes | Schema version for future evolution (currently 1) |
| workflow_id | UUID | Yes | Workflow definition ID |
| workflow_execution_id | UUID | Yes | Specific execution instance |
| workflow_definition_id | UUID | Yes | FK to the workflow definition |
| step_id | UUID or null | Yes | Null for workflow-level events |
| step_name | string or null | Yes | Human-readable, null for workflow-level events |
| timestamp | ISO8601 | Yes | Event occurrence time |

---

## Event Type: WorkflowStarted

Published when a workflow execution transitions from Pending → Running.

### Payload Schema

```json
{
  "type": "object",
  "required": ["input"],
  "properties": {
    "input": {
      "type": "object",
      "description": "Workflow input data as submitted"
    },
    "definition_name": {
      "type": "string",
      "description": "Workflow definition name"
    },
    "definition_version": {
      "type": "string",
      "description": "Workflow definition version (semver)"
    }
  }
}
```

### Example

```json
{
  "event_type": "WorkflowStarted",
  "schema_version": 1,
  "workflow_id": "a1b2c3d4-...",
  "workflow_execution_id": "e5f6g7h8-...",
  "workflow_definition_id": "i9j0k1l2-...",
  "step_id": null,
  "step_name": null,
  "timestamp": "2026-06-02T10:00:00Z",
  "payload": {
    "input": { "order_id": "ORD-123", "amount": 250.00 },
    "definition_name": "order-processing",
    "definition_version": "1.0.0"
  }
}
```

---

## Event Type: StepStarted

Published when the orchestrator begins executing a step.

### Payload Schema

```json
{
  "type": "object",
  "required": ["step_id", "step_name", "step_type"],
  "properties": {
    "step_id": {
      "type": "string",
      "format": "uuid",
      "description": "Step definition ID"
    },
    "step_name": {
      "type": "string",
      "description": "Step name from definition"
    },
    "step_type": {
      "type": "string",
      "enum": ["Task", "Decision", "Parallel", "Wait"],
      "description": "Type of step being executed"
    },
    "attempt": {
      "type": "integer",
      "minimum": 1,
      "description": "Retry attempt number (1 for first attempt)"
    },
    "input": {
      "type": "object",
      "description": "Step input after input_mapping resolution"
    }
  }
}
```

### Example

```json
{
  "event_type": "StepStarted",
  "schema_version": 1,
  "workflow_id": "a1b2c3d4-...",
  "workflow_execution_id": "e5f6g7h8-...",
  "workflow_definition_id": "i9j0k1l2-...",
  "step_id": "m3n4o5p6-...",
  "step_name": "validate-order",
  "timestamp": "2026-06-02T10:00:01Z",
  "payload": {
    "step_id": "m3n4o5p6-...",
    "step_name": "validate-order",
    "step_type": "Task",
    "attempt": 1,
    "input": { "order_id": "ORD-123" }
  }
}
```

---

## Event Type: StepCompleted

Published when a step finishes successfully.

### Payload Schema

```json
{
  "type": "object",
  "required": ["step_id", "output", "duration_ms"],
  "properties": {
    "step_id": {
      "type": "string",
      "format": "uuid"
    },
    "output": {
      "type": "object",
      "description": "Step output data"
    },
    "duration_ms": {
      "type": "integer",
      "minimum": 0,
      "description": "Step execution duration in milliseconds"
    },
    "next_step_id": {
      "type": "string",
      "format": "uuid",
      "description": "ID of the next step to execute (resolved from on_success)"
    }
  }
}
```

### Example

```json
{
  "event_type": "StepCompleted",
  "schema_version": 1,
  "workflow_id": "a1b2c3d4-...",
  "workflow_execution_id": "e5f6g7h8-...",
  "workflow_definition_id": "i9j0k1l2-...",
  "step_id": "m3n4o5p6-...",
  "step_name": "validate-order",
  "timestamp": "2026-06-02T10:00:03Z",
  "payload": {
    "step_id": "m3n4o5p6-...",
    "output": { "valid": true, "risk_level": "low" },
    "duration_ms": 2150,
    "next_step_id": "q7r8s9t0-..."
  }
}
```

---

## Event Type: StepFailed

Published when a step fails (after exhausting retries, or on non-retryable failure).

### Payload Schema

```json
{
  "type": "object",
  "required": ["step_id", "error_message", "attempt"],
  "properties": {
    "step_id": {
      "type": "string",
      "format": "uuid"
    },
    "error_message": {
      "type": "string",
      "description": "Human-readable error description"
    },
    "error_code": {
      "type": "string",
      "description": "Machine-readable error code (e.g., 'TIMEOUT', 'INVALID_INPUT', 'EXECUTION_ERROR')"
    },
    "attempt": {
      "type": "integer",
      "minimum": 1,
      "description": "Which attempt this failure occurred on"
    },
    "will_retry": {
      "type": "boolean",
      "description": "Whether the orchestrator will retry this step"
    },
    "next_retry_in_seconds": {
      "type": "integer",
      "description": "Seconds until next retry (only if will_retry is true)"
    }
  }
}
```

### Example (with retry)

```json
{
  "event_type": "StepFailed",
  "schema_version": 1,
  "workflow_id": "a1b2c3d4-...",
  "workflow_execution_id": "e5f6g7h8-...",
  "workflow_definition_id": "i9j0k1l2-...",
  "step_id": "m3n4o5p6-...",
  "step_name": "validate-order",
  "timestamp": "2026-06-02T10:00:04Z",
  "payload": {
    "step_id": "m3n4o5p6-...",
    "error_message": "External service timeout after 30s",
    "error_code": "TIMEOUT",
    "attempt": 1,
    "will_retry": true,
    "next_retry_in_seconds": 5
  }
}
```

### Example (final failure)

```json
{
  "event_type": "StepFailed",
  "schema_version": 1,
  "workflow_id": "a1b2c3d4-...",
  "workflow_execution_id": "e5f6g7h8-...",
  "workflow_definition_id": "i9j0k1l2-...",
  "step_id": "m3n4o5p6-...",
  "step_name": "validate-order",
  "timestamp": "2026-06-02T10:00:15Z",
  "payload": {
    "step_id": "m3n4o5p6-...",
    "error_message": "External service timeout after 30s",
    "error_code": "TIMEOUT",
    "attempt": 3,
    "will_retry": false,
    "next_retry_in_seconds": null
  }
}
```

---

## Event Type: WorkflowCompleted

Published when the workflow reaches the Completed state (all steps successful).

### Payload Schema

```json
{
  "type": "object",
  "required": ["result", "total_duration_ms"],
  "properties": {
    "result": {
      "type": "object",
      "description": "Final workflow output (aggregated from steps)"
    },
    "total_duration_ms": {
      "type": "integer",
      "minimum": 0,
      "description": "Total execution duration in milliseconds"
    },
    "steps_completed": {
      "type": "integer",
      "description": "Total number of steps that were executed"
    },
    "steps_total": {
      "type": "integer",
      "description": "Total number of steps in the definition"
    }
  }
}
```

### Example

```json
{
  "event_type": "WorkflowCompleted",
  "schema_version": 1,
  "workflow_id": "a1b2c3d4-...",
  "workflow_execution_id": "e5f6g7h8-...",
  "workflow_definition_id": "i9j0k1l2-...",
  "step_id": null,
  "step_name": null,
  "timestamp": "2026-06-02T10:00:30Z",
  "payload": {
    "result": { "order_id": "ORD-123", "status": "approved", "confirmation": "CNF-456" },
    "total_duration_ms": 30000,
    "steps_completed": 5,
    "steps_total": 5
  }
}
```

---

## Event Type: WorkflowFailed

Published when the workflow reaches the Failed state (step failure with no retry, timeout, or cancellation not requested).

### Payload Schema

```json
{
  "type": "object",
  "required": ["error", "failed_step_id"],
  "properties": {
    "error": {
      "type": "string",
      "description": "Root cause error message"
    },
    "error_code": {
      "type": "string",
      "description": "Machine-readable error code"
    },
    "failed_step_id": {
      "type": "string",
      "format": "uuid",
      "description": "ID of the step that caused workflow failure"
    },
    "failed_step_name": {
      "type": "string",
      "description": "Name of the step that caused workflow failure"
    },
    "total_duration_ms": {
      "type": "integer",
      "description": "Duration up to failure"
    },
    "steps_completed": {
      "type": "integer",
      "description": "Number of steps completed before failure"
    }
  }
}
```

### Example

```json
{
  "event_type": "WorkflowFailed",
  "schema_version": 1,
  "workflow_id": "a1b2c3d4-...",
  "workflow_execution_id": "e5f6g7h8-...",
  "workflow_definition_id": "i9j0k1l2-...",
  "step_id": null,
  "step_name": null,
  "timestamp": "2026-06-02T10:00:15Z",
  "payload": {
    "error": "Step 'validate-order' failed after 3 retries: External service timeout after 30s",
    "error_code": "STEP_FAILED",
    "failed_step_id": "m3n4o5p6-...",
    "failed_step_name": "validate-order",
    "total_duration_ms": 15000,
    "steps_completed": 0
  }
}
```

---

## Validation Rules

All events must validate against the following rules:

| Rule | Description |
|------|-------------|
| Event type is one of the 6 defined types | WorkflowStarted, StepStarted, StepCompleted, StepFailed, WorkflowCompleted, WorkflowFailed |
| schema_version is present and is an integer | Currently must be 1 |
| workflow_execution_id is a valid UUID | v4 UUID format |
| timestamp is valid ISO8601 | Including timezone (UTC recommended) |
| payload contains all required fields for the event type | Per schema above |
| step_id is present for step-level events | StepStarted, StepCompleted, StepFailed |
| step_id is null for workflow-level events | WorkflowStarted, WorkflowCompleted, WorkflowFailed |
| duration_ms is non-negative | For StepCompleted, WorkflowCompleted, WorkflowFailed |

---

## Consumer Guidance

Services consuming orchestration events should:

1. **Filter by event_type** to handle only relevant events
2. **Use workflow_execution_id (Kafka key)** to group events by execution
3. **Use timestamp** to order events within a batch (though Kafka guarantees partition ordering)
4. **Ignore unknown fields** — future schema versions may add fields
5. **Validate schema_version** — handle future versions gracefully
6. **Handle duplicates** — Kafka delivery semantics may redeliver; consumers should be idempotent
