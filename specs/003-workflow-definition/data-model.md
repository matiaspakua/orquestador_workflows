# Data Model: Workflow Definition

## Workflow Definition

The static specification of a workflow, stored in PostgreSQL and referenced by all executions.

```json
{
  "id": "UUID",
  "name": "string",
  "version": "semver string",
  "description": "string (optional)",
  "steps": [
    {
      "$ref": "#/definitions/WorkflowStep"
    }
  ],
  "input_schema": {
    "type": "object",
    "nullable": true
  },
  "timeout_seconds": "integer (optional, default: null — no workflow-level timeout)",
  "tags": {
    "string": "string (optional, key-value metadata)"
  },
  "created_at": "ISO8601 timestamp",
  "updated_at": "ISO8601 timestamp"
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | UUID | Yes | Persistent identifier, generated on creation |
| name | string | Yes | Human-readable name (unique within a version) |
| version | semver | Yes | Semantic version for the definition |
| description | string | No | Free-text description of the workflow |
| steps | array[WorkflowStep] | Yes | Ordered list of step definitions (at least 1) |
| input_schema | JSON Schema or null | No | Schema for validating workflow input at submission |
| timeout_seconds | integer or null | No | Max total execution time before workflow is auto-failed |
| tags | map[string]string | No | Arbitrary key-value metadata for classification |
| created_at | timestamp | Yes | Creation timestamp |
| updated_at | timestamp | Yes | Last modification timestamp |

---

## Workflow Execution

A single running instance of a workflow definition. Created when a workflow is submitted and tracks state through the lifecycle.

```json
{
  "id": "UUID",
  "workflow_definition_id": "UUID",
  "workflow_definition_version": "semver string",
  "status": "enum (Pending, Running, Completed, Failed, Cancelled)",
  "current_step_id": "UUID or null",
  "input": "JSON",
  "result": "JSON or null",
  "error": "text or null",
  "attempts": "integer (default: 1)",
  "started_at": "ISO8601 timestamp or null",
  "completed_at": "ISO8601 timestamp or null",
  "created_at": "ISO8601 timestamp"
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | UUID | Yes | Execution identifier, unique per run |
| workflow_definition_id | UUID | Yes | Foreign key → Workflow Definition |
| workflow_definition_version | semver | Yes | Snapshot of definition version at submission time |
| status | enum | Yes | Current lifecycle state |
| current_step_id | UUID or null | No | Currently executing step (null if not started or done) |
| input | JSON | Yes | Input data for this execution |
| result | JSON or null | No | Output result (set on completion) |
| error | text or null | No | Error message (set on failure) |
| attempts | integer | Yes | Number of execution attempts (for retries) |
| started_at | timestamp | No | When execution transitioned to Running |
| completed_at | timestamp | No | When execution reached terminal state |
| created_at | timestamp | Yes | When execution was created (Pending state) |

---

## Workflow Step (in a definition)

A single unit of work within a workflow definition.

```json
{
  "id": "UUID",
  "name": "string",
  "type": "enum (Task, Decision, Parallel, Wait)",
  "depends_on": ["UUID (step IDs)"],
  "timeout_seconds": "integer (default: 300)",
  "retry_policy": {
    "max_attempts": "integer (default: 1)",
    "backoff_seconds": "integer (default: 5)"
  },
  "config": "JSON (type-specific)",
  "on_success": "string (step ID or '__end__')",
  "on_failure": "string (step ID, '__fail__', or '__retry__')",
  "description": "string (optional)",
  "input_mapping": "JSON or null (optional)"
}
```

### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| id | UUID | Yes | — | Unique within the workflow definition |
| name | string | Yes | — | Human-readable step name |
| type | enum | Yes | — | Step type determining execution behavior |
| depends_on | array[UUID] | No | [] | Step IDs that must complete before this step starts |
| timeout_seconds | integer | No | 300 | Max execution time for this step |
| retry_policy | object | No | {"max_attempts": 1, "backoff_seconds": 5} | Retry configuration |
| config | JSON | Yes | — | Type-specific configuration (see below) |
| on_success | string | No | "__end__" | Next step on success, or sentinel |
| on_failure | string | No | "__fail__" | Next step on failure, or sentinel |
| description | string | No | — | Free-text description |
| input_mapping | JSON | No | null | Maps workflow input/previous step outputs to this step's input |

### Type-Specific Config

#### Task

```json
{
  "type": "Task",
  "config": {
    "action": "string (e.g., 'kafka:produce', 'http:call', 'python:script')",
    "target": "string (topic, URL, or script reference)",
    "payload_template": "JSON (optional, with {{mustache}} variable substitution)"
  }
}
```

#### Decision

```json
{
  "type": "Decision",
  "config": {
    "condition": "string (expression syntax, e.g., '{{result.status}} == \"approved\"')",
    "branches": {
      "true": "step ID or '__end__'",
      "false": "step ID or '__end__'"
    }
  }
}
```

#### Parallel

```json
{
  "type": "Parallel",
  "config": {
    "branches": [
      {
        "steps": ["array of inline WorkflowStep definitions"],
        "completion_policy": "enum (all, any, one) default: all"
      }
    ]
  }
}
```

#### Wait

```json
{
  "type": "Wait",
  "config": {
    "duration_seconds": "integer (if time-based)",
    "condition": "string (optional, expression for conditional wait)",
    "poll_interval_seconds": "integer (default: 10, for condition-based waits)"
  }
}
```

---

## Orchestration Event (Kafka message)

A message published to the `orchestration-events` Kafka topic representing a state change.

```json
{
  "event_type": "enum",
  "schema_version": "integer (default: 1)",
  "workflow_id": "UUID",
  "workflow_execution_id": "UUID",
  "workflow_definition_id": "UUID",
  "step_id": "UUID or null",
  "step_name": "string or null",
  "timestamp": "ISO8601 timestamp",
  "payload": "JSON (event-specific)"
}
```

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| event_type | enum | Yes | One of the 6 event types |
| schema_version | integer | Yes | Schema version for future evolution |
| workflow_id | UUID | Yes | Workflow definition ID |
| workflow_execution_id | UUID | Yes | Specific execution instance ID |
| workflow_definition_id | UUID | Yes | Reference to the definition |
| step_id | UUID or null | No | Step ID (null for workflow-level events) |
| step_name | string or null | No | Step name for human readability |
| timestamp | ISO8601 | Yes | When the event occurred |
| payload | JSON | Yes | Event-specific data (see contracts) |

Kafka key: `workflow_execution_id` (UUID) — ensures ordering per execution.

---

## Workflow Status

The lifecycle status of an execution with valid transitions.

### States

| State | Terminal | Description |
|-------|----------|-------------|
| Pending | No | Submitted but not yet started |
| Running | No | Actively executing steps |
| Completed | Yes | All steps executed successfully |
| Failed | Yes | Execution terminated due to error |
| Cancelled | Yes | Execution aborted by operator |

### Valid Transitions

```
Pending   → Running     (workflow accepted and started)
Pending   → Cancelled   (cancelled before execution begins)
Running   → Completed   (all steps succeed)
Running   → Failed      (step fails, no retry, or timeout)
Running   → Cancelled   (cancelled during execution)
```

**No other transitions are valid.** Terminal states (Completed, Failed, Cancelled) are immutable.

---

## Entity Relationship Diagram (Text)

```
WorkflowDefinition 1 ──── * WorkflowExecution
                        │
                        │ * (via FK workflow_definition_id)
                        ▼
                  WorkflowExecution 1 ──── * OrchestrationEvent
                                            (logged for tracing)

WorkflowDefinition 1 ──── * WorkflowStep
                        │
                        │ * (steps array embedded in definition)
                        ▼
                  WorkflowStep * ──── * WorkflowStep
                        │               (depends_on self-reference
                        │                for sequencing)
                        ▼
                  OrchestrationEvent (step_id references WorkflowStep.id)
```

---

## Persistence Strategy

| Entity | Store | Key | Notes |
|--------|-------|-----|-------|
| Workflow Definition | PostgreSQL | id (UUID) | Versioned, immutable after creation |
| Workflow Execution | PostgreSQL | id (UUID) | Updated as lifecycle progresses |
| Workflow Step | Embedded in Definition | id (UUID) | Stored as JSONB in definition record |
| Orchestration Event | Kafka topic + optional DB log | composite | Kafka is source of truth; DB log for querying |

PostgreSQL stores the authoritative state for definitions and executions. Kafka events provide the ordered event log for tracing and replay.
