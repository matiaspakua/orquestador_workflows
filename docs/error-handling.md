# Error Handling and Retry Policy

## Step-Level Retry

Every step has an optional `retry_policy` object:

```json
{
  "retry_policy": {
    "max_attempts": 3,
    "backoff_seconds": 5
  }
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `max_attempts` | 1 | Maximum number of execution attempts (including the first) |
| `backoff_seconds` | 5 | Fixed wait between attempts |

### Retry behaviour

1. Step executes (attempt 1).
2. If it fails and `max_attempts` > 1, the orchestrator waits `backoff_seconds` then re-enqueues.
3. After `max_attempts` exhausted, the step transitions to **Failed**.
4. The step's `on_failure` routing then applies.

### on_failure routing

| Value | Behaviour |
|-------|-----------|
| `__fail__` (default) | Fail the entire workflow |
| `__retry__` | Retry the same step (use `retry_policy` to bound) |
| `<step_id>` | Route to a specific compensating step |

---

## Workflow-Level Failure Handling

When any step reaches **Failed** with `on_failure: __fail__`, the orchestrator:

1. Emits a `StepFailed` event.
2. Cancels all in-flight parallel branches (best-effort).
3. Transitions the workflow execution to **Failed**.
4. Emits a `WorkflowFailed` event with the `failed_step_id` and `error_code`.

No further steps are executed after the workflow enters **Failed**.

---

## Timeout Escalation

Timeouts operate at two levels:

### Step timeout

Controlled by `timeout_seconds` per step (default: 300s).

1. Step starts; orchestrator starts a countdown timer.
2. If the timer expires before the step completes, the orchestrator:
   - Interrupts the step.
   - Sets step status to **Failed** with `error_code: TIMEOUT`.
   - Applies normal `on_failure` routing.

### Workflow timeout

Set at definition level via `timeout_seconds` (optional, default: no limit).

1. Workflow starts; orchestrator starts a global timer.
2. If the timer expires while the workflow is **Running**:
   - All in-flight steps are interrupted.
   - Workflow transitions to **Failed** with `error_code: WORKFLOW_TIMEOUT`.
   - `WorkflowFailed` event emitted.

---

## Error Codes

| Code | Trigger |
|------|---------|
| `STEP_ERROR` | Step raised an unhandled exception |
| `TIMEOUT` | Step exceeded `timeout_seconds` |
| `WORKFLOW_TIMEOUT` | Workflow exceeded workflow-level `timeout_seconds` |
| `VALIDATION_ERROR` | Step input failed schema validation |
| `DEPENDENCY_FAILED` | A `depends_on` step failed and no routing was defined |
