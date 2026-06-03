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

## Entry and Exit Conditions

### Pending
- **Entry**: Workflow submission received with valid definition
- **Exit to Running**: Orchestrator has capacity and all `depends_on` workflows (if any) are satisfied
- **Exit to Cancelled**: Operator sends cancel request

### Running
- **Entry**: All Pending pre-conditions met; first step enqueued
- **Exit to Completed**: Last step transitions to Completed with no pending branches
- **Exit to Failed**: A step transitions to Failed, `on_failure` is `__fail__`, and retry budget is exhausted (or `max_attempts` = 1)
- **Exit to Cancelled**: Operator cancel request received; in-flight steps are interrupted

### Completed / Failed / Cancelled
- **Entry**: Transition from Running or (Cancelled) from Pending
- **Exit**: None — terminal states are immutable

---

## Submission Validation Rules

A workflow submission is rejected before execution begins if any of the following are true:

1. **Step ID uniqueness**: Two or more steps share the same `id`
2. **Circular dependency**: The `depends_on` graph contains a cycle
3. **Invalid step name**: A step `name` is empty or exceeds 255 characters
4. **Invalid timeout**: A `timeout_seconds` value is ≤ 0 or is not an integer
5. **Invalid retry policy**: `max_attempts` < 1 or `backoff_seconds` < 0
6. **Decision branch count**: A Decision step has fewer than 2 branches or more than 2 branches (true/false only)
7. **Parallel minimum branches**: A Parallel step has fewer than 2 branches
8. **Per-type config missing**: A Task step is missing `action` or `target`
9. **Minimum step count**: The `steps` array is empty
10. **Dead `depends_on` reference**: A step references a `depends_on` ID that does not exist in the definition

---

## Concurrent Execution

See `docs/concurrent-execution.md` for details on resource contention and isolation guarantees.
