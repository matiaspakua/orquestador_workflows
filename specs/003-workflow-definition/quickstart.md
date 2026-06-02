# Quickstart: Workflow Definition

## Defining a Workflow

Workflows are defined in JSON or YAML format. The orchestrator accepts these definitions at submission time.

### Minimal Example (JSON)

```json
{
  "name": "hello-world",
  "version": "1.0.0",
  "steps": [
    {
      "id": "step-1",
      "name": "say-hello",
      "type": "Task",
      "config": {
        "action": "kafka:produce",
        "target": "events",
        "payload_template": { "message": "Hello, world!" }
      }
    }
  ]
}
```

### Minimal Example (YAML)

```yaml
name: hello-world
version: 1.0.0
steps:
  - id: step-1
    name: say-hello
    type: Task
    config:
      action: kafka:produce
      target: events
      payload_template:
        message: "Hello, world!"
```

---

## Step Types Reference

### Task

Executes a unit of work — produce a Kafka message, call an HTTP endpoint, run a script.

```yaml
type: Task
config:
  action: kafka:produce           # or http:call, python:script
  target: events                  # topic, URL, or script path
  payload_template:
    key: "{{input.order_id}}"
    value:
      status: processed
```

### Decision

Evaluates a condition and routes to one of two branches.

```yaml
type: Decision
config:
  condition: "{{steps.validate.valid}} == true"
  branches:
    true: approve-step
    false: reject-step
```

### Parallel

Runs multiple child branches concurrently. Completes when all (or any, or one) branches complete.

```yaml
type: Parallel
config:
  completion_policy: all
  branches:
    - steps:
        - id: check-inventory
          name: Check Inventory
          type: Task
          config:
            action: http:call
            target: /api/inventory/check
    - steps:
        - id: validate-address
          name: Validate Address
          type: Task
          config:
            action: http:call
            target: /api/address/validate
```

### Wait

Pauses execution for a duration or until a condition is met.

```yaml
type: Wait
config:
  duration_seconds: 60            # pause 60 seconds, or...
  condition: "{{external.event_received}} == true"
  poll_interval_seconds: 5        # check condition every 5s
```

---

## Connecting Steps

Steps are connected using three mechanisms:

| Mechanism | Field | Purpose |
|-----------|-------|---------|
| Dependency | `depends_on` | Step waits for listed steps to complete before starting |
| Success routing | `on_success` | Next step ID when step completes successfully |
| Failure routing | `on_failure` | Next step ID (or `__fail__`, `__retry__`) on failure |

### Sequential Flow

```yaml
steps:
  - id: step-a
    type: Task
    config: { action: kafka:produce, target: events }
    on_success: step-b
  - id: step-b
    type: Task
    depends_on: [step-a]
    config: { action: kafka:produce, target: events }
    on_success: __end__
```

### Conditional Flow

```yaml
steps:
  - id: check-status
    type: Decision
    config:
      condition: "{{input.amount}} > 100"
      branches:
        true: require-approval
        false: auto-approve
    on_success: __end__

  - id: require-approval
    type: Task
    depends_on: [check-status]
    config: { action: kafka:produce, target: events }
    on_success: __end__

  - id: auto-approve
    type: Task
    depends_on: [check-status]
    config: { action: kafka:produce, target: events }
    on_success: __end__
```

### Sentinel Values

| Sentinel | Meaning |
|----------|---------|
| `__end__` | Workflow ends successfully |
| `__fail__` | Workflow fails immediately |
| `__retry__` | Retry the current step |

---

## Submitting a Workflow

Submit a workflow by producing a message to the `events` Kafka topic with the following format:

```json
{
  "event_type": "WorkflowSubmission",
  "definition": { /* workflow definition JSON */ },
  "workflow_execution_id": "auto-generated UUID",
  "input": { /* workflow input data */ },
  "timestamp": "2026-06-02T10:00:00Z"
}
```

The orchestrator consumes submission events, validates the definition, creates a WorkflowExecution record, and begins processing.

---

## Tracing Execution via Kafka

Subscribe to the `orchestration-events` topic to trace workflow execution in real time.

### Using kafka-console-consumer

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orchestration-events \
  --property print.key=true \
  --property key.separator=" | "
```

### Filtering by Workflow

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orchestration-events \
  --property print.key=true \
  --partition 0 \
  --group my-trace-group
```

Filter events by `workflow_execution_id` using `jq`:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orchestration-events \
  --from-beginning 2>&1 | \
  jq 'select(.workflow_execution_id == "e5f6g7h8-...")'
```

### Expected Event Sequence

```
WorkflowStarted  →  StepStarted  →  StepCompleted  →  ...  →  WorkflowCompleted
                      (repeated for each step)
```

On failure:
```
WorkflowStarted  →  StepStarted  →  StepFailed  →  (retry)  →  WorkflowFailed
```

---

## Troubleshooting Validation Errors

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `Circular dependency detected` | Steps A→B→A forms a cycle | Remove one of the dependency links |
| `Step references unknown ID` | `depends_on` or routing references a non-existent step ID | Check step IDs match exactly |
| `Empty step name` | Step name is blank or missing | Provide a non-empty `name` for each step |
| `Invalid timeout value` | `timeout_seconds` is negative or zero | Use a positive integer (default: 300) |
| `Decision must have 2 branches` | Decision step has <2 branches in config | Define both `true` and `false` branch targets |
| `Parallel must have at least 2 branches` | Parallel step has <2 branches | Add more branches or use a different step type |
| `Step has no on_success or on_failure` | Step missing routing fields | Add `on_success` (defaults to `__end__`) |
| `Input schema validation failed` | Workflow input doesn't match `input_schema` | Check input against the defined schema |
| `Duplicate step ID` | Two steps share the same UUID | Generate unique IDs for each step |
| `Workflow has no steps` | Steps array is empty | Add at least one step |

---

## Example: 3-Step Workflow (SC-001)

This example matches Success Criterion SC-001 — a developer can implement this in under 30 minutes.

```yaml
name: order-processor
version: 1.0.0
input_schema:
  type: object
  required: [order_id, amount]
  properties:
    order_id: { type: string }
    amount: { type: number }

steps:
  - id: validate
    name: Validate Order
    type: Task
    timeout_seconds: 30
    config:
      action: http:call
      target: /api/orders/validate
      payload_template:
        order_id: "{{input.order_id}}"
    on_success: check-amount
    on_failure: __fail__

  - id: check-amount
    name: Check Amount
    type: Decision
    depends_on: [validate]
    config:
      condition: "{{steps.validate.result.amount}} > 1000"
      branches:
        true: require-manager
        false: auto-process

  - id: auto-process
    name: Auto Process
    type: Task
    depends_on: [check-amount]
    config:
      action: kafka:produce
      target: events
      payload_template:
        order_id: "{{input.order_id}}"
        status: auto-approved
    on_success: __end__

  - id: require-manager
    name: Require Manager Approval
    type: Task
    depends_on: [check-amount]
    config:
      action: kafka:produce
      target: events
      payload_template:
        order_id: "{{input.order_id}}"
        status: pending-approval
        approver: manager
    on_success: __end__
```

### Execution Flow

```
[submission] → validate → check-amount ─┬─→ auto-process → [complete]
                                         └─→ require-manager → [complete]
```

---

## Best Practices

1. **Start simple**: Begin with sequential Task steps, add branching and parallelism later
2. **Set timeouts**: Always configure `timeout_seconds` for Task steps to prevent hangs
3. **Use retries for transient failures**: Set `retry_policy.max_attempts` to 3 for external service calls
4. **Validate early**: Use `input_schema` to catch invalid submissions before execution
5. **Test with small input**: Submit workflows with minimal input data first, then scale up
6. **Trace every execution**: Subscribe to `orchestration-events` during development to verify event flow
7. **Version your definitions**: Always increment `version` when changing a workflow definition
8. **Name steps clearly**: Step names are included in events — they're your primary debugging tool
