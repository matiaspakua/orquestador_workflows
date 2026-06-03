# Workflow Step Types

## Overview

Every step in a workflow definition has a `type` field that determines its execution behavior. The four supported types are: **Task**, **Decision**, **Parallel**, and **Wait**.

All steps share a common set of base fields (see `docs/schemas/workflow-definition.json`). Type-specific behavior is configured via the `config` object.

---

## Task

The most common step type. Performs a single unit of work by invoking an action.

### Action types

| Action | Description |
|--------|-------------|
| `kafka:produce` | Publishes a message to a Kafka topic |
| `http:call` | Makes an HTTP request to a URL |
| `python:script` | Executes a Python script reference |

### Config schema

```json
{
  "action": "kafka:produce | http:call | python:script",
  "target": "topic name, URL, or script path",
  "payload_template": { "...": "optional JSON with {{mustache}} substitution" }
}
```

### Timeout

Default: 300 seconds. Override per-step with `timeout_seconds`. On timeout the step transitions to Failed and the workflow-level `on_failure` policy applies.

### Example

```json
{
  "id": "step-publish-order",
  "name": "Publish order event",
  "type": "Task",
  "config": {
    "action": "kafka:produce",
    "target": "orders",
    "payload_template": { "order_id": "{{input.order_id}}", "status": "received" }
  },
  "timeout_seconds": 30
}
```

---

## Decision

Routes execution to one of two branches based on a condition expression.

### Config schema

```json
{
  "condition": "expression string using {{mustache}} syntax",
  "branches": {
    "true": "step ID or __end__",
    "false": "step ID or __end__"
  }
}
```

### Condition evaluation

Expressions use `{{mustache}}` syntax to reference workflow input and previous step outputs. Examples:

- `{{result.status}} == "approved"`
- `{{input.amount}} > 1000`

### Example

```json
{
  "id": "step-check-amount",
  "name": "Check order amount",
  "type": "Decision",
  "config": {
    "condition": "{{input.amount}} > 500",
    "branches": {
      "true": "step-require-manager",
      "false": "step-auto-process"
    }
  }
}
```

---

## Parallel

Executes multiple branches of steps concurrently.

### Config schema

```json
{
  "branches": [
    {
      "steps": [ "...inline WorkflowStep definitions..." ]
    }
  ],
  "completion_policy": "all | any | one"
}
```

### Completion policies

| Policy | Behaviour |
|--------|-----------|
| `all` (default) | Wait for every branch to complete |
| `any` | Continue when the first branch completes |
| `one` | Continue when exactly one branch completes; cancel others |

### Minimum branches

A Parallel step requires at least 2 branches. Fewer branches is a validation error.

### Example

```json
{
  "id": "step-notify-all",
  "name": "Send notifications",
  "type": "Parallel",
  "config": {
    "completion_policy": "all",
    "branches": [
      { "steps": [{ "id": "s1", "name": "Email", "type": "Task", "config": { "action": "http:call", "target": "https://email-svc/send" } }] },
      { "steps": [{ "id": "s2", "name": "SMS",   "type": "Task", "config": { "action": "http:call", "target": "https://sms-svc/send"   } }] }
    ]
  }
}
```

---

## Wait

Pauses execution until a time duration elapses or a condition becomes true.

### Config schema

```json
{
  "duration_seconds": 60,
  "condition": "optional expression string",
  "poll_interval_seconds": 10
}
```

- If `duration_seconds` is set: the step waits exactly that many seconds.
- If `condition` is set: the step polls every `poll_interval_seconds` until the expression is true.
- Both can coexist: the step continues when either criterion is satisfied first.

### Example

```json
{
  "id": "step-wait-approval",
  "name": "Wait for manager approval",
  "type": "Wait",
  "config": {
    "condition": "{{approval.status}} == \"approved\"",
    "poll_interval_seconds": 30
  },
  "timeout_seconds": 3600
}
```
