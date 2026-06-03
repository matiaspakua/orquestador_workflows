# Concurrent Workflow Execution

## Overview

The orchestrator supports running multiple workflow executions concurrently. This document defines the guarantees, limits, and isolation strategy.

---

## FIFO Queuing Per Resource

When the concurrency limit is reached for a given resource (e.g., a Kafka topic or a downstream HTTP service), new executions targeting that resource are queued in FIFO order. They are unblocked as capacity becomes available.

```
Execution A ──► Topic X (capacity: 2) ──► processing
Execution B ──► Topic X               ──► processing
Execution C ──► Topic X               ──► QUEUED (waits for A or B)
Execution D ──► Topic Y               ──► processing (different resource, no wait)
```

---

## Configurable Concurrency Limits

Concurrency limits are configured per workflow definition:

```json
{
  "name": "order-processing",
  "version": "1.0.0",
  "max_concurrent_executions": 10,
  "steps": [ "..." ]
}
```

| Setting | Default | Description |
|---------|---------|-------------|
| `max_concurrent_executions` | unlimited | Maximum simultaneous Running instances of this definition |

When `max_concurrent_executions` is reached, new submissions enter **Pending** state and are started as existing executions complete.

---

## Isolation Guarantees

Each workflow execution is isolated by `workflow_execution_id`:

| Concern | Isolation mechanism |
|---------|---------------------|
| Kafka messages | Each execution uses its own Kafka key (`workflow_execution_id`) |
| PostgreSQL rows | Each execution has its own `workflow_executions` row and scoped `workflow_steps` rows |
| Kafka events | All `orchestration-events` messages carry `workflow_execution_id`; consumers filter by key |
| Retries | Retry counters are per-step per-execution — one execution's retry does not affect another |
| Cancellation | A cancel request targets a specific `workflow_execution_id` and does not affect sibling executions |

---

## Scheduling Fairness

The orchestrator uses a round-robin scheduler across pending executions to prevent starvation. If Execution A and Execution B are both pending, they alternate being given capacity rather than A always being drained first.

---

## Known Limitations (v1.0)

- No priority queue: all executions have equal scheduling weight.
- No per-step resource locking: two executions can target the same Kafka topic simultaneously.
- Concurrency limit is enforced per definition, not globally across all definitions.
