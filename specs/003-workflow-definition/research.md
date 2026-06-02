# Research: Workflow Definition Design Decisions

## Workflow State Machine

- **Decision**: Finite state machine with 5 states (Pending, Running, Completed, Failed, Cancelled)
- **Rationale**: Covers all execution outcomes without unnecessary complexity; matches common workflow patterns (Temporal, AWS Step Functions)
- **Alternatives**:
  - 7-state model (adds Suspended, Retrying) — overkill for initial version; adds exponential complexity to transition matrix
  - 4-state model (omits Cancelled) — incomplete; operators need ability to abort workflows
  - BPMN standard states — too many states for an internal orchestrator; designed for human-in-the-loop processes

### State Transition Diagram

```
                  ┌─────────────┐
                  │   Pending   │
                  └──────┬──────┘
                         │ submit
                         ▼
                  ┌─────────────┐
          ┌────── │   Running   │ ──────┐
          │       └──────┬──────┘       │
          │              │              │
          │              ├── complete   │
          │              ▼              │
          │       ┌─────────────┐       │
          │       │  Completed  │       │
          │       └─────────────┘       │
          │                             │
          │              ┌─────────────┐│
          ├── cancel ──▶ │  Cancelled  ││
          │              └─────────────┘│
          │                             │
          │  ┌──────────────────────┐   │
          │  │   timeout / error    │   │
          └──│                      │───┘
             ▼                      ▼
      ┌─────────────┐       ┌─────────────┐
      │   Failed    │       │   Failed    │
      └─────────────┘       └─────────────┘
```

### Valid Transitions

| From | To | Trigger |
|------|----|---------|
| Pending | Running | Workflow submitted and accepted |
| Pending | Cancelled | Workflow cancelled before execution |
| Running | Completed | All steps succeed |
| Running | Failed | Step fails (no retry) or timeout |
| Running | Cancelled | Workflow cancelled mid-execution |

### Invalid Transitions (explicitly prohibited)

- Pending → Completed (must run first)
- Running → Pending (no reversion)
- Completed → any state (terminal)
- Failed → any state (terminal)
- Cancelled → any state (terminal)

---

## Step Types

- **Decision**: 4 core types — Task, Decision, Parallel, Wait
- **Rationale**: Covers 80%+ of workflow patterns per SC-003; extensible design for future types
- **Alternatives**:
  - Single step type with routing config — less expressive; forces all branching logic into conditionals
  - BPMN standard — too heavy for internal orchestrator; steep learning curve for developers
  - 6-step model (adds SubWorkflow, HumanTask) — premature; can be added later via extensible type registry

### Step Type Behaviors

| Type | Behavior | Example Use Case |
|------|----------|-----------------|
| Task | Execute a unit of work (produce Kafka message, call external system) | "Send email notification" |
| Decision | Evaluate a condition and route to one of two branches | "If payment > $100, require approval" |
| Parallel | Execute multiple child steps concurrently; wait for all to complete | "Validate address AND check inventory simultaneously" |
| Wait | Pause execution for a duration or until a condition is met | "Wait 30 minutes before escalation" |

### Connection Rules

- **Sequential**: Step A depends_on Step B → A runs after B completes
- **Parallel**: Multiple steps declare no dependency on each other → run concurrently
- **Conditional branching**: Decision step uses on_success/on_failure to route to different downstream steps
- **Fan-out/Fan-in**: Parallel step contains child steps; parent completes when all children complete

---

## Event Schema

- **Decision**: JSON Schema with Kafka key as workflow_id, value as event payload
- **Rationale**: Extends existing producer/consumer message patterns; JSON Schema for validation
- **Alternatives**:
  - Avro/Protobuf — schema registry dependency adds infrastructure complexity; overkill for initial design
  - Plain JSON — no validation; risks silent contract breakage
  - CloudEvents — standardized but adds spec compliance burden; can adopt later if needed

### Kafka Topic Design

| Topic | Partition Key | Message Type | Description |
|-------|---------------|--------------|-------------|
| `orchestration-events` | workflow_id (UUID) | OrchestrationEvent | All workflow lifecycle events |

Single topic with workflow_id as partition key ensures events for a given workflow are ordered. Event type is embedded in the payload, allowing consumers to filter by type.

---

## Orchestrator Architecture

- **Decision**: Event-driven orchestrator (no REST API for internal orchestration)
- **Rationale**: Aligns with existing Kafka backbone; enables async, fault-tolerant coordination
- **Alternatives**:
  - REST/gRPC orchestrator — synchronous, tighter coupling; adds HTTP infrastructure
  - Database-polling orchestrator — simpler but less responsive; polling latency is non-deterministic

### Architecture Overview

```
Producer ──▶ Kafka ──▶ Orchestrator ──▶ Kafka ──▶ Consumer
               ▲          │                          │
               │          │                          │
               └──────────┴──────────────────────────┘
                     orchestration-events topic
```

The orchestrator consumes workflow submission events from producers, manages execution state, and emits lifecycle events that consumers react to. All coordination happens through Kafka — no direct HTTP calls between services.

---

## Validation Strategy

- **Decision**: Two-phase validation (definition validation at submission + runtime validation during execution)
- **Rationale**: Catch errors early (definition validation) while still handling runtime failures gracefully
- **Alternatives**:
  - Single-phase (definition only) — misses runtime errors like missing dependencies
  - Single-phase (runtime only) — wastes resources on invalid definitions

### Definition Validation Rules

1. All step IDs are unique within a workflow
2. No circular dependencies (detected via cycle detection algorithm)
3. All referenced step IDs in depends_on exist
4. Step names are non-empty strings
5. Timeout values are positive integers
6. Retry policy has valid max_attempts (>= 1) and backoff_seconds (>= 0)
7. Decision steps have exactly 2 branches (on_success, on_failure) or explicit targets
8. Parallel steps have at least 2 child steps
9. Config is valid per step type (validated against type-specific schema)
10. Workflow has at least 1 step

### Runtime Validation

1. Step execution matches declared type behavior
2. Timeout triggers at configured duration
3. Retry policy respected on failure
4. Concurrent step limits enforced
5. Resource contention detected and queued

---

## Decision Log

| # | Decision | Date | Rationale |
|---|----------|------|-----------|
| 1 | 5-state lifecycle | 2026-06-02 | Covers all outcomes; matches industry patterns |
| 2 | 4 step types | 2026-06-02 | 80% coverage; extensible |
| 3 | JSON Schema for events | 2026-06-02 | Zero new infra; format consistency |
| 4 | Event-driven orchestrator | 2026-06-02 | Aligns with Kafka backbone |
| 5 | Two-phase validation | 2026-06-02 | Catch early + handle runtime |
| 6 | Single orchestration topic | 2026-06-02 | Ordered events per workflow_id |

---

## Open Questions

1. Should the orchestrator support workflow-level timeout (max execution duration) in addition to per-step timeout? — Deferred to implementation phase.
2. How should concurrent workflows competing for the same resource be serialized? — Tentative: FIFO queue per resource, configurable concurrency limit.
3. Should orchestration events have a schema version field for future evolution? — Yes, add `schema_version: 1` to all event payloads.
