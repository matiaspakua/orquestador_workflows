---
description: "Design-definition of workflow lifecycle, step types, event schemas, and validation rules for the orchestrator"
---

# Tasks: Workflow Definition

**Input**: Design documents from `specs/003-workflow-definition/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/orchestration-events.md, checklists/requirements.md (quality gate — MUST pass before implementation)

**Tests**: No runtime code — tests deferred to implementation phase (orchestrator service). Validation rules in JSON Schema serve as contract tests.

**Organization**: Tasks are grouped by user story to enable independent delivery of each design increment.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- **`depends_on:`**: Explicit dependency annotation listing tasks that must be completed first
- Include exact file paths in descriptions

## Path Conventions

- **Documentation**: `docs/` at repository root — workflow lifecycle, step types, connection patterns, event flow
- **Schemas**: `docs/schemas/` — JSON Schema files for event validation
- **Event schemas**: `docs/schemas/events/` — per-event-type JSON Schema files
- **Examples**: `docs/examples/` — workflow definition examples
- Design phase only — no source code changes in `consumer/`, `producer/`, or `ui/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and documentation structure

- [ ] T001 Create `docs/schemas/` directory structure at `docs/schemas/` with `docs/schemas/events/` subdirectory for event JSON Schema files
- [ ] T002 [P] Create `docs/examples/` directory at `docs/examples/` for workflow definition example files

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core documentation and schemas that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T003 `depends_on: T001` Create workflow lifecycle state machine documentation in `docs/workflow-lifecycle.md` with ASCII state diagram showing all 5 states (Pending, Running, Completed, Failed, Cancelled) and valid transitions (Pending→Running, Pending→Cancelled, Running→Completed, Running→Failed, Running→Cancelled)
- [ ] T004 [P] `depends_on: T001` Create JSON Schema for WorkflowDefinition in `docs/schemas/workflow-definition.json` with `name`, `version`, `steps` array, `input_schema`, `timeout_seconds`, and `tags` fields per data-model.md
- [ ] T005 [P] `depends_on: T001` Create JSON Schema for WorkflowExecution in `docs/schemas/workflow-execution.json` with status transitions (`Pending`/`Running`/`Completed`/`Failed`/`Cancelled`), result tracking, error field, and timing fields per data-model.md

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Workflow Lifecycle Definition (Priority: P1) 🎯 MVP

**Goal**: Define the workflow lifecycle with all valid state transitions, entry/exit conditions, error handling, and retry policies — the foundational contract for the entire orchestrator

**Independent Test**: Review the lifecycle documentation against a reference workflow execution and verify every transition described is observable in the state model

### Implementation for User Story 1

- [ ] T006 [P] [US1] `depends_on: T003` Document valid state transitions with entry conditions and exit conditions for each state in `docs/workflow-lifecycle.md` (Pending: submit trigger, Running: accepted, Completed: all steps succeed, Failed: error condition, Cancelled: operator abort)
- [ ] T007 [P] [US1] `depends_on: T003, T006` Create workflow submission validation rules in `docs/workflow-lifecycle.md` covering rejection of invalid step definitions, circular dependency detection, and step ID uniqueness
- [ ] T008 [US1] `depends_on: T006` Create error handling and retry policy documentation in `docs/error-handling.md` covering step-level retry with `max_attempts` and `backoff_seconds`, workflow-level failure handling, and timeout escalation

**Checkpoint**: At this point, User Story 1 should be fully documented and independently reviewable

---

## Phase 4: User Story 2 — Workflow Step Types (Priority: P2)

**Goal**: Define all supported step types (Task, Decision, Parallel, Wait), their execution behavior, connection patterns, and timeout configuration

**Independent Test**: Create a sample workflow using each defined step type and verify the documented behavior covers all expected execution patterns

### Implementation for User Story 2

- [ ] T009 [P] [US2] `depends_on: T001` Document Task step type with execution behavior, `action` types (`kafka:produce`, `http:call`, `python:script`), `target`, and `payload_template` in `docs/step-types.md`
- [ ] T010 [P] [US2] `depends_on: T001` Document Decision step type with condition evaluation syntax (`{{mustache}}` expressions), `branches` configuration (true/false targets), and routing behavior in `docs/step-types.md`
- [ ] T011 [P] [US2] `depends_on: T001` Document Parallel step type with concurrent execution behavior, `branches` configuration with inline child steps, and `completion_policy` (all/any/one) in `docs/step-types.md`
- [ ] T012 [P] [US2] `depends_on: T001` Document Wait step type with time-based (`duration_seconds`) and condition-based (`condition` expression with `poll_interval_seconds`) unblocking behavior in `docs/step-types.md`
- [ ] T013 [US2] `depends_on: T009, T010, T011, T012` Document step connection patterns (sequential via `depends_on`, parallel via independent steps, conditional branching via Decision states) with visual ASCII diagrams in `docs/connection-patterns.md`
- [ ] T014 [US2] `depends_on: T009` Document step timeout behavior with default value (300s), configurable overrides per step via `timeout_seconds`, and timeout → Failed transition in `docs/step-types.md`

**Checkpoint**: At this point, User Stories 1 AND 2 should both be fully documented and independently reviewable

---

## Phase 5: User Story 3 — Orchestrator Event Flow (Priority: P3)

**Goal**: Define the complete Kafka event schema for orchestrator coordination — 6 event types enabling full workflow execution tracing

**Independent Test**: Trace a workflow execution through the event log and verify each documented event type appears at the expected execution point

### Implementation for User Story 3

- [ ] T015 [P] [US3] `depends_on: T001` Create JSON Schema for `WorkflowStarted` event in `docs/schemas/events/workflow-started.json` with `input`, `definition_name`, and `definition_version` payload fields per contracts/orchestration-events.md
- [ ] T016 [P] [US3] `depends_on: T001` Create JSON Schema for `StepStarted` event in `docs/schemas/events/step-started.json` with `step_id`, `step_name`, `step_type`, `attempt`, and `input` payload fields per contracts/orchestration-events.md
- [ ] T017 [P] [US3] `depends_on: T001` Create JSON Schema for `StepCompleted` event in `docs/schemas/events/step-completed.json` with `step_id`, `output`, `duration_ms`, and `next_step_id` payload fields per contracts/orchestration-events.md
- [ ] T018 [P] [US3] `depends_on: T001` Create JSON Schema for `StepFailed` event in `docs/schemas/events/step-failed.json` with `step_id`, `error_message`, `error_code`, `attempt`, `will_retry`, and `next_retry_in_seconds` payload fields per contracts/orchestration-events.md
- [ ] T019 [P] [US3] `depends_on: T001` Create JSON Schema for `WorkflowCompleted` event in `docs/schemas/events/workflow-completed.json` with `result`, `total_duration_ms`, `steps_completed`, and `steps_total` payload fields per contracts/orchestration-events.md
- [ ] T020 [P] [US3] `depends_on: T001` Create JSON Schema for `WorkflowFailed` event in `docs/schemas/events/workflow-failed.json` with `error`, `error_code`, `failed_step_id`, `failed_step_name`, `total_duration_ms`, and `steps_completed` payload fields per contracts/orchestration-events.md
- [ ] T021 [US3] `depends_on: T015, T016, T017, T018, T019, T020` Document event flow sequence for a complete workflow execution in `docs/event-flow.md` with sequential diagram showing `WorkflowStarted → StepStarted → StepCompleted → ... → WorkflowCompleted` and failure path showing `StepFailed → WorkflowFailed`
- [ ] T022 [US3] `depends_on: T021` Add `workflow_execution_id` correlation documentation in `docs/event-flow.md` showing how to trace a single workflow through all Kafka events using the Kafka key and `workflow_execution_id` field

**Checkpoint**: All user stories should now be independently reviewable

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Complete examples, validation rule hardening, and concurrent execution documentation

- [ ] T023 `depends_on: T002` Create complete three-step workflow example in `docs/examples/three-step-workflow.json` with validate → check-amount → auto-process/require-manager flow matching SC-001 (new developer implements in 30 min) per quickstart.md
- [ ] T024 [P] `depends_on: T003, T007` Create comprehensive workflow definition validation rules document in `docs/workflow-lifecycle.md` covering all 10 definition validation rules (step ID uniqueness, circular dependency detection, step name validity, timeout values, retry policy, Decision branch count, Parallel minimum branches, per-type config validation, minimum step count)
- [ ] T025 [P] `depends_on: T003` Create documentation for concurrent workflow execution and resource contention handling in `docs/concurrent-execution.md` covering FIFO queuing per resource, configurable concurrency limits, and isolation guarantees (FR-010)
- [ ] T026 `depends_on: T023, T004, T005` Run JSON Schema validation against all example workflows in `docs/examples/` to confirm 90%+ of intentionally invalid workflow definitions are rejected before execution resources are consumed (SC-005)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories. T006/T007 (lifecycle doc) before T008 (error handling).
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) — Step type docs (T009–T012) before connection patterns (T013). Timeout docs (T014) after at least Task step type (T009).
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) — Event schemas (T015–T020) before event flow sequence (T021). Correlation docs (T022) after event flow (T021).

### Within Each User Story

- Foundational schemas/docs before story-specific additions
- Core type definitions before connection/behavior documentation
- File creator before file modifier tasks
- Story complete and reviewable before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (T004, T005)
- Once Foundational phase completes, US1, US2, and US3 can all start in parallel
- Within US2: all four step type docs (T009–T012) can run in parallel
- Within US3: all six event JSON Schema files (T015–T020) can run in parallel
- Within Polish: T024 and T025 can run in parallel

---

## Parallel Example: User Story 2

```bash
# Launch all four step type documents together:
Task: "Document Task step type in docs/step-types.md"
Task: "Document Decision step type in docs/step-types.md"
Task: "Document Parallel step type in docs/step-types.md"
Task: "Document Wait step type in docs/step-types.md"
```

## Parallel Example: User Story 3

```bash
# Launch all six event JSON Schema files together:
Task: "Create JSON Schema for WorkflowStarted in docs/schemas/events/workflow-started.json"
Task: "Create JSON Schema for StepStarted in docs/schemas/events/step-started.json"
Task: "Create JSON Schema for StepCompleted in docs/schemas/events/step-completed.json"
Task: "Create JSON Schema for StepFailed in docs/schemas/events/step-failed.json"
Task: "Create JSON Schema for WorkflowCompleted in docs/schemas/events/workflow-completed.json"
Task: "Create JSON Schema for WorkflowFailed in docs/schemas/events/workflow-failed.json"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T002)
2. Complete Phase 2: Foundational (T003–T005)
3. Complete Phase 3: User Story 1 (T006–T008)
4. **STOP and REVIEW**: Review lifecycle docs against spec.md acceptance scenarios
5. Lifecycle definition is reviewable independently — deploy/demo if needed

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready (schemas + state machine)
2. Add User Story 1 → Lifecycle, validation, error handling docs → Review (MVP!)
3. Add User Story 2 → Step types, connections, timeout docs → Review
4. Add User Story 3 → Event schemas, flow, correlation docs → Review
5. Each story adds value without invalidating previous stories

### Parallel Team Strategy

With multiple developers:
1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (P1 — lifecycle definition)
   - Developer B: User Story 2 (P2 — step types)
   - Developer C: User Story 3 (P3 — event schemas)
3. Polish tasks (T023–T026) picked up by first available developer

---

## Notes

- [P] tasks = different files, no dependencies (or different sections of the same file that can be written independently)
- [Story] label maps task to specific user story for traceability
- Each user story should be independently reviewable and completable
- JSON Schema files in `docs/schemas/events/` map 1:1 to orchestration event types
- DOCUMENTATION ONLY — no runtime code changes in this feature
- All docs and schemas serve as input to the orchestrator implementation phase
- Review at each checkpoint to validate docs against spec.md acceptance scenarios
