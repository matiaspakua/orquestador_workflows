---
description: "Integration test suite for producer-consumer message flow via orchestrator"
---

# Tasks: Producer-Consumer Test

**Input**: Design documents from `specs/005-producer-consumer-test/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/, checklists/requirements.md (quality gate — MUST pass before implementation)

**Tests**: Test tasks included — this feature IS a test suite. Constitution Principle II mandates integration tests for inter-service contracts.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each scenario.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- **`depends_on:`**: Explicit dependency annotation listing tasks that must be completed first
- Include exact file paths in descriptions

## Path Conventions

Tests live under each component's `tests/` directory:
- **producer/tests/integration/** — Producer integration tests
- **consumer/tests/integration/** — Consumer integration tests
- **scripts/test/** — Test orchestration scripts

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic test structure

- [X] T001 Create Docker Compose test overlay (`docker-compose.test.yml`) that extends the main compose file with isolated Kafka topics and a test runner service
- [X] T002 [P] Install pytest and test dependencies in consumer/requirements.txt and producer/requirements.txt

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core test infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T003 `depends_on: T001, T002` Create shared test helpers module in producer/tests/conftest.py and consumer/tests/conftest.py with common fixtures (Kafka test client, topic manager, test data generators)
- [X] T004 `depends_on: T001` Implement Kafka test topic lifecycle manager that creates and tears down isolated topics with `test.{scenario_id}.{suffix}` pattern per the contract in contracts/message-contract.md
- [X] T005 [P] `depends_on: T001, T002` Implement TestScenario data model in producer/tests/models.py with fields for category, setup/execution/cleanup steps, and expected outcomes (per data-model.md)
- [X] T006 [P] `depends_on: T001, T002` Implement TestRun data model in consumer/tests/models.py with status tracking, timing, and result_summary fields (per data-model.md)
- [X] T007 `depends_on: T001` Create test runner script in scripts/test/run-integration.sh that orchestrates Docker Compose test execution

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Validate End-to-End Message Flow (Priority: P1) 🎯 MVP

**Goal**: Verify that a message flows correctly from producer through Kafka to consumer, with correct payload integrity and ordering

**Independent Test**: Run `pytest producer/tests/integration/test_message_flow.py consumer/tests/integration/test_consumer_message_flow.py -v` — sends a known message from producer and verifies consumer receives exact same payload

### Implementation for User Story 1

- [X] T008 [P] [US1] `depends_on: T003, T004, T005` Implement producer test harness that publishes a message with known payload and sequence number to a test topic in producer/tests/integration/test_message_flow.py
- [X] T009 [P] [US1] `depends_on: T003, T004, T006` Implement consumer test harness that subscribes to the test topic and captures received messages in consumer/tests/integration/test_consumer_message_flow.py
- [X] T010 [US1] `depends_on: T008, T009` Implement end-to-end message flow test that publishes 1 message and asserts consumer receives it within 30s (SC-001) in producer/tests/integration/test_message_flow.py
- [X] T011 [US1] `depends_on: T008, T009` Implement payload integrity test that publishes a message with structured JSON payload and asserts consumed payload matches exactly (FR-002) in producer/tests/integration/test_message_flow.py
- [X] T012 [US1] `depends_on: T008, T009` Implement message ordering test that publishes 10 messages with sequential ordering_key values to a single-partition test topic and asserts consumer receives them in the same order (FR-003) in producer/tests/integration/test_message_flow.py
- [X] T023 [US1] `depends_on: T008, T009` Implement consumer-late-join test that publishes a message before consumer starts, then starts consumer and asserts it picks up the message from Kafka (EC-1) in producer/tests/integration/test_message_flow.py

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Validate Error Handling and Recovery (Priority: P2)

**Goal**: Verify that producer and consumer handle errors gracefully — Kafka unavailability, invalid messages, network interruptions

**Independent Test**: Run `pytest producer/tests/integration/test_error_handling.py consumer/tests/integration/test_consumer_error_handling.py -v` — simulates each failure scenario in isolation and verifies graceful handling

### Implementation for User Story 2

- [X] T013 [P] [US2] `depends_on: T003, T004, T005, T007` Implement Kafka unavailable test that stops Kafka container, verifies producer retries 3 times with exponential backoff per contract retry policy (FR-004), and does not crash in producer/tests/integration/test_error_handling.py
- [X] T014 [P] [US2] `depends_on: T003, T004, T006, T007` Implement invalid message test that publishes a malformed message to the test topic and asserts consumer logs the error and continues processing (FR-005) in consumer/tests/integration/test_consumer_error_handling.py
- [X] T015 [US2] `depends_on: T003, T004, T006, T007` Implement network interruption test that simulates consumer disconnect by stopping the consumer container, verifies resume from last confirmed offset without data loss or duplication (FR-006) in consumer/tests/integration/test_consumer_error_handling.py
- [X] T024 [P] [US2] `depends_on: T003, T004, T006, T007` Implement duplicate message test that publishes the same message_id twice and asserts consumer processes it only once (EC-2, idempotency) in consumer/tests/integration/test_consumer_error_handling.py
- [X] T025 [P] [US2] `depends_on: T003, T004, T005, T007` Implement large payload test that publishes a message near the Kafka size limit and asserts consumer handles it with clear error messaging or processes it successfully (EC-3) in producer/tests/integration/test_error_handling.py

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Validate Orchestrator Coordination (Priority: P3)

**Goal**: Verify that the orchestrator correctly coordinates producer and consumer activities — start, monitor, stop, cancel workflows

**Independent Test**: Run `pytest producer/tests/integration/test_orchestrator.py consumer/tests/integration/test_consumer_orchestrator.py -v` — runs a complete workflow cycle and verifies each orchestration step

### Implementation for User Story 3

- [X] T016 [P] [US3] `depends_on: T008, T009` Implement orchestrator workflow start test that initiates a workflow, verifies producer begins publishing and consumer begins processing (FR-007) in producer/tests/integration/test_orchestrator.py
- [X] T017 [P] [US3] `depends_on: T008, T009` Implement workflow completion test that runs a full publish-consume cycle and asserts orchestrator marks workflow as Completed with persisted results (FR-007) in consumer/tests/integration/test_consumer_orchestrator.py
- [X] T018 [US3] `depends_on: T008, T009` Implement workflow cancellation test that cancels a running workflow and asserts producer stops publishing and consumer stops processing for that workflow (FR-008) in producer/tests/integration/test_orchestrator.py

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T019 [P] `depends_on: T003, T004, T006` Add performance benchmark test that verifies consumer processes 1,000 messages without data loss or corruption (SC-003) in consumer/tests/integration/test_consumer_performance.py
- [X] T020 `depends_on: T007` Configure pytest JUnit XML output for CI integration
- [X] T021 `depends_on: T004, T007` Add test isolation validation — verify test topics are cleaned up after each run and do not interfere with production topics (per quickstart.md troubleshooting)
- [X] T022 `depends_on: T001` Update docker-compose.test.yml with resource limits (per Constitution Principle IV: Performance Requirements)
- [X] T026 `depends_on: T007, T010` Add repeatability verification script in scripts/test/run-integration.sh that runs the full suite 3 times and asserts identical pass/fail results (FR-009)
- [X] T027 `depends_on: T007` Add no-manual-intervention verification test that asserts the full suite runs to completion via a single `docker-compose -f docker-compose.test.yml up --build` command without requiring any manual steps (SC-004) in scripts/test/run-integration.sh
- [X] T028 `depends_on: T007, T019` Add full-suite timing benchmark that runs all tests and asserts completion under 5 minutes with JUnit XML pass/fail output (SC-005) in scripts/test/run-integration.sh

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

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) — No dependencies on US1
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) — Depends on orchestrator integration in US1 test harness

### Within Each User Story

- Test harness before test implementation
- Simpler assertions before complex multi-step scenarios
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, US1 and US2 can start in parallel
- All tasks within a user story marked [P] can run in parallel
- Different test files within a story can be written in parallel

---

## Parallel Example: User Story 1

```bash
# Launch producer and consumer test harnesses together:
Task: "Implement producer test harness in producer/tests/integration/test_message_flow.py"
Task: "Implement consumer test harness in consumer/tests/integration/test_consumer_message_flow.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T002)
2. Complete Phase 2: Foundational (T003-T007)
3. Complete Phase 3: User Story 1 (T008-T012)
4. **STOP and VALIDATE**: Run `docker-compose -f docker-compose.test.yml up --build` to validate end-to-end flow
5. This validates the core pipeline — producer → Kafka → consumer — before adding error handling

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Validate (MVP!)
3. Add User Story 2 → Test independently → Validate
4. Add User Story 3 → Test independently → Validate
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:
1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (P1 — end-to-end message flow)
   - Developer B: User Story 2 (P2 — error handling)
3. Developer C: User Story 3 (P3 — orchestrator coordination, after US1 harness is stable)
4. Stories complete and test independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Tests MUST fail before implementation (Red-Green-Refactor per Constitution)
- Use isolated Kafka topics with `test.` prefix per contracts/message-contract.md
- Run full suite via `docker-compose -f docker-compose.test.yml up --build`
- Test results output as JUnit XML for CI pipeline consumption
