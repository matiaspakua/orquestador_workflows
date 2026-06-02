---
description: "Grafana dashboards for functional telemetry — component health, workflow metrics, message processing"
---

# Tasks: Grafana Telemetry

**Input**: Design documents from `specs/002-grafana-telemetry/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/metrics-contract.md

**Tests**: Test tasks are MANDATORY per the project Constitution (Principle II: Testing Standards). Integration test for `/metrics` endpoint included in Phase 6.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- **`depends_on:`**: Explicit dependency annotation listing tasks that must be completed first
- Include exact file paths in descriptions

## Path Conventions

- **producer/**: `producer/app.py`, `producer/requirements.txt`, `producer/Dockerfile`
- **consumer/**: `consumer/app.py`, `consumer/requirements.txt`, `consumer/Dockerfile`
- **Monitoring config**: `prometheus/prometheus.yml`, `grafana/provisioning/datasources/prometheus.yml`, `grafana/provisioning/dashboards/`
- **Docker Compose**: `docker-compose.yml` at repository root

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and dependency setup for Prometheus and Grafana

- [ ] T001 [P] Add `prometheus_client` to `producer/requirements.txt` and `consumer/requirements.txt`
- [ ] T002 [P] Configure Prometheus service in `docker-compose.yml` with port mapping, volumes, and network (scaffold `prometheus/prometheus.yml`)
- [ ] T003 [P] Configure Grafana service in `docker-compose.yml` with port mapping, volumes, network, and datasource provisioning directory

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core monitoring infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 [P] `depends_on: T001` Add `/metrics` HTTP endpoint using `prometheus_client` to `producer/app.py` exposing producer-specific metrics (`messages_published_total`, `errors_total`) on port 8000
- [ ] T005 [P] `depends_on: T001` Add `/metrics` HTTP endpoint using `prometheus_client` to `consumer/app.py` exposing consumer-specific metrics (`messages_consumed_total`, `consumer_lag`, `errors_total`) on port 8000
- [ ] T006 `depends_on: T002` Create `prometheus/prometheus.yml` scrape config targeting `producer:8000`, `consumer1:8000`, `consumer2:8000`
- [ ] T007 `depends_on: T003` Create Grafana datasource provisioning config in `grafana/provisioning/datasources/prometheus.yml` auto-connecting to Prometheus

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Component Health Dashboard (Priority: P1) 🎯 MVP

**Goal**: Single dashboard showing health of all components (Workflow Engine, Producer, Consumer) with healthy/unhealthy/no_data states and activity metrics

**Independent Test**: Deploy system, run workflows, open Grafana health dashboard — verify each component shows correct health indicators within 5 seconds (SC-001)

### Implementation for User Story 1

- [ ] T008 [P] [US1] `depends_on: T004` Add `health_status` gauge metric (1=healthy, 0=unhealthy) and `component_info` gauge to `producer/app.py`
- [ ] T009 [P] [US1] `depends_on: T005` Add `health_status` gauge metric (1=healthy, 0=unhealthy) and `component_info` gauge to `consumer/app.py`
- [ ] T010 [US1] `depends_on: T006, T007, T011` Create Grafana dashboard JSON `grafana/provisioning/dashboards/component-health.json` with uptime, health status, and activity panels per component
- [ ] T011 [US1] `depends_on: T007` Create Grafana dashboard provider config in `grafana/provisioning/dashboards/dashboard-providers.yml`

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Workflow Processing Metrics (Priority: P2)

**Goal**: Display total executed, running, completed, failed counts; avg/min/max duration with trends; error rates over time — all reflecting real-time state within 10 seconds (SC-002)

**Independent Test**: Execute workflows with known characteristics, open Grafana workflow metrics dashboard — verify counts, durations, and error rates match expected values

### Implementation for User Story 2

- [ ] T012 [P] [US2] `depends_on: T004` Add `workflow_executions_total` counter (labeled by `workflow_name`, `status`) and `workflow_running` gauge to `producer/app.py`
- [ ] T013 [P] [US2] `depends_on: T004` Add `workflow_duration_seconds` histogram (`workflow_duration_ms` in spec) to `producer/app.py` with appropriate buckets
- [ ] T014 [US2] `depends_on: T006, T007, T011` Create Grafana dashboard JSON `grafana/provisioning/dashboards/workflow-metrics.json` with execution counts, duration trends, and error rate panels

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Message Processing Metrics (Priority: P3)

**Goal**: Show publish/consume rates, total counts, consumer lag with per-instance breakdown and aggregate views — reflecting activity within 15 seconds (SC-003)

**Independent Test**: Produce known number of messages, open Grafana message metrics dashboard — verify publish/consume counts and rates match expected values

### Implementation for User Story 3

- [ ] T015 [P] [US3] `depends_on: T004` Add `messages_publish_rate` gauge (labeled by `topic`) to `producer/app.py`
- [ ] T016 [P] [US3] `depends_on: T005` Add `messages_consume_rate` gauge and `consumer_lag` gauge (labeled by `topic`, `partition`) to `consumer/app.py`
- [ ] T017 [US3] `depends_on: T006, T007, T011` Create Grafana dashboard JSON `grafana/provisioning/dashboards/message-metrics.json` with publish/consume rates, total counts, and lag graph panels

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories — edge-case handling, configuration, testing

- [ ] T018 `depends_on: T010, T014, T017` Add "No data" handling (null value display, null point mode, no-data text) for unreported components across all three dashboard JSONs in `grafana/provisioning/dashboards/`
- [ ] T019 [P] `depends_on: T010, T014, T017` Add configurable time range selector (5min, 1h, 24h, 7d) to all three dashboard JSONs in `grafana/provisioning/dashboards/`
- [ ] T020 [P] `depends_on: T006` Add Prometheus 30-day retention config (`--storage.tsdb.retention.time=30d`) in `prometheus/prometheus.yml`
- [ ] T021 `depends_on: T004, T005` Write integration test that verifies `GET /metrics` returns expected Prometheus-format metrics on producer and consumer endpoints in `producer/tests/integration/test_metrics.py` / `consumer/tests/integration/test_metrics.py`

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

- **User Story 1 (P1)**: Can start after Foundational — No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational — No dependencies on other stories; targets same `producer/app.py` as US1, so coordinate edits
- **User Story 3 (P3)**: Can start after Foundational — No dependencies on other stories; targets same `producer/app.py`/`consumer/app.py` as US1/US2, so coordinate edits

### Within Each User Story

- `/metrics` endpoint before metric-specific gauges and counters
- Metric exposure before dashboard visualization
- Individual metric tasks before dashboard creation
- Dashboards before cross-cutting polish (no-data, time ranges)

### Parallel Opportunities

- All Setup tasks marked [P] (T001, T002, T003) can run in parallel
- All Foundational tasks marked [P] (T004, T005) can run in parallel
- US1 parallel: T008 and T009 (producer + consumer health metrics)
- US2 parallel: T012 and T013 (counters + histograms in same file — coordinate edits)
- US3 parallel: T015 and T016 (producer + consumer message metrics)
- Dashboard JSONs (T010, T014, T017) can be created in parallel once T006, T007, T011 are done
- Polish tasks (T018, T019, T020) that touch different files can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch producer and consumer health metrics together:
Task: "Add health_status gauge to producer/app.py" (T008)
Task: "Add health_status gauge to consumer/app.py" (T009)
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T003)
2. Complete Phase 2: Foundational (T004-T007)
3. Complete Phase 3: User Story 1 (T008-T011)
4. **STOP and VALIDATE**: Deploy stack, open Grafana, verify component health dashboard shows all three services with correct status
5. This validates the core pipeline — component metrics → Prometheus scrape → Grafana dashboard — before adding workflow or message metrics

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Validate (MVP!)
3. Add User Story 2 → Test independently → Validate
4. Add User Story 3 → Test independently → Validate
5. Add Polish → Test independently → Validate
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:
1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (P1 — health dashboard)
   - Developer B: User Story 2 (P2 — workflow metrics)
   - Developer C: User Story 3 (P3 — message metrics)
3. Team coordinates edits to `producer/app.py` and `consumer/app.py` (metrics are additive — different sections of the same file)
4. Developer D: Polish phase after all stories are integrated

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- US1-US3 all add metrics to the same `producer/app.py` and `consumer/app.py` — use additive, non-destructive edits (append new metric registrations; do not refactor existing metric code)
- Run full stack via `docker-compose up --build` after each phase
- Prometheus accessible at `http://localhost:9090`, Grafana at `http://localhost:3000`
- Grafana dashboards are provisioned automatically from JSON files — no manual import required
