---
description: "Infrastructure monitoring with Prometheus, cAdvisor, Loki, promtail, and Alertmanager"
---

# Tasks: Prometheus Monitoring

**Input**: Design documents from `specs/004-prometheus-monitoring/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/monitoring-contract.md

**Tests**: Integration verification task included per Constitution Principle II. This feature adds monitoring infrastructure — observability is verified by confirming metric ingestion and log shipping.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- **`depends_on:`**: Explicit dependency annotation listing tasks that must be completed first
- Include exact file paths in descriptions

## Path Conventions

- **Prometheus stack config**: `prometheus/` (prometheus.yml, rules/*.yml)
- **Loki config**: `loki/` (loki-config.yml)
- **promtail config**: `promtail/` (promtail.yml)
- **Alertmanager config**: `alertmanager/` (alertmanager.yml)
- **Grafana provisioning**: `grafana/provisioning/datasources/`, `grafana/provisioning/dashboards/`
- **Docker Compose**: `docker-compose.yml` at repository root

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Add monitoring services to Docker Compose

- [ ] T001 Add Prometheus service to docker-compose.yml with port 9090, volumes for prometheus/ config, and event-network attachment
- [ ] T002 [P] Add cAdvisor service to docker-compose.yml with port 8080, volumes for container metric access, and event-network attachment
- [ ] T003 [P] Add Grafana Loki service to docker-compose.yml with port 3100, volumes for loki/ config and data, and event-network attachment
- [ ] T004 [P] Add promtail service to docker-compose.yml with volumes for docker socket and log files, and event-network attachment
- [ ] T005 [P] Add Alertmanager service to docker-compose.yml with port 9093, volumes for alertmanager/ config, and event-network attachment

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Configuration files that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T006 `depends_on: T001, T002` Create prometheus.yml scrape config in prometheus/prometheus.yml with targets for cAdvisor (:8080), producer (:8000/metrics), consumer1 (:8000/metrics), consumer2 (:8000/metrics), web-ui (:8000/metrics), Kafka JMX exporter, and Prometheus itself
- [ ] T007 [P] `depends_on: T003, T004` Create promtail.yml config in promtail/promtail.yml with scrape targets for all containers (producer, consumer1, consumer2, web-ui, kafka) shipping JSON logs to Loki
- [ ] T008 [P] `depends_on: T005` Create Alertmanager config in alertmanager/alertmanager.yml with web UI notification channel via webhook to Grafana, grouping by alertname and severity, and inhibition rules
- [ ] T009 `depends_on: T006, T007, T008` Create Docker network attachment for Prometheus stack services to access event-network — verify all monitoring containers can reach target services

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Container Resource Usage Monitoring (Priority: P1) 🎯 MVP

**Goal**: CPU, memory, disk, and network metrics per container visible in Grafana with historical trends

**Independent Test**: Confirm `curl localhost:9090/api/v1/query?query=container_cpu_usage_percent` returns data for each container

### Implementation for User Story 1

- [ ] T010 [P] [US1] `depends_on: T006` Create Prometheus recording rules for CPU and memory per container in prometheus/rules/resource-metrics.yml — `container_cpu_usage_percent`, `container_memory_usage_percent`, `container_memory_usage_bytes`
- [ ] T011 [P] [US1] `depends_on: T006` Create Prometheus recording rules for disk I/O and network I/O per container in prometheus/rules/resource-metrics.yml — `container_disk_read_bytes_total`, `container_disk_write_bytes_total`, `container_network_rx_bytes_total`, `container_network_tx_bytes_total`
- [ ] T012 [US1] `depends_on: T006` Add default Grafana dashboard datasource config for Prometheus in grafana/provisioning/datasources/prometheus.yml with URL `http://prometheus:9090` and `access: proxy`
- [ ] T013 [US1] `depends_on: T012` Create Grafana dashboard JSON for Container Resources in grafana/provisioning/dashboards/container-resources.json with CPU (area chart, 5m avg), memory (area chart, 5m avg), disk (stacked bar), network (stepped line) panels per container, configurable time range per SC-001

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Centralized Log Viewer (Priority: P2)

**Goal**: Unified log stream from all components with search by keyword, component, severity, and time range

**Independent Test**: Confirm `curl -G 'http://localhost:3100/loki/api/v1/query_range' --data-urlencode 'query={container="producer"}'` returns log entries

### Implementation for User Story 2

- [ ] T014 [P] [US2] Configure structured JSON logging in producer/app.py using python-json-logger — format per contracts/monitoring-contract.md with fields: timestamp, level, name, message, component=producer, container=producer
- [ ] T015 [P] [US2] Configure structured JSON logging in consumer/app.py using python-json-logger — format per contracts/monitoring-contract.md with fields: timestamp, level, name, message, component=consumer, container=consumer1/consumer2
- [ ] T016 [P] [US2] Configure structured JSON logging in ui/app.py using python-json-logger — format per contracts/monitoring-contract.md with fields: timestamp, level, name, message, component=web-ui, container=web-ui
- [ ] T017 [US2] `depends_on: T007` Create Loki log query presets for common searches (by component, by severity, by keyword) in docs/loki-queries.md
- [ ] T018 [US2] `depends_on: T007, T012` Add Grafana dashboard JSON for Log Explorer in grafana/provisioning/dashboards/log-explorer.json with LogQL query editor, severity filter (DEBUG/INFO/WARN/ERROR/FATAL), component filter, time range picker, and log results table

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Infrastructure Alerts (Priority: P3)

**Goal**: Configurable alert rules for CPU, memory, disk thresholds with auto-resolve and dashboard

**Independent Test**: Confirm `curl localhost:9093/api/v2/alerts` returns empty alert list (no thresholds breached) after 5 minutes of steady-state

### Implementation for User Story 3

- [ ] T019 [P] [US3] `depends_on: T006` Create CPU alert rule in prometheus/rules/alerts.yml — `ContainerCPUWarning` (> 80% for 2min → warning), `ContainerCPUCritical` (> 90% for 2min → critical)
- [ ] T020 [P] [US3] `depends_on: T006` Create memory alert rule in prometheus/rules/alerts.yml — `ContainerMemoryWarning` (> 80% for 2min → warning), `ContainerMemoryCritical` (> 90% for 2min → critical)
- [ ] T021 [P] [US3] `depends_on: T006` Create disk alert rule in prometheus/rules/alerts.yml — `ContainerDiskWarning` (> 80% for 5min → warning), `ContainerDiskCritical` (> 90% for 5min → critical)
- [ ] T022 [US3] `depends_on: T008, T019, T020, T021` Create Grafana dashboard JSON for Alerts in grafana/provisioning/dashboards/alerts.json with firing/resolved alerts table, severity badge, affected container, current value, duration, and status filter (firing/resolved/all)

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Retention policies, overhead guarantees, restart resilience, and end-to-end verification

- [ ] T023 [P] `depends_on: T006` Add Prometheus 30-day retention config in prometheus/prometheus.yml via `storage.tsdb.retention.time: 30d` (FR-008)
- [ ] T024 [P] `depends_on: T003` Add Loki 7-day retention config in loki/loki-config.yml via `table_manager.retention_period: 168h` and `compactor.retention_enabled: true` (FR-009)
- [ ] T025 `depends_on: T006` Add resource limit annotations to Prometheus scrape config in prometheus/prometheus.yml — set `scrape_timeout: 5s`, `scrape_interval: 15s`, and `max_samples: 1000` per target to ensure <5% overhead (SC-004)
- [ ] T026 `depends_on: T010, T011` Add container restart gap handling in prometheus/rules/resource-metrics.yml — use `or` operator with `vector(0)` to fill gaps on container restart, add `container_restart_indicator` metric that spikes to 1 during restart for break detection
- [ ] T027 `depends_on: T006, T007, T008, T012, T013, T018, T022` Run integration verification — confirm all components report metrics via `/_health` and `:8000/metrics`, logs appear in Loki, dashboards load in Grafana, alert rules parse without error

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) — JSON logging tasks (T014-T016) are independent of US1; dashboard tasks (T017-T018) depend on promtail config but not on US1
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) — Alert rules (T019-T021) are independent of US1/US2; alert dashboard (T022) depends on Alertmanager config

### Within Each User Story

- Rules before dashboards
- Infrastructure config before feature-level config
- Recording rules before alert rules (shared metric namespace)
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel
- Tasks marked [P] within each phase/story can run in parallel
- JSON log configuration tasks (T014, T015, T016) can run in parallel across services
- Alert rule creation tasks (T019, T020, T021) can run in parallel
- Dashboard creation tasks (T013, T018, T022) can run in parallel after their dependencies

---

## Parallel Example: User Story 1

```bash
# Launch recording rule creation together:
Task: "Create CPU/memory recording rules in prometheus/rules/resource-metrics.yml"
Task: "Create disk/network recording rules in prometheus/rules/resource-metrics.yml"

# Then launch grafana config and dashboard after datasource is ready:
Task: "Add Grafana datasource config in grafana/provisioning/datasources/prometheus.yml"
Task: "Create Container Resources dashboard in grafana/provisioning/dashboards/container-resources.json"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T005)
2. Complete Phase 2: Foundational (T006-T009)
3. Complete Phase 3: User Story 1 (T010-T013)
4. **STOP and VALIDATE**: Open Grafana, check Container Resources dashboard shows live CPU/memory for all containers
5. This validates the core monitoring pipeline — metrics → Prometheus → Grafana — before adding logs and alerts

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Validate (MVP! Container metrics visible)
3. Add User Story 2 → Test independently → Validate (Centralized log viewer working)
4. Add User Story 3 → Test independently → Validate (Alerts trigger and resolve)
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (P1 — container resource monitoring)
   - Developer B: User Story 2 (P2 — centralized logging)
   - Developer C: User Story 3 (P3 — infrastructure alerts)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Prometheus rules use `record:` naming convention per contracts/monitoring-contract.md
- All Python services already have python-json-logger in requirements.txt
- Retention periods: Prometheus TSDB 30d (FR-008), Loki 7d (FR-009)
- Metrics overhead <5% (SC-004) is enforced via scrape interval and timeout tuning
- Alerts trigger within 1 minute (SC-003) via evaluation_interval and for: duration
- Validate by curling /metrics endpoints before checking Prometheus targets
