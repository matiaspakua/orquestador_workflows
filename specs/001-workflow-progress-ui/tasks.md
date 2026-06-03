---
description: "Task list for Workflow Progress UI feature — real-time dashboard, detail views, filtering"
---

# Tasks: Workflow Progress UI

**Input**: Design documents from `specs/001-workflow-progress-ui/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/README.md, checklists/requirements.md (quality gate — MUST pass before implementation)

**Tests**: Test tasks are MANDATORY per the project Constitution (Principle II: Testing Standards). Every feature and bug fix MUST include tests. Unit, integration, and end-to-end test tasks MUST be generated for each user story.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- **`depends_on:`**: Explicit dependency annotation listing tasks that must be completed first
- Include exact file paths in descriptions

## Path Conventions

- **ui/templates/** — Jinja2 templates (extends existing base + new workflow views)
- **ui/static/** — Static assets (CSS, JS)
- **ui/app.py** — Flask application (extend with new routes)
- **ui/services/** — Python service modules (DB query layer)
- **scripts/** — Database schema and test scripts

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [x] T001 Create workflow service module in `ui/services/workflow_service.py` with module structure, `get_db_connection()` helper (reusing existing psycopg2 pattern from `ui/app.py`), and placeholder for query methods
- [x] T002 [P] Add Flask SSE support — create `Response` stream generator and `/api/workflows/stream` route skeleton in `ui/app.py` using `text/event-stream` content type

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T003 `depends_on: T001` Create base Jinja2 template in `ui/templates/base.html` with HTML5 doctype, `lang="es"`, navigation bar, content block, and CSS framework (reuse existing `ui/static/css/style.css` patterns)
- [x] T004 [P] Create database migration script in `scripts/init-workflow-db.sql` with `CREATE TABLE IF NOT EXISTS workflow_executions` and `workflow_steps` per data-model.md DDL, including indexes and FK with CASCADE
- [x] T005 `depends_on: T001` Implement workflow data access layer in `ui/services/workflow_service.py` with methods: `get_workflow_executions(page, per_page, status, date_from, date_to, search)`, `get_workflow_execution(id)`, `get_workflow_steps(execution_id)`, and `get_workflow_execution_count()` — all using safe parameterized queries

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — View Workflow Execution List (Priority: P1) 🎯 MVP

**Goal**: Dashboard listing all workflow executions with status, start time, duration — paginated at 50 items with real-time SSE updates

**Independent Test**: Open `/workflows` with known workflow data; verify every execution appears with correct status, pagination works at 50 items, and running status updates appear without page refresh

### Tests for User Story 1 (OPTIONAL — only if tests requested) ⚠️

> **NOTE**: Write these tests FIRST, ensure they FAIL before implementation. Place in `ui/tests/test_workflows.py`.

- [x] [P] [US1] Flask test client test for `GET /api/workflows` — verify paginated JSON response with expected fields (id, name, status, started_at, duration), status code 200, and `X-Total-Count` header
- [x] [P] [US1] Flask test client test for `GET /api/workflows` with `?page=2&per_page=50` — verify offset shifts correctly
- [x] [US1] Flask test client test for `GET /api/workflows/stream` — verify SSE response with `text/event-stream` content type and `data: ` formatted output
- [x] [US1] Flask test client test for `GET /workflows` — verify rendered HTML contains expected workflow names and status badges

### Implementation for User Story 1

- [x] T006 [P] [US1] `depends_on: T005` Implement workflow list route `GET /workflows` in `ui/app.py` that queries `workflow_service.get_workflow_executions()` with page/per_page parameters (default 50) and renders `workflow_list.html`
- [x] T007 [P] [US1] `depends_on: T003` Create workflow list template in `ui/templates/workflow_list.html` extending `base.html` — show table with columns: status badge (color-coded: Pending=gray, Running=blue, Completed=green, Failed=red), workflow name (linked to detail), start time, duration; empty state when no executions
- [x] T008 [US1] `depends_on: T002, T005` Implement SSE endpoint `GET /api/workflows/stream` in `ui/app.py` that queries recent status changes and yields `event: status_update\ndata: {json}\n\n` per SSE protocol; handle client disconnect with generator cleanup
- [x] T009 [US1] `depends_on: T007, T008` Add client-side SSE listener in `ui/templates/workflow_list.html` using `EventSource('/api/workflows/stream')` with `onmessage` handler that updates status badges client-side; fallback to polling via `setInterval(fetch, 10000)` on `EventSource.onerror`
- [x] T010 [US1] `depends_on: T006, T007` Add pagination controls in `ui/templates/workflow_list.html` (Previous/Next buttons, page indicator) and URL parameter handling (`?page=N`) in `ui/app.py` workflow list route

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 — View Workflow Execution Details (Priority: P2)

**Goal**: Click into a workflow execution to see step-by-step progress with status, duration, error messages, and empty-state handling

**Independent Test**: Open `/workflows/<id>` for a workflow with steps; verify each step shows correct status/duration, failed steps show error messages, pending steps are clearly marked

### Tests for User Story 2 (OPTIONAL — only if tests requested) ⚠️

> **NOTE**: Write these tests FIRST, ensure they FAIL before implementation. Place in `ui/tests/test_workflows.py`.

- [x] [P] [US2] Flask test client test for `GET /api/workflows/<id>` — verify JSON response includes execution fields and nested `steps[]` array ordered by `sequence_order`
- [x] [P] [US2] Flask test client test for `GET /workflows/<id>` — verify rendered HTML contains step names and status badges
- [x] [US2] Flask test client test for `GET /workflows/<id>` with failed step — verify error message text appears in rendered HTML
- [x] [US2] Flask test client test for `GET /workflows/<id>` with no steps — verify "Esperando que comience la ejecución" message appears

### Implementation for User Story 2

- [x] T011 [P] [US2] `depends_on: T005` Implement workflow detail route `GET /workflows/<id>` in `ui/app.py` that loads workflow execution via `get_workflow_execution(id)` and steps via `get_workflow_steps(id)`, then renders `workflow_detail.html`
- [x] T012 [P] [US2] `depends_on: T003` Create workflow detail template in `ui/templates/workflow_detail.html` extending `base.html` — show execution summary header (name, status badge, timestamps, duration) and ordered step list with columns: sequence number, step name, step type badge, status badge (with Failed highlighted in red), duration, error message if present
- [x] T013 [US2] `depends_on: T012` Add error message display for failed steps in `ui/templates/workflow_detail.html` — expandable/collapsible error detail section per failed step, red highlighted status badge, distinct visual treatment for Failed vs Skipped vs Completed vs Running vs Pending
- [x] T014 [US2] `depends_on: T012` Add empty state handling in `ui/templates/workflow_detail.html` — when steps array is empty, display centered message "Esperando que comience la ejecución" with a subtle spinner animation; hide step table

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 — Filter and Search Workflows (Priority: P3)

**Goal**: Filter workflow list by status, date range, and search by name — all via query parameters on the existing list route

**Independent Test**: Apply each filter type (status=Failed, date_from=2026-01-01, search=test) and verify only matching workflows appear; test combinations

### Tests for User Story 3 (OPTIONAL — only if tests requested) ⚠️

> **NOTE**: Write these tests FIRST, ensure they FAIL before implementation. Place in `ui/tests/test_workflows.py`.

- [x] [P] [US3] Flask test client test for `GET /api/workflows?status=Failed` — verify only failed workflows returned
- [x] [P] [US3] Flask test client test for `GET /api/workflows?date_from=2026-01-01&date_to=2026-06-01` — verify only executions within range
- [x] [P] [US3] Flask test client test for `GET /api/workflows?search=test-workflow` — verify name/identifier matches
- [x] [US3] Flask test client test for combined filters `?status=Running&search=etl&date_from=2026-05-01` — verify intersection

### Implementation for User Story 3

- [x] T015 [P] [US3] `depends_on: T006` Add `status` query parameter to workflow list route in `ui/app.py` — pass to `get_workflow_executions(status=...)`; accept Pending, Running, Completed, Failed; validate against enum
- [x] T016 [P] [US3] `depends_on: T006` Add `date_from` and `date_to` query parameters to workflow list route in `ui/app.py` — pass to `get_workflow_executions(date_from=..., date_to=...)`; parse ISO date strings; return 400 on invalid format
- [x] T017 [P] [US3] `depends_on: T006` Add `search` query parameter to workflow list route in `ui/app.py` — pass to `get_workflow_executions(search=...)`; use `ILIKE` on `name` column; min 2 characters
- [x] T018 [US3] `depends_on: T007` Create filter form in `ui/templates/workflow_list.html` above the workflow table — status dropdown (all/Pending/Running/Completed/Failed), date range inputs (date_from, date_to with `type="date"`), search text input with placeholder "Buscar por nombre..."; form uses GET to preserve filter state in URL; clear filters link

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Edge case handling, error states, infrastructure configuration, and validation

- [x] T019 `depends_on: T003` Add disconnected state UI in `ui/templates/base.html` — banner across top of page "Orquestador desconectado — Verifique el estado del sistema" shown when SSE/polling detects no heartbeat for >30s; hidden by default, shown via JS class toggle
- [x] T020 `depends_on: T007` Add stale data indicator in `ui/templates/workflow_list.html` — subtle warning bar "Eventos no disponibles — usando modo polling" with icon when SSE `onerror` fires and polling fallback activates; auto-dismiss on SSE reconnect
- [x] T021 [P] `depends_on: T002` Add error boundary for orchestrator connection failures in `ui/app.py` — catch `psycopg2.OperationalError` in workflow routes, return 503 with JSON `{"error": "base_datos_no_disponible", "message": "No se pudo conectar con la base de datos"}` and render degraded template state
- [x] T022 `depends_on: T004` Update `docker-compose.yml` with `WEB_UI` environment variables for database connection (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD) if not already present; add workflow service to health check
- [x] T023 `depends_on: T019, T020, T021, T022` Run quickstart.md validation — start fresh Docker Compose environment, verify `/workflows` loads, `/workflows/<id>` loads, SSE stream returns events, filter form submits correctly, disconnected state appears when postgres is stopped; add any missing instructions to quickstart.md

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
  - T003 (base template) blocks T006, T007 (US1 templates/routes need base layout)
  - T005 (service layer) blocks all route tasks (T006, T008, T011, T015-T017)
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) — No dependencies on US1
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) — Modifies US1 routes/templates but does not block or depend on US1

### Within Each User Story

- Service layer before routes (T005 before T006, T008, T011)
- Base template before story templates (T003 before T007, T012)
- Routes before template integration (T006 before T009, T010)
- Core implementation before edge cases (US2 tasks before T013, T014)

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel (T001, T002)
- All Foundational tasks marked [P] can run in parallel (T004 with T003/T005)
- Once Foundational phase completes, US1 and US2 can start in parallel (different files)
- US3 tasks T015, T016, T017 can run in parallel (all modify different query params)
- US2 tasks T011 and T012 can run in parallel (route vs template)
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch route and template tasks together:
Task: "T006 Implement workflow list route in ui/app.py"
Task: "T007 Create workflow list template in ui/templates/workflow_list.html"

# After both complete, launch SSE and pagination:
Task: "T008 Implement SSE endpoint in ui/app.py"
Task: "T009 Add client-side SSE listener in workflow_list.html"
Task: "T010 Add pagination controls"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T002)
2. Complete Phase 2: Foundational (T003-T005)
3. Complete Phase 3: User Story 1 (T006-T010)
4. **STOP and VALIDATE**: Open `/workflows` — verify list renders, pagination works, SSE updates appear
5. This delivers the core monitoring dashboard before adding detail/filter views

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → VALIDATE (MVP!)
3. Add User Story 2 → Test independently → VALIDATE
4. Add User Story 3 → Test independently → VALIDATE
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (P1 — list dashboard with SSE)
   - Developer B: User Story 2 (P2 — detail view with step progress)
3. Developer C: User Story 3 (P3 — filters, after US1 route is stable)
4. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Tests (when included) MUST fail before implementation (Red-Green-Refactor per Constitution)
- All user-facing strings in Spanish (`lang="es"`) per project locale constraint
- SSE fallback to polling at 10s interval per research.md
- Pagination default: 50 items per page per FR-010
- Status color convention: Pending=gray, Running=blue, Completed=green, Failed=red
- Workflow detail empty state: "Esperando que comience la ejecución"
- Filter minimum search length: 2 characters
