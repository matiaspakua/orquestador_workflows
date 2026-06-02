# Implementation Plan: Workflow Progress UI

**Branch**: `001-workflow-progress-ui` | **Date**: 2026-06-02 | **Spec**: specs/001-workflow-progress-ui/spec.md

**Input**: Feature specification from `specs/001-workflow-progress-ui/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Real-time web dashboard for monitoring workflow execution progress. Extends existing Flask/Jinja2 UI with workflow list, detail views, filtering, and auto-refresh. Covers FR-001 through FR-010 (pagination at 50 items, SSE real-time updates, status/date/name filters, error display, offline handling) with Spanish locale.

## Technical Context

**Language/Version**: Python 3.14

**Primary Dependencies**: Flask, Jinja2, JavaScript (fetch API + EventSource for SSE), psycopg2, pytest, Selenium

**Storage**: PostgreSQL 15 (workflow_executions, workflow_steps tables)

**Testing**: pytest with Flask test client for API assertions; Selenium for E2E browser scenarios

**Target Platform**: Linux (Docker containers), desktop/laptop browser

**Project Type**: Web application with backend microservices (event-driven)

**Performance Goals**: SC-001 dashboard < 3s load; SC-002 detail view < 2s; SC-003 SSE updates < 5s; SC-004 filtered results < 2s

**Constraints**: Single locale (Spanish), no authentication, graceful degradation when orchestrator/event stream is unavailable

**Scale/Scope**: Up to 50 items per page, hundreds of steps grouped/paginated, < 1k workflow executions initially

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Principle I (Code Quality)**: ✅ Extends existing Flask/Jinja2 UI following established patterns (app.py, templates/). No new standalone libraries. Linting and review required before merge.

- **Principle II (Testing Standards)**: ✅ Tests written before implementation. Flask test client for API routes (list/detail/filter); Selenium for E2E browser scenarios covering all US acceptance criteria. Integration tests for SSE fallback to polling.

- **Principle III (UX Consistency)**: ✅ Single Spanish locale consistent with existing UI (lang="es"). Uses same CSS design system (gradient headers, card layout, status badges). Empty/disconnected states with user-facing messages, not technical errors.

- **Principle IV (Performance Requirements)**: ✅ SC-001 through SC-004 define measurable targets. Server-side pagination (50 items) and database-indexed queries ensure sub-3s dashboard load. SSE provides < 5s real-time updates.

- **Principle V (Architecture & Observability)**: ✅ UI extends existing Flask app — no new services. SSE or polling fallback ensures graceful degradation. Workflow execution progress is observable end-to-end.

- **Technology & Infrastructure**: ✅ Docker Compose deployment stays unchanged (web-ui container reuses existing build). PostgreSQL 15, event-network (bridge) all consistent.

**Result**: ALL GATES PASS ✅ — No violations requiring justification.

## Project Structure

### Documentation (this feature)

```text
specs/001-workflow-progress-ui/
├── spec.md               # Feature specification
├── plan.md               # This file (/speckit.plan command output)
├── research.md           # Phase 0 output (/speckit.plan command)
├── data-model.md         # Phase 1 output (/speckit.plan command)
├── quickstart.md         # Phase 1 output (/speckit.plan command)
└── contracts/            # Phase 1 output (/speckit.plan command)
```

### Source Code (repository root)

```text
consumer/
├── app.py
├── Dockerfile
├── requirements.txt
└── tests/

producer/
├── app.py
├── Dockerfile
├── requirements.txt
└── tests/

ui/
├── app.py                # Extended with workflow routes
├── Dockerfile
├── requirements.txt
├── static/
│   └── css/
│       └── style.css
└── templates/
    ├── base.html          # New: shared layout template
    ├── index.html         # Existing dashboard (extended)
    ├── workflows.html     # New: workflow list dashboard
    └── workflow_detail.html # New: step-by-step detail view

scripts/
├── init-db.sql           # Extended with workflow schema
└── test/
```

**Structure Decision**: Maintains existing flat project structure (consumer/, producer/, ui/) with no new top-level directories. All new code lives inside ui/app.py (new routes), ui/templates/ (new Jinja2 templates), and scripts/init-db.sql (new schema). Tests co-located under ui/tests/.

## Complexity Tracking

> No violations to justify — all constitution gates pass.
