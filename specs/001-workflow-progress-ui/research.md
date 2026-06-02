# Research: Workflow Progress UI

## Language & Framework

**Decision**: Python 3.14, Flask + Jinja2 (extends existing ui/)

**Rationale**: Existing web UI already uses Flask + Jinja2; extending avoids duplicate work. The current ui/app.py provides the Flask application pattern, database connection via psycopg2, and JSON API endpoints — all reusable.

**Alternatives**:
- Vue.js: New dependency with build pipeline; overkill for a monitoring dashboard
- React: Same as Vue.js — adds unnecessary complexity for page-based views
- FastAPI: Not in existing stack; async not required for dashboard read queries

## Real-time Updates

**Decision**: Server-Sent Events (SSE) via EventSource API for live status updates, with automatic fallback to polling at 10s interval

**Rationale**: SSE is simpler than WebSockets for unidirectional server→client updates (status changes, new executions). Native browser support via EventSource API. Graceful degradation to polling when orchestrator/event stream is unavailable (edge case from spec).

**Alternatives**:
- WebSocket: Bidirectional communication not needed; higher server complexity
- Polling only (setInterval): Higher latency for status changes; SC-003 requires < 5s — SSE achieves sub-second
- Long polling: Works but SSE is more efficient and standard

## Performance

**Decision**: Server-side pagination at 50 items with SQL LIMIT/OFFSET, database-indexed queries on status and started_at, query-level date range filtering

**Rationale**: SC-001 requires 3s dashboard load with >50 items. Server-side pagination prevents full-table scans. Indexes on (status, started_at) support the most common filter/sort combination. Date range filter uses a single indexed column.

**Alternatives**:
- Virtual scrolling (JS-based): Adds frontend complexity, still requires all data in memory
- Client-side pagination: Infeasible for >50 items — violates SC-001
- Cursor-based pagination: More efficient than OFFSET at scale but adds URL complexity; not needed at <1k execution volume

## Testing

**Decision**: pytest with Flask test client for route assertions and API response validation; Selenium for E2E browser scenarios covering all acceptance criteria

**Rationale**: Flask test client enables fast, isolated testing of route handlers, response status codes, and JSON structure. Selenium provides full browser rendering tests for the Jinja2 templates (pagination buttons, filter form submission, real-time updates via SSE).

**Alternatives**:
- Playwright: New dependency; Selenium is already familiar and sufficient
- unittest: Built-in but less ergonomic for fixture-heavy test scenarios
- pytest-flask: Lightweight wrapper that pairs well with existing pattern

## Architecture

**Decision**: Extend ui/app.py with new Flask routes under /workflows/ namespace; new SQL queries query workflow_executions and workflow_steps tables in existing PostgreSQL 15 instance

**Rationale**: The existing ui/ Flask app already connects to PostgreSQL and serves JSON APIs. Adding workflow routes follows the established pattern (see /api/stats, /api/events/<type>, /api/consumers). No new services, no new databases.

**Alternatives**:
- New microservice: Unnecessary — workflow data lives in the same PostgreSQL instance
- Orchestrator-proxied API: Adds latency and coupling; direct DB reads are simple and fast
- GraphQL: Overkill for two entity types with fixed query patterns

## Edge Cases

- Orchestrator disconnected: Graceful degradation to polling mode with a visual staleness indicator; placeholder template sections for "orquestador desconectado"
- Hundreds of steps: Paginated or grouped within the detail view; per-step sequence_order enables ordered display
- Empty execution: Detail view shows "Esperando que comience la ejecución" with empty step list
- Event stream unavailable: SSE EventSource fires onerror → client switches to polling with 10s interval and visual warning
