# Contracts: Workflow Progress UI

This feature has **no external API contracts**.

All data access is read-only and performed via direct SQL queries against the PostgreSQL database from the existing Flask application.

## Data Contracts

The internal data contracts are defined in [`../data-model.md`](../data-model.md):

- **Workflow Execution** — core entity for the list view (US1) and detail view (US2)
- **Workflow Step** — core entity for the step-by-step progress view (US2)
- **Workflow Status** — enumeration controlling filter behavior (US3)

## Route Design (Informational)

The following Flask routes are added to `ui/app.py` for internal reference. These are not external contracts and may change during implementation:

| Route | Method | Description |
|-------|--------|-------------|
| `/workflows` | GET | Render workflow list dashboard |
| `/workflows/<id>` | GET | Render workflow detail view |
| `/api/workflows` | GET | JSON list (paginated, filtered) |
| `/api/workflows/<id>` | GET | JSON execution + steps |
| `/api/workflows/stream` | GET | SSE endpoint for live updates |

## Kafka Contracts

No new Kafka topics are introduced. The UI reads execution data directly from PostgreSQL, not from Kafka events. Real-time updates use SSE derived from database state, not from Kafka consumer groups.
