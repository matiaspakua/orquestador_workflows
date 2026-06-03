# Orquestador de Workflows

**Event-driven workflow orchestration system** — 4 microservices coordinated by a central orchestrator through Apache Kafka, with real-time progress visible in a web dashboard, and full infrastructure observability via Prometheus + Grafana + Loki.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          EVENT-DRIVEN PIPELINE                          │
│  ┌─────────────┐   publish    ┌────────────┐   consume   ┌──────────┐  │
│  │  Producer   │────events───►│   Kafka    │────events──►│Consumer 1│  │
│  │             │              │  (Broker)  │             ├──────────┤  │
│  └─────────────┘              └──────┬─────┘             │Consumer 2│  │
│                                      │                   └──────────┘  │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    WORKFLOW ORCHESTRATION                         │  │
│  │  ┌────────────┐  task  ┌─────────────────────────────────────┐  │  │
│  │  │Orchestrator│───────►│            Workers (4 services)     │  │  │
│  │  │            │◄result─│  1. OrderValidator                  │  │  │
│  │  │  manages   │        │  2. FraudChecker                    │  │  │
│  │  │  lifecycle │        │  3. InventoryChecker                │  │  │
│  │  │            │        │  4. NotificationSender              │  │  │
│  │  └─────┬──────┘        └─────────────────────────────────────┘  │  │
│  │        │ writes progress                                          │  │
│  │        ▼                                                          │  │
│  │  ┌────────────┐   workflow_executions + workflow_steps            │  │
│  │  │ PostgreSQL │◄──── visible in Web UI /workflows ─────────────  │  │
│  │  └────────────┘                                                   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    OBSERVABILITY STACK                            │  │
│  │  Prometheus:9090  Grafana:3000  Loki:3100  Alertmanager:9093     │  │
│  │  cAdvisor:8082    promtail (log shipping)                        │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  Web UI:5000 │ Kafka UI:8080 │ pgAdmin:8081 │ Grafana:3000            │
└─────────────────────────────────────────────────────────────────────────┘
```

### Services

| Service | Port | Role |
|---------|------|------|
| **Orchestrator** | 8000/metrics | Creates & coordinates workflow executions |
| **Workers** | 8000/metrics | 4 workers: OrderValidator, FraudChecker, InventoryChecker, NotificationSender |
| **Producer** | 8000/metrics | Generates random domain events → Kafka |
| **Consumer 1/2** | 8000/metrics | Processes domain events from Kafka → PostgreSQL |
| **Web UI** | 5000 | Workflow progress dashboard (real-time SSE) |
| **Kafka** | 9092 | Message broker |
| **PostgreSQL** | 5432 | State store |
| **Prometheus** | 9090 | Metrics scraping + alerting |
| **Grafana** | 3000 | 6 pre-provisioned dashboards |
| **Loki** | 3100 | Centralized log aggregation |
| **Alertmanager** | 9093 | Alert routing |
| **cAdvisor** | 8082 | Container resource metrics |
| **Kafka UI** | 8080 | Kafka topic browser |
| **pgAdmin** | 8081 | PostgreSQL admin |

### Order Processing Workflow (4 steps)

```
[OrderValidator] ──► [FraudChecker] ──► [InventoryChecker] ──► [NotificationSender]
      │                    │                    │                       │
  Validates           Flags orders         Confirms items           Sends email
  amount/fields       > $5,000             in stock                 confirmation
                      (→ DLQ)              10% stockout (→ DLQ)     Always OK
```

Each order is a separate workflow execution visible in the Web UI at **`http://localhost:5000/workflows`**.

---

## Quickstart

### 1. Prerequisites

```bash
docker --version        # Docker 24+
docker compose version  # Compose v2.20+
cp .env.example .env    # edit credentials if needed
```

### 2. Start the full stack

```bash
docker compose up --build -d
```

Wait ~30 s for Kafka and PostgreSQL to become healthy (`docker compose ps`).

### 3. Open the Web UI

```
http://localhost:5000/workflows
```

Workflow executions appear every 60 seconds (configurable via `WORKFLOW_INTERVAL`).
Each execution shows its 4 steps with live status updates — no page refresh needed.

### 4. Open Grafana

```
http://localhost:3000   (admin / admin)
```

Pre-provisioned dashboards are available immediately — no manual import required.

### 5. Verify Prometheus targets

```
http://localhost:9090/targets
```

All 8 scrape targets should show `UP`.

---

## Configuration

All settings live in `.env`:

```dotenv
# PostgreSQL
POSTGRES_USER=eventuser
POSTGRES_PASSWORD=eventpass
POSTGRES_DB=eventdb

# Kafka
KAFKA_BROKER=kafka:29092
KAFKA_TOPIC=data-events
CONSUMER_GROUP=data-processors

# Producer
PRODUCER_INTERVAL=5       # seconds between generated events

# Orchestrator
WORKFLOW_INTERVAL=60      # seconds between new workflow executions
```

---

## Web UI Guide

### Workflow List  `http://localhost:5000/workflows`

- **Status badges**: gray = Pending, blue = Running, green = Completed, red = Failed
- **Filters**: filter by status, date range, or name search
- **Real-time**: SSE updates status badges live; falls back to polling every 10 s

### Workflow Detail  `http://localhost:5000/workflows/<id>`

- Execution summary: name, status, timestamps, total duration
- Step table: all 4 steps with status, duration, and error message if failed
- Auto-refreshes every 5 s for Running/Pending executions

### What to expect

| Scenario | Frequency | Why |
|----------|-----------|-----|
| Completed (all 4 steps green) | ~75% | Orders ≤ $5,000 with stock |
| Failed at FraudChecker | ~15% | Orders > $5,000 flagged |
| Failed at InventoryChecker | ~10% | Random stockout simulation |

---

## Monitoring Guide

### Grafana Dashboards

| Dashboard | What it shows |
|-----------|---------------|
| **Component Health** | Health status, publish/consume rates, errors per service |
| **Workflow Metrics** | Execution counts, p50/p95/p99 duration histogram, error rate |
| **Message Metrics** | Kafka publish/consume rates, consumer lag |
| **Container Resources** | CPU %, memory %, disk I/O, network per container |
| **Log Explorer** | Full log stream with container + severity filters (Loki) |
| **Infrastructure Alerts** | Firing/resolved alerts table with severity badges |

### Key Prometheus metrics

| Metric | Source |
|--------|--------|
| `health_status{component=X}` | all services |
| `orchestrator_workflows_completed_total{status}` | orchestrator |
| `orchestrator_workflow_duration_seconds` | orchestrator |
| `worker_tasks_processed_total{worker,status}` | workers |
| `messages_published_total` / `messages_consumed_total` | producer/consumer |
| `consumer_lag{topic,partition}` | consumer |

### Loki log queries

```logql
# All errors
{container=~".+"} | json | level = "ERROR"

# Trace one workflow execution
{container=~".+"} | json | workflow_execution_id = "<uuid>"

# Worker failures
{container="workers"} | json | level = "ERROR"
```

See [`docs/loki-queries.md`](docs/loki-queries.md) for more presets.

### Alerts

Prometheus fires alerts (via Alertmanager) when:

| Alert | Threshold | Duration |
|-------|-----------|----------|
| `ContainerCPUWarning` | CPU > 80% | 2 min |
| `ContainerCPUCritical` | CPU > 90% | 2 min |
| `ContainerMemoryWarning` | Memory > 80% | 2 min |
| `ContainerMemoryCritical` | Memory > 90% | 2 min |
| `ContainerDiskWarning` | Disk I/O > 80% | 5 min |
| `ContainerDiskCritical` | Disk I/O > 90% | 5 min |

---

## Development

### Run UI tests

```bash
cd ui && python3 -m pytest tests/ -v
# 23 tests, all routes, SSE, error boundary, 503 on DB failure
```

### Run integration tests (requires Docker)

```bash
docker compose -f docker-compose.test.yml up --build
```

### Create Kafka topics explicitly

```bash
docker compose exec kafka /scripts/init-topics.sh
```

### Add a new workflow step

1. Add the step to `orchestrator/app.py` → `WORKFLOW_DEF['steps']`
2. Add the worker function to `workers/app.py` → `WORKERS` list
3. Add the topic to `scripts/init-topics.sh`
4. Restart: `docker compose restart orchestrator workers`

---

## Project Structure

```
orquestador_workflows/
├── orchestrator/          # Workflow lifecycle manager (state machine + Kafka)
├── workers/               # 4 worker services (one process, 4 threads)
├── producer/              # Domain event generator
├── consumer/              # Domain event processor (2 instances)
├── ui/                    # Flask web dashboard + SSE + 23 tests
├── prometheus/            # Scrape config + recording rules + alert rules
├── grafana/               # 6 pre-provisioned dashboards + datasources
├── loki/                  # Log aggregation config (7-day retention)
├── promtail/              # Log shipping (docker_sd_configs)
├── alertmanager/          # Alert routing + inhibit rules
├── docs/                  # Workflow lifecycle, step types, event schemas
│   └── schemas/           # JSON Schema for 8 workflow/event types
├── scripts/               # SQL init (DB, workflow tables, idempotency)
│   └── init-topics.sh     # Explicit Kafka topic creation
├── specs/                 # 5 feature specifications (001-005)
├── docker-compose.yml     # Full 14-service stack
└── docker-compose.test.yml# Integration test overlay
```

---

## Security Notes

- **`.env` is gitignored** — never commit it; use `.env.example` as the template.
- `pgAdmin` credentials in `docker-compose.yml` are for local development only.
- Flask runs with `debug=False` by default (`FLASK_DEBUG=0`).
- Prometheus metrics endpoints are on the internal Docker network only.

---

## Feature Status

| Feature | Status | Description |
|---------|--------|-------------|
| [001 Workflow Progress UI](specs/001-workflow-progress-ui/) | ✅ Complete | Real-time dashboard, SSE, filters, 23 tests |
| [002 Grafana Telemetry](specs/002-grafana-telemetry/) | ✅ Complete | Prometheus metrics on all services + 3 dashboards |
| [003 Workflow Definition](specs/003-workflow-definition/) | ✅ Complete | Lifecycle docs, step types, 8 JSON schemas |
| [004 Prometheus Monitoring](specs/004-prometheus-monitoring/) | ✅ Complete | cAdvisor, Loki, alerts, 3 more dashboards |
| [005 Producer-Consumer Test](specs/005-producer-consumer-test/) | ✅ Complete | Full integration test suite |
