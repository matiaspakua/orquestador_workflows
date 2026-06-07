<div align="center">

# 🎯 Orquestador de Workflows

### Event-driven microservice orchestration with real-time monitoring

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Flask](https://img.shields.io/badge/Flask-3.0-000000?logo=flask&logoColor=white)](https://flask.palletsprojects.com)
[![Prometheus](https://img.shields.io/badge/Prometheus-2.51-E6522C?logo=prometheus&logoColor=white)](https://prometheus.io)
[![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana&logoColor=white)](https://grafana.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docker.com)
[![gRPC](https://img.shields.io/badge/gRPC-1.62-244c5a?logo=grpc&logoColor=white)](https://grpc.io)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)

<br/>

A complete **event-driven orchestration platform** that coordinates 4 independent microservices through Apache Kafka, tracks every execution step in real time through a web dashboard, and observes the entire stack with Prometheus, Grafana, and Loki — all running locally with a single `docker compose up`.

</div>

---

## ✨ Features

| | Feature | Detail |
|---|---------|--------|
| 🔄 | **Workflow Orchestration** | State-machine lifecycle coordinator — Pending → Running → Completed/Failed |
| 🧩 | **4 Independent Workers** | OrderValidator · FraudChecker · InventoryChecker · NotificationSender |
| 📡 | **Real-time UI** | Flask + Server-Sent Events; falls back to 10s polling; 30s heartbeat watchdog |
| 📊 | **6 Grafana Dashboards** | Component health, workflow metrics, message rates, container resources, logs, alerts |
| 🔍 | **Centralized Logging** | Loki + promtail; JSON-structured logs; searchable by container, severity, or execution ID |
| 🚨 | **Infrastructure Alerts** | Prometheus rules for CPU/memory/disk (warning >80%, critical >90%); routed via Alertmanager |
| 🛡️ | **Idempotency & DLQ** | `processed_events` table prevents duplicate processing; `orchestration-dlq` topic for failures |
| 🧪 | **Comprehensive Tests** | Unit tests, integration tests (REST + gRPC), Playwright E2E tests |
| 📐 | **gRPC & REST APIs** | Dual-protocol API: REST (Flask) + gRPC (port 50051) for workflow orchestration |
| ✅ | **30+ Automated Tests** | Flask test-client, mocked DB, gRPC stubs, Playwright browser tests |

---

## 🏗️ Architecture

```
                        ┌─────────────────────────────────────────┐
                        │        WORKFLOW ORCHESTRATION            │
                        │                                          │
  ┌──────────┐          │  ┌─────────────┐      ┌──────────────┐  │
  │  Web UI  │◄─reads──►│  │ Orchestrator│─task─►│   Workers   │  │
  │ :5000    │          │  │             │◄─res──│  (4 threads) │  │
  │ gRPC:50051│         │  └──────┬──────┘      └──────────────┘  │
  └──────────┘          │         │ writes                          │
                        │  ┌──────▼──────┐  ┌──────────────────┐  │
  ┌──────────┐          │  │ PostgreSQL  │  │    Apache Kafka  │  │
  │ Producer │─events──►│  │             │  │  8 topics · DLQ  │  │
  │          │          │  └─────────────┘  └──────────────────┘  │
  └──────────┘          └─────────────────────────────────────────┘
  ┌──────────┐
  │Consumer 1│          ┌─────────────────────────────────────────┐
  │Consumer 2│          │         OBSERVABILITY STACK              │
  └──────────┘          │  Prometheus · Grafana · Loki · promtail  │
                        │  Alertmanager · cAdvisor                 │
                        └─────────────────────────────────────────┘
```

---

## 🚀 Quickstart

### Prerequisites

```bash
docker --version        # Docker 24+
docker compose version  # Compose v2.20+
```

### 1. Clone and configure

```bash
git clone https://github.com/matiaspakua/orquestador_workflows.git
cd orquestador_workflows
cp .env.example .env
```

### 2. Start the full stack

```bash
docker compose up --build -d
```

Wait ~30 seconds for Kafka and PostgreSQL to initialise:

```bash
docker compose ps   # all services should show "healthy" or "running"
```

### 3. Open the interfaces

| Interface | URL | Credentials |
|-----------|-----|-------------|
| **Workflow Dashboard** | http://localhost:5000/workflows | — |
| **gRPC API** | localhost:50051 | — |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090/targets | — |
| **Kafka UI** | http://localhost:8080 | — |
| **pgAdmin** | http://localhost:8081 | admin@event.com / admin123 |

### 4. Watch it run

After ~60 seconds, workflow executions start appearing in the dashboard. Each execution shows all 4 steps updating live. No page refresh needed.

---

## 🧪 Testing

### Unit + Integration Tests

```bash
# Unit tests (no Docker)
python -m pytest ui/tests/ common/ tests_support/ -v --cov=ui/services --cov=common

# Full test suite
bash scripts/run-all-tests.sh

# E2E tests (requires running UI on localhost:5000)
E2E_TESTS=1 python -m pytest ui/tests_e2e/ -v

# gRPC + REST Integration
python -m pytest common/test_integration_grpc.py -v
```

### Docker Integration Test Suite

```bash
docker compose -f docker-compose.test.yml up --build
```

### Playwright E2E

```bash
pip install pytest-playwright
playwright install chromium
E2E_TESTS=1 python -m pytest ui/tests_e2e/ -v
```

---

## 📁 Project Structure

```
orquestador_workflows/
├── common/                    # Shared modules
│   ├── config.py              # Environment configuration
│   ├── db.py                  # Database connection pooling
│   ├── grpc_service.py        # gRPC server implementation
│   ├── grpc_stub.py           # gRPC client stub
│   ├── health.py              # Health check endpoints
│   ├── logging_setup.py       # JSON logging setup
│   └── test_*.py              # Shared unit tests
│
├── orchestrator/              # Workflow state machine
├── workers/                   # 4 worker services
├── producer/                  # Domain event generator
├── consumer/                  # Domain event processor
├── ui/                        # Flask dashboard + gRPC
│   ├── app.py                 # Routes + gRPC server
│   ├── services/              # Parameterised DB queries
│   ├── templates/             # Jinja2 templates
│   ├── tests/                 # Unit tests (mocked DB)
│   └── tests_e2e/             # Playwright E2E tests
│
├── proto/                     # Protocol Buffers definitions
├── prometheus/                # Scrape config + rules
├── grafana/provisioning/      # Dashboard JSONs
├── docs/                      # Documentation
├── scripts/                   # Init SQL, test runner
├── tests_support/             # Test helper library
└── .env.example               # Configuration template
```

---

## 📊 Observability

### Grafana dashboards (pre-provisioned)

| Dashboard | Key panels |
|-----------|-----------|
| **Component Health** | Health status per service, publish/consume rates, error trends |
| **Workflow Metrics** | Execution counts, p50/p95/p99 duration histogram, error rate |
| **Message Metrics** | Kafka publish/consume rates, consumer lag per partition |
| **Container Resources** | CPU %, memory %, disk I/O, network per container |
| **Log Explorer** | Full log stream with container/severity filters |
| **Infrastructure Alerts** | Firing alerts with severity badges |

---

## 🔒 Security Notes

- **`.env` is gitignored** — never commit it; use `.env.example` as the template
- Flask runs with `debug=False` by default (`FLASK_DEBUG=0`)
- Prometheus `/metrics` endpoints are on the internal Docker network only
- All Kafka messages use `enable_auto_commit=False` — offsets committed on successful processing
- gRPC runs on internal Docker network (port 50051)

---

## 📄 License

MIT — see [LICENSE](LICENSE)

---

<div align="center">

Built as a portfolio project demonstrating event-driven architecture, real-time observability, and spec-first development.

*Python · Kafka · PostgreSQL · Flask · Prometheus · Grafana · Loki · Docker · gRPC*

</div>
