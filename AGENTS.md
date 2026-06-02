<!-- SPECKIT START -->
All services use Python 3.14, Docker Compose, Kafka 7.4.0, PostgreSQL 15.

Current feature: specs/005-producer-consumer-test/ — integration test suite (28 tasks across 6 phases).

Full project architecture:
- specs/001-workflow-progress-ui/ — Real-time dashboard for workflow execution monitoring (Flask + Jinja2, SSE)
- specs/002-grafana-telemetry/ — Functional telemetry dashboards via Grafana + Prometheus (prometheus_client, /metrics endpoint)
- specs/003-workflow-definition/ — Workflow lifecycle, step types, orchestration event schemas (JSON Schema, Kafka events)
- specs/004-prometheus-monitoring/ — Infrastructure monitoring: Prometheus + cAdvisor + Loki + Alertmanager
- specs/005-producer-consumer-test/ — Integration test suite for producer → Kafka → consumer message flow
<!-- SPECKIT END -->
