# Implementation Plan: Prometheus Monitoring

**Branch**: `004-prometheus-monitoring` | **Date**: 2026-06-02 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/004-prometheus-monitoring/spec.md`

## Summary

Infrastructure monitoring stack — Prometheus for resource metrics, Grafana Loki for log aggregation, Alertmanager for alert handling. Runs alongside system components in Docker Compose.

## Technical Context

**Language/Version**: Python 3.14

**Primary Dependencies**: Prometheus (latest), Grafana Loki (3.x), promtail (3.x), Alertmanager (latest), cAdvisor (latest), Docker Compose

**Storage**: Prometheus TSDB (local volume, 30-day retention), Loki filesystem/object storage (7-day retention)

**Testing**: Manual verification of dashboard rendering; `docker compose logs` for log aggregation validation; alert rule testing via threshold override

**Target Platform**: Linux Docker containers (local dev via Docker Compose)

**Project Type**: Multi-service Docker Compose application with monitoring sidecars

**Performance Goals**: Metrics scrape <1s per container; log search <10s across 24h window; alert fires <1min after threshold breach

**Constraints**: <5% overhead on monitored components; no changes to existing service business logic; all monitoring containers on event-network

**Scale/Scope**: 5 container types (producer, consumer1, consumer2, web-ui, orchestrator), multi-instance via Prometheus labels

## Constitution Check

1. **Small, Reversible Steps** — PASS. Each user story maps to an independent component (Prometheus for US1, Loki for US2, Alertmanager for US3). Monitoring services are additive in docker-compose and can be removed without touching application code.

2. **Progressive Disclosure** — PASS. Resource dashboards (US1, P1) are the default view. Log aggregation (US2, P2) is a separate tab/section. Alert configuration (US3, P3) is exposed via a configuration file, not the default view.

3. **Functional First, Cosmetic Last** — PASS. Working metric collection and log aggregation are implemented first. Dashboard layout, color schemes, and UI polish are deferred until core pipelines are verified.

4. **Prefer Working Software Over Documentation** — PASS. The primary deliverable is a running monitoring stack visible at known ports. Documentation supports reproducibility and maintenance.

5. **Separate Features from Infrastructure** — PASS. Prometheus, Loki, Alertmanager, and cAdvisor are defined in docker-compose.yml alongside existing services with no cross-contamination of application business logic.

## Project Structure

### Documentation (this feature)

```text
specs/004-prometheus-monitoring/
├── plan.md                # This file
├── research.md            # Technology decisions
├── data-model.md          # Entity definitions
├── quickstart.md          # How to access and use the monitoring stack
├── contracts/
│   └── monitoring-contract.md  # Port, label, metric, and log conventions
├── checklists/            # Existing checklists
└── spec.md                # Feature specification
```

### Source Code (repository root)

```text
docker-compose.yml           # +prometheus, +loki, +promtail, +alertmanager, +cadvisor

prometheus/
├── prometheus.yml           # Scrape config for cAdvisor, Alertmanager, and self
├── alert-rules.yml          # Alert rule definitions (CPU, memory, disk)

loki/
└── loki-config.yml          # Loki storage, retention, and ingestion config

promtail/
└── promtail-config.yml      # Log scrape config targeting Docker socket

alertmanager/
└── alertmanager.yml         # Alert routing, inhibition, and receivers config
```

**Structure Decision**: All monitoring configuration lives in top-level directories alongside the existing `prometheus/` directory (from spec 002). No new source directories — the stack is entirely configuration-driven (YAML files mounted as Docker volumes).

## Complexity Tracking

No violations. The architecture is additive, non-invasive, and follows the same pattern established by spec 002 (Grafana Telemetry).
