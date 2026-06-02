# Implementation Plan: Grafana Telemetry

**Branch**: `002-grafana-telemetry` | **Date**: 2026-06-02 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/002-grafana-telemetry/spec.md`

## Summary

Grafana dashboards for functional telemetry — component health, workflow metrics, and message processing metrics. Each component exposes metrics via a lightweight `/metrics` HTTP endpoint consumed by Prometheus and visualized in Grafana.

## Technical Context

**Language/Version**: Python 3.14

**Primary Dependencies**: prometheus_client (Python library), Grafana OSS (latest), Prometheus (latest), Docker Compose

**Storage**: Prometheus TSDB (local volume, 30-day retention)

**Testing**: pytest for metric endpoint contracts; manual verification of dashboard rendering

**Target Platform**: Linux Docker containers (local dev via Docker Compose)

**Project Type**: Web monitoring stack (multi-service Docker Compose application)

**Performance Goals**: Metrics scrape completes in <1s per component; dashboard loads in <5s

**Constraints**: <15s delay for message metrics; <200MB additional memory per monitored component; no modification to existing business logic

**Scale/Scope**: 3 component types (producer, consumer, workflow-engine), multi-instance support via Prometheus labels

## Constitution Check

1. **Small, Reversible Steps** — PASS. Each user story maps to an independent dashboard panel. Metrics are additive and non-breaking. Prometheus and Grafana can be added without touching existing service code.
2. **Progressive Disclosure** — PASS. Dashboards are organized by user story priority: health (US1) always visible first, workflow metrics (US2) in a dedicated row, message metrics (US3) in a separate row.
3. **Functional First, Cosmetic Last** — PASS. The implementation starts with metric exposition (prometheus_client endpoint), then Prometheus scraping, then core dashboard panels. Styling and layout refinements are last.
4. **Prefer Working Software Over Documentation** — PASS. The primary deliverable is a working dashboard visible in the browser. Documentation (this plan, quickstart, contracts) supports reproducibility.
5. **Separate Features from Infrastructure** — PASS. Monitoring infrastructure (Prometheus, Grafana) is defined in docker-compose.yml alongside the existing services, with no cross-contamination of business logic.

## Project Structure

### Documentation (this feature)

```text
specs/002-grafana-telemetry/
├── plan.md              # This file
├── research.md          # Technology decisions
├── data-model.md        # Entity definitions
├── quickstart.md        # How to access and extend dashboards
├── contracts/
│   └── metrics-contract.md  # /metrics endpoint contract
├── checklists/          # Existing checklists
└── spec.md              # Feature specification
```

### Source Code (repository root)

```text
docker-compose.yml        # +prometheus, +grafana services
prometheus/
└── prometheus.yml        # Scrape config targeting all component ports

# Each existing service adds a /metrics endpoint on port 8000
src/
├── producer/             # +prometheus_client metrics
├── consumer1/            # +prometheus_client metrics
├── consumer2/            # +prometheus_client metrics
└── web-ui/               # No metrics (infra-facing, not user-facing)
```

**Structure Decision**: Monitoring services added to the existing Docker Compose file alongside current services. Each component exposes metrics on its own port 8000, avoiding changes to existing API endpoints (port 5000 for web UI, Kafka native ports). No new source directories — changes are additive within existing component packages.

## Complexity Tracking

No violations. The architecture is additive and non-invasive.
