# Monitoring Contract

## Prometheus Metric Naming Conventions

All custom metrics follow the [Prometheus naming convention](https://prometheus.io/docs/practices/naming/):

- **Namespaces**: Use `component_name` as prefix (e.g., `producer_messages_sent_total`)
- **Units** are suffixed: `_total` for counters, `_seconds` for durations, `_bytes` for sizes, `_percent` for ratios
- **Underscores** separate words, no camelCase or hyphens
- **No generic names** — every metric must be uniquely identifiable

Example:
```
producer_messages_sent_total{container="producer-1"} 1024
consumer_processing_duration_seconds{container="consumer-1"} 0.45
```

## Label Conventions

### Resource Metrics (cAdvisor)

| Label               | Description                                           | Example               |
|---------------------|-------------------------------------------------------|-----------------------|
| `name`              | Docker container name (cAdvisor default)              | `producer-1`          |
| `image`             | Docker image name                                     | `orquestador-producer`|
| `container_label_component` | Custom label for logical grouping              | `producer`            |
| `instance`          | Host:port of the cAdvisor target                     | `cadvisor:8080`       |

### Log Entries (Loki)

| Label               | Description                                           | Source                 |
|---------------------|-------------------------------------------------------|------------------------|
| `container_name`    | Docker container name                                 | Docker log driver      |
| `component`         | Logical component name                                | JSON log field         |
| `severity`          | Log severity level                                    | JSON log field         |
| `compose_project`   | Docker Compose project name                           | Docker label           |
| `workflow_id`       | Workflow identifier (when available)                  | JSON log field         |

**All labels must be lowercase** with underscores separating words.

## Log Format Standard

All services produce structured JSON logs on stdout/stderr using Python's `json-logger` or equivalent:

```json
{
  "timestamp": "2026-06-02T10:30:00Z",
  "component": "producer",
  "severity": "INFO",
  "message": "Message sent successfully",
  "labels": {
    "workflow_id": "wf-abc123",
    "step_id": "step-1"
  }
}
```

Required fields: `timestamp` (RFC3339), `component`, `severity`, `message`.

Optional fields: `labels` (map of key-value pairs for additional context).

**Severity values**: `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`.

## Alert Rule Format

Alert rules are defined in YAML following the [Prometheus alerting rules format](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/):

```yaml
groups:
  - name: <group_name>
    rules:
      - alert: <AlertName>
        expr: <PromQL expression>
        for: <duration>
        labels:
          severity: <warning|critical>
        annotations:
          summary: "<human-readable summary>"
          description: "<human-readable description with $labels>"
```

**Pre-configured rules** (see `prometheus/alert-rules.yml`):

| Alert Name | Metric | Threshold | For | Severity |
|---|---|---|---|---|
| HighCpuUsage | CPU | > 80% | 2m | warning |
| CriticalCpuUsage | CPU | > 95% | 1m | critical |
| HighMemoryUsage | Memory | > 80% | 2m | warning |
| CriticalMemoryUsage | Memory | > 95% | 1m | critical |
| HighDiskIoUsage | Disk I/O | > 100MB/s | 2m | warning |
| ContainerDown | Container up | == 0 | 1m | critical |

## Port Conventions

| Service        | Port   | Protocol | Purpose                    |
|----------------|--------|----------|----------------------------|
| Prometheus     | 9090   | HTTP     | Web UI, API, query         |
| Alertmanager   | 9093   | HTTP     | Web UI, API, alert dispatch|
| Loki           | 3100   | HTTP     | Log ingestion and query API|
| cAdvisor       | 8080   | HTTP     | Metrics endpoint, web UI   |
| promtail       | (none) | —        | No HTTP port, pushes to Loki|

All monitoring ports are mapped on localhost only (`127.0.0.1`) in docker-compose to avoid external exposure.

## Service Dependencies

```
cAdvisor (metrics) ──► Prometheus ──► Alertmanager (alerts)
                    │                   │
                    │                   └── Web UI (port :9093)
                    │
                    └── Grafana (spec 002, consumes Prometheus datasource)

Containers (logs) ──► promtail ──► Loki ──► Grafana (Loki datasource)
```

## Prometheus Scrape Configuration

All scrape targets use the following standard configuration:

```yaml
scrape_configs:
  - job_name: '<service_name>'
    scrape_interval: 15s
    scrape_timeout: 10s
    static_configs:
      - targets: ['<host>:<port>']
        labels:
          job: '<service_name>'
```
