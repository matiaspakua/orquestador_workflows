## Container Metric

| Field               | Type       | Description                                      | Source                |
|---------------------|------------|--------------------------------------------------|-----------------------|
| container_name      | string     | Docker container name                            | cAdvisor              |
| cpu_percent         | float      | CPU usage as percentage of a core                | cAdvisor              |
| memory_usage_bytes  | integer    | Current memory usage in bytes                    | cAdvisor              |
| memory_limit_bytes  | integer    | Memory limit configured for the container        | cAdvisor              |
| memory_percent      | float      | Memory usage as percentage of limit              | cAdvisor (computed)   |
| disk_read_bytes_total | counter  | Cumulative bytes read from disk                  | cAdvisor              |
| disk_write_bytes_total| counter  | Cumulative bytes written to disk                 | cAdvisor              |
| network_rx_bytes_total | counter | Cumulative bytes received on network            | cAdvisor              |
| network_tx_bytes_total | counter | Cumulative bytes transmitted on network         | cAdvisor              |
| timestamp           | timestamp  | Collection time                                  | Prometheus scrape     |

**Prometheus metrics** (cAdvisor exposes these; the above fields map to the following PromQL expressions or raw cAdvisor metric names):

| Field                | PromQL / cAdvisor Metric                                                            |
|----------------------|--------------------------------------------------------------------------------------|
| cpu_percent          | `rate(container_cpu_usage_seconds_total[5s]) * 100`                                  |
| memory_usage_bytes   | `container_memory_working_set_bytes`                                                 |
| memory_limit_bytes   | `container_spec_memory_limit_bytes`                                                  |
| memory_percent       | `container_memory_working_set_bytes / container_spec_memory_limit_bytes * 100`       |
| disk_read_bytes_total| `container_fs_reads_bytes_total`                                                     |
| disk_write_bytes_total| `container_fs_writes_bytes_total`                                                   |
| network_rx_bytes_total | `container_network_receive_bytes_total`                                             |
| network_tx_bytes_total | `container_network_transmit_bytes_total`                                            |

## Log Entry

| Field            | Type              | Description                                         |
|------------------|-------------------|-----------------------------------------------------|
| component        | string            | Logical component name (producer, consumer, web-ui, orchestrator, kafka) |
| severity         | enum (DEBUG, INFO, WARN, ERROR, FATAL) | Log severity level                   |
| message          | text              | Free-form log message                               |
| source_container | string            | Docker container name that produced the log         |
| timestamp        | timestamp         | Time the log was emitted (RFC3339)                  |
| labels           | map<string,string> | Additional context — workflow_id, step_id, etc.    |

**Loki label mapping** (promtail extracts from JSON log stream):

| Log Entry Field   | Loki Label             |
|-------------------|------------------------|
| component         | `component`            |
| severity          | `severity`             |
| source_container  | `container_name` (Docker driver label) |
| labels.*          | Inferred from JSON keys |

## Alert Rule

| Field             | Type                            | Description                                          |
|-------------------|---------------------------------|------------------------------------------------------|
| name              | string                          | Unique rule identifier                                |
| metric_type       | enum (cpu, memory, disk, network) | Which metric the rule monitors                      |
| threshold_operator| enum (gt, lt, gte, lte)         | Comparison operator for the threshold                |
| threshold_value   | float                           | Value to compare against                             |
| severity          | enum (warning, critical)        | Alert severity level                                 |
| duration_seconds  | integer                         | How long the condition must persist before firing (for) |

**Prometheus recording rule format** (defined in `prometheus/alert-rules.yml`):

```yaml
groups:
  - name: container_alerts
    rules:
      - alert: HighCpuUsage
        expr: (rate(container_cpu_usage_seconds_total{name=~".+"}[5m]) * 100) > 80
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Container {{ $labels.name }} CPU usage > 80%"
```

## Alert Instance

| Field              | Type                        | Description                                  |
|--------------------|-----------------------------|----------------------------------------------|
| rule_name          | string (FK → Alert Rule)    | Which rule triggered this alert              |
| status             | enum (firing, resolved)     | Current alert state                          |
| started_at         | timestamp                   | When the alert first fired                   |
| resolved_at        | timestamp (nullable)        | When the alert auto-resolved                 |
| current_value      | float                       | Current metric value at last evaluation      |
| affected_container | string                      | Container that triggered the alert           |

**Note**: Alert instances are not stored in a custom database — they are managed entirely by Prometheus Alertmanager and visible via its API (`/api/v2/alerts`) and web UI.
