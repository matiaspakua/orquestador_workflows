# Data Model: Grafana Telemetry

## Component Telemetry

Exposed by every monitored service on GET /metrics.

| Field | Type | Labels | Description |
|-------|------|--------|-------------|
| `component_info` | gauge | `component`, `instance_id`, `version` | Static metadata; value always 1 |
| `uptime_seconds` | counter | `component`, `instance_id` | Seconds since process start |
| `health_status` | gauge | `component`, `instance_id` | 1 = healthy, 0 = unhealthy |
| `messages_processed_total` | counter | `component`, `instance_id` | Total messages processed since start |
| `errors_total` | counter | `component`, `instance_id`, `error_type` | Total errors by category |
| `last_heartbeat_timestamp` | gauge | `component`, `instance_id` | Unix timestamp of last activity |

## Workflow Metric

Exposed by the Workflow Engine component.

| Field | Type | Labels | Description |
|-------|------|--------|-------------|
| `workflow_executions_total` | counter | `workflow_name`, `status` | Total executions grouped by status |
| `workflow_running` | gauge | `workflow_name` | Currently running workflow count |
| `workflow_duration_seconds` | histogram | `workflow_name` | Execution duration (buckets: 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120) |
| `workflow_error_rate` | gauge | `workflow_name` | Ratio of failed to total executions (recent window) |

### Computed from histograms

- `avg`: avg(`workflow_duration_seconds`)
- `min`: min bucket lower bound with observations
- `max`: max bucket upper bound with observations
- `p50`, `p95`, `p99`: histogram_quantile() queries

## Message Metric

Exposed by Producer and Consumer components.

| Field | Type | Labels | Description |
|-------|------|--------|-------------|
| `messages_published_total` | counter | `component`, `instance_id`, `topic` | Total messages published |
| `messages_consumed_total` | counter | `component`, `instance_id`, `topic` | Total messages consumed |
| `messages_publish_rate` | gauge | `component`, `instance_id`, `topic` | Messages per second (recent window) |
| `messages_consume_rate` | gauge | `component`, `instance_id`, `topic` | Messages per second (recent window) |
| `consumer_lag` | gauge | `component`, `instance_id`, `topic`, `partition` | Kafka consumer lag (from `kafka-python` `highwatermark` - `offset`) |
