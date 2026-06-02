# Metrics Contract: /metrics Endpoint

## Endpoint Definition

- **Path**: `GET /metrics`
- **Port**: `8000` (all services — separate from application ports)
- **Protocol**: HTTP
- **Format**: [Prometheus exposition format](https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md) (plain text, `content-type: text/plain; version=0.0.4`)
- **Response Code**: `200 OK` (always, even if no metrics yet)

## Label Conventions

| Label | Allowed Values | Description |
|-------|---------------|-------------|
| `component` | `producer`, `consumer`, `workflow_engine` | Component type |
| `instance_id` | Unique string per process instance | e.g., `consumer-1`, `consumer-2` |
| `topic` | Kafka topic name | Only on message metrics |
| `status` | `running`, `completed`, `failed` | Workflow status |
| `workflow_name` | Workflow type identifier | e.g., `order_processing` |
| `error_type` | Error category string | e.g., `kafka_connection`, `timeout` |
| `partition` | Integer | Kafka partition ID |

## Required Metrics Per Component

### All Components (base)

```
# HELP component_info Static component metadata
# TYPE component_info gauge
component_info{component="<type>",instance_id="<id>",version="<semver>"} 1

# HELP uptime_seconds Seconds since process start
# TYPE uptime_seconds counter
uptime_seconds{component="<type>",instance_id="<id>"} <float>

# HELP health_status Component health (1=healthy, 0=unhealthy)
# TYPE health_status gauge
health_status{component="<type>",instance_id="<id>"} <1|0>

# HELP errors_total Total errors by category
# TYPE errors_total counter
errors_total{component="<type>",instance_id="<id>",error_type="<category>"} <int>
```

### Workflow Engine (adds to base)

```
# HELP workflow_executions_total Total workflow executions by name and status
# TYPE workflow_executions_total counter
workflow_executions_total{workflow_name="<name>",status="<status>"} <int>

# HELP workflow_running Currently running workflows
# TYPE workflow_running gauge
workflow_running{workflow_name="<name>"} <int>

# HELP workflow_duration_seconds Workflow execution duration histogram
# TYPE workflow_duration_seconds histogram
workflow_duration_seconds_bucket{workflow_name="<name>",le="<bucket>"} <int>
workflow_duration_seconds_sum{workflow_name="<name>"} <float>
workflow_duration_seconds_count{workflow_name="<name>"} <int>
```

### Producer (adds to base)

```
# HELP messages_published_total Total messages published by topic
# TYPE messages_published_total counter
messages_published_total{component="producer",instance_id="<id>",topic="<topic>"} <int>

# HELP messages_publish_rate Current publish rate in msg/s
# TYPE messages_publish_rate gauge
messages_publish_rate{component="producer",instance_id="<id>",topic="<topic>"} <float>
```

### Consumer (adds to base)

```
# HELP messages_consumed_total Total messages consumed by topic
# TYPE messages_consumed_total counter
messages_consumed_total{component="consumer",instance_id="<id>",topic="<topic>"} <int>

# HELP messages_consume_rate Current consume rate in msg/s
# TYPE messages_consume_rate gauge
messages_consume_rate{component="consumer",instance_id="<id>",topic="<topic>"} <float>

# HELP consumer_lag Kafka consumer lag by partition
# TYPE consumer_lag gauge
consumer_lag{component="consumer",instance_id="<id>",topic="<topic>",partition="<int>"} <int>
```

## Implementation Notes

- All counters MUST be initialized to 0 at process start (Prometheus handles this automatically)
- Histogram buckets MUST include the default Prometheus buckets plus workflow-specific values
- The `/metrics` endpoint MUST be served on a dedicated thread to avoid blocking application logic
- Metric names MUST use snake_case (Prometheus convention)
- There MUST be no authentication on the `/metrics` endpoint (Prometheus scrapes unauthenticated)
