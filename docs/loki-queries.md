# Loki Query Presets

Common LogQL queries for the Log Explorer dashboard and ad-hoc debugging.

## By Component

```logql
# All producer logs
{container="producer"}

# All consumer logs (any instance)
{container=~"consumer.*"}

# Web UI logs
{container="web-ui"}

# Kafka broker logs
{container="kafka"}
```

## By Severity

```logql
# Errors across all containers
{container=~".+"} | json | level = "ERROR"

# Warnings and above
{container=~".+"} | json | level =~ "WARNING|ERROR"

# Info and below (verbose)
{container=~".+"} | json | level =~ "DEBUG|INFO"
```

## By Keyword

```logql
# All Kafka-related errors
{container=~".+"} | json | message =~ "(?i)kafka"

# Database connection issues
{container=~".+"} | json | message =~ "(?i)postgres|psycopg|connection"

# Message publish activity
{container="producer"} | json | message =~ "(?i)published|publish"

# Message consume activity
{container=~"consumer.*"} | json | message =~ "(?i)consumed|processed"
```

## By workflow_execution_id (Correlation)

```logql
# Trace all log lines for a specific workflow execution
{container=~".+"} | json | workflow_execution_id = "replace-with-uuid"
```

## Performance and Rate Queries

```logql
# Log rate per container over 1 minute
sum by (container) (rate({container=~".+"}[1m]))

# Error rate per container
sum by (container) (rate({container=~".+"} | json | level="ERROR" [5m]))
```

## Time Range Filtering

When using Grafana, set the time range picker in the dashboard header.  
For ad-hoc CLI queries:

```bash
# Last 15 minutes of errors
curl -G 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={container=~".+"} | json | level="ERROR"' \
  --data-urlencode "start=$(date -v -15M +%s)000000000" \
  --data-urlencode "end=$(date +%s)000000000" \
  | jq '.data.result[].values[][1]'
```
