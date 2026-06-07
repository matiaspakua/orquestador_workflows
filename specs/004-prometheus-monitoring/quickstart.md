# Prometheus Monitoring — Quickstart

## Accessing Monitoring Dashboards

| Service        | URL                              | Credentials                          |
|----------------|----------------------------------|--------------------------------------|
| Prometheus     | http://localhost:9090             | None                                 |
| Alertmanager   | http://localhost:9093             | None                                 |
| Loki           | http://localhost:3100/ready       | None (health check)                  |
| cAdvisor       | http://localhost:8080             | None                                 |
| Grafana*       | http://localhost:3000             | `admin` / `admin` (spec 002)         |

*\* Grafana is configured as part of spec 002. Add Prometheus and Loki as data sources to view metrics and logs.*

## Pre-configured Alert Rules

| Alert Name | Metric | Threshold | For | Severity | Action |
|---|---|---|---|---|---|
| HighCpuUsage | CPU | > 80% | 2m | warning | Check container processes, scale if needed |
| CriticalCpuUsage | CPU | > 95% | 1m | critical | Immediate investigation required |
| HighMemoryUsage | Memory | > 80% | 2m | warning | Review memory limits, consider scaling |
| CriticalMemoryUsage | Memory | > 95% | 1m | critical | Out-of-memory risk, investigate immediately |
| HighDiskIoUsage | Disk I/O | > 100MB/s | 2m | warning | Check for I/O-intensive operations |
| ContainerDown | Container up | == 0 | 1m | critical | Container has stopped — check Docker logs |

## Searching Logs

### Via Grafana Explore
1. Open Grafana (http://localhost:3000) and navigate to **Explore** (compass icon).
2. Select the **Loki** data source.
3. Use LogQL queries:
   - `{component="producer"}` — all logs from the producer
   - `{component=~"consumer.*"}` — logs from both consumers
   - `{severity="ERROR"}` — all error logs
   - `{component="web-ui"} |= "timeout"` — web-ui logs containing "timeout"

### Via Loki API (direct)
```bash
# Query last hour of producer logs
curl -G http://localhost:3100/loki/api/v1/query_range \
  --data-urlencode 'query={component="producer"}' \
  --data-urlencode 'start=1h' | jq
```

## Adding a New Alert Rule

1. Open `prometheus/alert-rules.yml`.
2. Add a new rule under the existing `groups[].rules` list:

```yaml
- alert: HighNetworkTraffic
  expr: rate(container_network_receive_bytes_total{name=~".+"}[5m]) > 1e8
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "Container {{ $labels.name }} network RX > 100 MB/s"
```

3. Reload Prometheus configuration without restarting:
```bash
curl -X POST http://localhost:9090/-/reload
```

4. Verify the rule appears at **http://localhost:9090/alerts**.

## Troubleshooting

### Metrics not appearing

1. Verify cAdvisor is running: `docker compose ps cadvisor`
2. Check cAdvisor metrics endpoint: `curl http://localhost:8080/metrics | head -20`
3. Verify Prometheus can scrape cAdvisor: visit **http://localhost:9090/targets** — cAdvisor should show `UP`
4. Check Prometheus config: `curl http://localhost:9090/api/v1/status/config`

### Logs not appearing in Loki

1. Check promtail is running: `docker compose ps promtail`
2. Verify promtail can reach Loki: `docker compose logs promtail | tail -20`
3. Check Loki readiness: `curl http://localhost:3100/ready`
4. Verify logs are labeled correctly: Query `{container_name=~".+"}` in Grafana Explore

### Alerts not firing

1. Check Alertmanager status: `docker compose ps alertmanager`
2. Verify Prometheus can reach Alertmanager: visit **http://localhost:9090/status** — Alertmanager URL should be configured
3. Check alert rules are loaded: visit **http://localhost:9090/rules**
4. Manually test a rule by temporarily lowering the threshold in `prometheus/alert-rules.yml` and reloading

### Retention not working

- **Prometheus** (30 days): Retention is set via `--storage.tsdb.retention.time=30d` in the Prometheus command or `PROMETHEUS_RETENTION` env var
- **Loki** (7 days): Retention is configured in `loki/loki-config.yml` under `table_manager.retention_period: 168h` (7 × 24h)
- To verify: check the data directory sizes with `docker compose exec prometheus du -sh /prometheus`
