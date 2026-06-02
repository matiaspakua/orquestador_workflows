# Quickstart: Grafana Telemetry

## Accessing Dashboards

1. Start the full stack:
   ```bash
   docker compose up -d
   ```

2. Access Grafana at [http://localhost:3000](http://localhost:3000)
   - Default credentials: `admin` / `admin`

3. Navigate to **Dashboards > Browse** to see the available dashboards.

## Built-in Dashboards

| Dashboard | User Story | Description |
|-----------|-----------|-------------|
| **Component Health** | US1 (P1) | Health status, uptime, error counts for all components. Single-pane-of-glass view. |
| **Workflow Metrics** | US2 (P2) | Execution counts by status, duration percentiles, error rate trend. |
| **Message Metrics** | US3 (P3) | Publish/consume rates, total counts, consumer lag by partition. |

## Time Range Controls

Every dashboard supports configurable time ranges via the top-right time picker:
- **Presets**: Last 5 minutes, 15 minutes, 30 minutes, 1 hour, 3 hours, 6 hours, 12 hours, 24 hours, 7 days, 30 days
- **Custom**: Any absolute or relative range via the picker

## Adding New Metrics

1. **In your service code**:
   ```python
   from prometheus_client import Counter, Gauge, Histogram, start_http_server

   # Define a new metric
   my_counter = Counter('my_metric_total', 'Description', ['label1', 'label2'])
   my_counter.labels(label1='val1', label2='val2').inc()

   # Start /metrics server (already done in the base service class)
   start_http_server(8000)
   ```

2. **Verify exposition**:
   ```bash
   curl http://localhost:8000/metrics | grep my_metric
   ```

3. **Add to Prometheus auto-discovery**: New ports are picked up automatically if added to the `targets` list in `prometheus/prometheus.yml`.

4. **Create a Grafana panel**: Open any dashboard, click **Add > Visualization**, select the Prometheus data source, and query your metric.
