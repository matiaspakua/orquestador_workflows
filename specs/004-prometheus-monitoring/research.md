## Resource Metrics Collection
- **Decision**: cAdvisor for per-container CPU/memory/disk/network metrics
- **Rationale**: Runs as a Docker container, zero code changes to services, Prometheus-native endpoint, exposes all four resource types (CPU, memory, disk, network) out of the box
- **Alternatives**: Prometheus Node Exporter (host-level only, cannot attribute metrics per container), Docker stats API (no Prometheus integration, requires polling from application code), Prometheus Docker SD (service discovery only, not metric collection)

## Log Aggregation
- **Decision**: Grafana Loki + promtail
- **Rationale**: Purpose-built for Docker logs, native Prometheus/Grafana integration, low resource footprint (no indexing, just metadata labels), supports structured JSON log parsing
- **Alternatives**: ELK stack (Elasticsearch is resource-heavy for this scale, requires more memory and CPU), Graylog (additional JVM dependency, overkill for 5 containers), Fluentd + Elasticsearch (complex pipeline, higher operational overhead)

## Alerting
- **Decision**: Prometheus Alertmanager with web UI
- **Rationale**: Native Prometheus integration, supports auto-resolve out of the box, rule templating with Go templates, inhibition rules to reduce noise
- **Alternatives**: Grafana alerts (dashboard-scoped only, no auto-resolve without additional configuration), external services like PagerDuty/OpsGenie (out of scope — spec explicitly excludes notification dispatch)

## Retention Strategy
- **Decision**: Prometheus TSDB with 30-day retention, Loki with 7-day retention
- **Rationale**: Matches FR-008 (metrics 30 days) and FR-009 (logs 7 days); Prometheus compacts older data automatically; Loki uses filesystem storage with configurable retention period; both support retention via simple YAML configuration

## Log Format Standard
- **Decision**: JSON-structured logging via Python's `json-logger` library
- **Rationale**: Promtail can parse JSON log lines natively and extract structured fields (severity, component, message) as Loki labels; avoids need for custom regex parsing
- **Alternatives**: Plain text logs (requires regex parsing in promtail, more fragile), Logstash-style format (adds unnecessary complexity for this scale)

## Service Discovery
- **Decision**: Docker Compose project name + container name labels in Prometheus
- **Rationale**: All containers are known at compose time; static targets in `prometheus.yml` are simpler and more predictable than dynamic service discovery (Consul, Docker SD) for a fixed set of services
