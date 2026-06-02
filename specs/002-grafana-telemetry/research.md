## Metrics Collection
- **Decision**: prometheus_client Python library, /metrics HTTP endpoint per component
- **Rationale**: Industry standard for Python services; no additional infrastructure needed; minimal overhead (~1ms per scrape)
- **Alternatives**: statsd (requires aggregator daemon, adds deployment complexity), OpenTelemetry (richer feature set but significantly heavier dependency and more complex configuration for a pure metrics use case)

## Dashboard Platform
- **Decision**: Grafana OSS with Prometheus data source
- **Rationale**: Mature ecosystem, Kubernetes-native, supports configurable time ranges with built-in time picker, rich alerting capabilities, free and open source
- **Alternatives**: Datadog (SaaS, per-host licensing cost), Graphite + Graphana (legacy stack, less active development)

## Historical Storage
- **Decision**: Prometheus TSDB (retention 30d)
- **Rationale**: Prometheus handles 30-day retention natively with `--storage.tsdb.retention.time=30d`; no additional storage service needed; single-binary deployment via Docker
- **Alternatives**: InfluxDB (requires separate service, adds operational overhead), Thanos (object-store based, overkill for this scale)

## Multi-instance Support
- **Decision**: Prometheus labels per instance (`component`, `instance_id`, `topic`)
- **Rationale**: Enables per-instance breakdown and aggregate views via Grafana without duplicating metric definitions; Prometheus handles label-based aggregation natively
- **Alternatives**: Separate metric names per instance (pollutes metric namespace), separate Prometheus jobs (overkill for 2-3 consumer instances)
