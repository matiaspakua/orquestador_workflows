# Project Audit: orquestador_workflows

## Summary
This audit reviews the repository structure, runtime code, Docker orchestration, documentation, and dependency management. The project is a good start for a Kafka-backed event-driven workflow system, but it contains several critical gaps, ambiguous assumptions, and missing operational controls.

## Key Findings

### 1. Architecture and Implementation Gaps
- The README describes an orchestrator and monitoring stack, but the codebase only contains `producer`, `consumer`, and `ui`. There is no orchestrator service or monitoring stack implemented in code or Docker Compose.
- `docker-compose.yml` has no `docker-compose.dev.yml`, despite README references to it.
- Kafka event schema management and contract enforcement are absent. The system publishes arbitrary JSON payload references without schema validation, versioning, or a registry.
- The producer writes full payloads to PostgreSQL and emits only a reference in Kafka. This coupling is valid but not documented as an explicit design decision. It also implies strong data consistency and retry semantics that are not handled.

### 2. Dependency and Packaging Issues
- `requirements.txt` files include `datetime`, which is part of the Python standard library and should not be listed as an install dependency.
- Dependencies are not version-pinned anywhere. This reduces reproducibility and makes builds fragile across environments.
- `ui/requirements.txt` includes `flask` but no production-ready WSGI server such as `gunicorn`.

### 3. Docker and Runtime Inconsistencies
- `producer/Dockerfile`, `consumer/Dockerfile`, and `ui/Dockerfile` expose port `8000`, but the Flask UI listens on port `5000` and producer/consumer do not expose any HTTP metrics port at all.
- Dockerfiles contain comments about Prometheus metrics but there is no actual metrics instrumentation or `/metrics` endpoint in the services.
- The `ui` Dockerfile exposes both `8000` and `5000`, which is confusing and inconsistent with the actual application.
- `docker-compose.yml` does not define resource limits (`cpus`, `mem_limit`) for containers, which is a best practice for Docker Compose deployments.

### 4. Database and Schema Issues
- `scripts/init-db.sql` uses `gen_random_uuid()` without creating or verifying the `pgcrypto` extension. PostgreSQL 15 Alpine will likely fail this SQL unless the extension is enabled.
- The SQL initialization file creates `system_metrics`, but no code writes to or uses it.
- `producer` and `consumer` default to `eventstore` if environment variables are missing, but `.env.example` and `.env` define `eventdb`. This mismatch can hide configuration errors if environment variables are not set.
- There is no migration framework; the database setup relies on a single raw SQL script.

### 5. Security and Operational Risks
- `.gitignore` does not include `.env`, so sensitive environment configuration can be committed to the repository.
- A `.env` file exists in the repo root with database and Kafka settings in plain text. This is a secret management risk.
- `pgAdmin` uses default credentials in `docker-compose.yml` (`admin@event.com` / `admin123`). This is insecure for any shared or production-like environment.
- `kafka-ui` is pinned to `latest`, which is non-deterministic and can introduce compatibility drift.
- There are no health checks for `producer`, `consumer`, or `web-ui` services.

### 6. Logging and Monitoring Gaps
- Logging is JSON formatted, but there is no centralized logging or structured log collection configured.
- Comments and Dockerfiles reference Prometheus and metrics, but no actual metrics export is implemented.
- There is no monitoring endpoint, no metrics instrumentation, no alerting, and no tracing.

### 7. Testing and Quality Assurance
- No test files are present in the repository.
- The README mentions integration tests in `specs/005-producer-consumer-test/`, but there is no code-based automated test suite.
- Without tests, changes to Kafka behavior, database schema, or consumer logic cannot be validated automatically.

### 8. UI and API Concerns
- `ui/app.py` runs Flask with `debug=True` by default, which is unsafe for production and can leak sensitive information.
- The UI code opens many raw PostgreSQL connections without pooling.
- API routes return generic error messages containing raw exception text, which may leak internal state.

### 9. Kafka and Event Processing Risks
- Consumers use `enable_auto_commit=False` but call `commit_async()` only on success. This is acceptable, but there is no dead-letter queue or retry policy for failed events.
- There is no idempotency or duplicate detection in consumer processing. Reprocessing the same event may lead to duplicate state changes.
- Topic creation is allowed via `KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'`. This can hide configuration problems and is usually discouraged in production.
- `kafka` healthcheck inside the container uses `localhost:9092`; this may be unreliable depending on listener configuration.

## Recommendations

### Short-term fixes
- Remove `datetime` from all `requirements.txt` files.
- Add `.env` and appropriate Python artifacts to `.gitignore`.
- Pin dependency versions in `requirements.txt` or use a lock file.
- Add a `docker-compose.dev.yml` or remove README references if it is not provided.
- Fix the Dockerfile port exposure to match actual service ports and remove misleading Prometheus comments.
- Disable `debug=True` in `ui/app.py` for non-development environments.
- Add a `CREATE EXTENSION IF NOT EXISTS pgcrypto;` or change UUID generation to avoid `gen_random_uuid()` dependency.

### Medium-term improvements
- Add schema validation for Kafka events and/or integrate a Schema Registry or contract validation layer.
- Add schema migration tooling such as Flyway for PostgreSQL.
- Add service healthchecks for producer, consumer, and web-ui.
- Introduce metrics instrumentation and export a Prometheus endpoint if monitoring is required.
- Add at least unit tests and integration tests for producer, consumer, and API behavior.

### Security and stability hardening
- Remove the tracked `.env` file from the repository and replace it with a `.env.example` only.
- Replace `kafka-ui:latest` with a pinned version.
- Add container memory/cpu limits in `docker-compose.yml`.
- Consider a secrets management approach for production credentials.
- Add a dead-letter queue or retry policy for failed Kafka messages.
- Add explicit topic creation and configuration rather than relying on auto-creation.

## Overall Assessment
The repo demonstrates a functioning event pipeline prototype, but it should not be treated as production-ready. The biggest risks are secret leakage, missing operational controls, absent testing, and incomplete implementation of key architectural components such as orchestrator and monitoring. Addressing the above findings will improve reliability, security, and maintainability.
