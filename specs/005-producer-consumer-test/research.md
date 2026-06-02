# Research: Producer-Consumer Test

## Language & Runtime

**Decision**: Python 3.11 for all microservices (consumer, producer, UI)

**Rationale**: All existing components are already implemented in Python 3.11 (confirmed via Dockerfiles using `python:3.11-slim`). Maintaining a single language across the backend ensures consistent testing tooling, shared Kafka/PostgreSQL libraries, and developer familiarity.

**Alternatives considered**:
- Go: Higher throughput but would introduce polyglot complexity for a testing feature
- Node.js: Not present in the existing stack; no benefit for event-processing workloads

## Testing Framework

**Decision**: pytest with Docker Compose integration

**Rationale**: pytest is the de facto Python testing framework with strong fixture support, parallel execution (pytest-xdist), and excellent integration with Docker-based test environments. The existing `docker-compose.test.yml` provides the isolated environment.

**Alternatives considered**:
- unittest: Built-in but less ergonomic for complex integration scenarios
- behave: Cucumber-style BDD adds overhead without clear benefit for this test scope

## Messaging & Contracts

**Decision**: Reuse existing Confluent Kafka 7.4.0 with isolated test topics

**Rationale**: The existing Kafka infrastructure (cp-kafka 7.4.0, cp-zookeeper 7.4.0) provides the message backbone. Test topics with unique prefixes ensure production isolation. kafka-python library is already used in both producer and consumer components.

## Database Testing

**Decision**: Isolated PostgreSQL 15 test database with transactional test fixtures

**Rationale**: Tests require database isolation for reliable assertions. PostgreSQL 15 (already pinned in docker-compose.yml) supports schema-per-test patterns and transactional rollback for cleanup.

## Architecture Context

The user specified that the orchestrator shall support microservices using gRPC, API REST, or events. For this testing feature:
- Events (Kafka) are the primary communication mode being tested
- gRPC and REST patterns will be tested in future feature cycles
- The UI uses GSAP and motion.dev animation libraries (frontend concern, out of scope for backend integration tests)
