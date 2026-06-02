# Quickstart: Producer-Consumer Tests

## Prerequisites

- Docker and Docker Compose installed
- All services built: `docker-compose build`

## Running Tests

### Full test suite

```bash
docker-compose -f docker-compose.test.yml up --build
```

### Run a specific test scenario

```bash
docker-compose -f docker-compose.test.yml run --rm test-runner pytest tests/integration/test_message_flow.py -v
```

## Test Scenarios

| Scenario | Command | Expected Duration |
|----------|---------|-----------------|
| End-to-end message flow | `pytest tests/integration/test_message_flow.py` | < 30s |
| Error handling (Kafka down) | `pytest tests/integration/test_error_handling.py` | < 60s |
| Orchestrator coordination | `pytest tests/integration/test_orchestrator.py` | < 45s |
| Full suite | `docker-compose -f docker-compose.test.yml up` | < 5 min |

## Interpreting Results

- All tests pass: Exit code 0
- Any test fails: Exit code 1 with failure details in stdout
- Test results also written to `test-results/` directory as JUnit XML

## Troubleshooting

| Issue | Likely Cause | Fix |
|-------|-------------|-----|
| Kafka connection refused | Kafka container not ready | Wait 10s, retry |
| Duplicate message assertions | At-least-once delivery | Ensure consumer is idempotent |
| Test timeout | Component startup delay | Increase `KAFKA_TIMEOUT` env var |
