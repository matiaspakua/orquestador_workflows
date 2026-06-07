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
docker-compose -f docker-compose.test.yml run --rm test-runner pytest producer/tests/integration/test_message_flow.py consumer/tests/integration/test_consumer_message_flow.py -v
```

### CI invocation (clean exit code)

The Kafka-unavailable test (T013) deliberately stops and restarts the
`test-kafka` container. Do **not** use `--abort-on-container-exit` /
`--exit-code-from`: those flags tear the whole stack down the moment Kafka
stops, killing the test runner mid-suite. Instead isolate the runner with
`compose run`, which is unaffected when a sibling container restarts:

```bash
docker-compose -f docker-compose.test.yml up -d postgres-test kafka-test
docker-compose -f docker-compose.test.yml run --rm test-runner scripts/test/run-integration.sh
rc=$?
docker-compose -f docker-compose.test.yml down -v
exit $rc
```

## Test Scenarios

| Scenario | Command | Expected Duration |
|----------|---------|-----------------|
| End-to-end message flow | `pytest producer/tests/integration/test_message_flow.py` | < 30s |
| Error handling | `pytest producer/tests/integration/test_error_handling.py consumer/tests/integration/test_consumer_error_handling.py` | < 60s |
| Orchestrator coordination | `pytest producer/tests/integration/test_orchestrator.py consumer/tests/integration/test_consumer_orchestrator.py` | < 45s |
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
