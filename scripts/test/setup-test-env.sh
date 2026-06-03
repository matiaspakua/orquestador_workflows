#!/usr/bin/env bash
# Build the isolated test stack and wait for Kafka/Postgres to be healthy.
#
# Optional helper — `docker-compose -f docker-compose.test.yml up --build` already
# handles this via healthcheck-gated depends_on. Useful when running pytest
# against the test brokers manually.
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.test.yml}"

echo "Building test images..."
docker-compose -f "$COMPOSE_FILE" build

echo "Starting infrastructure (postgres-test, kafka-test)..."
docker-compose -f "$COMPOSE_FILE" up -d postgres-test kafka-test

echo "Waiting for Kafka to report healthy..."
for _ in $(seq 1 30); do
    status=$(docker inspect -f '{{.State.Health.Status}}' test-kafka 2>/dev/null || echo "starting")
    if [ "$status" = "healthy" ]; then
        echo "Kafka is healthy."
        exit 0
    fi
    sleep 2
done

echo "Kafka did not become healthy in time." >&2
exit 1
