#!/usr/bin/env bash
# Explicit Kafka topic creation (AUDIT: avoid relying on auto-create).
# Run once after Kafka starts: docker compose exec kafka /scripts/init-topics.sh

set -euo pipefail

KAFKA_BIN="/usr/bin/kafka-topics"
BROKER="kafka:29092"
PARTITIONS="${KAFKA_PARTITIONS:-1}"
REPLICATION="${KAFKA_REPLICATION:-1}"

create_topic() {
  local topic="$1"
  local retention_ms="${2:-604800000}"  # 7 days default

  if $KAFKA_BIN --list --bootstrap-server "$BROKER" 2>/dev/null | grep -qx "$topic"; then
    echo "[SKIP] Topic already exists: $topic"
  else
    $KAFKA_BIN --create \
      --bootstrap-server "$BROKER" \
      --topic "$topic" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION" \
      --config retention.ms="$retention_ms"
    echo "[OK]   Created topic: $topic"
  fi
}

echo "=== Initializing Kafka topics ==="

# Core event pipeline
create_topic "data-events"

# Workflow orchestration
create_topic "orchestration-events"
create_topic "orchestration-results"
create_topic "orchestration-dlq"   "2592000000"  # 30 days for DLQ

# Worker topics (4 services)
create_topic "order-validation"
create_topic "fraud-check"
create_topic "inventory-check"
create_topic "notification-send"

echo "=== All topics ready ==="
$KAFKA_BIN --list --bootstrap-server "$BROKER"
