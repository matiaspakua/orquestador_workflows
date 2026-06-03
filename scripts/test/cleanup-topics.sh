#!/usr/bin/env bash
# Manual cleanup of orphaned `test.*` Kafka topics (spec Edge Case 4 / CI helper).
#
# The suite auto-purges orphans at run start, but this provides the standalone
# cleanup path referenced by the spec for CI environments.
#
# Usage:
#   scripts/test/cleanup-topics.sh                       # uses test broker
#   KAFKA_CONTAINER=test-kafka scripts/test/cleanup-topics.sh
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-test-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_LOCAL:-localhost:9092}"

echo "Listing test.* topics on ${KAFKA_CONTAINER}..."
topics=$(docker exec "$KAFKA_CONTAINER" kafka-topics --list --bootstrap-server "$BOOTSTRAP" \
    | grep '^test\.' || true)

if [ -z "$topics" ]; then
    echo "No orphaned test.* topics found."
    exit 0
fi

echo "Deleting:"
echo "$topics"
while IFS= read -r t; do
    [ -z "$t" ] && continue
    docker exec "$KAFKA_CONTAINER" kafka-topics --delete --topic "$t" \
        --bootstrap-server "$BOOTSTRAP" || true
done <<< "$topics"

echo "Done."
