# Message Contract: Producer ↔ Consumer via Kafka

## Topic Naming

Test topics follow the pattern: `test.{scenario_id}.{suffix}`

Examples:
- `test.a1b2c3d4.messages` — Main message topic
- `test.a1b2c3d4.results` — Results/ack topic

## Message Schema (JSON)

### Producer → Consumer (Workflow Event)

```json
{
  "message_id": "uuid",
  "workflow_id": "uuid",
  "scenario": "string",
  "payload": {},
  "sequence": "integer",
  "produced_at": "iso8601-timestamp",
  "signature": "string (hmac or none)"
}
```

### Consumer → Producer (Acknowledgement)

```json
{
  "message_id": "uuid",
  "status": "processed | failed | skipped",
  "error": "string | null",
  "consumed_at": "iso8601-timestamp",
  "processing_duration_ms": "integer"
}
```

## Delivery Semantics

- **At-least-once**: Messages may be delivered more than once
- **Consumer idempotency**: Duplicate messages with the same `message_id` are handled without side effects
- **Ordering**: Messages within a topic partition maintain order; cross-partition ordering is not guaranteed

## Retry Policy

- Producer retries: 3 attempts with exponential backoff (1s, 2s, 4s)
- Consumer retry: Automatic rebalance on failure; unprocessed messages are retried
- Max retry before dead letter: 5
