# Data Model: Producer-Consumer Test

## TestScenario

| Field | Type | Description |
|-------|------|-------------|
| id | UUID | Unique test scenario identifier |
| name | String | Human-readable test name |
| category | Enum | message-flow, error-handling, orchestrator-coordination |
| setup_steps | Text[] | Ordered list of setup actions |
| execution_steps | Text[] | Ordered list of execution actions |
| expected_outcomes | Text[] | Assertions that must pass for success |
| cleanup_steps | Text[] | Teardown actions after test completes |
| dependencies | String[] | Required services/components |

## TestMessage

| Field | Type | Description |
|-------|------|-------------|
| id | UUID | Unique message identifier for tracing |
| payload | JSON | Message content body |
| topic | String | Kafka topic name (prefixed for test isolation) |
| produced_at | Timestamp | When the producer sent the message |
| consumed_at | Timestamp | When the consumer received the message |
| ordering_key | Integer | Sequence number for order validation |

## TestRun

| Field | Type | Description |
|-------|------|-------------|
| id | UUID | Unique run identifier |
| scenario | UUID | Reference to TestScenario |
| status | Enum | running, passed, failed, errored |
| started_at | Timestamp | When the test run started |
| completed_at | Timestamp | When the test run finished |
| result_summary | Text | Pass/fail details and error messages |
| message_count | Integer | Number of messages processed in the run |

## Relationships

- A **TestScenario** produces zero or more **TestMessage** records during execution
- A **TestRun** executes a single **TestScenario** and tracks all **TestMessage** activity
- **TestMessage** records link producer activity to consumer activity via the Kafka offset
