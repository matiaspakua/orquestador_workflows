# Feature Specification: Producer-Consumer Test

**Feature Branch**: `005-producer-consumer-test`

**Created**: 2026-06-02

**Status**: Draft

**Input**: User description: "Write tests with producer and consumer using the orchestrator"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Validate End-to-End Message Flow (Priority: P1)

As a developer, I want to run tests that verify a message flows correctly from the producer through Kafka to the consumer, coordinated by the orchestrator, so I can confirm the core event-driven pipeline works end-to-end.

**Why this priority**: The producer → Kafka → consumer flow is the fundamental communication path in the system. Without validated end-to-end flow, there is no confidence the system works at all.

**Independent Test**: Can be fully tested by running a test that sends a known message from the producer and verifies the consumer receives and processes the exact same message.

**Acceptance Scenarios**:

1. **Given** the orchestrator, producer, and consumer are all running, **When** the producer publishes a message to a Kafka topic, **Then** the consumer receives and processes that message within a defined time threshold.
2. **Given** the producer publishes a message with specific payload data, **When** the consumer processes it, **Then** the consumed message payload matches the published payload exactly.
3. **Given** multiple messages are published in sequence, **When** the consumer processes them, **Then** messages are processed in the same order they were published.

---

### User Story 2 - Validate Error Handling and Recovery (Priority: P2)

As a developer, I want to verify that the producer and consumer handle errors gracefully — including Kafka unavailability, invalid messages, and network interruptions — so the system is resilient in production.

**Why this priority**: The system must remain stable when components fail. Testing error handling prevents cascading failures in production.

**Independent Test**: Can be fully tested by simulating each failure scenario in isolation (e.g., stop Kafka, publish invalid message, disconnect network) and verifying the components handle it without crashing or losing data.

**Acceptance Scenarios**:

1. **Given** Kafka is temporarily unavailable, **When** the producer attempts to publish a message, **Then** the producer retries according to the defined retry policy and does not crash.
2. **Given** the consumer receives an invalid or malformed message, **When** it attempts to process it, **Then** the consumer logs the error, skips the invalid message, and continues processing valid messages.
3. **Given** a network interruption occurs during message processing, **When** connectivity is restored, **Then** the consumer resumes processing from the last confirmed offset without data loss or duplication.

---

### User Story 3 - Validate Orchestrator Coordination (Priority: P3)

As a developer, I want to verify that the orchestrator correctly coordinates producer and consumer activities — including starting, monitoring, and stopping workflows — so the full orchestration lifecycle is reliable.

**Why this priority**: The orchestrator is the central coordination point. If it fails to manage producer/consumer interactions correctly, workflows cannot complete reliably.

**Independent Test**: Can be fully tested by running a complete workflow cycle — orchestrator starts producer, producer sends messages, consumer processes them, orchestrator completes the workflow — and verifying each step succeeds.

**Acceptance Scenarios**:

1. **Given** the orchestrator initiates a workflow involving the producer, **When** the workflow starts, **Then** the producer begins publishing messages as defined by the workflow steps.
2. **Given** the consumer has finished processing all required messages for a workflow, **When** the workflow completes, **Then** the orchestrator marks the workflow as Completed with all results persisted.
3. **Given** the orchestrator cancels a running workflow, **When** the cancellation is processed, **Then** the producer stops publishing and the consumer stops processing for that workflow.

---

### Edge Cases

- What happens when the producer publishes a message before the consumer is ready — the consumer picks up the message from Kafka when it starts (Kafka retains messages).
- How does the system handle duplicate messages — the consumer is idempotent and does not process the same message twice (at-least-once semantics with deduplication).
- What happens when a test produces a very large message payload — the system handles payloads up to the configured Kafka message size limit with clear error messaging for oversize payloads.
- How are test messages isolated from production messages — tests use dedicated Kafka topics or topic prefixes to avoid interference.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Tests MUST verify that a message published by the producer is successfully consumed by the consumer via Kafka.
- **FR-002**: Tests MUST verify message payload integrity — consumed data matches published data exactly.
- **FR-003**: Tests MUST verify message ordering is preserved when messages are published and consumed in sequence.
- **FR-004**: Tests MUST verify the producer retries publication when Kafka is temporarily unavailable, according to the defined retry policy.
- **FR-005**: Tests MUST verify the consumer handles invalid messages by logging the error and continuing without crashing.
- **FR-006**: Tests MUST verify the consumer resumes processing from the correct offset after a network interruption.
- **FR-007**: Tests MUST verify the orchestrator correctly starts and stops producer/consumer activities as part of workflow execution.
- **FR-008**: Tests MUST verify the orchestrator handles workflow cancellation, including stopping producer publishing and consumer processing.
- **FR-009**: Tests MUST be repeatable — running the same test multiple times produces the same results.
- **FR-010**: Tests MUST run within the existing Docker Compose test environment without requiring additional infrastructure.

### Key Entities

- **Test Scenario**: A defined test case covering a specific producer-consumer interaction. Key attributes: name, setup steps, execution steps, expected outcomes, cleanup steps.
- **Message Payload**: The data content of a Kafka message used in tests. Key attributes: content, format, size.
- **Test Environment**: The Docker Compose environment configured to run the tests. Key attributes: component versions, topic configuration, network settings.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The end-to-end message flow test (producer → Kafka → consumer) completes successfully in under 30 seconds.
- **SC-002**: Error handling tests demonstrate that the producer uses 3 total attempts (initial + 2 retries) with exponential backoff (1s, 2s, 4s) when Kafka is unavailable.
- **SC-003**: The consumer processes at least 1,000 messages without data loss or corruption in a single test run.
- **SC-004**: All tests run to completion within the Docker Compose test environment without requiring manual intervention.
- **SC-005**: The complete test suite runs in under 5 minutes and produces a clear pass/fail report for each test scenario.

## Assumptions

- The existing Docker Compose test environment (`docker-compose.test.yml`) will be used as the test execution environment.
- Tests are automated and can be run via a single command, not requiring manual setup steps.
- Kafka topics used for tests are isolated from any development or production topics.
- The test suite covers both happy path and error scenarios as described in the requirements.
- Test results are reported in a standard format (pass/fail with error details) that can be consumed by the CI pipeline.
- The consumer implements at-least-once delivery semantics with idempotent processing, so tests must account for potential duplicate deliveries.
