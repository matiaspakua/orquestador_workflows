# Feature Specification: Grafana Telemetry

**Feature Branch**: `002-grafana-telemetry`

**Created**: 2026-06-02

**Status**: Draft

**Input**: User description: "Add Grafana for functional component telemetry (not logs)"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - View Component Health Dashboard (Priority: P1)

As an operator, I want to see a single dashboard showing the health and activity of all system components so I can quickly assess whether the system is operating normally.

**Why this priority**: Without a central health view, operators must check each component individually. This is the core value of adding telemetry.

**Independent Test**: Can be fully tested by deploying the system, running a set of workflows, and verifying each component's health indicators appear on the dashboard with correct values.

**Acceptance Scenarios**:

1. **Given** all components are running normally, **When** the operator opens the health dashboard, **Then** each component (Workflow Engine, Producer, Consumer) shows a healthy status.
2. **Given** a component has been stopped, **When** the operator views the dashboard, **Then** that component is clearly marked as unhealthy or unreachable.
3. **Given** the system has processed workflows over the last hour, **When** the operator views the dashboard, **Then** activity metrics (messages processed, workflows completed) reflect the actual processing volume.

---

### User Story 2 - Monitor Workflow Processing Metrics (Priority: P2)

As an operator, I want to see real-time and historical metrics about workflow execution so I can detect anomalies, bottlenecks, or degradation in processing performance.

**Why this priority**: Functional telemetry is about understanding system behavior — workflow metrics directly reveal the operational health of the orchestrator.

**Independent Test**: Can be fully tested by executing workflows with known characteristics and verifying that the dashboard displays the corresponding metrics (counts, durations, error rates).

**Acceptance Scenarios**:

1. **Given** workflows are being executed, **When** the operator views the workflow metrics dashboard, **Then** the number of workflows executed, currently running, completed, and failed are displayed.
2. **Given** a workflow takes longer than expected, **When** the operator inspects processing time metrics, **Then** they can identify outliers and trends in execution duration.
3. **Given** errors have occurred during workflow execution, **When** the operator checks error rate metrics, **Then** the error count and rate over time are visible.

---

### User Story 3 - View Message Processing Metrics (Priority: P3)

As an operator, I want to see metrics about Kafka message production and consumption so I can verify the event-driven communication between components is functioning correctly.

**Why this priority**: Kafka is the backbone of the system — understanding message flow helps identify connectivity issues or processing backlogs.

**Independent Test**: Can be fully tested by producing a known number of messages and verifying the dashboard shows the correct production and consumption counts.

**Acceptance Scenarios**:

1. **Given** the producer is publishing messages to Kafka, **When** the operator views message metrics, **Then** the publish rate and total published count are displayed.
2. **Given** the consumer is processing messages from Kafka, **When** the operator views message metrics, **Then** the consumption rate, total consumed count, and any processing lag are displayed.
3. **Given** message processing is delayed (consumer lag), **When** the operator inspects the dashboard, **Then** the lag indicator clearly shows the backlog.

---

### Edge Cases

- What happens when a component is newly deployed and has not yet reported metrics — the dashboard shows "No data" rather than incorrect zeroes or error states.
- How does the system handle a temporary spike in metrics — the dashboard should support configurable time ranges (last 5 minutes, 1 hour, 24 hours, etc.) so operators can distinguish spikes from trends.
- What happens when the telemetry data store is unavailable — the dashboard displays a clear "Data source unavailable" message.
- How are metrics aggregated when multiple instances of a component exist — dashboards support per-instance breakdown and aggregate views.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST expose functional telemetry metrics for each component (Workflow Engine, Producer, Consumer).
- **FR-002**: System MUST provide a dashboard showing the health status of all components at a glance.
- **FR-003**: System MUST display workflow execution metrics: total executed, currently running, completed, failed.
- **FR-004**: System MUST display workflow processing duration metrics showing average, minimum, maximum, and trend over time.
- **FR-005**: System MUST display message production and consumption metrics: publish rate, consume rate, total counts, and consumer lag.
- **FR-006**: System MUST support configurable time ranges for metric visualization (last 5 minutes, 1 hour, 24 hours, 7 days).
- **FR-007**: System MUST retain historical metrics for at least 30 days to enable trend analysis.
- **FR-008**: System MUST clearly indicate when a component is not reporting metrics (unhealthy or disconnected).
- **FR-009**: Dashboards MUST be accessible through the existing monitoring URLs without requiring additional authentication setup.

### Key Entities

- **Component Telemetry**: Health and activity data reported by each system component (Workflow Engine, Producer, Consumer). Key metrics: uptime, status, processing rates, error counts.
- **Workflow Metric**: Execution data about workflows. Key metrics: counts by status, processing durations, failure rates.
- **Message Metric**: Data about Kafka message flow. Key metrics: publish/consume rates, total counts, consumer lag.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Operators can assess the health of all system components within 5 seconds of opening the dashboard.
- **SC-002**: Workflow execution metrics reflect real-time state within 10 seconds of a workflow completing or failing.
- **SC-003**: Message processing metrics show production and consumption activity with less than 15 seconds of delay.
- **SC-004**: Operators can identify a failed workflow and view its error metrics without leaving the telemetry dashboard.
- **SC-005**: The telemetry system operates without degrading the performance of the monitored components.

## Assumptions

- The operator accessing telemetry dashboards has network access to the monitoring infrastructure (same local network or VPN as the system).
- Metrics are collected from components via a lightweight mechanism that does not require code changes to existing component business logic.
- Historical metrics are stored by the telemetry platform itself; no additional storage provisioning is required beyond what the platform provides by default.
- The Grafana dashboards are served on a dedicated port and do not interfere with the existing Web UI on port 5000.
- Authentication for the telemetry dashboards follows whatever pattern is configured for other monitoring tools in the environment (Kafka UI, pgAdmin).
- Functional telemetry metrics are separate from technical logs (CPU, memory, disk) — this feature focuses on application-level behavior.
