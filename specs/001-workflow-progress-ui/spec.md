# Feature Specification: Workflow Progress UI

**Feature Branch**: `001-workflow-progress-ui`

**Created**: 2026-06-02

**Status**: Complete — All 39 tasks done. DB schema, service layer, Flask routes, SSE+polling, templates, disconnected banner, 503 error boundary, docker-compose wired. 23 tests passing.

**Input**: User description: "Implement a UI to view the execution progress of the orchestrator"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - View Workflow Execution List (Priority: P1)

As an operator, I want to see a dashboard listing all workflow executions with their current status so I can monitor the health of the system at a glance.

**Why this priority**: This is the core value of the feature — without seeing the list of workflows and their status, the operator has no way to monitor execution progress.

**Independent Test**: Can be fully tested by deploying a known set of workflows, viewing the list, and verifying every workflow appears with its correct status.

**Acceptance Scenarios**:

1. **Given** the system has running, completed, and failed workflow executions, **When** the operator opens the dashboard, **Then** all executions are displayed with their status, start time, and duration.
2. **Given** there are more than 50 workflow executions, **When** the list loads, **Then** the operator can page or scroll through all results without performance degradation.
3. **Given** a workflow execution is currently running, **When** the dashboard is open, **Then** the status is shown as "Running" with the elapsed time updating.

---

### User Story 2 - View Workflow Execution Details (Priority: P2)

As an operator, I want to click on a specific workflow execution to see its detailed step-by-step progress so I can identify which steps succeeded, failed, or are in progress.

**Why this priority**: Operators need to diagnose issues when workflows fail — details provide visibility into where and why failures occurred.

**Independent Test**: Can be fully tested by running a multi-step workflow, opening its detail view, and verifying each step's status and duration are displayed.

**Acceptance Scenarios**:

1. **Given** a workflow execution has completed, **When** the operator opens its details, **Then** each step is displayed with its status, duration, and timestamps.
2. **Given** a workflow execution has failed, **When** the operator opens its details, **Then** the failed step is highlighted with the error message visible.
3. **Given** a running workflow has completed steps and pending steps, **When** the operator views details, **Then** completed steps are marked done and pending steps are clearly indicated.

---

### User Story 3 - Filter and Search Workflows (Priority: P3)

As an operator, I want to filter and search the workflow list by status, date range, or workflow name so I can quickly find executions of interest.

**Why this priority**: Paging through hundreds of executions without filtering is inefficient — this enhances the core view with productivity tools.

**Independent Test**: Can be fully tested by creating workflows with different statuses and names, then applying each filter and verifying only matching workflows appear.

**Acceptance Scenarios**:

1. **Given** workflows exist with different statuses, **When** the operator selects a status filter (e.g., "Failed"), **Then** only failed workflows are displayed.
2. **Given** workflows exist across multiple days, **When** the operator sets a date range, **Then** only workflows within that range are shown.
3. **Given** workflows with distinct names exist, **When** the operator types a search term, **Then** workflow names matching the term are shown.

---

### Edge Cases

- What happens when the orchestrator is not running — the UI shows a clear "disconnected" state with guidance to verify system status.
- How does the system handle real-time updates when the event stream is unavailable — the UI degrades gracefully to polling mode with a visual indicator of staleness.
- How are workflows with hundreds of steps displayed — steps are grouped or paginated to avoid overwhelming the operator.
- What happens when a workflow execution has no steps yet (just created) — the detail view shows "Waiting for execution to begin" with an empty step list.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST display a list of all workflow executions with status, start time, and duration.
- **FR-002**: System MUST show the current status of each workflow execution (Running, Completed, Failed, Pending).
- **FR-003**: System MUST provide a detail view for each workflow execution showing individual step progress.
- **FR-004**: System MUST update the execution status in real-time without requiring manual page refresh.
- **FR-005**: System MUST allow operators to filter workflows by status.
- **FR-006**: System MUST allow operators to filter workflows by date range.
- **FR-007**: System MUST allow operators to search workflows by name or identifier.
- **FR-008**: System MUST display error messages for failed steps within the workflow detail view.
- **FR-009**: System MUST handle offline/no-data states with clear user-facing messages, not technical errors.
- **FR-010**: System MUST paginate or virtual-scroll the workflow list when exceeding 50 items.

### Key Entities

- **Workflow Execution**: Represents a single run of a workflow definition. Key attributes: status, start time, end time, duration, name.
- **Workflow Step**: Represents a single step within a workflow execution. Key attributes: name, status, start time, end time, duration, error message (if failed).
- **Workflow Status**: Tracks the lifecycle of an execution (Pending, Running, Completed, Failed).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Operators can see the full list of active workflow executions within 3 seconds of loading the dashboard.
- **SC-002**: The detail view for a workflow with up to 50 steps loads within 2 seconds.
- **SC-003**: Real-time status updates reflect on the dashboard within 5 seconds of the underlying execution state change.
- **SC-004**: Operators using filters or search receive filtered results in under 2 seconds.
- **SC-005**: Operators can successfully identify a failed workflow and see its error details without leaving the UI or consulting logs.

## Assumptions

- The operator accesses the UI from a desktop or laptop browser with a standard internet connection.
- The system runs within a local network or VPN with low-latency connectivity between components.
- Only one locale (Spanish) is required for the initial version — the current project language.
- The existing Web UI project (in the `ui/` directory) will be extended rather than creating a separate application.
- Authentication is out of scope for this feature; it will reuse whatever access control exists on the current Web UI.
- Real-time updates assume a continuous data feed from the orchestrator backend so the dashboard reflects live state without manual refresh.
