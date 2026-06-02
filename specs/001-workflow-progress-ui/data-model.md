# Data Model: Workflow Progress UI

## Workflow Execution

Represents a single run of a workflow definition.

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| id | UUID | PK, DEFAULT gen_random_uuid() | Unique execution identifier |
| name | VARCHAR(255) | NOT NULL | Workflow definition name |
| status | VARCHAR(20) | NOT NULL, DEFAULT 'Pending' | Current execution status |
| started_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | When execution started |
| completed_at | TIMESTAMP | NULLABLE | When execution finished |
| duration | INTERVAL | GENERATED ALWAYS AS (completed_at - started_at) | Calculated duration |

**Indexes**:
- `idx_workflow_executions_status` ON (status)
- `idx_workflow_executions_started_at` ON (started_at DESC)
- `idx_workflow_executions_status_started` ON (status, started_at DESC)
- `idx_workflow_executions_name` ON (name)

## Workflow Step

Represents a single step within a workflow execution.

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| id | UUID | PK, DEFAULT gen_random_uuid() | Unique step identifier |
| workflow_execution_id | UUID | FK → workflow_executions(id), NOT NULL | Parent execution |
| name | VARCHAR(255) | NOT NULL | Step name |
| step_type | VARCHAR(50) | NOT NULL | Task, Decision, Parallel, Wait |
| status | VARCHAR(20) | NOT NULL, DEFAULT 'Pending' | Step execution status |
| started_at | TIMESTAMP | NULLABLE | When step started |
| completed_at | TIMESTAMP | NULLABLE | When step finished |
| duration | INTERVAL | GENERATED ALWAYS AS (completed_at - started_at) | Calculated duration |
| error_message | TEXT | NULLABLE | Error details if failed |
| sequence_order | INTEGER | NOT NULL | Display ordering within execution |

**Indexes**:
- `idx_workflow_steps_execution` ON (workflow_execution_id)
- `idx_workflow_steps_execution_order` ON (workflow_execution_id, sequence_order)

**Foreign Key**: `fk_workflow_steps_execution` → workflow_executions(id) ON DELETE CASCADE

## Workflow Status

Enumerated values tracking the lifecycle of an execution or step.

| Value | Description |
|-------|-------------|
| Pending | Created but not yet started |
| Running | Currently executing |
| Completed | Finished successfully |
| Failed | Finished with an error |
| Skipped | Step skipped (steps only) |

## Relationships

- A **Workflow Execution** contains zero or more **Workflow Steps**
- Steps are ordered by `sequence_order` within an execution
- When an execution is deleted (CASCADE), all its steps are removed

## SQL DDL

```sql
CREATE TABLE workflow_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Pending',
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    duration INTERVAL GENERATED ALWAYS AS (completed_at - started_at) STORED
);

CREATE TABLE workflow_steps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    step_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration INTERVAL GENERATED ALWAYS AS (completed_at - started_at) STORED,
    error_message TEXT,
    sequence_order INTEGER NOT NULL
);

CREATE INDEX idx_workflow_executions_status ON workflow_executions(status);
CREATE INDEX idx_workflow_executions_started_at ON workflow_executions(started_at DESC);
CREATE INDEX idx_workflow_executions_status_started ON workflow_executions(status, started_at DESC);
CREATE INDEX idx_workflow_executions_name ON workflow_executions(name);
CREATE INDEX idx_workflow_steps_execution ON workflow_steps(workflow_execution_id);
CREATE INDEX idx_workflow_steps_execution_order ON workflow_steps(workflow_execution_id, sequence_order);
```
