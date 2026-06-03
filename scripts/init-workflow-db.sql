-- Workflow Progress UI schema (feature 001)
-- Depends on pgcrypto extension created in init-db.sql

CREATE TABLE IF NOT EXISTS workflow_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Pending',
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    duration INTERVAL GENERATED ALWAYS AS (completed_at - started_at) STORED
);

CREATE TABLE IF NOT EXISTS workflow_steps (
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

CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON workflow_executions(status);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_started_at ON workflow_executions(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_status_started ON workflow_executions(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_name ON workflow_executions(name);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_execution ON workflow_steps(workflow_execution_id);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_execution_order ON workflow_steps(workflow_execution_id, sequence_order);
