"""
Unit tests for workflow_service.py with mocked DB connection.
"""

from unittest.mock import patch, MagicMock
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services import workflow_service


class MockCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self._idx = 0
        self.executed_query = None
        self.executed_params = None

    def execute(self, query, params=None):
        self.executed_query = query
        self.executed_params = params

    def fetchone(self):
        if self.rows:
            return self.rows[0]
        return None

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __iter__(self):
        return iter(self.rows)


class MockConn:
    def __init__(self, rows=None):
        self.cursor_obj = MockCursor(rows)
        self.closed = False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def test_get_workflow_executions():
    rows = [
        {"id": "1", "name": "Test", "status": "Completed", "started_at": None, "completed_at": None, "duration_seconds": 60},
        {"id": "2", "name": "Test2", "status": "Running", "started_at": None, "completed_at": None, "duration_seconds": None},
    ]
    mock_conn = MockConn(rows)
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        results = workflow_service.get_workflow_executions(page=1, per_page=50)
    assert len(results) == 2
    assert results[0]["id"] == "1"
    assert results[1]["status"] == "Running"


def test_get_workflow_executions_with_status_filter():
    rows = [{"id": "1", "name": "Test", "status": "Completed", "started_at": None, "completed_at": None, "duration_seconds": 60}]
    mock_conn = MockConn(rows)
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        results = workflow_service.get_workflow_executions(status="Completed")
    assert len(results) == 1
    assert mock_conn.cursor_obj.executed_params[0] == "Completed"


def test_get_workflow_executions_with_search():
    rows = []
    mock_conn = MockConn(rows)
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        results = workflow_service.get_workflow_executions(search="test-workflow")
    assert len(results) == 0
    assert mock_conn.cursor_obj.executed_params[0] == "%test-workflow%"


def test_get_workflow_execution_count():
    mock_conn = MockConn([{"total": 5}])
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        count = workflow_service.get_workflow_execution_count()
    assert count == 5


def test_get_workflow_execution_count_with_filter():
    mock_conn = MockConn([{"total": 3}])
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        count = workflow_service.get_workflow_execution_count(status="Failed")
    assert count == 3


def test_get_workflow_execution_found():
    mock_conn = MockConn([{"id": "abc", "name": "Test", "status": "Running", "started_at": None, "completed_at": None, "duration_seconds": None}])
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        result = workflow_service.get_workflow_execution("abc")
    assert result is not None
    assert result["id"] == "abc"


def test_get_workflow_execution_not_found():
    mock_conn = MockConn([])
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        result = workflow_service.get_workflow_execution("does-not-exist")
    assert result is None


def test_get_workflow_steps():
    rows = [
        {"id": "s1", "workflow_execution_id": "abc", "name": "Step 1", "step_type": "Task",
         "status": "Completed", "started_at": None, "completed_at": None, "duration_seconds": 30,
         "error_message": None, "sequence_order": 1},
        {"id": "s2", "workflow_execution_id": "abc", "name": "Step 2", "step_type": "Task",
         "status": "Failed", "started_at": None, "completed_at": None, "duration_seconds": None,
         "error_message": "Error", "sequence_order": 2},
    ]
    mock_conn = MockConn(rows)
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        steps = workflow_service.get_workflow_steps("abc")
    assert len(steps) == 2
    assert steps[0]["sequence_order"] == 1
    assert steps[1]["status"] == "Failed"


def test_get_workflow_steps_empty():
    mock_conn = MockConn([])
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        steps = workflow_service.get_workflow_steps("abc")
    assert steps == []


def test_get_recent_status_changes():
    rows = [{"id": "1", "name": "Test", "status": "Running", "duration_seconds": None}]
    mock_conn = MockConn(rows)
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        results = workflow_service.get_recent_status_changes()
    assert len(results) == 1


def test_connection_closed_on_exception():
    mock_conn = MockConn([])
    mock_conn.cursor_obj.execute = MagicMock(side_effect=Exception("DB error"))
    with patch("services.workflow_service.get_db_connection", return_value=mock_conn):
        try:
            workflow_service.get_workflow_executions()
        except Exception:
            pass
    assert mock_conn.closed, "Connection should be closed even on exception"
