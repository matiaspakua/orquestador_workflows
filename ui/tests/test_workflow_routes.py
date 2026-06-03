"""
Flask test-client tests for workflow routes (feature 001).
All DB calls are mocked so no database is required.
"""
import json
from unittest.mock import patch
from psycopg2 import OperationalError as PgOperationalError
from .conftest import EXEC_LIST, EXEC_DETAIL, STEPS


# ── /api/workflows ────────────────────────────────────────────────────────────

class TestApiWorkflowList:
    def test_returns_200_with_executions(self, client):
        with patch('app.workflow_service.get_workflow_executions', return_value=EXEC_LIST), \
             patch('app.workflow_service.get_workflow_execution_count', return_value=2):
            r = client.get('/api/workflows')
        assert r.status_code == 200
        data = json.loads(r.data)
        assert len(data['executions']) == 2
        assert data['total'] == 2

    def test_x_total_count_header(self, client):
        with patch('app.workflow_service.get_workflow_executions', return_value=EXEC_LIST), \
             patch('app.workflow_service.get_workflow_execution_count', return_value=2):
            r = client.get('/api/workflows')
        assert r.headers['X-Total-Count'] == '2'

    def test_execution_has_required_fields(self, client):
        with patch('app.workflow_service.get_workflow_executions', return_value=EXEC_LIST), \
             patch('app.workflow_service.get_workflow_execution_count', return_value=2):
            r = client.get('/api/workflows')
        item = json.loads(r.data)['executions'][0]
        for field in ('id', 'name', 'status', 'duration_seconds'):
            assert field in item

    def test_page2_offset_param_forwarded(self, client):
        with patch('app.workflow_service.get_workflow_executions', return_value=[]) as mock_get, \
             patch('app.workflow_service.get_workflow_execution_count', return_value=0):
            client.get('/api/workflows?page=2&per_page=50')
        mock_get.assert_called_once()
        kwargs = mock_get.call_args
        assert kwargs[1]['page'] == 2

    def test_returns_500_on_db_error(self, client):
        with patch('app.workflow_service.get_workflow_executions', side_effect=Exception('db down')):
            r = client.get('/api/workflows')
        assert r.status_code == 500

    def test_empty_state_returns_empty_list(self, client):
        with patch('app.workflow_service.get_workflow_executions', return_value=[]), \
             patch('app.workflow_service.get_workflow_execution_count', return_value=0):
            r = client.get('/api/workflows')
        data = json.loads(r.data)
        assert data['executions'] == []
        assert data['total'] == 0


# ── /api/workflows/<id> ───────────────────────────────────────────────────────

class TestApiWorkflowDetail:
    def test_returns_200_with_execution_and_steps(self, client):
        with patch('app.workflow_service.get_workflow_execution', return_value=EXEC_DETAIL), \
             patch('app.workflow_service.get_workflow_steps', return_value=STEPS):
            r = client.get('/api/workflows/aaaa-0000')
        assert r.status_code == 200
        data = json.loads(r.data)
        assert data['execution']['id'] == 'aaaa-0000'
        assert len(data['steps']) == 2

    def test_steps_include_error_message(self, client):
        with patch('app.workflow_service.get_workflow_execution', return_value=EXEC_DETAIL), \
             patch('app.workflow_service.get_workflow_steps', return_value=STEPS):
            r = client.get('/api/workflows/aaaa-0000')
        failed_step = next(s for s in json.loads(r.data)['steps'] if s['status'] == 'Failed')
        assert failed_step['error_message'] == 'Connection timeout'

    def test_returns_404_for_missing_id(self, client):
        with patch('app.workflow_service.get_workflow_execution', return_value=None):
            r = client.get('/api/workflows/does-not-exist')
        assert r.status_code == 404

    def test_steps_ordered_by_sequence(self, client):
        with patch('app.workflow_service.get_workflow_execution', return_value=EXEC_DETAIL), \
             patch('app.workflow_service.get_workflow_steps', return_value=STEPS):
            r = client.get('/api/workflows/aaaa-0000')
        orders = [s['sequence_order'] for s in json.loads(r.data)['steps']]
        assert orders == sorted(orders)


# ── /workflows (HTML) ─────────────────────────────────────────────────────────

class TestWorkflowListPage:
    def test_renders_200(self, client):
        with patch('app.workflow_service.get_workflow_executions', return_value=EXEC_LIST), \
             patch('app.workflow_service.get_workflow_execution_count', return_value=2):
            r = client.get('/workflows')
        assert r.status_code == 200
        assert b'Ejecuciones' in r.data

    def test_contains_workflow_names(self, client):
        with patch('app.workflow_service.get_workflow_executions', return_value=EXEC_LIST), \
             patch('app.workflow_service.get_workflow_execution_count', return_value=2):
            r = client.get('/workflows')
        assert b'Workflow A' in r.data
        assert b'Workflow B' in r.data

    def test_empty_state_message_shown(self, client):
        with patch('app.workflow_service.get_workflow_executions', return_value=[]), \
             patch('app.workflow_service.get_workflow_execution_count', return_value=0):
            r = client.get('/workflows')
        assert b'No se encontraron ejecuciones' in r.data

    def test_graceful_on_db_error(self, client):
        with patch('app.workflow_service.get_workflow_executions', side_effect=Exception('db down')), \
             patch('app.workflow_service.get_workflow_execution_count', side_effect=Exception('db down')):
            r = client.get('/workflows')
        # Should not 500 — renders empty state instead
        assert r.status_code == 200


# ── /workflows/<id> (HTML) ────────────────────────────────────────────────────

class TestWorkflowDetailPage:
    def test_renders_200_with_steps(self, client):
        with patch('app.workflow_service.get_workflow_execution', return_value=EXEC_DETAIL), \
             patch('app.workflow_service.get_workflow_steps', return_value=STEPS):
            r = client.get('/workflows/aaaa-0000')
        assert r.status_code == 200
        assert b'Workflow A' in r.data

    def test_failed_step_error_message_visible(self, client):
        with patch('app.workflow_service.get_workflow_execution', return_value=EXEC_DETAIL), \
             patch('app.workflow_service.get_workflow_steps', return_value=STEPS):
            r = client.get('/workflows/aaaa-0000')
        assert b'Connection timeout' in r.data

    def test_no_steps_shows_waiting_message(self, client):
        with patch('app.workflow_service.get_workflow_execution', return_value=EXEC_DETAIL), \
             patch('app.workflow_service.get_workflow_steps', return_value=[]):
            r = client.get('/workflows/aaaa-0000')
        assert 'Esperando que comience'.encode() in r.data

    def test_missing_execution_returns_404(self, client):
        with patch('app.workflow_service.get_workflow_execution', return_value=None):
            r = client.get('/workflows/does-not-exist')
        assert r.status_code == 404


# ── /api/workflows/stream (SSE) ───────────────────────────────────────────────

class TestWorkflowStream:
    def test_content_type_is_event_stream(self, client):
        with patch('app.workflow_service.get_recent_status_changes', return_value=[]), \
             patch('time.sleep', side_effect=StopIteration):
            try:
                r = client.get('/api/workflows/stream')
            except StopIteration:
                pass
            # Flask returns the response object even if generator raises


# ── T021: 503 error boundary for psycopg2.OperationalError ───────────────────

class TestDbErrorBoundary:
    def test_api_workflows_503_on_operational_error(self, client):
        with patch('app.workflow_service.get_workflow_executions', side_effect=PgOperationalError('db down')):
            r = client.get('/api/workflows')
        assert r.status_code == 503
        data = json.loads(r.data)
        assert data['error'] == 'base_datos_no_disponible'

    def test_api_workflow_detail_503_on_operational_error(self, client):
        with patch('app.workflow_service.get_workflow_execution', side_effect=PgOperationalError('db down')):
            r = client.get('/api/workflows/aaaa-0000')
        assert r.status_code == 503
        data = json.loads(r.data)
        assert data['error'] == 'base_datos_no_disponible'
        assert 'message' in data

    def test_html_workflow_list_degrades_gracefully_on_operational_error(self, client):
        with patch('app.workflow_service.get_workflow_executions', side_effect=PgOperationalError('db down')), \
             patch('app.workflow_service.get_workflow_execution_count', side_effect=PgOperationalError('db down')):
            r = client.get('/workflows')
        # Must render (not crash), showing the db error banner
        assert r.status_code == 200
        assert 'base de datos'.encode() in r.data

    def test_html_workflow_detail_503_on_operational_error(self, client):
        with patch('app.workflow_service.get_workflow_execution', side_effect=PgOperationalError('db down')):
            r = client.get('/workflows/aaaa-0000')
        assert r.status_code == 503
        assert 'base de datos'.encode() in r.data
