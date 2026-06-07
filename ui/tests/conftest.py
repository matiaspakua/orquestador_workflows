import os
import sys

import pytest

# Put the ui/ directory on the path so 'services' import works
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import app as flask_app

EXEC_LIST = [
    {
        'id': 'aaaa-0000',
        'name': 'Workflow A',
        'status': 'Completed',
        'started_at': None,
        'completed_at': None,
        'duration_seconds': 90,
    },
    {
        'id': 'bbbb-1111',
        'name': 'Workflow B',
        'status': 'Running',
        'started_at': None,
        'completed_at': None,
        'duration_seconds': None,
    },
]

EXEC_DETAIL = {
    'id': 'aaaa-0000',
    'name': 'Workflow A',
    'status': 'Completed',
    'started_at': None,
    'completed_at': None,
    'duration_seconds': 90,
}

STEPS = [
    {
        'id': 'step-0001',
        'workflow_execution_id': 'aaaa-0000',
        'name': 'Step 1',
        'step_type': 'Task',
        'status': 'Completed',
        'started_at': None,
        'completed_at': None,
        'duration_seconds': 30,
        'error_message': None,
        'sequence_order': 1,
    },
    {
        'id': 'step-0002',
        'workflow_execution_id': 'aaaa-0000',
        'name': 'Step 2',
        'step_type': 'Task',
        'status': 'Failed',
        'started_at': None,
        'completed_at': None,
        'duration_seconds': None,
        'error_message': 'Connection timeout',
        'sequence_order': 2,
    },
]


@pytest.fixture()
def client():
    flask_app.app.config['TESTING'] = True
    with flask_app.app.test_client() as c:
        yield c
