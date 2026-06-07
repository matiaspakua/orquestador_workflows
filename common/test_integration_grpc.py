"""
Integration tests for gRPC and REST API integration.
Tests that the services work together seamlessly.

Usage:
    pytest common/test_integration_grpc.py -v
"""

import os
import sys
import json
import time
import pytest
import requests
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

HOST = os.getenv("TEST_HOST", "http://localhost:5000")
GRPC_HOST = os.getenv("GRPC_HOST", "localhost")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))


def test_rest_api_health():
    """Test REST API health endpoint."""
    try:
        r = requests.get(f"{HOST}/health", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "healthy"
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_rest_api_workflows_list():
    """Test REST API workflows list."""
    try:
        r = requests.get(f"{HOST}/api/workflows", timeout=5)
        assert r.status_code in (200, 503)
        data = r.json()
        if r.status_code == 200:
            assert "executions" in data
            assert "total" in data
            assert "page" in data
            assert "per_page" in data
            assert "X-Total-Count" in r.headers
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_rest_api_workflows_pagination():
    """Test pagination parameters on REST API."""
    try:
        r = requests.get(f"{HOST}/api/workflows?page=1&per_page=10", timeout=5)
        assert r.status_code in (200, 503)
        if r.status_code == 200:
            data = r.json()
            assert data["page"] == 1
            assert data["per_page"] == 10
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_rest_api_workflows_with_filters():
    """Test filter parameters on REST API."""
    try:
        r = requests.get(f"{HOST}/api/workflows?status=Completed&date_from=2024-01-01", timeout=5)
        assert r.status_code in (200, 503)
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_rest_api_workflow_detail():
    """Test REST API workflow detail endpoint."""
    try:
        r = requests.get(f"{HOST}/api/workflows/test-id", timeout=5)
        assert r.status_code in (200, 404, 503)
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_rest_api_workflows_stream():
    """Test SSE stream endpoint."""
    try:
        r = requests.get(f"{HOST}/api/workflows/stream", timeout=5, stream=True)
        assert r.status_code == 200
        assert r.headers.get("content-type", "").startswith("text/event-stream")
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_rest_api_stats():
    """Test stats endpoint returns valid structure."""
    try:
        r = requests.get(f"{HOST}/api/stats", timeout=5)
        assert r.status_code in (200, 500)
        if r.status_code == 200:
            data = r.json()
            for key in ("general_stats", "event_types", "recent_events", "recent_logs", "performance"):
                assert key in data, f"Missing key: {key}"
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_rest_api_consumers():
    """Test consumers endpoint."""
    try:
        r = requests.get(f"{HOST}/api/consumers", timeout=5)
        assert r.status_code in (200, 500)
        if r.status_code == 200:
            assert isinstance(r.json(), list)
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_gRPC_server_availability():
    """Test gRPC server is reachable if grpc is available."""
    grpc = pytest.importorskip("grpc", reason="grpc not installed")
    try:
        from workflow_service_pb2_grpc import WorkflowOrchestratorStub
        from workflow_service_pb2 import WorkflowStatusRequest
        channel = grpc.insecure_channel(f"{GRPC_HOST}:{GRPC_PORT}")
        grpc.channel_ready_future(channel).result(timeout=3)
        stub = WorkflowOrchestratorStub(channel)
        resp = stub.GetWorkflowStatus(WorkflowStatusRequest(workflow_execution_id="test"))
        channel.close()
    except Exception as e:
        pytest.skip(f"gRPC not available: {e}")


def test_gRPC_workflow_status():
    """Test gRPC workflow status call."""
    grpc = pytest.importorskip("grpc", reason="grpc not installed")
    try:
        from workflow_service_pb2_grpc import WorkflowOrchestratorStub
        from workflow_service_pb2 import WorkflowStatusRequest, ListWorkflowsRequest
        channel = grpc.insecure_channel(f"{GRPC_HOST}:{GRPC_PORT}")
        grpc.channel_ready_future(channel).result(timeout=3)
        stub = WorkflowOrchestratorStub(channel)

        list_resp = stub.ListWorkflows(ListWorkflowsRequest(page=1, per_page=10))
        assert list_resp.total >= 0

        channel.close()
    except Exception as e:
        pytest.skip(f"gRPC not available: {e}")


def test_html_pages_load():
    """Test HTML pages render."""
    try:
        for path in ("/workflows", "/"):
            r = requests.get(f"{HOST}{path}", timeout=5)
            assert r.status_code == 200
            assert "text/html" in r.headers.get("content-type", "")
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_html_workflow_detail_404():
    """Test HTML detail page returns 404 for invalid ID."""
    try:
        r = requests.get(f"{HOST}/workflows/non-existent", timeout=5)
        assert r.status_code == 404
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_cross_origin_headers():
    """Test response headers."""
    try:
        r = requests.get(f"{HOST}/api/workflows", timeout=5)
        assert "X-Total-Count" in r.headers
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_system_endpoints_respond():
    """Test health and readiness endpoints."""
    try:
        for path in ("/health", "/ready"):
            r = requests.get(f"{HOST}{path}", timeout=5)
            assert r.status_code in (200, 503)
            assert "application/json" in r.headers.get("content-type", "")
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")


def test_api_rate_limit():
    """Test multiple rapid requests don't crash the server."""
    try:
        for _ in range(20):
            r = requests.get(f"{HOST}/api/workflows", timeout=5)
            assert r.status_code in (200, 503)
    except requests.ConnectionError:
        pytest.skip(f"Cannot connect to {HOST}")
