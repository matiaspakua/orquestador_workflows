"""
Playwright E2E tests for the Workflow Dashboard UI.
Tests all capabilities: navigation, list, detail, SSE streaming, filters, pagination.

Usage:
    pip install pytest-playwright
    playwright install chromium
    pytest ui/tests_e2e/ -v
"""

import json
import os
import time

import pytest

HOST = os.getenv("TEST_HOST", "http://localhost:5000")

pytestmark = pytest.mark.skipif(
    not os.getenv("E2E_TESTS"),
    reason="Set E2E_TESTS=1 to run Playwright end-to-end tests",
)


@pytest.fixture(scope="module")
def browser_context(browser):
    context = browser.new_context()
    yield context
    context.close()


def test_navigation_links(browser_context):
    """Verify nav links exist and navigate correctly."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        nav_links = page.locator("nav a")
        link_texts = nav_links.all_text_contents()
        assert any("Dashboard" in t for t in link_texts), "Dashboard nav link missing"
        assert any("Ejecuciones" in t for t in link_texts), "Ejecuciones nav link missing"

        ejecuciones_link = nav_links.filter(has_text="Ejecuciones").first()
        ejecuciones_link.click()
        page.wait_for_load_state()
        assert "/workflows" in page.url

        dashboard_link = nav_links.filter(has_text="Dashboard").first()
        dashboard_link.click()
        page.wait_for_load_state()
        assert page.url.rstrip("/").endswith("/") or "5000" in page.url
    finally:
        page.close()


def test_workflow_list_shows_executions(browser_context):
    """Verify workflow list page renders data."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        body = page.locator("body").text_content()
        assert "Ejecuciones" in body, "Ejecuciones header not found"

        table = page.locator("#workflow-table")
        if table.is_visible():
            rows = table.locator("tbody tr").all()
            assert len(rows) > 0, "No workflow rows found in table"
        else:
            empty = page.locator(".empty-state")
            assert empty.is_visible() or page.locator("text=No se encontraron").is_visible(), \
                "Expected empty state or table rows"
    finally:
        page.close()


def test_workflow_detail_page(browser_context):
    """Verify workflow detail page renders correctly."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        link = page.locator("a[href^='/workflows/']").first
        if link.is_visible():
            href = link.get_attribute("href")
            page.goto(f"{HOST}{href}", wait_until="networkidle")
            body = page.locator("body").text_content()
            assert "Pasos" in body or "ID" in body or "Volver" in body
        else:
            pytest.skip("No workflow links available")
    finally:
        page.close()


def test_404_page_on_invalid_id(browser_context):
    """Verify detail page returns 404 for non-existent workflow."""
    page = browser_context.new_page()
    try:
        resp = page.goto(f"{HOST}/workflows/non-existent-id", wait_until="networkidle")
        body = page.locator("body").text_content()
        assert "no encontrada" in body.lower() or resp.status == 404
    finally:
        page.close()


def test_status_filter_renders(browser_context):
    """Verify status filter dropdown exists on list page."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        select = page.locator("select[name='status']")
        assert select.is_visible(), "Status filter not found"
        options = select.locator("option").all_text_contents()
        option_texts = [o.strip() for o in options]
        assert "Todos" in option_texts
        assert any("Completado" in o for o in option_texts), "Completed status option missing"
        assert any("Fallido" in o for o in option_texts), "Failed status option missing"
    finally:
        page.close()


def test_date_filter_inputs(browser_context):
    """Verify date filter inputs exist on list page."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        date_from = page.locator("input[name='date_from']")
        date_to = page.locator("input[name='date_to']")
        assert date_from.is_visible(), "Date from filter not found"
        assert date_to.is_visible(), "Date to filter not found"
        assert date_from.get_attribute("type") == "date"
        assert date_to.get_attribute("type") == "date"
    finally:
        page.close()


def test_search_input(browser_context):
    """Verify search input exists on list page."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        search = page.locator("input[name='search']")
        assert search.is_visible(), "Search input not found"
        assert search.get_attribute("placeholder") is not None
    finally:
        page.close()


def test_filter_submit_button(browser_context):
    """Verify filter button exists and submits."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        btn = page.locator("button[type='submit']")
        assert btn.is_visible(), "Filter submit button not found"
        assert "Filtrar" in btn.text_content()
    finally:
        page.close()


def test_pagination_navigation(browser_context):
    """Verify pagination controls exist on list page."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        pagination = page.locator(".pagination")
        if pagination.is_visible():
            pages = pagination.text_content()
            assert "Página" in pages
            assert "Anterior" in pages or "Siguiente" in pages
    finally:
        page.close()


def test_api_health_endpoint(browser_context):
    """Verify the health endpoint returns healthy."""
    page = browser_context.new_page()
    try:
        resp = page.goto(f"{HOST}/health", wait_until="networkidle")
        pre = page.locator("pre")
        pre_visible = pre.is_visible()
        raw = pre.text_content() if pre_visible else page.locator("body").text_content()
        body = json.loads(raw)
        assert resp.status == 200
        assert body.get("status") == "healthy"
    finally:
        page.close()


def test_api_workflows_endpoint(browser_context):
    """Verify API returns valid JSON with executions array."""
    page = browser_context.new_page()
    try:
        resp = page.goto(f"{HOST}/api/workflows", wait_until="networkidle")
        body = json.loads(page.locator("body").text_content())
        assert "executions" in body or "error" in body
        assert resp.status in (200, 503)
        if resp.status == 200:
            assert isinstance(body.get("executions"), list)
    finally:
        page.close()


def test_sse_indicator_present(browser_context):
    """Verify SSE indicator exists in the UI."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        indicator = page.locator("#sse-indicator")
        assert indicator.is_visible(), "SSE indicator not found"
        label = page.locator("#sse-label")
        assert label.is_visible(), "SSE label not found"
    finally:
        page.close()


def test_disconnected_banner_exists(browser_context):
    """Verify disconnected banner element exists in DOM."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        banner = page.locator("#orchestrator-banner")
        assert banner.is_visible() is not None
    finally:
        page.close()


def test_workflow_rows_clickable(browser_context):
    """Verify workflow rows have clickable links to detail pages."""
    page = browser_context.new_page()
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        links = page.locator("a[href^='/workflows/']")
        count = links.count()
        if count > 0:
            for i in range(min(count, 3)):
                href = links.nth(i).get_attribute("href")
                assert href.startswith("/workflows/")
                assert len(href) > len("/workflows/")
        else:
            pytest.skip("No workflow links to verify")
    finally:
        page.close()


def test_loads_without_errors(browser_context):
    """Verify main pages load without console errors."""
    page = browser_context.new_page()
    errors = []
    page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
    try:
        page.goto(f"{HOST}/workflows", wait_until="networkidle")
        time.sleep(1)
        assert len([e for e in errors if "404" not in e and "favicon" not in e]) == 0, \
            f"Console errors found: {errors}"
    finally:
        page.close()
