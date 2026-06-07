import os
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "e2e: mark test as end-to-end Playwright test",
    )


@pytest.fixture(scope="session")
def browser(request):
    """Create a browser instance for Playwright tests."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") == "1"
        browser = p.chromium.launch(headless=headless)
        yield browser
        browser.close()
