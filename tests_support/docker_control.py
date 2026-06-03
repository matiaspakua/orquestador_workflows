"""Helpers to drive sibling Docker containers from inside the test runner.

Failure-injection tests (Kafka unavailable T013, network interruption T015) need
to stop and restart containers. The test-runner mounts the host Docker socket so
the ``docker`` CLI controls sibling containers by name. When the socket/CLI is
unavailable (e.g. running pytest locally without Docker), ``docker_available()``
returns False and the relevant tests skip rather than fail.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import time

logger = logging.getLogger(__name__)


def docker_available() -> bool:
    """Return True if the docker CLI is present and the daemon is reachable."""
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            capture_output=True,
            timeout=10,
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return False


def _run(args: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    logger.info("docker %s", " ".join(args))
    return subprocess.run(
        ["docker", *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def stop_container(name: str, timeout: int = 30) -> None:
    """Stop a running container by name (no-op friendly on errors)."""
    _run(["stop", "-t", "5", name], timeout=timeout)


def start_container(name: str, timeout: int = 30) -> None:
    """Start a stopped container by name."""
    _run(["start", name], timeout=timeout)


def container_running(name: str) -> bool:
    try:
        out = _run(["inspect", "-f", "{{.State.Running}}", name])
        return out.stdout.strip() == "true"
    except subprocess.CalledProcessError:
        return False


def wait_until_running(name: str, timeout_s: float = 60.0) -> bool:
    """Block until the named container reports Running, or timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if container_running(name):
            return True
        time.sleep(1.0)
    return False
