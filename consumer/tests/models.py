"""Consumer-side test data models (T006).

Implements the ``TestRun`` entity from
``specs/005-producer-consumer-test/data-model.md`` — status tracking, timing,
and a result summary for a single execution of a ``TestScenario``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class RunStatus(str, Enum):
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERRORED = "errored"


@dataclass
class TestRun:
    """Tracks a single execution of a TestScenario."""

    __test__ = False  # data-model entity, not a pytest test class

    scenario: str  # UUID reference to the TestScenario
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: RunStatus = RunStatus.RUNNING
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    result_summary: str = ""
    message_count: int = 0

    def finish(self, status: RunStatus, *, summary: str = "", message_count: int | None = None) -> None:
        """Mark the run finished with a terminal status and result details."""
        self.status = status
        self.completed_at = datetime.now(timezone.utc)
        self.result_summary = summary
        if message_count is not None:
            self.message_count = message_count

    @property
    def duration_s(self) -> float | None:
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()
