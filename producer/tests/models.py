"""Producer-side test data models (T005).

Implements the ``TestScenario`` and ``TestMessage`` entities from
``specs/005-producer-consumer-test/data-model.md``. These give tests a typed,
self-documenting way to declare a scenario (category, setup/execution/cleanup
steps, expected outcomes) and to describe the messages they exchange.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ScenarioCategory(str, Enum):
    MESSAGE_FLOW = "message-flow"
    ERROR_HANDLING = "error-handling"
    ORCHESTRATOR_COORDINATION = "orchestrator-coordination"


@dataclass
class TestScenario:
    """A defined test case covering a specific producer-consumer interaction."""

    __test__ = False  # data-model entity, not a pytest test class

    name: str
    category: ScenarioCategory
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    setup_steps: list[str] = field(default_factory=list)
    execution_steps: list[str] = field(default_factory=list)
    expected_outcomes: list[str] = field(default_factory=list)
    cleanup_steps: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Accept a plain string for category and coerce to the enum.
        if not isinstance(self.category, ScenarioCategory):
            self.category = ScenarioCategory(self.category)


@dataclass
class TestMessage:
    """The data content of a Kafka message used in tests (data-model.md)."""

    __test__ = False  # data-model entity, not a pytest test class

    payload: dict[str, Any]
    topic: str
    ordering_key: int
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    produced_at: datetime | None = None
    consumed_at: datetime | None = None
