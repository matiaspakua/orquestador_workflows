"""Kafka message contract helpers.

Implements the Producer -> Consumer message schema defined in
``specs/005-producer-consumer-test/contracts/message-contract.md``::

    {
      "message_id": "uuid",
      "workflow_id": "uuid",
      "scenario": "string",
      "payload": {},
      "sequence": "integer",
      "produced_at": "iso8601-timestamp",
      "signature": "string (hmac or none)"
    }

The acknowledgement schema (Consumer -> Producer) is also provided so
orchestrator-coordination tests can assert ack shape.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from datetime import datetime, timezone
from typing import Any

# Kafka broker default ``message.max.bytes`` (1 MiB). Pinned explicitly in
# docker-compose.test.yml so the large-payload edge case (T025) has a known
# boundary to probe.
KAFKA_MESSAGE_MAX_BYTES = 1_048_576

REQUIRED_MESSAGE_FIELDS = (
    "message_id",
    "workflow_id",
    "scenario",
    "payload",
    "sequence",
    "produced_at",
)

# Field -> expected python type for validation. ``signature`` is optional.
_FIELD_TYPES: dict[str, type | tuple[type, ...]] = {
    "message_id": str,
    "workflow_id": str,
    "scenario": str,
    "payload": dict,
    "sequence": int,
    "produced_at": str,
}


def _now_iso() -> str:
    """Return the current UTC time as an ISO8601 string with timezone."""
    return datetime.now(timezone.utc).isoformat()


def sign_payload(payload: dict[str, Any], secret: str | None) -> str:
    """Return an HMAC-SHA256 signature of ``payload`` or ``"none"`` when unsigned."""
    if not secret:
        return "none"
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def build_message(
    *,
    scenario: str,
    payload: dict[str, Any],
    sequence: int,
    workflow_id: str | None = None,
    message_id: str | None = None,
    secret: str | None = None,
) -> dict[str, Any]:
    """Build a contract-compliant Producer -> Consumer message.

    ``message_id`` defaults to a fresh UUID; pass an explicit value to exercise
    the duplicate/idempotency path (same ``message_id`` twice -> processed once).
    """
    msg = {
        "message_id": message_id or str(uuid.uuid4()),
        "workflow_id": workflow_id or str(uuid.uuid4()),
        "scenario": scenario,
        "payload": payload,
        "sequence": sequence,
        "produced_at": _now_iso(),
    }
    msg["signature"] = sign_payload(payload, secret)
    return msg


def build_ack(
    *,
    message_id: str,
    status: str = "processed",
    error: str | None = None,
    processing_duration_ms: int = 0,
) -> dict[str, Any]:
    """Build a contract-compliant Consumer -> Producer acknowledgement."""
    assert status in ("processed", "failed", "skipped")
    return {
        "message_id": message_id,
        "status": status,
        "error": error,
        "consumed_at": _now_iso(),
        "processing_duration_ms": processing_duration_ms,
    }


def is_valid_message(value: Any) -> bool:
    """Return True if ``value`` satisfies the message contract.

    Used by the consumer harness to decide whether a message is "invalid or
    malformed" (FR-005 / CHK010): non-dict, missing a required field, or a field
    with the wrong type all count as invalid.
    """
    if not isinstance(value, dict):
        return False
    for field in REQUIRED_MESSAGE_FIELDS:
        if field not in value:
            return False
    for field, expected in _FIELD_TYPES.items():
        # bool is a subclass of int — reject it for the integer ``sequence`` field.
        if expected is int and isinstance(value[field], bool):
            return False
        if not isinstance(value[field], expected):
            return False
    return True


def validate_message(value: Any) -> None:
    """Raise ``ValueError`` with a specific reason if ``value`` is not valid."""
    if not isinstance(value, dict):
        raise ValueError(f"message must be a JSON object, got {type(value).__name__}")
    missing = [f for f in REQUIRED_MESSAGE_FIELDS if f not in value]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")
    for field, expected in _FIELD_TYPES.items():
        if expected is int and isinstance(value[field], bool):
            raise ValueError(f"field '{field}' must be an integer, got bool")
        if not isinstance(value[field], expected):
            raise ValueError(
                f"field '{field}' must be {getattr(expected, '__name__', expected)}, "
                f"got {type(value[field]).__name__}"
            )
