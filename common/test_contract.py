"""
Unit tests for the message contract helpers (tests_support/contract.py).
"""

import sys
import os
import json
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tests_support.contract import (
    build_message,
    build_ack,
    is_valid_message,
    validate_message,
    sign_payload,
    KAFKA_MESSAGE_MAX_BYTES,
)


class TestBuildMessage:
    def test_builds_valid_message(self):
        msg = build_message(scenario="test", payload={"key": "value"}, sequence=1)
        assert msg["scenario"] == "test"
        assert msg["payload"] == {"key": "value"}
        assert msg["sequence"] == 1
        assert "message_id" in msg
        assert "workflow_id" in msg
        assert "produced_at" in msg
        assert "signature" in msg

    def test_message_id_custom(self):
        mid = str(uuid.uuid4())
        msg = build_message(scenario="test", payload={}, sequence=1, message_id=mid)
        assert msg["message_id"] == mid

    def test_payload_includes_types(self):
        payload = {"int": 42, "float": 3.14, "str": "hello", "list": [1, 2], "dict": {"a": 1}}
        msg = build_message(scenario="test", payload=payload, sequence=1)
        assert msg["payload"] == payload


class TestBuildAck:
    def test_builds_ack(self):
        ack = build_ack(message_id="mid-1", status="processed")
        assert ack["message_id"] == "mid-1"
        assert ack["status"] == "processed"
        assert "consumed_at" in ack
        assert "processing_duration_ms" in ack

    def test_ack_failed_status(self):
        ack = build_ack(message_id="mid-1", status="failed", error="something broke")
        assert ack["status"] == "failed"
        assert ack["error"] == "something broke"

    def test_invalid_status_raises(self):
        import pytest
        with pytest.raises(AssertionError):
            build_ack(message_id="mid-1", status="invalid")


class TestIsValidMessage:
    def test_valid_message(self):
        msg = build_message(scenario="test", payload={}, sequence=1)
        assert is_valid_message(msg) is True

    def test_missing_field(self):
        msg = build_message(scenario="test", payload={}, sequence=1)
        del msg["payload"]
        assert is_valid_message(msg) is False

    def test_wrong_type(self):
        msg = build_message(scenario="test", payload={}, sequence=1)
        msg["sequence"] = "not-an-int"
        assert is_valid_message(msg) is False

    def test_non_dict(self):
        assert is_valid_message("string") is False
        assert is_valid_message(123) is False
        assert is_valid_message(None) is False

    def test_bool_not_int(self):
        msg = build_message(scenario="test", payload={}, sequence=1)
        msg["sequence"] = True
        assert is_valid_message(msg) is False

    def test_extra_fields_ok(self):
        msg = build_message(scenario="test", payload={}, sequence=1)
        msg["extra"] = "ignored"
        assert is_valid_message(msg) is True


class TestValidateMessage:
    def test_valid_passes(self):
        msg = build_message(scenario="test", payload={}, sequence=1)
        validate_message(msg)

    def test_missing_field_raises(self):
        import pytest
        msg = build_message(scenario="test", payload={}, sequence=1)
        del msg["sequence"]
        with pytest.raises(ValueError, match="missing required"):
            validate_message(msg)

    def test_non_dict_raises(self):
        import pytest
        with pytest.raises(ValueError, match="must be a JSON object"):
            validate_message("not-a-dict")


class TestSignPayload:
    def test_no_secret_returns_none(self):
        assert sign_payload({}, None) == "none"
        assert sign_payload({"a": 1}, "") == "none"

    def test_secret_produces_hmac(self):
        sig = sign_payload({"key": "value"}, "mysecret")
        assert sig != "none"
        assert len(sig) == 64

    def test_same_payload_same_signature(self):
        sig1 = sign_payload({"a": 1, "b": 2}, "secret")
        sig2 = sign_payload({"a": 1, "b": 2}, "secret")
        assert sig1 == sig2


class TestConstants:
    def test_kafka_max_bytes(self):
        assert KAFKA_MESSAGE_MAX_BYTES == 1_048_576
