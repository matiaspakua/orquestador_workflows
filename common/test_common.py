"""
Unit tests for common/shared modules.
"""

import os
import sys
import json
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.config import env_str, env_int, env_bool, POSTGRES_CONFIG, KAFKA_BOOTSTRAP
from common.db import get_pool, close_pool


class TestConfig:
    def test_env_str_with_default(self):
        with patch.dict(os.environ, {}, clear=True):
            assert env_str("NONEXISTENT", "default") == "default"

    def test_env_str_with_value(self):
        with patch.dict(os.environ, {"MY_VAR": "hello"}, clear=True):
            assert env_str("MY_VAR", "default") == "hello"

    def test_env_int_with_default(self):
        with patch.dict(os.environ, {}, clear=True):
            assert env_int("NONEXISTENT", 42) == 42

    def test_env_int_with_value(self):
        with patch.dict(os.environ, {"MY_INT": "99"}, clear=True):
            assert env_int("MY_INT", 42) == 99

    def test_env_bool_true_values(self):
        for val in ("1", "true", "yes"):
            with patch.dict(os.environ, {"FLAG": val}, clear=True):
                assert env_bool("FLAG") is True

    def test_env_bool_false_values(self):
        for val in ("0", "false", "no", ""):
            with patch.dict(os.environ, {"FLAG": val}, clear=True):
                assert env_bool("FLAG") is False

    def test_postgres_config_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            cfg = POSTGRES_CONFIG
            assert cfg["host"] == "postgres"
            assert cfg["port"] == 5432
            assert cfg["database"] == "eventdb"

    def test_kafka_bootstrap_default(self):
        with patch.dict(os.environ, {}, clear=True):
            assert KAFKA_BOOTSTRAP == "kafka:29092"


class TestDb:
    def test_get_pool_creates_pool(self):
        close_pool()
        with patch("common.db.pg_pool.ThreadedConnectionPool") as mock_pool:
            pool = get_pool(minconn=1, maxconn=2)
            assert pool is not None
            close_pool()

    def test_close_pool_clears(self):
        close_pool()
        with patch("common.db.pg_pool.ThreadedConnectionPool"):
            get_pool(minconn=1, maxconn=2)
            close_pool()
            from common.db import _connection_pool
            assert _connection_pool is None
