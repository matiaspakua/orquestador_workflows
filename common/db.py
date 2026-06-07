import logging
import time

import psycopg2
from psycopg2 import pool as pg_pool

from .config import POSTGRES_CONFIG

logger = logging.getLogger(__name__)

_connection_pool: pg_pool.ThreadedConnectionPool | None = None


def get_pool(minconn: int = 2, maxconn: int = 10):
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pg_pool.ThreadedConnectionPool(
            minconn, maxconn, **POSTGRES_CONFIG
        )
    return _connection_pool


def close_pool():
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None


def get_connection():
    pool = get_pool()
    return pool.getconn()


def put_connection(conn):
    pool = get_pool()
    try:
        pool.putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def wait_for_db(max_retries: int = 15, interval: int = 2) -> bool:
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.autocommit = True
            conn.close()
            logger.info("Database connection established")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"DB retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(interval)
            else:
                logger.error(f"DB unavailable after {max_retries} attempts: {e}")
                return False
    return False
