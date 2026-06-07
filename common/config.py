import os


def env_str(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def env_int(key: str, default: int = 0) -> int:
    return int(os.getenv(key, str(default)))


def env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key, str(default)).lower()
    return val in ("1", "true", "yes")


POSTGRES_CONFIG = {
    "host": env_str("POSTGRES_HOST", "postgres"),
    "port": env_int("POSTGRES_PORT", 5432),
    "database": env_str("POSTGRES_DB", "eventdb"),
    "user": env_str("POSTGRES_USER", "eventuser"),
    "password": env_str("POSTGRES_PASSWORD", "eventpass"),
}

KAFKA_BOOTSTRAP = env_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = env_str("KAFKA_TOPIC", "data-events")
METRICS_PORT = env_int("METRICS_PORT", 8000)
