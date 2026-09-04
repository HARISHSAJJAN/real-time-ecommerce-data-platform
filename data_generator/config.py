"""Configuration for the e-commerce event generator, loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


@dataclass(frozen=True)
class GeneratorConfig:
    """Runtime configuration for the data generator.

    All values default to sensible local-development settings and can be
    overridden through environment variables (see .env.example).
    """

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
    )
    topic_events: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_EVENTS", "ecommerce-events"))
    topic_dlq: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_DLQ", "ecommerce-dead-letter"))

    events_per_second: float = field(default_factory=lambda: _env_float("EVENTS_PER_SECOND", 20))
    num_users: int = field(default_factory=lambda: _env_int("NUM_USERS", 1000))
    num_products: int = field(default_factory=lambda: _env_int("NUM_PRODUCTS", 200))

    malformed_event_rate: float = field(default_factory=lambda: _env_float("MALFORMED_EVENT_RATE", 0.02))
    duplicate_event_rate: float = field(default_factory=lambda: _env_float("DUPLICATE_EVENT_RATE", 0.03))

    random_seed: int | None = field(
        default_factory=lambda: (
            int(os.getenv("RANDOM_SEED")) if os.getenv("RANDOM_SEED") not in (None, "") else None
        )
    )

    def __post_init__(self) -> None:
        if self.events_per_second <= 0:
            raise ValueError("events_per_second must be positive")
        if self.num_users <= 0 or self.num_products <= 0:
            raise ValueError("num_users and num_products must be positive")
        if not (0 <= self.malformed_event_rate <= 1):
            raise ValueError("malformed_event_rate must be between 0 and 1")
        if not (0 <= self.duplicate_event_rate <= 1):
            raise ValueError("duplicate_event_rate must be between 0 and 1")
