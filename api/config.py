"""FastAPI service configuration, loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    trino_host: str = field(default_factory=lambda: os.getenv("TRINO_HOST", "trino"))
    trino_port: int = field(default_factory=lambda: int(os.getenv("TRINO_PORT", "8080")))
    trino_user: str = field(default_factory=lambda: os.getenv("TRINO_USER", "trino"))
    trino_catalog: str = field(default_factory=lambda: os.getenv("TRINO_CATALOG", "starrocks"))
    trino_schema: str = field(default_factory=lambda: os.getenv("TRINO_SCHEMA", "ecommerce"))

    api_host: str = field(default_factory=lambda: os.getenv("API_HOST", "0.0.0.0"))
    api_port: int = field(default_factory=lambda: int(os.getenv("API_PORT", "8000")))
    log_level: str = field(default_factory=lambda: os.getenv("API_LOG_LEVEL", "INFO"))
    cors_origins: list[str] = field(
        default_factory=lambda: [
            origin.strip()
            for origin in os.getenv("API_CORS_ORIGINS", "http://localhost:8501").split(",")
            if origin.strip()
        ]
    )


settings = Settings()
