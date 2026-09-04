"""Shared FastAPI dependencies (repository/service singletons)."""
from __future__ import annotations

from functools import lru_cache

from api.config import settings
from api.repositories.trino_repository import TrinoRepository
from api.services.analytics_service import AnalyticsService


@lru_cache
def get_repository() -> TrinoRepository:
    return TrinoRepository(settings)


@lru_cache
def get_analytics_service() -> AnalyticsService:
    return AnalyticsService(get_repository())
