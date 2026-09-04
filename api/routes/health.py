"""Health check endpoint."""
from __future__ import annotations

from fastapi import APIRouter, Depends

from api.dependencies import get_repository
from api.models.responses import HealthStatus
from api.repositories.trino_repository import TrinoRepository

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthStatus)
def health(repository: TrinoRepository = Depends(get_repository)) -> HealthStatus:
    reachable = repository.is_reachable()
    return HealthStatus(status="ok" if reachable else "degraded", trino_reachable=reachable)
