"""Analytics metrics endpoints."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_analytics_service
from api.models.responses import (
    CountriesResponse,
    DevicesResponse,
    EventsResponse,
    FunnelResponse,
    ProductsResponse,
    RevenueResponse,
    TopProductsResponse,
)
from api.services.analytics_service import AnalyticsService, AnalyticsUnavailableError

logger = logging.getLogger("api.routes.metrics")
router = APIRouter(prefix="/metrics", tags=["metrics"])


def _handle(fn):
    try:
        return fn()
    except AnalyticsUnavailableError as exc:
        logger.error("Analytics query failed: %s", exc)
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/revenue", response_model=RevenueResponse)
def get_revenue(
    days: int = Query(default=14, ge=1, le=365),
    service: AnalyticsService = Depends(get_analytics_service),
) -> RevenueResponse:
    return _handle(lambda: service.get_revenue(days=days))


@router.get("/events", response_model=EventsResponse)
def get_events(service: AnalyticsService = Depends(get_analytics_service)) -> EventsResponse:
    return _handle(service.get_events)


@router.get("/products", response_model=ProductsResponse)
def get_products(
    limit: int = Query(default=50, ge=1, le=500),
    service: AnalyticsService = Depends(get_analytics_service),
) -> ProductsResponse:
    return _handle(lambda: service.get_products(limit=limit))


@router.get("/countries", response_model=CountriesResponse)
def get_countries(service: AnalyticsService = Depends(get_analytics_service)) -> CountriesResponse:
    return _handle(service.get_countries)


@router.get("/devices", response_model=DevicesResponse)
def get_devices(service: AnalyticsService = Depends(get_analytics_service)) -> DevicesResponse:
    return _handle(service.get_devices)


@router.get("/funnel", response_model=FunnelResponse)
def get_funnel(service: AnalyticsService = Depends(get_analytics_service)) -> FunnelResponse:
    return _handle(service.get_funnel)


@router.get("/top-products", response_model=TopProductsResponse)
def get_top_products(
    limit: int = Query(default=10, ge=1, le=100),
    service: AnalyticsService = Depends(get_analytics_service),
) -> TopProductsResponse:
    return _handle(lambda: service.get_top_products(limit=limit))
