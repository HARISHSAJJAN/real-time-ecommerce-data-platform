"""Pydantic response models for the analytics API."""
from __future__ import annotations

from pydantic import BaseModel


class HealthStatus(BaseModel):
    status: str
    trino_reachable: bool


class RevenueSummary(BaseModel):
    total_revenue: float
    total_purchases: int
    average_order_value: float


class DailyRevenue(BaseModel):
    event_date: str
    revenue: float
    purchase_count: int


class RevenueResponse(BaseModel):
    summary: RevenueSummary
    daily: list[DailyRevenue]
    by_country: list["CountryRevenue"]


class CountryRevenue(BaseModel):
    country: str
    revenue: float
    purchase_count: int


class EventTypeCount(BaseModel):
    event_type: str
    event_count: int


class EventsResponse(BaseModel):
    total_events: int
    active_users: int
    by_type: list[EventTypeCount]


class ProductMetric(BaseModel):
    product_id: str
    name: str | None = None
    category: str | None = None
    views: int
    cart_additions: int
    purchases: int
    revenue: float
    conversion_rate: float


class ProductsResponse(BaseModel):
    products: list[ProductMetric]


class CountryMetric(BaseModel):
    country: str
    event_count: int
    revenue: float


class CountriesResponse(BaseModel):
    countries: list[CountryMetric]


class DeviceMetric(BaseModel):
    device_type: str
    event_count: int


class DevicesResponse(BaseModel):
    devices: list[DeviceMetric]


class FunnelStage(BaseModel):
    stage: str
    count: int
    conversion_from_previous: float | None = None


class FunnelResponse(BaseModel):
    stages: list[FunnelStage]
    view_to_purchase_rate: float


class TopProduct(BaseModel):
    product_id: str
    name: str | None = None
    revenue: float
    purchases: int


class TopProductsResponse(BaseModel):
    products: list[TopProduct]


RevenueResponse.model_rebuild()
