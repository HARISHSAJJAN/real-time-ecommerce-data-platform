"""Business logic layer: turns repository rows into API response models."""
from __future__ import annotations

import logging

from api.models.responses import (
    CountriesResponse,
    CountryMetric,
    CountryRevenue,
    DailyRevenue,
    DeviceMetric,
    DevicesResponse,
    EventsResponse,
    EventTypeCount,
    FunnelResponse,
    FunnelStage,
    ProductMetric,
    ProductsResponse,
    RevenueResponse,
    RevenueSummary,
    TopProduct,
    TopProductsResponse,
)
from api.repositories.trino_repository import TrinoRepository

logger = logging.getLogger("api.analytics_service")


class AnalyticsUnavailableError(RuntimeError):
    """Raised when the underlying Trino/StarRocks analytical layer cannot be reached."""


class AnalyticsService:
    def __init__(self, repository: TrinoRepository):
        self._repo = repository

    def _safe(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - deliberately broad: any Trino failure maps to 503
            raise AnalyticsUnavailableError(
                "Analytical data source is currently unreachable. Please retry shortly."
            ) from exc

    def get_revenue(self, days: int = 14) -> RevenueResponse:
        summary_row = self._safe(self._repo.revenue_summary)
        daily_rows = self._safe(self._repo.daily_revenue, days)
        country_rows = self._safe(self._repo.revenue_by_country)
        return RevenueResponse(
            summary=RevenueSummary(**summary_row),
            daily=[DailyRevenue(**row) for row in daily_rows],
            by_country=[CountryRevenue(**row) for row in country_rows],
        )

    def get_events(self) -> EventsResponse:
        total = self._safe(self._repo.total_events)
        active_users = self._safe(self._repo.active_users)
        by_type_rows = self._safe(self._repo.events_by_type)
        return EventsResponse(
            total_events=total,
            active_users=active_users,
            by_type=[EventTypeCount(**row) for row in by_type_rows],
        )

    def get_products(self, limit: int = 50) -> ProductsResponse:
        rows = self._safe(self._repo.product_performance, limit)
        return ProductsResponse(
            products=[
                ProductMetric(
                    product_id=row["product_id"],
                    name=row.get("name"),
                    category=row.get("category"),
                    views=row.get("views") or 0,
                    cart_additions=row.get("cart_additions") or 0,
                    purchases=row.get("purchases") or 0,
                    revenue=row.get("revenue") or 0.0,
                    conversion_rate=row.get("conversion_rate") or 0.0,
                )
                for row in rows
            ]
        )

    def get_countries(self) -> CountriesResponse:
        rows = self._safe(self._repo.country_metrics)
        return CountriesResponse(
            countries=[
                CountryMetric(
                    country=row["country"],
                    event_count=row["event_count"],
                    revenue=row.get("revenue") or 0.0,
                )
                for row in rows
            ]
        )

    def get_devices(self) -> DevicesResponse:
        rows = self._safe(self._repo.device_distribution)
        return DevicesResponse(
            devices=[DeviceMetric(device_type=row["device_type"], event_count=row["event_count"]) for row in rows]
        )

    def get_funnel(self) -> FunnelResponse:
        counts = self._safe(self._repo.funnel_counts)
        views = counts.get("views") or 0
        cart_additions = counts.get("cart_additions") or 0
        purchases = counts.get("purchases") or 0

        stages = [
            FunnelStage(stage="product_view", count=views, conversion_from_previous=None),
            FunnelStage(
                stage="add_to_cart",
                count=cart_additions,
                conversion_from_previous=round(cart_additions / views, 4) if views else 0.0,
            ),
            FunnelStage(
                stage="purchase",
                count=purchases,
                conversion_from_previous=round(purchases / cart_additions, 4) if cart_additions else 0.0,
            ),
        ]
        return FunnelResponse(
            stages=stages,
            view_to_purchase_rate=round(purchases / views, 4) if views else 0.0,
        )

    def get_top_products(self, limit: int = 10) -> TopProductsResponse:
        rows = self._safe(self._repo.top_products, limit)
        return TopProductsResponse(
            products=[
                TopProduct(
                    product_id=row["product_id"],
                    name=row.get("name"),
                    revenue=row.get("revenue") or 0.0,
                    purchases=row.get("purchases") or 0,
                )
                for row in rows
            ]
        )
