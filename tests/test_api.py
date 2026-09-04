"""Tests for the FastAPI analytics endpoints, with Trino mocked out."""
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_analytics_service, get_repository
from api.main import app
from api.repositories.trino_repository import TrinoRepository
from api.services.analytics_service import AnalyticsService, AnalyticsUnavailableError


@pytest.fixture
def mock_repository() -> MagicMock:
    repo = MagicMock(spec=TrinoRepository)
    repo.is_reachable.return_value = True
    repo.revenue_summary.return_value = {
        "total_revenue": 1234.56,
        "total_purchases": 42,
        "average_order_value": 29.4,
    }
    repo.daily_revenue.return_value = [{"event_date": "2026-01-01", "revenue": 500.0, "purchase_count": 10}]
    repo.revenue_by_country.return_value = [{"country": "US", "revenue": 900.0, "purchase_count": 30}]
    repo.total_events.return_value = 10000
    repo.active_users.return_value = 250
    repo.events_by_type.return_value = [{"event_type": "product_view", "event_count": 5000}]
    repo.product_performance.return_value = [
        {
            "product_id": "p1",
            "name": "Widget",
            "category": "electronics",
            "views": 100,
            "cart_additions": 20,
            "purchases": 5,
            "revenue": 250.0,
            "conversion_rate": 0.05,
        }
    ]
    repo.country_metrics.return_value = [{"country": "US", "event_count": 1000, "revenue": 900.0}]
    repo.device_distribution.return_value = [{"device_type": "mobile", "event_count": 600}]
    repo.funnel_counts.return_value = {"views": 1000, "cart_additions": 200, "purchases": 50}
    repo.top_products.return_value = [{"product_id": "p1", "name": "Widget", "revenue": 250.0, "purchases": 5}]
    return repo


@pytest.fixture
def client(mock_repository: MagicMock) -> TestClient:
    app.dependency_overrides[get_repository] = lambda: mock_repository
    app.dependency_overrides[get_analytics_service] = lambda: AnalyticsService(mock_repository)
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health_ok(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "trino_reachable": True}


def test_revenue_endpoint(client: TestClient) -> None:
    response = client.get("/metrics/revenue")
    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["total_revenue"] == 1234.56
    assert body["daily"][0]["event_date"] == "2026-01-01"
    assert body["by_country"][0]["country"] == "US"


def test_events_endpoint(client: TestClient) -> None:
    response = client.get("/metrics/events")
    assert response.status_code == 200
    body = response.json()
    assert body["total_events"] == 10000
    assert body["active_users"] == 250
    assert body["by_type"][0]["event_type"] == "product_view"


def test_products_endpoint(client: TestClient) -> None:
    response = client.get("/metrics/products")
    assert response.status_code == 200
    assert response.json()["products"][0]["product_id"] == "p1"


def test_countries_endpoint(client: TestClient) -> None:
    response = client.get("/metrics/countries")
    assert response.status_code == 200
    assert response.json()["countries"][0]["country"] == "US"


def test_devices_endpoint(client: TestClient) -> None:
    response = client.get("/metrics/devices")
    assert response.status_code == 200
    assert response.json()["devices"][0]["device_type"] == "mobile"


def test_funnel_endpoint(client: TestClient) -> None:
    response = client.get("/metrics/funnel")
    assert response.status_code == 200
    body = response.json()
    assert body["stages"][0]["stage"] == "product_view"
    assert body["view_to_purchase_rate"] == 0.05


def test_top_products_endpoint(client: TestClient) -> None:
    response = client.get("/metrics/top-products?limit=5")
    assert response.status_code == 200
    assert response.json()["products"][0]["revenue"] == 250.0


def test_revenue_endpoint_returns_503_when_trino_unreachable(mock_repository: MagicMock) -> None:
    mock_repository.revenue_summary.side_effect = Exception("connection refused")
    app.dependency_overrides[get_analytics_service] = lambda: AnalyticsService(mock_repository)
    with TestClient(app) as client:
        response = client.get("/metrics/revenue")
    app.dependency_overrides.clear()
    assert response.status_code == 503


def test_invalid_limit_returns_422(client: TestClient) -> None:
    response = client.get("/metrics/top-products?limit=0")
    assert response.status_code == 422
