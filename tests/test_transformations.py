"""Tests for spark/transformations.py: normalization and windowed aggregations."""
from datetime import datetime, timezone

from spark.transformations import (
    event_volume_by_window,
    geographic_analytics_by_window,
    normalize_and_enrich,
    product_performance_by_window,
    revenue_by_window,
)

# Timezone-aware so PySpark converts it unambiguously to an internal UTC
# instant regardless of the host machine's local timezone (a naive datetime
# would instead be interpreted in the driver's local system timezone, which
# is a well-known PySpark quirk unrelated to the pipeline's own logic - in
# production, timestamps arrive as ISO-8601 strings with an explicit UTC
# offset, so this ambiguity never occurs there).
T0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)


def test_normalize_and_enrich_derives_expected_fields(spark) -> None:
    rows = [
        {
            "event_id": "e1",
            "event_timestamp": T0,
            "user_id": "u1",
            "session_id": "s1",
            "product_id": "p1",
            "event_type": "purchase",
            "quantity": 3,
            "price": 10.0,
            "currency": "usd",
            "device_type": "DESKTOP",
            "country": "us",
            "payment_method": "credit_card",
        }
    ]
    df = spark.createDataFrame(rows)
    enriched = normalize_and_enrich(df).collect()[0]

    assert enriched["total_amount"] == 30.0
    assert enriched["country"] == "US"
    assert enriched["device_type"] == "desktop"
    assert enriched["currency"] == "USD"
    assert enriched["event_date"] == T0.date()
    assert enriched["event_hour"] == 10
    assert enriched["is_purchase_event"] is True


def _purchase_row(ts, amount, product_id="p1", country="US"):
    return {
        "event_timestamp": ts,
        "event_type": "purchase",
        "total_amount": amount,
        "product_id": product_id,
        "country": country,
    }


def test_revenue_by_window_aggregates_purchases(spark) -> None:
    rows = [
        {"event_timestamp": T0, "event_type": "purchase", "total_amount": 100.0, "product_id": "p1", "country": "US"},
        {"event_timestamp": T0, "event_type": "purchase", "total_amount": 50.0, "product_id": "p2", "country": "US"},
        {"event_timestamp": T0, "event_type": "product_view", "total_amount": 0.0, "product_id": "p1", "country": "US"},
    ]
    df = spark.createDataFrame(rows)
    result = revenue_by_window(df, "1 hour").collect()

    assert len(result) == 1
    assert result[0]["revenue"] == 150.0
    assert result[0]["number_of_purchases"] == 2
    assert result[0]["average_order_value"] == 75.0


def test_event_volume_by_window_counts_by_type(spark) -> None:
    rows = [
        {"event_timestamp": T0, "event_type": "product_view", "total_amount": 0.0, "product_id": "p1", "country": "US"},
        {"event_timestamp": T0, "event_type": "product_view", "total_amount": 0.0, "product_id": "p1", "country": "US"},
        {"event_timestamp": T0, "event_type": "purchase", "total_amount": 20.0, "product_id": "p1", "country": "US"},
    ]
    df = spark.createDataFrame(rows)
    result = {row["event_type"]: row["event_count"] for row in event_volume_by_window(df, "1 hour").collect()}

    assert result["product_view"] == 2
    assert result["purchase"] == 1


def test_product_performance_by_window_computes_conversion_rate(spark) -> None:
    rows = [
        {"event_timestamp": T0, "event_type": "product_view", "total_amount": 0.0, "product_id": "p1", "country": "US"},
        {"event_timestamp": T0, "event_type": "product_view", "total_amount": 0.0, "product_id": "p1", "country": "US"},
        {"event_timestamp": T0, "event_type": "add_to_cart", "total_amount": 0.0, "product_id": "p1", "country": "US"},
        {"event_timestamp": T0, "event_type": "purchase", "total_amount": 25.0, "product_id": "p1", "country": "US"},
    ]
    df = spark.createDataFrame(rows)
    result = product_performance_by_window(df, "1 hour").collect()[0]

    assert result["views"] == 2
    assert result["cart_additions"] == 1
    assert result["purchases"] == 1
    assert result["revenue"] == 25.0
    assert result["conversion_rate"] == 0.5


def test_geographic_analytics_by_window_groups_by_country(spark) -> None:
    rows = [
        {"event_timestamp": T0, "event_type": "purchase", "total_amount": 40.0, "product_id": "p1", "country": "US"},
        {"event_timestamp": T0, "event_type": "product_view", "total_amount": 0.0, "product_id": "p1", "country": "DE"},
    ]
    df = spark.createDataFrame(rows)
    result = {row["country"]: row for row in geographic_analytics_by_window(df, "1 hour").collect()}

    assert result["US"]["event_count"] == 1
    assert result["US"]["revenue"] == 40.0
    assert result["DE"]["event_count"] == 1
    assert result["DE"]["revenue"] == 0.0
