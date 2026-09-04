"""Tests for spark/data_quality.py validation and deduplication logic."""
from datetime import datetime, timedelta

from spark.data_quality import add_validation_columns, deduplicate_events, split_valid_invalid
from spark.schemas import RAW_EVENT_SCHEMA

VALID_ROW = {
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_timestamp": "2026-01-01T10:00:00+00:00",
    "user_id": "u1",
    "session_id": "s1",
    "product_id": "p1",
    "event_type": "purchase",
    "quantity": 2,
    "price": 19.99,
    "currency": "USD",
    "device_type": "desktop",
    "country": "US",
    "payment_method": "credit_card",
    "_malformed_reason": None,
}


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=RAW_EVENT_SCHEMA)


def test_valid_row_passes_validation(spark) -> None:
    df = _df(spark, [VALID_ROW])
    validated = add_validation_columns(df)
    result = validated.collect()[0]
    assert result["is_valid"] is True
    assert result["validation_error"] is None


def test_missing_required_field_is_rejected(spark) -> None:
    row = dict(VALID_ROW, user_id=None)
    df = _df(spark, [row])
    validated = add_validation_columns(df)
    result = validated.collect()[0]
    assert result["is_valid"] is False
    assert result["validation_error"] == "missing_required_field"


def test_invalid_uuid_is_rejected(spark) -> None:
    row = dict(VALID_ROW, event_id="not-a-uuid")
    df = _df(spark, [row])
    validated = add_validation_columns(df)
    result = validated.collect()[0]
    assert result["is_valid"] is False
    assert result["validation_error"] == "invalid_event_id_uuid"


def test_negative_price_is_rejected(spark) -> None:
    row = dict(VALID_ROW, price=-5.0)
    df = _df(spark, [row])
    validated = add_validation_columns(df)
    result = validated.collect()[0]
    assert result["is_valid"] is False
    assert result["validation_error"] == "negative_or_null_price"


def test_invalid_event_type_is_rejected(spark) -> None:
    row = dict(VALID_ROW, event_type="checkout_abandoned")
    df = _df(spark, [row])
    validated = add_validation_columns(df)
    result = validated.collect()[0]
    assert result["is_valid"] is False
    assert result["validation_error"] == "invalid_event_type"


def test_split_valid_invalid_partitions_correctly(spark) -> None:
    bad_row = dict(VALID_ROW, event_id="not-a-uuid")
    df = _df(spark, [VALID_ROW, bad_row])
    valid, invalid = split_valid_invalid(df)
    assert valid.count() == 1
    assert invalid.count() == 1


def test_deduplicate_events_drops_repeated_event_id(spark) -> None:
    df = _df(spark, [VALID_ROW, VALID_ROW])
    valid, _ = split_valid_invalid(df)
    deduped = deduplicate_events(valid, "10 minutes")
    assert deduped.count() == 1


def test_deduplicate_events_keeps_distinct_event_ids(spark) -> None:
    second_row = dict(VALID_ROW, event_id="22222222-2222-2222-2222-222222222222")
    df = _df(spark, [VALID_ROW, second_row])
    valid, _ = split_valid_invalid(df)
    deduped = deduplicate_events(valid, "10 minutes")
    assert deduped.count() == 2
