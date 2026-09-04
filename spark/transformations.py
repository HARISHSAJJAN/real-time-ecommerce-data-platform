"""Transformation and windowed-aggregation logic for the streaming pipeline.

All functions are pure: they take a DataFrame and return a new DataFrame,
which keeps them independently unit-testable (see tests/test_transformations.py)
without needing a running Kafka or StarRocks instance.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PURCHASE_EVENT_TYPES = ["purchase", "payment"]


def normalize_and_enrich(df: DataFrame) -> DataFrame:
    """Normalize raw fields and derive analytics-friendly columns.

    Applied after validation/deduplication, to the already-clean stream.
    `event_timestamp` is expected to already be a parsed, watermarked
    timestamp column at this point (see `data_quality.deduplicate_events`) -
    this function only derives *new* columns from it and must not
    reconstruct or rename it, which would detach it from its watermark.
    """
    return (
        df.withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("device_type", F.lower(F.trim(F.col("device_type"))))
        .withColumn("currency", F.coalesce(F.upper(F.trim(F.col("currency"))), F.lit("USD")))
        .withColumn("total_amount", F.round(F.col("price") * F.col("quantity"), 2))
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("event_hour", F.hour(F.col("event_timestamp")))
        .withColumn(
            "is_purchase_event",
            F.col("event_type").isin(PURCHASE_EVENT_TYPES),
        )
    )


def revenue_by_window(df: DataFrame, window_duration: str) -> DataFrame:
    """Revenue, purchase count, and average order value per tumbling window.

    Only `purchase` events are counted toward revenue (a `payment` event
    confirms funds capture for the same order and would double-count if
    included here; funnel analytics below track payment separately).

    Expects `df` to already carry a watermark on `event_timestamp` (set once,
    upstream, in `data_quality.deduplicate_events`) - Structured Streaming
    disallows declaring a second watermark later in the same query's
    lineage, so this function deliberately does not call `withWatermark`
    itself. In batch/test contexts (no watermark present), the aggregation
    below still works identically; only streaming state-eviction behavior
    depends on the watermark actually being set.
    """
    purchases = df.filter(F.col("event_type") == "purchase")
    return purchases.groupBy(F.window("event_timestamp", window_duration)).agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.count("*").alias("number_of_purchases"),
        F.round(F.avg("total_amount"), 2).alias("average_order_value"),
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "revenue",
        "number_of_purchases",
        "average_order_value",
    )


def event_volume_by_window(df: DataFrame, window_duration: str) -> DataFrame:
    """Total event count and per-event-type breakdown per tumbling window.

    See `revenue_by_window` docstring for why no `withWatermark` call
    appears here - the watermark on `event_timestamp` is inherited from
    `data_quality.deduplicate_events`.
    """
    return df.groupBy(
        F.window("event_timestamp", window_duration), F.col("event_type")
    ).agg(F.count("*").alias("event_count")).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "event_type",
        "event_count",
    )


def product_performance_by_window(df: DataFrame, window_duration: str) -> DataFrame:
    """Views, cart adds, purchases, revenue and conversion rate per product per window.

    See `revenue_by_window` docstring for why no `withWatermark` call
    appears here - the watermark on `event_timestamp` is inherited from
    `data_quality.deduplicate_events`.
    """
    filtered = df.filter(F.col("product_id").isNotNull())
    agg = filtered.groupBy(
        F.window("event_timestamp", window_duration), F.col("product_id")
    ).agg(
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_additions"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.round(
            F.sum(F.when(F.col("event_type") == "purchase", F.col("total_amount")).otherwise(0.0)), 2
        ).alias("revenue"),
    )
    return agg.withColumn(
        "conversion_rate",
        F.when(F.col("views") > 0, F.round(F.col("purchases") / F.col("views"), 4)).otherwise(0.0),
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "product_id",
        "views",
        "cart_additions",
        "purchases",
        "revenue",
        "conversion_rate",
    )


def geographic_analytics_by_window(df: DataFrame, window_duration: str) -> DataFrame:
    """Event volume and revenue by country per tumbling window.

    See `revenue_by_window` docstring for why no `withWatermark` call
    appears here - the watermark on `event_timestamp` is inherited from
    `data_quality.deduplicate_events`.
    """
    return df.groupBy(
        F.window("event_timestamp", window_duration), F.col("country")
    ).agg(
        F.count("*").alias("event_count"),
        F.round(
            F.sum(F.when(F.col("event_type") == "purchase", F.col("total_amount")).otherwise(0.0)), 2
        ).alias("revenue"),
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "country",
        "event_count",
        "revenue",
    )
