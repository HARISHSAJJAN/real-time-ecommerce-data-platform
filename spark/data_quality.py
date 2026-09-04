"""Reusable data-quality validation logic for the streaming pipeline.

Validation is expressed purely as Spark column expressions so it can run
inside a Structured Streaming micro-batch without collecting data to the
driver. Every rule below mirrors a constraint already enforced by the
producer-side Pydantic model (data_generator/models.py); Spark re-validates
independently because Kafka is an untrusted boundary - a different producer,
a bug, or a manual `kafka-console-producer` message could publish anything.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.schemas import VALID_DEVICE_TYPES, VALID_EVENT_TYPES

UUID_REGEX = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

REQUIRED_STRING_FIELDS = ["event_id", "user_id", "session_id", "event_type", "device_type", "country"]


def add_validation_columns(df: DataFrame) -> DataFrame:
    """Attach `is_valid` (bool) and `validation_error` (first-failing-rule string).

    Rule evaluation order matches the order data-quality issues are most
    informative in: structural problems (missing fields, bad UUID) are
    reported before semantic ones (negative price) so a single malformed
    record surfaces its most actionable cause.
    """
    df = df.withColumn("parsed_timestamp", F.to_timestamp("event_timestamp"))

    missing_required = F.lit(False)
    for field in REQUIRED_STRING_FIELDS:
        missing_required = missing_required | F.col(field).isNull() | (F.trim(F.col(field)) == "")

    conditions = [
        (missing_required, "missing_required_field"),
        (~F.col("event_id").rlike(UUID_REGEX), "invalid_event_id_uuid"),
        (F.col("parsed_timestamp").isNull(), "invalid_event_timestamp"),
        (~F.col("event_type").isin(list(VALID_EVENT_TYPES)), "invalid_event_type"),
        (~F.col("device_type").isin(list(VALID_DEVICE_TYPES)), "invalid_device_type"),
        (F.col("price").isNull() | (F.col("price") < 0), "negative_or_null_price"),
        (F.col("quantity").isNull() | (F.col("quantity") < 0), "negative_or_null_quantity"),
    ]

    error_col = F.lit(None).cast("string")
    for condition, reason in reversed(conditions):
        error_col = F.when(condition, F.lit(reason)).otherwise(error_col)

    df = df.withColumn("validation_error", error_col)
    df = df.withColumn("is_valid", F.col("validation_error").isNull())
    return df


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a validated DataFrame into (valid_events, invalid_events)."""
    validated = add_validation_columns(df)
    valid = validated.filter(F.col("is_valid")).drop("validation_error", "is_valid")
    invalid = validated.filter(~F.col("is_valid"))
    return valid, invalid


def deduplicate_events(df: DataFrame, watermark_delay: str) -> DataFrame:
    """Drop duplicate events by event_id within the watermark window.

    This is the *only* place the pipeline declares a watermark. Structured
    Streaming disallows redefining the watermark later in the same query's
    lineage once a stateful operator (this dropDuplicates) already
    establishes one - attempting to call `withWatermark` again in a
    downstream aggregation raises "Redefining watermark is disallowed".
    Every windowed aggregation in transformations.py therefore relies on the
    watermark set here propagating through `event_timestamp` rather than
    declaring its own.

    Structured Streaming bounds the deduplication state store using this
    watermark: event_ids older than (max event time seen - watermark_delay)
    are safely evicted from state, which is what keeps this operation's
    memory footprint bounded on an unbounded stream. Duplicates arriving
    after the watermark has passed are accepted as new records - this is the
    deliberate trade-off between perfect exactly-once dedup and bounded
    state (see README "Design Decisions" for the full rationale).
    """
    normalized = df.withColumn("event_timestamp", F.col("parsed_timestamp")).drop("parsed_timestamp")
    return normalized.withWatermark("event_timestamp", watermark_delay).dropDuplicates(["event_id"])
