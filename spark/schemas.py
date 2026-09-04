"""Spark StructType schemas for the raw Kafka event payload.

Structured Streaming cannot infer schema from a Kafka topic (each message is
opaque bytes), so the JSON contract published by data_generator/models.py is
mirrored here explicitly. Fields are intentionally typed as permissively as
possible (e.g. StringType for enums) so that malformed values fail validation
in data_quality.py with a clear reason, rather than being silently dropped by
Spark's JSON parser during schema enforcement.
"""
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Raw event schema as published to the `ecommerce-events` Kafka topic.
# event_timestamp and event_id are read as strings and validated/parsed
# explicitly in transformations.py, since malformed producers may send
# non-conforming values that we want to route to the dead-letter topic
# instead of having Spark's JSON parser turn them into silent nulls.
RAW_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("_malformed_reason", StringType(), True),
    ]
)

VALID_EVENT_TYPES = {
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "payment",
    "refund",
    "login",
    "logout",
}

VALID_DEVICE_TYPES = {"desktop", "mobile", "tablet"}

PRODUCT_EVENT_TYPES = {
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "payment",
    "refund",
}
