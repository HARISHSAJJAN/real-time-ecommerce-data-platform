"""PySpark Structured Streaming job: Kafka -> validate -> dedupe -> StarRocks.

Pipeline stages
----------------
1. Read the raw `ecommerce-events` Kafka topic.
2. Validate every record (spark/data_quality.py). Invalid records are
   forwarded, unmodified, to the `ecommerce-dead-letter` Kafka topic together
   with the reason validation failed - nothing is silently dropped.
3. Deduplicate valid records by `event_id` within a bounded watermark window.
4. Normalize and enrich the clean stream (spark/transformations.py).
5. Fan the enriched stream out to five independent streaming queries:
   the raw event fact table, and four windowed aggregations (revenue,
   event volume, product performance, geographic). Each query has its own
   checkpoint directory so it can fail and recover independently.

All writes to StarRocks go through `foreachBatch` + JDBC because Structured
Streaming has no built-in StarRocks sink. The windowed aggregation tables in
StarRocks use the Primary Key model (see starrocks/schemas), so repeated
`update`-mode micro-batch writes for the same window naturally upsert instead
of accumulating duplicate rows.
"""
from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession

from spark.config import SparkJobConfig
from spark.data_quality import deduplicate_events, split_valid_invalid
from spark.schemas import RAW_EVENT_SCHEMA
from spark.transformations import (
    event_volume_by_window,
    geographic_analytics_by_window,
    normalize_and_enrich,
    product_performance_by_window,
    revenue_by_window,
)
from pyspark.sql import functions as F

logger = logging.getLogger("spark.streaming_job")


def build_spark_session(config: SparkJobConfig) -> SparkSession:
    return (
        SparkSession.builder.appName("ecommerce-streaming-pipeline")
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, config: SparkJobConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.topic_events)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_kafka_df: DataFrame) -> DataFrame:
    """Parse the Kafka value bytes into the typed event schema."""
    return raw_kafka_df.select(
        F.from_json(F.col("value").cast("string"), RAW_EVENT_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_ingest_time"),
    ).select("data.*", "kafka_ingest_time")


def write_dlq_batch(batch_df: DataFrame, batch_id: int, config: SparkJobConfig) -> None:
    count = batch_df.count()
    if count == 0:
        return
    logger.warning("Batch %d: routing %d invalid events to dead-letter topic", batch_id, count)
    (
        batch_df.select(
            F.coalesce(F.col("event_id"), F.lit("unknown")).cast("string").alias("key"),
            F.to_json(F.struct([c for c in batch_df.columns])).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("topic", config.topic_dlq)
        .save()
    )


def write_starrocks_batch(batch_df: DataFrame, batch_id: int, config: SparkJobConfig, table: str) -> None:
    count = batch_df.count()
    if count == 0:
        return
    logger.info("Batch %d: writing %d rows to StarRocks table %s", batch_id, count, table)
    (
        batch_df.write.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", config.jdbc_url)
        .option("dbtable", table)
        .option("user", config.starrocks_user)
        .option("password", config.starrocks_password)
        .mode("append")
        .save()
    )


def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    config = SparkJobConfig()
    spark = build_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    raw = parse_events(read_kafka_stream(spark, config))
    valid_raw, invalid_raw = split_valid_invalid(raw)
    deduped = deduplicate_events(valid_raw, config.watermark_delay)
    processed = normalize_and_enrich(deduped)

    queries = []

    queries.append(
        invalid_raw.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_dlq_batch(batch_df, batch_id, config)
        )
        .option("checkpointLocation", f"{config.checkpoint_dir}/dead_letter")
        .trigger(processingTime=config.trigger_interval)
        .start()
    )

    fact_columns = [
        "event_id",
        "event_timestamp",
        "user_id",
        "session_id",
        "product_id",
        "event_type",
        "quantity",
        "price",
        "currency",
        "total_amount",
        "device_type",
        "country",
        "payment_method",
        "event_date",
        "event_hour",
        "is_purchase_event",
    ]
    queries.append(
        processed.select(*fact_columns)
        .writeStream.foreachBatch(
            lambda batch_df, batch_id: write_starrocks_batch(batch_df, batch_id, config, "events")
        )
        .option("checkpointLocation", f"{config.checkpoint_dir}/fact_events")
        .outputMode("append")
        .trigger(processingTime=config.trigger_interval)
        .start()
    )

    aggregations = [
        ("revenue_metrics", revenue_by_window),
        ("event_volume_metrics", event_volume_by_window),
        ("product_metrics", product_performance_by_window),
        ("geo_metrics", geographic_analytics_by_window),
    ]
    for table, agg_fn in aggregations:
        agg_df = agg_fn(processed, config.window_duration)
        queries.append(
            agg_df.writeStream.foreachBatch(
                lambda batch_df, batch_id, t=table: write_starrocks_batch(batch_df, batch_id, config, t)
            )
            .option("checkpointLocation", f"{config.checkpoint_dir}/{table}")
            .outputMode("update")
            .trigger(processingTime=config.trigger_interval)
            .start()
        )

    logger.info("Started %d streaming queries", len(queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
