"""Configuration for the PySpark Structured Streaming job, loaded from the environment."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _get(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class SparkJobConfig:
    kafka_bootstrap_servers: str = _get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic_events: str = _get("KAFKA_TOPIC_EVENTS", "ecommerce-events")
    topic_dlq: str = _get("KAFKA_TOPIC_DLQ", "ecommerce-dead-letter")
    consumer_group: str = _get("KAFKA_CONSUMER_GROUP", "spark-streaming-consumer")

    starrocks_host: str = _get("STARROCKS_HOST", "starrocks")
    starrocks_query_port: str = _get("STARROCKS_PORT", "9030")
    starrocks_database: str = _get("STARROCKS_DATABASE", "ecommerce")
    starrocks_user: str = _get("STARROCKS_USER", "root")
    starrocks_password: str = _get("STARROCKS_PASSWORD", "")

    checkpoint_dir: str = _get("SPARK_CHECKPOINT_DIR", "/opt/spark-checkpoints")
    trigger_interval: str = _get("SPARK_TRIGGER_INTERVAL", "10 seconds")
    watermark_delay: str = _get("SPARK_WATERMARK_DELAY", "2 minutes")
    window_duration: str = _get("SPARK_WINDOW_DURATION", "1 minute")
    shuffle_partitions: str = _get("SPARK_SHUFFLE_PARTITIONS", "8")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.starrocks_host}:{self.starrocks_query_port}/{self.starrocks_database}"
