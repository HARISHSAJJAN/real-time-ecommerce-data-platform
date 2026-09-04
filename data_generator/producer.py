"""Kafka producer wrapper for publishing generated e-commerce events."""
from __future__ import annotations

import json
import logging
import signal
import time
from typing import Any

from confluent_kafka import Producer

from data_generator.config import GeneratorConfig
from data_generator.generator import EcommerceDataGenerator
from data_generator.metadata import PipelineRunRecorder

logger = logging.getLogger("data_generator.producer")


class EventProducer:
    """Publishes generated events to Kafka at a configurable rate.

    Uses `user_id` as the Kafka message key so that all events for a given
    user land on the same partition, preserving per-user event ordering
    within a partition (important for session-based streaming logic).
    """

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.generator = EcommerceDataGenerator(config)
        self._producer = Producer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "client.id": "ecommerce-event-generator",
                # Favor delivery reliability over raw throughput for a local demo.
                "acks": "all",
                "retries": 5,
                "linger.ms": 50,
                "compression.type": "snappy",
            }
        )
        self._running = False
        self._stats = {"valid": 0, "malformed": 0, "duplicate": 0, "delivery_failures": 0}
        self._run_recorder = PipelineRunRecorder(service_name="data-generator")

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            self._stats["delivery_failures"] += 1
            logger.error("Delivery failed for record %s: %s", msg.key(), err)

    def _publish(self, payload: dict[str, Any], key: str) -> None:
        self._producer.produce(
            topic=self.config.topic_events,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=self._delivery_callback,
        )
        self._producer.poll(0)

    def run(self) -> None:
        """Run the continuous publish loop until interrupted (SIGINT/SIGTERM)."""
        self._running = True
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        self._run_recorder.start()

        interval = 1.0 / self.config.events_per_second
        logger.info(
            "Starting event generator: %.2f events/sec, %d users, %d products, topic=%s",
            self.config.events_per_second,
            self.config.num_users,
            self.config.num_products,
            self.config.topic_events,
        )

        next_log = time.monotonic() + 10
        while self._running:
            start = time.monotonic()
            payload, kind = self.generator.next_event()
            key = str(payload.get("user_id") or "unknown")
            self._publish(payload, key)
            self._stats[kind] += 1

            if time.monotonic() >= next_log:
                logger.info("Stats so far: %s", self._stats)
                next_log = time.monotonic() + 10

            elapsed = time.monotonic() - start
            sleep_for = interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

        self.shutdown()

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("Received shutdown signal (%s), draining producer...", signum)
        self._running = False

    def shutdown(self) -> None:
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d messages were not delivered before shutdown", remaining)
        logger.info("Final stats: %s", self._stats)
        self._run_recorder.finish(self._stats)
