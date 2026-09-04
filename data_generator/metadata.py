"""Records generator run metadata (start/stop, event counts) into PostgreSQL.

This is deliberately best-effort: the generator's job is to publish events to
Kafka, and a PostgreSQL outage must never stop that. Every call here catches
and logs its own exceptions rather than propagating them.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone

import psycopg2

logger = logging.getLogger("data_generator.metadata")


class PipelineRunRecorder:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.run_id = str(uuid.uuid4())
        self._enabled = True

    def _connect(self):
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DATABASE", "platform_metadata"),
            user=os.getenv("POSTGRES_USER", "platform_admin"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            connect_timeout=5,
        )

    def start(self) -> None:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pipeline_runs (run_id, service_name, status, started_at) "
                    "VALUES (%s, %s, 'running', %s)",
                    (self.run_id, self.service_name, datetime.now(timezone.utc)),
                )
        except Exception as exc:  # noqa: BLE001 - metadata logging must never crash the generator
            logger.warning("Could not record pipeline run start in PostgreSQL: %s", exc)
            self._enabled = False

    def finish(self, stats: dict) -> None:
        if not self._enabled:
            return
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline_runs SET status = 'stopped', ended_at = %s, "
                    "events_valid = %s, events_malformed = %s, events_duplicate = %s WHERE run_id = %s",
                    (
                        datetime.now(timezone.utc),
                        stats.get("valid", 0),
                        stats.get("malformed", 0),
                        stats.get("duplicate", 0),
                        self.run_id,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not record pipeline run completion in PostgreSQL: %s", exc)
