"""Data-access layer: runs SQL against StarRocks through Trino.

Keeping every query in this module (rather than scattered through route
handlers) means the SQL surface area is easy to audit and test independently
of FastAPI, and route handlers stay free of connection/cursor plumbing.
"""
from __future__ import annotations

import logging
from typing import Any

import trino

from api.config import Settings

logger = logging.getLogger("api.trino_repository")


class TrinoRepository:
    def __init__(self, settings: Settings):
        self._settings = settings

    def _connect(self) -> trino.dbapi.Connection:
        return trino.dbapi.connect(
            host=self._settings.trino_host,
            port=self._settings.trino_port,
            user=self._settings.trino_user,
            catalog=self._settings.trino_catalog,
            schema=self._settings.trino_schema,
        )

    def _query(self, sql: str) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception:
            logger.exception("Trino query failed: %s", sql)
            raise
        finally:
            conn.close()

    def is_reachable(self) -> bool:
        try:
            self._query("SELECT 1")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------- revenue
    def revenue_summary(self) -> dict[str, Any]:
        rows = self._query(
            """
            SELECT
                COALESCE(ROUND(SUM(total_amount), 2), 0) AS total_revenue,
                COUNT(*) AS total_purchases,
                COALESCE(ROUND(AVG(total_amount), 2), 0) AS average_order_value
            FROM events
            WHERE event_type = 'purchase'
            """
        )
        return rows[0] if rows else {"total_revenue": 0, "total_purchases": 0, "average_order_value": 0}

    def daily_revenue(self, days: int = 14) -> list[dict[str, Any]]:
        return self._query(
            f"""
            SELECT CAST(event_date AS VARCHAR) AS event_date,
                   ROUND(SUM(total_amount), 2) AS revenue,
                   COUNT(*) AS purchase_count
            FROM events
            WHERE event_type = 'purchase'
            GROUP BY event_date
            ORDER BY event_date DESC
            LIMIT {int(days)}
            """
        )

    def revenue_by_country(self) -> list[dict[str, Any]]:
        return self._query(
            """
            SELECT country, ROUND(SUM(total_amount), 2) AS revenue, COUNT(*) AS purchase_count
            FROM events
            WHERE event_type = 'purchase'
            GROUP BY country
            ORDER BY revenue DESC
            """
        )

    # -------------------------------------------------------------- events
    def total_events(self) -> int:
        rows = self._query("SELECT COUNT(*) AS total FROM events")
        return int(rows[0]["total"]) if rows else 0

    def events_by_type(self) -> list[dict[str, Any]]:
        return self._query(
            """
            SELECT event_type, COUNT(*) AS event_count
            FROM events
            GROUP BY event_type
            ORDER BY event_count DESC
            """
        )

    def active_users(self) -> int:
        rows = self._query("SELECT COUNT(DISTINCT user_id) AS active_users FROM events")
        return int(rows[0]["active_users"]) if rows else 0

    def device_distribution(self) -> list[dict[str, Any]]:
        return self._query(
            """
            SELECT device_type, COUNT(*) AS event_count
            FROM events
            GROUP BY device_type
            ORDER BY event_count DESC
            """
        )

    # ------------------------------------------------------------ products
    def top_products(self, limit: int = 10) -> list[dict[str, Any]]:
        return self._query(
            f"""
            SELECT e.product_id,
                   p.name,
                   ROUND(SUM(e.total_amount), 2) AS revenue,
                   COUNT(*) AS purchases
            FROM events e
            LEFT JOIN products p ON e.product_id = p.product_id
            WHERE e.event_type = 'purchase'
            GROUP BY e.product_id, p.name
            ORDER BY revenue DESC
            LIMIT {int(limit)}
            """
        )

    def product_performance(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._query(
            f"""
            SELECT e.product_id,
                   p.name,
                   p.category,
                   SUM(CASE WHEN e.event_type = 'product_view' THEN 1 ELSE 0 END) AS views,
                   SUM(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_additions,
                   SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
                   ROUND(SUM(CASE WHEN e.event_type = 'purchase' THEN e.total_amount ELSE 0 END), 2) AS revenue,
                   ROUND(
                       SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) * 1.0
                       / NULLIF(SUM(CASE WHEN e.event_type = 'product_view' THEN 1 ELSE 0 END), 0),
                       4
                   ) AS conversion_rate
            FROM events e
            LEFT JOIN products p ON e.product_id = p.product_id
            GROUP BY e.product_id, p.name, p.category
            ORDER BY revenue DESC
            LIMIT {int(limit)}
            """
        )

    # ------------------------------------------------------------ countries
    def country_metrics(self) -> list[dict[str, Any]]:
        return self._query(
            """
            SELECT country,
                   COUNT(*) AS event_count,
                   ROUND(SUM(CASE WHEN event_type = 'purchase' THEN total_amount ELSE 0 END), 2) AS revenue
            FROM events
            GROUP BY country
            ORDER BY event_count DESC
            """
        )

    # -------------------------------------------------------------- funnel
    def funnel_counts(self) -> dict[str, int]:
        rows = self._query(
            """
            SELECT
                SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS views,
                SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_additions,
                SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
            FROM events
            """
        )
        return rows[0] if rows else {"views": 0, "cart_additions": 0, "purchases": 0}
