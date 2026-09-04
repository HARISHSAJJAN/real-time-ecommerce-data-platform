"""Seed the StarRocks `users` and `products` dimension tables.

Reuses the same generator used by the Kafka producer (with the same random
seed) so that the dimension tables are consistent with the user_id/product_id
values that will appear in the event stream. StarRocks Primary Key tables
upsert on plain INSERT, so this script is safe to re-run.
"""
from __future__ import annotations

import logging
import os
import sys

import pymysql
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from data_generator.config import GeneratorConfig  # noqa: E402
from data_generator.generator import EcommerceDataGenerator  # noqa: E402

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_dimensions")

BATCH_SIZE = 500


def get_connection():
    return pymysql.connect(
        host=os.getenv("STARROCKS_HOST", "localhost"),
        port=int(os.getenv("STARROCKS_QUERY_PORT_HOST", os.getenv("STARROCKS_PORT", "9030"))),
        user=os.getenv("STARROCKS_USER", "root"),
        password=os.getenv("STARROCKS_PASSWORD", ""),
        database=os.getenv("STARROCKS_DATABASE", "ecommerce"),
        autocommit=True,
    )


def seed_users(cursor, users) -> None:
    sql = "INSERT INTO users (user_id, country, signup_date, preferred_device) VALUES (%s, %s, %s, %s)"
    rows = [(u.user_id, u.country, u.signup_date.strftime("%Y-%m-%d %H:%M:%S"), u.preferred_device.value) for u in users]
    for i in range(0, len(rows), BATCH_SIZE):
        cursor.executemany(sql, rows[i : i + BATCH_SIZE])
    logger.info("Seeded %d users", len(rows))


def seed_products(cursor, products) -> None:
    sql = "INSERT INTO products (product_id, name, category, price, currency) VALUES (%s, %s, %s, %s, %s)"
    rows = [(p.product_id, p.name, p.category, p.price, p.currency) for p in products]
    for i in range(0, len(rows), BATCH_SIZE):
        cursor.executemany(sql, rows[i : i + BATCH_SIZE])
    logger.info("Seeded %d products", len(rows))


def main() -> None:
    config = GeneratorConfig()
    generator = EcommerceDataGenerator(config)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            seed_users(cursor, generator.users)
            seed_products(cursor, generator.products)
    finally:
        conn.close()

    logger.info("Dimension seeding complete.")


if __name__ == "__main__":
    main()
