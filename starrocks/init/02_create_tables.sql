-- ==============================================================================
-- StarRocks analytical schema for the Real-Time E-Commerce Data Platform.
--
-- Table model choices (see docs/data-model.md for the full rationale):
--   - users / products: PRIMARY KEY model. Small, slowly-changing dimensions
--     that are periodically re-seeded; primary key gives cheap upserts.
--   - events: DUPLICATE KEY model. High-volume, append-only fact data where
--     every row is immutable once written - there is nothing to upsert, so
--     the duplicate key model avoids the merge-on-write overhead of a
--     primary/unique key table and maximizes ingest throughput. Partitioned
--     by day (dynamic partitioning) for partition pruning on time-range
--     queries, bucketed by user_id for even data distribution.
--   - *_metrics tables: PRIMARY KEY model, keyed by the aggregation's
--     group-by columns (e.g. window_start + product_id). Spark's streaming
--     aggregations run in `update` output mode and re-emit a window's totals
--     on every micro-batch until its watermark passes; the primary key model
--     makes each re-emit an idempotent upsert instead of an accumulating
--     duplicate row.
-- ==============================================================================

USE ecommerce;

-- ---------------------------------------------------------------- dimensions

CREATE TABLE IF NOT EXISTS users (
    user_id            VARCHAR(36)  NOT NULL,
    country             VARCHAR(8),
    signup_date         DATETIME,
    preferred_device    VARCHAR(16)
) PRIMARY KEY (user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS products (
    product_id  VARCHAR(36)  NOT NULL,
    name        VARCHAR(256),
    category    VARCHAR(64),
    price       DECIMAL(12, 2),
    currency    VARCHAR(8)
) PRIMARY KEY (product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- --------------------------------------------------------------------- facts

-- Key columns (event_date, event_type, user_id) must be declared first, in
-- the same order as the DUPLICATE KEY clause below - StarRocks requires the
-- key columns to be a prefix of the column list.
CREATE TABLE IF NOT EXISTS events (
    event_date          DATE            NOT NULL,
    event_type          VARCHAR(32)     NOT NULL,
    user_id             VARCHAR(36)     NOT NULL,
    event_id            VARCHAR(36)     NOT NULL,
    event_timestamp     DATETIME        NOT NULL,
    session_id          VARCHAR(36)     NOT NULL,
    product_id          VARCHAR(36),
    quantity            INT,
    price               DECIMAL(12, 2),
    currency            VARCHAR(8),
    total_amount        DECIMAL(14, 2),
    device_type         VARCHAR(16),
    country             VARCHAR(8),
    payment_method      VARCHAR(32),
    event_hour          TINYINT,
    is_purchase_event   BOOLEAN
) ENGINE = OLAP
DUPLICATE KEY(event_date, event_type, user_id)
PARTITION BY RANGE(event_date) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16"
);

-- A "purchases" fact is intentionally not materialized as a separate table:
-- it is simply events filtered to event_type = 'purchase'. Duplicating it
-- would double storage and double the write path for no analytical benefit,
-- since StarRocks/Trino can filter the partition-pruned events table cheaply.
CREATE VIEW IF NOT EXISTS purchases AS
SELECT *
FROM events
WHERE event_type = 'purchase';

-- ---------------------------------------------------------------- aggregates

CREATE TABLE IF NOT EXISTS revenue_metrics (
    window_start            DATETIME NOT NULL,
    window_end              DATETIME,
    revenue                 DECIMAL(14, 2),
    number_of_purchases     BIGINT,
    average_order_value     DECIMAL(12, 2)
) PRIMARY KEY (window_start)
DISTRIBUTED BY HASH(window_start) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS event_volume_metrics (
    window_start    DATETIME        NOT NULL,
    event_type      VARCHAR(32)     NOT NULL,
    window_end      DATETIME,
    event_count     BIGINT
) PRIMARY KEY (window_start, event_type)
DISTRIBUTED BY HASH(window_start) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS product_metrics (
    window_start        DATETIME        NOT NULL,
    product_id           VARCHAR(36)     NOT NULL,
    window_end           DATETIME,
    views                 BIGINT,
    cart_additions        BIGINT,
    purchases             BIGINT,
    revenue               DECIMAL(14, 2),
    conversion_rate       DECIMAL(8, 4)
) PRIMARY KEY (window_start, product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS geo_metrics (
    window_start    DATETIME        NOT NULL,
    country          VARCHAR(8)      NOT NULL,
    window_end       DATETIME,
    event_count      BIGINT,
    revenue          DECIMAL(14, 2)
) PRIMARY KEY (window_start, country)
DISTRIBUTED BY HASH(country) BUCKETS 8
PROPERTIES ("replication_num" = "1");
