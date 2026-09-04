# Data Model

## Star-schema-inspired design

The analytical schema in StarRocks (`ecommerce` database) follows a
simplified star schema: a small number of dimensions describing "who" and
"what", one high-volume fact table describing "what happened", and a set of
pre-aggregated tables that make common dashboard queries cheap.

```
        dim: users                dim: products
      (user_id, country,        (product_id, name,
       signup_date,               category, price,
       preferred_device)          currency)
              \                        /
               \                      /
                fact: events (event_id, event_timestamp,
                    user_id, product_id, event_type,
                    quantity, price, total_amount,
                    device_type, country, payment_method,
                    event_date, event_hour, is_purchase_event)
               /                      \
              /                        \
  agg: product_metrics          agg: revenue_metrics
  (window, product_id,          (window, revenue,
   views, cart_additions,        number_of_purchases,
   purchases, revenue,           average_order_value)
   conversion_rate)

  agg: event_volume_metrics     agg: geo_metrics
  (window, event_type,          (window, country,
   event_count)                  event_count, revenue)
```

## Why these tables and no others

- **No `dim_date`.** StarRocks and Trino both have full native date/time
  functions (`DATE_TRUNC`, `EXTRACT`, etc.), and `events` already carries
  `event_date`/`event_hour` as derived columns. A physical date dimension
  would add a join for no analytical benefit at this data volume - a common,
  deliberate omission in modern OLAP schemas that support native temporal
  functions.
- **No separate `fact_purchases` table.** `purchases` is exposed as a SQL
  `VIEW` (`WHERE event_type = 'purchase'`) over `events` rather than a
  materialized table. Materializing it would double the write path and
  storage for data that is already cheap to filter out of a partition-pruned
  fact table.
- **Four aggregate tables, not one.** Each corresponds to one of Spark's
  independent windowed streaming queries (revenue, event volume, product
  performance, geography) from the project requirements. Keeping them
  separate (rather than one wide table with nullable columns) keeps each
  query's schema honest about what it actually measures, and lets each
  streaming query fail/restart independently via its own checkpoint.

## Table model choices in StarRocks

StarRocks supports several table models; using the right one per table
matters for both correctness and performance:

| Table | Model | Key | Why |
|---|---|---|---|
| `users` | **Primary Key** | `user_id` | Small dimension, periodically re-seeded. Primary Key tables make re-running the seed script an idempotent upsert instead of accumulating duplicate rows. |
| `products` | **Primary Key** | `product_id` | Same reasoning as `users`. |
| `events` | **Duplicate Key** | `(event_date, event_type, user_id)` | High-volume, append-only, immutable rows - there is nothing to upsert. Duplicate Key avoids the merge-on-write overhead a Primary/Unique Key table would pay on every insert, maximizing streaming ingest throughput. Partitioned by day (dynamic partitioning) for time-range partition pruning; bucketed by `user_id` for even write/read distribution. |
| `revenue_metrics`, `event_volume_metrics`, `product_metrics`, `geo_metrics` | **Primary Key** | the aggregation's group-by columns (e.g. `window_start, product_id`) | Spark's windowed aggregations run in Structured Streaming's `update` output mode: a window's totals are re-emitted on every micro-batch until its watermark passes. A Primary Key table turns each re-emit into an idempotent upsert on the same row, so StarRocks always reflects the latest total per window instead of accumulating one row per micro-batch per window. |

## Idempotency and correctness

Two mechanisms work together to keep the pipeline correct under retries and
restarts, without claiming a stronger guarantee than the architecture
actually provides:

1. **Deduplication by `event_id`** (Spark, watermarked `dropDuplicates`)
   handles duplicates introduced by the *producer* side (e.g. a retried
   publish after a delivery-ack timeout).
2. **Primary Key upserts** (StarRocks) handle duplicates introduced by the
   *aggregation* side (`update`-mode re-emits of the same window).

Together these make the pipeline **idempotent within the watermark window**:
re-processing the same micro-batch, or restarting Spark from its last
checkpoint, converges to the same StarRocks state rather than double-counting.
This is a practical, testable form of correctness - it is not the same claim
as formally guaranteed exactly-once end-to-end processing, which would
additionally require transactional, offset-aware sink writes that this
project's JDBC sink does not implement.
