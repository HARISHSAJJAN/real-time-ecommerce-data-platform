# Learning Guide: From Beginner to Interview-Ready

This guide walks through every technology in this project, explaining the
concept first in general terms, then pointing at exactly where it is used
here so you can read the real code alongside the explanation.

---

## Python

### Kafka producer
`data_generator/producer.py` wraps `confluent_kafka.Producer`. Key ideas:
- `produce()` is asynchronous - it queues the message and returns immediately.
  `poll(0)` lets the client process delivery callbacks without blocking.
- The **delivery callback** (`_delivery_callback`) is how you find out whether
  a message actually reached the broker; ignoring it (a common beginner
  mistake) means silent message loss.
- `flush()` on shutdown blocks until every queued message is either delivered
  or fails, which is why `shutdown()` calls it before the process exits.

### Configuration
`data_generator/config.py` and `api/config.py` both load settings from
environment variables via a `dataclass`, rather than hardcoding values. This
is the twelve-factor-app pattern: the same code runs differently in dev,
Docker, or CI purely by changing environment variables, with no code change.

### Pydantic
`data_generator/models.py` defines `EcommerceEvent` as a `pydantic.BaseModel`.
Pydantic validates types and constraints (`Field(gt=0)`, custom
`@field_validator`s) at object-construction time and raises a structured
`ValidationError` on failure - this is what lets the generator's malformed
events be *detected* as malformed later in `tests/test_generator.py`, by
attempting to construct the same model and asserting it raises.

---

## Kafka

- **Topic**: a named, append-only log (`ecommerce-events`,
  `ecommerce-dead-letter` here). Producers append; consumers read at their
  own pace.
- **Partition**: a topic is split into partitions for parallelism. This
  project uses 6 partitions for `ecommerce-events` (`docker-compose.yml`,
  `kafka-init` service). Kafka guarantees ordering *within* a partition only,
  which is why the producer keys messages by `user_id`
  (`producer.py:_publish`) - it guarantees all of one user's events land on
  the same partition and are therefore processed in order.
- **Offset**: each partition is a numbered sequence; a consumer's offset is
  "how far it has read." Structured Streaming manages Kafka offsets itself
  via its checkpoint directory rather than relying on Kafka's built-in
  consumer-group offset commits - see "checkpointing" below.
- **Consumer group**: a set of consumers sharing the work of reading a
  topic's partitions. `KAFKA_CONSUMER_GROUP` is set in config for
  documentation/tooling purposes, though Spark's own offset management means
  it does not strictly rely on consumer-group coordination the way a plain
  Kafka consumer would.
- **Retention**: how long Kafka keeps messages regardless of whether they've
  been consumed (time- or size-based). Not explicitly configured here (the
  broker's defaults apply), but it is what makes Kafka **replayable** - the
  reason `startingOffsets=earliest` in `spark/streaming_job.py` can process
  a topic's full history when the job starts cold.

---

## Spark

- **SparkSession**: the entry point to all Spark functionality
  (`spark/streaming_job.py:build_spark_session`).
- **DataFrame**: a distributed, immutable table of data with a known schema.
  Every function in `spark/transformations.py` takes one DataFrame and
  returns a new one - transformations never mutate in place.
- **Transformation vs. action**: `.select()`, `.filter()`, `.withColumn()`,
  `.groupBy()` are *transformations* - they build up a query plan lazily and
  do no work yet. `.collect()`, `.count()` are *actions* - they trigger
  actual execution. This is why `tests/test_transformations.py` calls
  `.collect()` at the end of each test: nothing runs until then.
- **Lazy evaluation**: Spark doesn't execute a transformation until an action
  forces it, which lets Spark's optimizer (Catalyst) look at the *whole*
  chain of transformations and rewrite/optimize it before running anything.
- **Partitions (Spark's, not Kafka's)**: a Spark DataFrame is itself split
  into partitions distributed across executors. `spark.sql.shuffle.partitions`
  (`spark/config.py`) controls how many partitions a shuffle (see below)
  produces.
- **Shuffle**: an expensive redistribution of data across the cluster,
  triggered by operations like `groupBy` or `join` where matching rows may
  live on different partitions. Every `.groupBy(...)` call in
  `spark/transformations.py` triggers a shuffle - this is inherent to
  aggregation, not something to "fix", but it's why shuffle partition count
  is a real tuning knob (see docs/architecture.md's scaling section).
- **Joins**: combining two DataFrames on a key. `sql/product_analytics.sql`
  and `api/repositories/trino_repository.py` both join `events` to
  `products` on `product_id` to attach product names to metrics.
- **Aggregations**: `groupBy(...).agg(F.sum(...), F.count(...), ...)` -
  used throughout `spark/transformations.py` to compute revenue, counts, and
  conversion rates per window.

---

## Spark Structured Streaming

- **Streaming DataFrame**: created via `spark.readStream` instead of
  `spark.read` (`spark/streaming_job.py:read_kafka_stream`). It looks and
  behaves like a normal DataFrame in code, but represents an *unbounded*
  table that grows as new Kafka messages arrive.
- **Micro-batches**: Structured Streaming doesn't process one event at a
  time; it polls the source on a fixed `trigger` interval
  (`SPARK_TRIGGER_INTERVAL`, default 10 seconds) and processes everything
  new as one small batch DataFrame - `foreachBatch` callbacks
  (`write_dlq_batch`, `write_starrocks_batch`) receive exactly this
  micro-batch DataFrame plus a `batch_id`.
- **Checkpointing**: each streaming query writes its processed Kafka offsets
  and any stateful aggregation state to a checkpoint directory
  (`option("checkpointLocation", ...)` on every query in
  `streaming_job.py`). On restart, the query resumes exactly where it left
  off instead of reprocessing or skipping data. Every one of this project's
  five streaming queries gets its own checkpoint subdirectory, so they
  recover independently.
- **Watermark**: a declared bound on "how late is late" for event-time data
  (`.withWatermark("event_timestamp", "2 minutes")` in
  `spark/data_quality.py` and `spark/transformations.py`). It lets Spark
  safely *discard* old state (finalize a window, evict a seen-event-id from
  the dedup store) instead of keeping unbounded memory for data that will
  never arrive.
- **Window**: `F.window("event_timestamp", "1 minute")` groups event-time
  timestamps into fixed tumbling buckets for aggregation - see every
  function in `spark/transformations.py`.
- **Output modes**: `append` (a result row, once emitted, is never changed -
  used for the raw fact-table write) vs. `update` (a result row can be
  re-emitted with a new value as more data arrives within the watermark -
  used for all four windowed aggregations, matched with StarRocks' Primary
  Key upsert semantics; see docs/data-model.md).

---

## StarRocks

- **OLAP** (Online Analytical Processing): optimized for `SUM`/`COUNT`/
  `GROUP BY` over large tables, as opposed to OLTP's optimization for many
  small single-row reads/writes. StarRocks stores data column-wise, so a
  query touching 5 of `events`' 16 columns only reads those 5 columns off
  disk.
- **Table models**: see docs/data-model.md's table for the concrete
  reasoning per table. In short: Duplicate Key for immutable append-only
  data (`events`), Primary Key for anything that needs upserts (dimensions,
  streaming aggregates).
- **Partitioning**: `events` is partitioned by day
  (`starrocks/init/02_create_tables.sql`), using StarRocks' *dynamic
  partitioning* feature so new daily partitions are created automatically.
  A query filtered to a date range only scans the relevant partitions.
- **Bucketing**: within a partition, `DISTRIBUTED BY HASH(user_id) BUCKETS
  16` spreads rows across 16 physical buckets by a hash of `user_id`, for
  parallel scan/write throughput.
- **Indexing**: not explicitly added here (StarRocks automatically maintains
  a lightweight sparse index on the sort/duplicate key columns) - a
  deliberate choice to keep the schema simple at this data volume rather
  than add bitmap/bloom-filter indexes that would matter more at much larger
  scale.
- **Analytical queries**: see `sql/*.sql` for concrete examples of the kind
  of query StarRocks is built for.

---

## Trino

- **Distributed SQL**: Trino parses a SQL query, builds a distributed
  execution plan, and pushes work down to connected data sources (here, just
  StarRocks via the `mysql` connector) - it does not store data itself.
- **Coordinator / workers**: in a full deployment, a coordinator node plans
  queries and workers execute them in parallel. This project's `trino`
  service runs both roles in a single container, appropriate for local dev.
- **Connectors**: pluggable adapters that let Trino query different systems
  through one SQL interface. `trino/catalog/starrocks.properties` configures
  the built-in `mysql` connector against StarRocks' MySQL-wire-protocol port
  - see docs/architecture.md's "Design Decisions" for why there's no
  dedicated StarRocks connector used here.
- **Federation**: Trino's headline capability - querying (and even joining)
  multiple heterogeneous data sources through one SQL dialect. Only one
  catalog is configured in this project, but the API and `sql/*.sql` queries
  are written against Trino specifically so that adding a second catalog
  later would not require changing how they're written.

---

## Data Engineering concepts

- **ETL vs. ELT**: ETL transforms data *before* loading it into the
  destination; ELT loads raw data first and transforms it there. This
  project is closer to ETL: Spark validates/cleans/aggregates *before*
  writing to StarRocks, rather than dumping raw JSON into StarRocks and
  transforming with SQL afterward.
- **Batch vs. streaming**: batch processes a bounded, already-complete
  dataset (e.g. yesterday's log file); streaming processes an unbounded,
  continuously-arriving dataset. This project is streaming end to end, but
  every Structured Streaming query still executes as a sequence of small
  batches internally (see "micro-batches" above) - the line between the two
  is blurrier than it first appears.
- **Data lake vs. data warehouse**: a lake stores raw/semi-structured files
  cheaply for later processing; a warehouse stores structured, modeled data
  optimized for query performance. This project only has a warehouse
  (StarRocks) - there is no raw-file lake layer, a deliberate simplification
  given the modest local data volume.
- **Fact vs. dimension**: a fact table records *events/measurements*
  (`events` - one row per thing that happened); a dimension table records
  *descriptive entities* referenced by facts (`users`, `products`). See
  docs/data-model.md.
- **Schema design**: see docs/data-model.md in full.
- **Data quality**: validating data against explicit rules rather than
  trusting it. `spark/data_quality.py` is the concrete implementation;
  `docs/architecture.md`'s failure-scenarios table explains what happens to
  data that fails.
- **Idempotency**: an operation that produces the same end state no matter
  how many times it is applied. This project achieves it at two layers -
  event-level dedup (Spark) and window-level upsert (StarRocks Primary Key
  tables) - see docs/data-model.md's "Idempotency and correctness" section
  for the precise claim (and its limits).
- **Exactly-once vs. at-least-once processing**: at-least-once means a
  record might be processed more than once (but never dropped); exactly-once
  means it's processed as if exactly once, typically achieved by combining
  at-least-once delivery with idempotent writes. This project is honest
  about landing in the middle: Kafka delivery and Spark's checkpointed
  offsets give at-least-once *delivery*, and the idempotency mechanisms above
  make repeated processing *converge* to the correct result - which is a
  common, practical way to achieve effectively-exactly-once outcomes without
  a distributed transaction protocol.
