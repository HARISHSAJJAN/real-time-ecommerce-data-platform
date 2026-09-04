# Interview Preparation

56 questions across three tiers. Each has a short interview-ready answer, a
deeper explanation, and how it maps to this specific project.

---

## Beginner

### 1. What is ETL?
**Short:** Extract, Transform, Load - pulling data from a source, cleaning/
reshaping it, and loading it into a destination.
**Detail:** ETL implies transformation happens *before* loading, typically in
a separate processing layer, as opposed to ELT where raw data lands first
and gets transformed in the warehouse afterward.
**This project:** Spark extracts from Kafka, transforms (validates, dedupes,
normalizes, aggregates) in `spark/transformations.py` and
`spark/data_quality.py`, then loads into StarRocks - classic ETL.

### 2. What is a data pipeline?
**Short:** An automated sequence of steps that moves data from a source to a
destination, usually with transformation along the way.
**Detail:** Pipelines can be batch (scheduled, bounded runs) or streaming
(continuous, unbounded). Reliability concerns - retries, idempotency, dead
letters - become central once a pipeline runs unattended.
**This project:** generator -> Kafka -> Spark -> StarRocks -> Trino -> API ->
dashboard is the full pipeline; see docs/architecture.md.

### 3. What is Kafka?
**Short:** A distributed, durable, publish-subscribe event streaming
platform.
**Detail:** Producers append records to topics; consumers read them
independently at their own pace; records persist for a configurable
retention period regardless of whether they've been read, which is what
makes replay possible.
**This project:** the buffer between the data generator and Spark
(`docker-compose.yml`'s `kafka` service; topics created in `kafka-init`).

### 4. What is Spark?
**Short:** A distributed data processing engine for large-scale batch and
streaming workloads.
**Detail:** Spark represents data as distributed DataFrames/RDDs, builds a
lazy execution plan, and runs it across a cluster of executors, handling
parallelism and fault tolerance for you.
**This project:** `spark/streaming_job.py` runs Structured Streaming locally
(`local[*]`) inside its own container.

### 5. What is SQL, and why does it still matter in a "big data" project?
**Short:** Structured Query Language - the standard language for querying
relational and analytical data.
**Detail:** Even distributed, non-relational-feeling systems (Spark,
StarRocks, Trino) expose SQL as their primary interface because it is the
common language analysts, engineers, and BI tools all already speak.
**This project:** `sql/*.sql` are runnable Trino queries against the same
data the API and dashboard use.

### 6. What is a Kafka topic?
**Short:** A named, append-only log that producers write to and consumers
read from.
**Detail:** Topics are the unit of organization in Kafka; each is split into
partitions for parallelism.
**This project:** `ecommerce-events` (main topic) and `ecommerce-dead-letter`
(invalid records).

### 7. What's a REST API, and why put one in front of the warehouse?
**Short:** An HTTP interface exposing resources/operations, here read-only
analytics.
**Detail:** It abstracts the query layer (Trino/SQL) behind a stable, typed
contract, so any client (dashboard, another service, a script) can consume
metrics without knowing SQL or the warehouse's schema.
**This project:** `api/main.py` and `api/routes/metrics.py`.

### 8. What is a dashboard, and what makes one "real-time"?
**Short:** A visual, usually auto-refreshing, summary of metrics.
**Detail:** "Real-time" is relative - it means low enough latency between an
event happening and it appearing on screen that the delay isn't
operationally meaningful; it's rarely literally instantaneous.
**This project:** the Streamlit dashboard polls the API every
`DASHBOARD_REFRESH_SECONDS` (default 15s); the underlying data itself lags
by roughly a window duration plus a watermark delay (~2-3 minutes) - see
docs/troubleshooting.md.

### 9. What is Docker, and why use Docker Compose here?
**Short:** Docker packages an application and its dependencies into a
portable container; Compose orchestrates multiple containers as one stack.
**Detail:** Compose lets you define every service's image/build, environment,
networking, and startup order (`depends_on` + health checks) in one
declarative file, so the whole platform starts with one command.
**This project:** `docker-compose.yml` defines all nine services.

### 10. What is a primary key, and does this project use one?
**Short:** A column (or columns) that uniquely identifies a row.
**Detail:** Beyond uniqueness, in StarRocks a Primary Key table also changes
write semantics: inserting a row with an existing key *replaces* it
(upsert) rather than adding a duplicate.
**This project:** `users.user_id`, `products.product_id`, and every
`*_metrics` table's window/dimension columns are Primary Keys - see
docs/data-model.md.

---

## Intermediate

### 11. Kafka partition vs. consumer group - what's the difference?
**Short:** A partition is a physical split of a topic's data; a consumer
group is a set of consumers that divide up a topic's partitions between
them.
**Detail:** Within a group, each partition is read by exactly one consumer
at a time (so parallelism is capped by partition count); across groups, the
same data can be read independently and repeatedly.
**This project:** `ecommerce-events` has 6 partitions; Spark's Kafka source
manages its own offsets rather than relying on classic consumer-group
rebalancing, but the concept still governs how much read parallelism is
available.

### 12. Spark transformation vs. action - what's the difference?
**Short:** A transformation builds a new DataFrame lazily (no execution
yet); an action triggers actual computation.
**Detail:** `.filter()`, `.select()`, `.groupBy()` are transformations.
`.collect()`, `.count()`, writing to a sink are actions. This laziness lets
Spark's Catalyst optimizer see and rewrite the *whole* chain before running
anything.
**This project:** every function in `spark/transformations.py` is a chain of
transformations; `tests/test_transformations.py` calls `.collect()` to
force execution and assert on results.

### 13. What causes a Spark shuffle, and why is it expensive?
**Short:** Operations like `groupBy`/`join` that require rows with the same
key to be co-located on one executor trigger a shuffle - expensive because it
moves data across the network and to disk.
**Detail:** Shuffle involves writing partitioned intermediate output to
disk, transferring it over the network, and re-reading it - by far the most
common Spark performance bottleneck.
**This project:** every `.groupBy(F.window(...))` in
`spark/transformations.py` triggers one; `spark.sql.shuffle.partitions`
(`spark/config.py`) tunes how many output partitions that shuffle produces.

### 14. What is a watermark in Structured Streaming?
**Short:** A declared threshold for how late event-time data is allowed to
arrive before Spark stops waiting for it.
**Detail:** It bounds the state Spark must keep for windowed aggregations
and stateful dedup - without one, Spark would have to retain state forever
on an unbounded stream.
**This project:** `SPARK_WATERMARK_DELAY` (default 2 minutes), applied in
`spark/data_quality.py::deduplicate_events` and every aggregation function
in `spark/transformations.py`.

### 15. How does deduplication work in this pipeline?
**Short:** By `event_id`, using `dropDuplicates` under a watermark.
**Detail:** Spark keeps a bounded state store of event_ids seen within the
watermark window; a repeat of an event_id already in that store is dropped.
Once an event_id ages out of the watermark, its slot is freed - so a very
late duplicate (older than the watermark) would not be caught, a deliberate
bounded-memory trade-off.
**This project:** `spark/data_quality.py::deduplicate_events`; exercised
directly by the generator's `duplicate_event_rate` and tested in
`tests/test_data_quality.py`.

### 16. Why use StarRocks instead of a plain relational database?
**Short:** StarRocks is a columnar OLAP engine built for fast aggregation
over large tables - the workload this project's dashboard queries actually
are.
**Detail:** Row-oriented OLTP databases (like PostgreSQL) are optimized for
many small transactional reads/writes touching whole rows; columnar OLAP
engines read only the columns a query needs and are built for `GROUP BY`/
`SUM` over millions of rows.
**This project:** see docs/data-model.md's table-model rationale.

### 17. What is Trino, and how is it different from a database?
**Short:** A distributed SQL query engine with no storage of its own - it
queries other systems ("connectors") using one SQL interface.
**Detail:** Trino separates *compute* (query planning/execution) from
*storage* (wherever the data actually lives), and can even join across
multiple different storage systems in one query.
**This project:** `trino/catalog/starrocks.properties` configures Trino to
query StarRocks via the MySQL-wire-protocol connector.

### 18. What's the difference between a fact table and a dimension table?
**Short:** Fact tables record events/measurements; dimension tables record
the descriptive entities those events reference.
**Detail:** Facts tend to be high-volume and append-only; dimensions tend to
be smaller and slowly-changing, and are joined onto facts for readable
labels/attributes.
**This project:** `events` is the fact table; `users`/`products` are
dimensions - see docs/data-model.md.

### 19. What does "idempotent" mean in a data pipeline, and why does it matter?
**Short:** An operation that produces the same result no matter how many
times it's applied.
**Detail:** Failures and retries are inevitable in distributed systems;
idempotency is what lets you safely retry (or replay from a checkpoint)
without corrupting downstream data with duplicates.
**This project:** event-level dedup plus StarRocks Primary Key upserts make
re-processing a micro-batch converge instead of double-count - see
docs/data-model.md's "Idempotency and correctness."

### 20. What is a checkpoint in Structured Streaming, and what does it store?
**Short:** A directory where a streaming query persists its processed Kafka
offsets and any stateful operator state (e.g. dedup, aggregation state).
**Detail:** On restart, the query reads its checkpoint and resumes exactly
where it left off, rather than reprocessing everything or skipping data.
**This project:** each of the five streaming queries in
`spark/streaming_job.py` has its own checkpoint subdirectory under
`/opt/spark-checkpoints`.

### 21. What is backpressure, and does this pipeline handle it?
**Short:** A mechanism to slow a producer (or a stage) down when a
downstream consumer can't keep up.
**Detail:** Kafka naturally decouples producer and consumer rates by
buffering; Structured Streaming's micro-batch model self-paces by only
pulling as much as it can process per trigger interval.
**This project:** the data generator publishes independently of Spark's
processing rate; Kafka's retention buffers the gap if Spark falls behind.

### 22. Why validate data again in Spark if the producer already validates with Pydantic?
**Short:** Because Kafka is a trust boundary - anything could publish to a
topic, not just this project's own generator.
**Detail:** Defense in depth: client-side validation improves the common
case, but a robust pipeline can't assume every producer is well-behaved.
**This project:** `spark/data_quality.py` independently re-validates every
field Pydantic already checks in `data_generator/models.py`.

### 23. What's the difference between `append` and `update` output modes?
**Short:** `append` only ever emits new, final rows; `update` can re-emit a
previously-emitted row with a new value.
**Detail:** Windowed aggregations are inherently incomplete until their
watermark passes, so they need `update` mode to correct earlier partial
totals as more data arrives.
**This project:** the raw `events` write uses `append`; all four windowed
aggregation queries use `update`, matched to StarRocks Primary Key upserts.

### 24. How would you test a Spark transformation without running Kafka?
**Short:** Treat the transformation function as pure - pass it a small
in-memory batch DataFrame and assert on the collected output.
**Detail:** Because Structured Streaming DataFrame *transformations* are the
same API as batch DataFrame transformations, functions written to take/
return DataFrames can be unit tested with `spark.createDataFrame(...)` and
no streaming source at all.
**This project:** `tests/test_transformations.py` and
`tests/test_data_quality.py` do exactly this.

### 25. What happens if two micro-batches try to write the same StarRocks row at once?
**Short:** For Primary Key tables, the later write wins (last-write-wins
upsert); for Duplicate Key tables, both rows are kept.
**Detail:** This is why the choice of table model per table matters -
`events` (Duplicate Key) is fine with concurrent appends, while
`*_metrics` (Primary Key) is specifically designed so repeated writes for
the same window converge instead of duplicate.
**This project:** see docs/data-model.md.

---

## Project-specific

### 26. Why did you choose Kafka for this project?
**Short:** It decouples the event generator from the processing layer and
gives durable, replayable, ordered-per-key delivery.
**Detail/Project:** The generator can publish faster or slower than Spark
processes without either side blocking; if Spark restarts, it resumes from
its checkpoint instead of losing data sitting in Kafka. Partitioning by
`user_id` gives per-user ordering, which matters for session-based logic.

### 27. Why Spark instead of a plain Python Kafka consumer?
**Short:** Because the pipeline needs stateful, watermarked, windowed
aggregation with checkpointed fault tolerance - Spark provides all of that
out of the box; a hand-rolled consumer would have to reimplement it.
**Detail/Project:** `spark/transformations.py`'s windowed aggregations and
`spark/data_quality.py`'s watermarked dedup would require manually managing
in-memory state, eviction, and recovery in plain Python - substantial,
error-prone work Structured Streaming already solves.

### 28. Why StarRocks instead of PostgreSQL for the analytical layer?
**Short:** The workload is aggregation-heavy over a high-volume fact table -
StarRocks' columnar storage and table models (see Q16) are built for
exactly that; PostgreSQL is kept, but scoped to small operational metadata
instead.
**Detail/Project:** see docs/data-model.md and docs/architecture.md's
"Design decisions."

### 29. Why put Trino in front of StarRocks instead of querying StarRocks directly?
**Short:** To demonstrate the federated-query pattern and decouple the API's
query interface from a specific backend.
**Detail/Project:** Documented honestly in docs/architecture.md: StarRocks
has no official Trino connector, so this uses Trino's `mysql` connector
against StarRocks' MySQL-wire-protocol port - a real, working, but pragmatic
choice, not a purpose-built integration.

### 30. How does your pipeline handle duplicate events end to end?
**Short:** Two layers - Spark drops repeats by `event_id` within a watermark
window, and StarRocks Primary Key tables upsert repeated aggregate writes.
**Detail/Project:** The generator deliberately re-publishes ~3% of events
(`DUPLICATE_EVENT_RATE`) to exercise this; `spark/data_quality.py`'s
`deduplicate_events` is unit-tested directly in
`tests/test_data_quality.py`.

### 31. What happens when a malformed event arrives?
**Short:** It's flagged by `spark/data_quality.py` with a specific reason
code and routed to the `ecommerce-dead-letter` Kafka topic - never silently
dropped.
**Detail/Project:** The generator deliberately emits ~2% malformed events
(`MALFORMED_EVENT_RATE`) covering missing fields, bad UUIDs, invalid enum
values, and negative numbers, specifically so this path is exercised
end-to-end rather than only in unit tests.

### 32. How would you scale this system to handle much higher throughput?
**Short:** More Kafka partitions, a real multi-executor Spark cluster
instead of `local[*]`, and StarRocks Stream Load/the native Spark connector
instead of row-by-row JDBC writes.
**Detail/Project:** Full breakdown in docs/architecture.md's "Scaling
considerations" section, comparing today's 20 events/sec setup against a
hypothetical 10,000 events/sec target.

### 33. How would you guarantee data correctness in this pipeline?
**Short:** I wouldn't claim a stronger guarantee than the architecture
provides - I'd describe it precisely as "idempotent within the watermark
window," achieved via event-level dedup plus Primary Key upserts.
**Detail/Project:** This is a deliberate framing choice in
docs/data-model.md: it explains what correctness guarantee actually holds
and what would be additionally required (transactional, offset-aware sink
writes) for a stronger exactly-once claim.

### 34. What happens when Spark crashes?
**Short:** Each streaming query recovers independently from its own
checkpoint directory and resumes from its last committed offset.
**Detail/Project:** `docker-compose.yml` sets `restart: unless-stopped` on
`spark-streaming`; on restart, `spark/streaming_job.py` re-attaches to the
checkpoints under `/opt/spark-checkpoints/<query-name>` (a named Docker
volume, so state survives container recreation, not just process restart).

### 35. What happens when the API can't reach Trino?
**Short:** Every repository call is wrapped so the failure becomes an HTTP
503 with a clear message, instead of a 500 or a hang; `/health` also reports
`trino_reachable: false`.
**Detail/Project:** `api/services/analytics_service.py::_safe` and
`api/routes/health.py`; the dashboard shows a warning banner when it detects
this via `/health` (`dashboard/app.py`).

### 36. How would you process 1 billion events/day with this architecture?
**Short:** Same architectural shape, but every "local dev" choice would need
to become a production one: a real multi-broker Kafka cluster, a real Spark
cluster, StarRocks' bulk-load path instead of JDBC, and horizontal API/
dashboard scaling behind a load balancer with query caching.
**Detail/Project:** 1B events/day is ~11,600 events/sec sustained - well
past what a single-container local stack is built for, but the same
validate -> dedupe -> aggregate shape holds; see docs/architecture.md's
scaling section for the concrete list of what changes.

### 37. Why does the `events` fact table use Duplicate Key instead of Primary Key?
**Short:** Because events are immutable once written - there's nothing to
upsert, and Duplicate Key avoids the merge-on-write overhead a Primary Key
table pays on every insert.
**Detail/Project:** See docs/data-model.md's table-model rationale table.

### 38. Why is `purchases` a view instead of its own table?
**Short:** It's just `events` filtered to `event_type = 'purchase'` - a
separate table would double storage and the write path for no benefit,
since StarRocks can filter the partition-pruned `events` table cheaply.
**Detail/Project:** `starrocks/init/02_create_tables.sql`.

### 39. Why does the data generator key Kafka messages by `user_id`?
**Short:** So all of one user's events land on the same partition and stay
ordered relative to each other.
**Detail/Project:** `data_generator/producer.py::_publish`; this matters
because session-based logic (which event followed which, for a given user)
depends on seeing that user's events in order.

### 40. Why does PostgreSQL exist in this stack at all, if StarRocks is the warehouse?
**Short:** It holds operational metadata (pipeline run history, dashboard
config) - small, transactional state that doesn't belong in an OLAP
warehouse, not a duplicate of the analytical data.
**Detail/Project:** `postgres/init/01_schema.sql`;
`data_generator/metadata.py` writes a `pipeline_runs` row at generator
start/stop, deliberately best-effort so a Postgres outage never blocks event
publishing.

### 41. Why choose a 1-minute window with a 2-minute watermark, specifically?
**Short:** Short enough windows to feel responsive on a 15-second-refresh
dashboard; a watermark long enough to absorb realistic local-network delay
without letting state grow unbounded.
**Detail/Project:** Full reasoning in docs/architecture.md's "Windowing
choices" section, including the direct trade-off between freshness and how
much late data gets included.

### 42. Walk me through what happens from a user clicking "buy" to it showing on the dashboard.
**Short:** Generator publishes a `purchase` event to Kafka -> Spark validates/
dedupes/enriches it in its next micro-batch -> it's written to the `events`
fact table and rolled into the next `revenue_metrics` window once that
window's watermark passes -> Trino queries that table -> the API serves it
-> the dashboard picks it up on its next 15-second poll.
**Detail/Project:** See docs/architecture.md's "Data flow: the life of one
event" for the full numbered walkthrough.

### 43. How is SQL kept out of your route handlers?
**Short:** A repository/service split - `TrinoRepository` owns every SQL
string, `AnalyticsService` turns rows into typed response models, and route
handlers only call service methods.
**Detail/Project:** `api/repositories/trino_repository.py`,
`api/services/analytics_service.py`, `api/routes/metrics.py`.

### 44. How did you unit test the API without a running Trino/StarRocks?
**Short:** FastAPI dependency overrides - swap the real `TrinoRepository`
for a `MagicMock` with canned return values, so tests exercise real routing/
serialization logic without any network call.
**Detail/Project:** `tests/test_api.py`; also demonstrates the 503 path by
making the mock raise.

### 45. What's one bug you actually hit building this, and how did you find it?
**Short:** `uuid.uuid4()` ignores `random.seed()` because it reads from
`os.urandom`, not Python's `random` module - so the generator's
"deterministic with a fixed seed" requirement silently failed.
**Detail/Project:** Caught by `tests/test_generator.py::
test_deterministic_with_fixed_seed` actually failing on a real run (not a
hypothetical) - fixed by generating IDs via
`uuid.UUID(int=random.getrandbits(128))`, which *is* driven by the seeded
`random` module. See `data_generator/generator.py::_seeded_uuid`.

### 46. What's a StarRocks-specific bug you hit, and how did you find it?
**Short:** StarRocks requires DUPLICATE KEY columns to be a strict
column-order prefix of the table - my first `events` DDL listed
`event_id`/`event_timestamp` before the key columns and StarRocks rejected
it at `CREATE TABLE` time.
**Detail/Project:** Found by actually running `docker compose run
starrocks-init` against a live StarRocks container, not by inspection -
fixed by reordering `starrocks/init/02_create_tables.sql` so
`(event_date, event_type, user_id)` are the first three declared columns.

### 46b. What's a Trino-specific integration bug you hit?
**Short:** Trino's `mysql` connector could only see StarRocks' `purchases`
VIEW, not any of its actual tables, because StarRocks reports its wire
protocol as MySQL version "5.1.0" and the JDBC driver's default
`INFORMATION_SCHEMA`-based table discovery mishandles that.
**Detail/Project:** Found by connecting through the Trino Python client and
running `SHOW TABLES` against a live stack, not by reading documentation -
fixed with `mysql.jdbc.use-information-schema=false` in
`trino/catalog/starrocks.properties`, forcing the driver to use legacy
`SHOW`-based metadata calls instead. A good example of why "should work
based on the docs" and "verified against a running system" are different
claims.

### 47. Why does the dashboard call the API instead of querying Trino directly?
**Short:** Keeps one query/typing layer (the API) instead of duplicating SQL
and connection logic in two frontends.
**Detail/Project:** `dashboard/api_client.py` is a thin `requests` wrapper
around the same endpoints `curl`/Swagger UI would hit - the dashboard has no
SQL or Trino client code at all.

### 48. How does the funnel conversion-rate calculation actually work?
**Short:** Count events by type across the whole dataset (or a window),
then divide consecutive stage counts: cart-adds/views, purchases/cart-adds.
**Detail/Project:** `api/repositories/trino_repository.py::funnel_counts`
plus `AnalyticsService.get_funnel`; `sql/funnel_analysis.sql` shows the
equivalent ad-hoc SQL, including a per-session variant.

### 49. What would you add first if you kept working on this project?
**Short:** A schema registry for the Kafka payload (Avro/Protobuf with
Confluent Schema Registry) to make the event contract enforceable at the
Kafka layer, not just downstream in Spark.
**Detail/Project:** See docs/architecture.md-adjacent "Future Improvements"
in the README - deliberately not implemented, to avoid overclaiming scope
this project doesn't actually cover.

### 50. Why does the Spark job write to StarRocks via JDBC instead of Stream Load?
**Short:** Reliability and simplicity for a local-dev project - a plain JDBC
`foreachBatch` write is far fewer moving parts to get right than configuring
StarRocks' native bulk-load path or the official Spark connector.
**Detail/Project:** Explicitly documented as a trade-off in
docs/architecture.md's scaling section: JDBC is the right choice at this
project's throughput, and the same section names what to switch to at much
higher volume.

### 51. How do you know the pipeline is actually working, beyond "the dashboard shows numbers"?
**Short:** Structured logging at every stage (generator stats, Spark batch
row counts, API request logs) plus `scripts/health_check.sh` checking every
service's health endpoint independently.
**Detail/Project:** `data_generator/producer.py` logs periodic stats;
`spark/streaming_job.py::write_starrocks_batch`/`write_dlq_batch` log row
counts per micro-batch; `api/main.py`'s logging middleware logs every
request/status/latency.

### 52. What's the difference between the `events` fact table and the `*_metrics` tables, conceptually?
**Short:** `events` is grain-level (one row per thing that happened);
`*_metrics` tables are pre-aggregated summaries at a coarser grain (per
window, sometimes per window+dimension).
**Detail/Project:** Both are queryable through Trino; `sql/revenue.sql`
shows both a from-scratch aggregate query over `events` and a cheap read of
the pre-aggregated `revenue_metrics` table for comparison.

### 53. How would you add a new metric to this system, end to end?
**Short:** Add a transformation/aggregation function in
`spark/transformations.py`, a StarRocks table for it, a streaming query in
`streaming_job.py` writing to that table, a repository method + response
model + route in the API, and a dashboard component - the same layered
pattern every existing metric follows.
**Detail/Project:** Point at `revenue_by_window` -> `revenue_metrics` ->
`TrinoRepository.revenue_summary` -> `AnalyticsService.get_revenue` ->
`GET /metrics/revenue` -> `dashboard/components/revenue.py` as the concrete
template to copy.

### 54. What's the honest limitation of this project as a "real-time" system?
**Short:** End-to-end freshness is on the order of minutes (window duration
+ watermark delay + dashboard poll interval), not milliseconds - and it runs
single-node, so none of the failure/scale claims apply beyond what a laptop
demonstrates.
**Detail/Project:** Called out explicitly in docs/architecture.md's "What
this project does not claim" section, and in the README - deliberately
avoiding buzzword inflation like "processed billions of events" or
"production-grade."

### 55. Why occasionally generate malformed/duplicate events instead of only clean data?
**Short:** Because a pipeline's data-quality and dedup logic is only proven
by actually exercising it - clean-only test data would never touch those
code paths in an end-to-end run.
**Detail/Project:** `data_generator/generator.py::next_event` rolls against
`MALFORMED_EVENT_RATE`/`DUPLICATE_EVENT_RATE` on every event so the dead-
letter topic and StarRocks upserts have real traffic to handle whenever the
full stack is running, not just in unit tests.

### 56. If you had to justify Kafka + Spark instead of "just use a cron job and pandas," what would you say?
**Short:** The requirement is *continuous* processing of an *unbounded*
stream with sub-minute freshness and fault-tolerant recovery - a cron+pandas
batch job could approximate the same end state, but would either run
constantly (reinventing streaming badly) or accept much coarser freshness.
**Detail/Project:** This is a legitimate trade-off, not a strawman - for a
much smaller data volume or a genuinely batch-shaped business need
(e.g. a nightly report), the simpler cron+pandas approach would be the
*better* engineering choice; this project's architecture is justified
specifically by the "continuous, near-real-time, growing volume" framing in
its own business scenario, not by default.
