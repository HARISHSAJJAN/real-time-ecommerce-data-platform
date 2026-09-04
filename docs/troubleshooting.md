# Troubleshooting

## `docker compose up` fails to pull images / "unexpected EOF"

Several images used here (StarRocks ~2.5GB, Spark base image, Trino) are
large. On a slow or unstable connection, `docker compose up` can time out
mid-pull. Docker's layer cache is resumable - simply re-run the same command;
completed layers are not re-downloaded:

```bash
docker compose up -d kafka postgres starrocks
```

## StarRocks container is "unhealthy" or `starrocks-init` fails to connect

StarRocks' all-in-one image needs 60-120 seconds to initialize both its
Frontend (FE) and Backend (BE) processes on first boot. Watch its logs and
wait for the health check before running `starrocks-init`:

```bash
docker compose logs -f starrocks
# or
docker inspect --format='{{json .State.Health}}' ecommerce-starrocks
```

If it never turns healthy, check that ports 8030/9030/8040 are not already
bound by another process on the host (`netstat -ano | findstr 9030` on
Windows), and that Docker Desktop has enough memory allocated (StarRocks
recommends at least 4GB free for local dev).

## `spark-streaming` container keeps restarting

Check its logs first:

```bash
docker compose logs -f spark-streaming
```

Common causes:

- **Kafka not reachable yet.** The `kafka-init` dependency should prevent
  this, but if you started `spark-streaming` manually before Kafka finished
  forming its cluster, restart it: `docker compose restart spark-streaming`.
- **StarRocks table doesn't exist yet.** `spark-streaming` depends on
  `starrocks-init` completing successfully; if you skipped it, run
  `docker compose run --rm starrocks-init` (or `make init-db`) and restart
  the job.
- **JDBC connection refused.** Confirm `STARROCKS_HOST`/`STARROCKS_PORT` in
  `.env` match the service name (`starrocks`) and port (`9030`) used inside
  the Docker network - not the host-mapped port.

## No data shows up in the dashboard

The pipeline has several stages that each take a moment to warm up:

1. `data-generator` must be running and successfully publishing (check
   `docker compose logs data-generator` for periodic "Stats so far" lines).
2. `spark-streaming` processes on a `SPARK_TRIGGER_INTERVAL` (default 10s)
   and only emits a window's aggregate once its watermark passes
   (default 2 minutes past the window's end) - so **expect roughly a 2-3
   minute delay** before the aggregate tables show data, even though the
   raw `events` fact table fills in near real time.
3. Query `events` directly through Trino to confirm data is flowing before
   assuming the dashboard/API is broken:
   ```bash
   trino --server localhost:8080 --catalog starrocks --schema ecommerce \
     --execute "SELECT COUNT(*) FROM events"
   ```

## `trino` catalog queries fail with a schema/table-not-found error

Trino's `mysql` connector maps StarRocks databases to Trino schemas and
StarRocks tables to Trino tables via `information_schema`. If you created
tables after Trino already started, restart Trino so it refreshes its
metadata cache: `docker compose restart trino`.

## API returns `503 Analytical data source is currently unreachable`

This means the API's Trino connection failed. Check:

```bash
curl http://localhost:8000/health
docker compose logs trino
```

Most often this is Trino still starting up (its own health check has a 30s
`start_period`) or StarRocks being unreachable from Trino.

## Running tests locally without the full Docker stack

- `tests/test_generator.py` and `tests/test_api.py` need no external
  services (the API tests mock the Trino repository).
- `tests/test_data_quality.py` and `tests/test_transformations.py` need a
  local PySpark install and a **Java 11 or 17** JDK on `PATH`/`JAVA_HOME`.
  PySpark 3.5.x does not support very new JDKs (24+) - if `pytest` fails
  with `UnsupportedOperationException: getSubject is not supported` or
  similar `java.lang.reflect` errors, install Temurin 17 and point
  `JAVA_HOME` at it before running pytest; this does not affect the
  Dockerized Spark job, which ships its own compatible JDK inside the
  `bitnami/spark:3.5.1` base image.

## Port already in use

If `8000`, `8030`, `8080`, `8501`, `9030`, `29092`, or `5432` are already
bound on your host, either stop the conflicting process or remap the port
in `docker-compose.yml` (left side of the `"host:container"` mapping).

## Windows-specific: paths with spaces

If you cloned this repository into a path containing spaces (as this
project's own working directory does), Docker Compose handles it fine, but
if you write your own scripts that reference the repo path, remember to
quote it.
