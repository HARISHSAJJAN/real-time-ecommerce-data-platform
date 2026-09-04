-- ==============================================================================
-- PostgreSQL metadata schema.
--
-- PostgreSQL is intentionally NOT used to store analytical event data -
-- StarRocks already fills that role. It holds operational metadata about the
-- pipeline itself: generator/job run history and dashboard configuration -
-- the kind of small, transactional, frequently-updated state a relational
-- database is a better fit for than an OLAP warehouse.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           UUID PRIMARY KEY,
    service_name     VARCHAR(64) NOT NULL,
    status           VARCHAR(16) NOT NULL DEFAULT 'running',
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at         TIMESTAMPTZ,
    events_valid     BIGINT DEFAULT 0,
    events_malformed BIGINT DEFAULT 0,
    events_duplicate BIGINT DEFAULT 0,
    notes            TEXT
);

CREATE TABLE IF NOT EXISTS dashboard_config (
    key         VARCHAR(64) PRIMARY KEY,
    value       VARCHAR(256) NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO dashboard_config (key, value)
VALUES ('refresh_seconds', '15')
ON CONFLICT (key) DO NOTHING;
