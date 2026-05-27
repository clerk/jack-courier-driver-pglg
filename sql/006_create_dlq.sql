CREATE TABLE IF NOT EXISTS {schema}.{prefix}_dlq (
    dlq_id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id               BIGINT      NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    producer_id      TEXT        NOT NULL,
    job_type         TEXT        NOT NULL,
    payload          BYTEA       NOT NULL,
    run_at           TIMESTAMPTZ,
    trace_id         TEXT        NOT NULL DEFAULT '',
    shadow           BOOLEAN     NOT NULL DEFAULT false,
    reason           TEXT        NOT NULL,
    error_message    TEXT        NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS {prefix}_dlq_dead_lettered_at_idx
    ON {schema}.{prefix}_dlq (dead_lettered_at);

CREATE INDEX IF NOT EXISTS {prefix}_dlq_job_type_idx
    ON {schema}.{prefix}_dlq (job_type, dead_lettered_at);
