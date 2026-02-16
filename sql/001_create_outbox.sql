CREATE TABLE IF NOT EXISTS {schema}.{prefix}_jobs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    producer_id TEXT        NOT NULL,
    job_type    TEXT        NOT NULL,
    payload     BYTEA       NOT NULL DEFAULT '',
    run_at      TIMESTAMPTZ,
    trace_id    TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);
