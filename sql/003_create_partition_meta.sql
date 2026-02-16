CREATE TABLE IF NOT EXISTS {schema}.{prefix}_partition_meta (
    partition_name TEXT        PRIMARY KEY,
    lower_bound    TIMESTAMPTZ NOT NULL,
    upper_bound    TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
