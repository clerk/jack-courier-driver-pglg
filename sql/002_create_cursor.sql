CREATE TABLE IF NOT EXISTS {schema}.{prefix}_cursor (
    slot_name  TEXT PRIMARY KEY,
    last_lsn   PG_LSN      NOT NULL DEFAULT '0/0',
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
