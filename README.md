# jack-courier-driver-pglg

A [jack-courier-lib](https://github.com/clerk/jack-courier-lib) driver that uses PostgreSQL logical replication (WAL streaming) to source jobs from an outbox table and deliver them to jack-service.

## How It Works

```
┌─────────────────┐    INSERT (same tx)     ┌──────────────────┐
│  Domain Service  │ ────────────────────►  │  Outbox Table     │
│  (business logic)│                         │  (partitioned)    │
└─────────────────┘                         └────────┬─────────┘
                                                     │ WAL stream
                                                     ▼
                                            ┌──────────────────┐
                                            │  pglg Driver      │
                                            │  (this library)   │
                                            └────────┬─────────┘
                                                     │ submit([]Job)
                                                     ▼
                                            ┌──────────────────┐
                                            │  jack-service     │
                                            │  (gRPC)           │
                                            └──────────────────┘
```

1. The domain service INSERTs a row into the outbox table **within the same transaction** as its business logic (transactional outbox pattern).
2. The pglg driver streams WAL changes via PostgreSQL logical replication, parses INSERT events, batches them into `courier.Job` objects, and calls `submit()` to deliver them to jack-service.
3. On success, the driver advances its cursor (LSN). On gRPC failure, it reconnects and re-streams from the last cursor position (at-least-once delivery).

## Setup

### 1. Prerequisites

The PostgreSQL instance must have logical decoding enabled:

```
wal_level = logical
max_replication_slots >= 1   (one per service using this driver)
max_wal_senders >= 1
```

On Cloud SQL, set the `cloudsql.logical_decoding` flag to `on`.

### 2. Create outbox tables, publication, and replication slot

Run the setup tool once per service (not at runtime):

```bash
go run github.com/clerk/jack-courier-driver-pglg/cmd/pglg-setup create \
    --conn-string="postgres://user:pass@host:5432/db" \
    --schema=public \
    --prefix=billing_outbox \
    --publication=billing_outbox_pub \
    --slot=billing_outbox_slot
```

This creates:
- `{schema}.{prefix}_jobs` — partitioned outbox table
- `{schema}.{prefix}_cursor` — LSN progress tracking
- `{schema}.{prefix}_partition_meta` — partition registry
- A PostgreSQL publication (INSERT-only) on the outbox table
- A logical replication slot using the `pgoutput` plugin
- An initial partition covering the current hour

To tear down (dev/test only):

```bash
go run github.com/clerk/jack-courier-driver-pglg/cmd/pglg-setup destroy \
    --conn-string="..." --prefix=billing_outbox \
    --publication=billing_outbox_pub --slot=billing_outbox_slot
```

### 3. Service: write to the outbox table

In your service's transaction, INSERT into the outbox table alongside your business logic:

```sql
INSERT INTO billing_outbox_jobs (producer_id, job_type, payload, run_at, trace_id)
VALUES ($1, $2, $3, $4, $5);
```

Example with clerk_go's `PerformTx`:

```go
txErr := db.PerformTx(ctx, func(tx database.Tx) (bool, error) {
    // Business logic
    if err := userRepo.Insert(ctx, tx, user); err != nil {
        return true, err
    }

    // Outbox write — same transaction guarantees atomicity
    _, err := tx.ExecContext(ctx,
        `INSERT INTO billing_outbox_jobs (producer_id, job_type, payload, trace_id)
         VALUES ($1, $2, $3, $4)`,
        "billing-service", "charge_customer", payloadJSON, traceID,
    )
    return err != nil, err
})
```

### 4. Service: register and run the driver

```go
package main

import (
    "log"
    "os"

    courier "github.com/clerk/jack-courier-lib"
    pglg "github.com/clerk/jack-courier-driver-pglg"
)

func main() {
    driver, err := pglg.New(pglg.Config{
        ConnString:      os.Getenv("PGLG_CONN_STRING"),
        SlotName:        "billing_outbox_slot",
        PublicationName: "billing_outbox_pub",
        TablePrefix:     "billing_outbox",
    })
    if err != nil {
        log.Fatal(err)
    }

    courier.RegisterDriver(driver)
    courier.Main()
}
```

Or use `ConfigFromEnv()` to read all config from `PGLG_*` environment variables.

## Outbox Table Schema

```sql
CREATE TABLE {schema}.{prefix}_jobs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    producer_id TEXT        NOT NULL,
    job_type    TEXT        NOT NULL,
    payload     BYTEA       NOT NULL DEFAULT '',
    run_at      TIMESTAMPTZ,
    trace_id    TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);
```

| Column | courier.Job Field | Notes |
|--------|-------------------|-------|
| `id` | `CorrelationID` | Auto-generated, stringified by driver |
| `producer_id` | `ProducerID` | Must match registered producer in jack-service |
| `job_type` | `JobType` | Must match registered job type in jack-service |
| `payload` | `Payload` | Opaque bytes, typically JSON |
| `run_at` | `RunAt` | NULL = immediate execution |
| `trace_id` | `TraceID` | Distributed tracing correlation ID |

## Configuration

### Config struct

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `ConnString` | Yes | — | PostgreSQL connection string |
| `SlotName` | Yes | — | Logical replication slot name |
| `PublicationName` | Yes | — | Publication name |
| `Schema` | No | `public` | Database schema |
| `TablePrefix` | No | `outbox` | Table name prefix |
| `MaxBatchSize` | No | `100` | Max jobs per submit() call |
| `BatchTimeout` | No | `1s` | Max wait before flushing partial batch |
| `StandbyInterval` | No | `10s` | Keepalive interval to Postgres |
| `PartitionInterval` | No | `1h` | Duration of each partition |
| `PartitionLookAhead` | No | `12h` | How far ahead to pre-create partitions |
| `PartitionRetention` | No | `3h` | How long to keep old partitions |
| `PartitionMaintInterval` | No | `5m` | Partition maintenance loop interval |
| `ReconnectInitialDelay` | No | `1s` | Initial reconnection delay |
| `ReconnectMaxDelay` | No | `30s` | Max reconnection delay |
| `Logger` | No | `slog.Default()` | Structured logger |

### Environment variables (via ConfigFromEnv)

| Variable | Maps to |
|----------|---------|
| `PGLG_CONN_STRING` | `ConnString` |
| `PGLG_SLOT_NAME` | `SlotName` |
| `PGLG_PUBLICATION_NAME` | `PublicationName` |
| `PGLG_SCHEMA` | `Schema` |
| `PGLG_TABLE_PREFIX` | `TablePrefix` |
| `PGLG_MAX_BATCH_SIZE` | `MaxBatchSize` |
| `PGLG_BATCH_TIMEOUT` | `BatchTimeout` |
| `PGLG_STANDBY_INTERVAL` | `StandbyInterval` |

## Delivery Guarantees

- **At-least-once delivery.** If the gRPC call to jack-service fails, the cursor is not advanced. The driver reconnects and re-streams from the last saved position.
- **Per-job rejections** (unknown job type, invalid payload) are logged as warnings and the cursor advances past them. These are permanent failures that retrying won't fix.

## Partition Management

The driver automatically manages partitions in a background goroutine:
- Creates partitions up to 12 hours ahead (configurable)
- Drops partitions older than 3 hours (configurable)
- Runs every 5 minutes (configurable)
- Errors are logged but don't crash the driver
