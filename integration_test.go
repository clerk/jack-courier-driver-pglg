//go:build integration

package pglg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	courier "github.com/clerk/jack-courier-lib"
)

type integrationEnv struct {
	connString  string
	schema      string
	prefix      string
	publication string
	slot        string
	pool        *pgxpool.Pool
}

func newIntegrationEnv(t *testing.T) *integrationEnv {
	t.Helper()

	cs := os.Getenv("PGLG_INTEGRATION_CONN_STRING")
	if cs == "" {
		t.Skip("PGLG_INTEGRATION_CONN_STRING not set; run via `make integration`")
	}

	env := &integrationEnv{
		connString:  cs,
		schema:      envOrDefault("PGLG_INTEGRATION_SCHEMA", "public"),
		prefix:      envOrDefault("PGLG_INTEGRATION_PREFIX", "outbox"),
		publication: envOrDefault("PGLG_INTEGRATION_PUBLICATION", "outbox_pub"),
		slot:        envOrDefault("PGLG_INTEGRATION_SLOT", "outbox_slot"),
	}

	pool, err := pgxpool.New(t.Context(), env.connString)
	require.NoError(t, err)
	env.pool = pool
	t.Cleanup(pool.Close)

	env.reset(t)
	return env
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// reset gives each test a fresh starting state: the replication slot and
// publication are dropped and recreated, and the data tables are truncated.
// The slot's confirmed_flush_lsn is reset to the current WAL position on
// recreate, so prior tests cannot influence which messages this test sees.
func (e *integrationEnv) reset(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	// Wait for any previous driver to release the slot before dropping it.
	require.Eventually(t, func() bool {
		var active bool
		err := e.pool.QueryRow(ctx,
			`SELECT active FROM pg_replication_slots WHERE slot_name = $1`,
			e.slot,
		).Scan(&active)
		if errors.Is(err, pgx.ErrNoRows) {
			return true
		}
		return err == nil && !active
	}, 5*time.Second, 20*time.Millisecond, "slot %q still active", e.slot)

	var slotExists bool
	err := e.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		e.slot,
	).Scan(&slotExists)
	require.NoError(t, err)
	if slotExists {
		_, err = e.pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", e.slot)
		require.NoError(t, err)
	}

	_, err = e.pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", e.publication))
	require.NoError(t, err)

	for _, table := range []string{
		e.prefix + "_jobs",
		e.prefix + "_cursor",
		e.prefix + "_dlq",
	} {
		_, err := e.pool.Exec(ctx, fmt.Sprintf("TRUNCATE %s.%s", e.schema, table))
		require.NoErrorf(t, err, "truncate %s", table)
	}

	_, err = e.pool.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s.%s_jobs WITH (publish = 'insert')",
		e.publication, e.schema, e.prefix,
	))
	require.NoError(t, err)

	_, err = e.pool.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", e.slot)
	require.NoError(t, err)
}

func (e *integrationEnv) newDriver(t *testing.T) *Driver {
	t.Helper()
	cfg := Config{
		ConnString:      e.connString,
		SlotName:        e.slot,
		PublicationName: e.publication,
		Schema:          e.schema,
		TablePrefix:     e.prefix,
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Statsd:          &statsd.NoOpClient{},
	}
	d, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { d.pool.Close() })
	return d
}

func (e *integrationEnv) insertJob(t *testing.T, producer, jobType, traceID string, payload []byte, shadow bool) {
	t.Helper()
	ctx := t.Context()
	_, err := e.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.%s_jobs (producer_id, job_type, payload, run_at, trace_id, shadow)
		VALUES ($1, $2, $3, NOW(), $4, $5)
	`, e.schema, e.prefix), producer, jobType, payload, traceID, shadow)
	require.NoError(t, err)
}

func awaitClean(t *testing.T, cancel context.CancelFunc, runErr <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestIntegration_SingleRow_HappyPath(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	submitted := make(chan []courier.Job, 1)
	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		select {
		case submitted <- jobs:
		default:
		}

		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}

		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	env.insertJob(t, "producer-a", "send_email", "trace-1", []byte(`{"to":"x"}`), false)

	select {
	case jobs := <-submitted:
		require.Len(t, jobs, 1)
		assert.Equal(t, "producer-a", jobs[0].ProducerID)
		assert.Equal(t, "send_email", jobs[0].JobType)
		assert.Equal(t, []byte(`{"to":"x"}`), jobs[0].Payload)
		assert.Equal(t, "trace-1", jobs[0].TraceID)
		assert.False(t, jobs[0].Shadow)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for driver to submit the row")
	}

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 2*time.Second, 20*time.Millisecond, "cursor was not advanced after flush")

	awaitClean(t, cancel, runErr)
}

func TestIntegration_TwoTransactions_TwoBatches(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	submitted := make(chan []courier.Job, 4)
	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	env.insertJob(t, "producer-a", "send_email", "trace-1", []byte(`{"i":1}`), false)

	// we wait for the first batch to be submitted before inserting the second row so they don't arrive
	// in the batch timeout and get grouped together
	var first []courier.Job
	select {
	case first = <-submitted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first batch")
	}
	require.Len(t, first, 1, "first batch should have exactly one job")
	assert.Equal(t, "trace-1", first[0].TraceID)
	assert.Equal(t, []byte(`{"i":1}`), first[0].Payload)

	env.insertJob(t, "producer-a", "send_email", "trace-2", []byte(`{"i":2}`), false)

	var second []courier.Job
	select {
	case second = <-submitted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for second batch")
	}
	require.Len(t, second, 1, "second batch should have exactly one job")
	assert.Equal(t, "trace-2", second[0].TraceID)
	assert.Equal(t, []byte(`{"i":2}`), second[0].Payload)

	select {
	case extra := <-submitted:
		t.Fatalf("unexpected third submit call with %d jobs", len(extra))
	case <-time.After(500 * time.Millisecond):
	}

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 2*time.Second, 20*time.Millisecond, "cursor was not advanced after second batch")

	awaitClean(t, cancel, runErr)
}

func TestIntegration_MultipleRowsInOneTx_SingleBatch(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	submitted := make(chan []courier.Job, 4)
	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{
				CorrelationID: j.CorrelationID,
				JobID:         fmt.Sprintf("job-%d", i),
			}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	tx, err := env.pool.Begin(ctx)
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.%s_jobs (producer_id, job_type, payload, run_at, trace_id, shadow)
			VALUES ($1, $2, $3, NOW(), $4, false)
		`, env.schema, env.prefix),
			"producer-a",
			"send_email",
			[]byte(fmt.Sprintf(`{"i":%d}`, i)),
			fmt.Sprintf("trace-%d", i),
		)
		require.NoErrorf(t, err, "insert row %d", i)
	}
	require.NoError(t, tx.Commit(ctx))

	var batch []courier.Job
	select {
	case batch = <-submitted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for driver to submit the batch")
	}

	require.Len(t, batch, 3, "expected one batch of three jobs")
	for i, j := range batch {
		expected := i + 1
		assert.Equal(t, fmt.Sprintf("trace-%d", expected), j.TraceID, "row %d trace_id", expected)
		assert.Equal(t, []byte(fmt.Sprintf(`{"i":%d}`, expected)), j.Payload, "row %d payload", expected)
		assert.Equal(t, "producer-a", j.ProducerID, "row %d producer_id", expected)
		assert.Equal(t, "send_email", j.JobType, "row %d job_type", expected)
	}

	// no additional because all three are in a single commit
	select {
	case extra := <-submitted:
		t.Fatalf("unexpected second submit call with %d jobs", len(extra))
	case <-time.After(500 * time.Millisecond):
	}

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 2*time.Second, 20*time.Millisecond, "cursor was not advanced after batch flush")

	awaitClean(t, cancel, runErr)
}

func TestIntegration_LargeBatch_OverMaxBatchSizeFlushesInOneSubmit(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	submitted := make(chan []courier.Job, 4)
	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	// MaxBatchSize default is 100. Inserting 105 rows in one transaction
	// crosses the threshold, so the inline flush at walCommit fires (no
	// wait for BatchTimeout). The driver does not chunk: all 105 rows go
	// in a single submit call.
	const totalRows = 105

	tx, err := env.pool.Begin(ctx)
	require.NoError(t, err)
	for i := 0; i < totalRows; i++ {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.%s_jobs (producer_id, job_type, payload, run_at, trace_id, shadow)
			VALUES ($1, $2, $3, NOW(), $4, false)
		`, env.schema, env.prefix),
			"producer-a", "send_email",
			[]byte(fmt.Sprintf(`{"i":%d}`, i)),
			fmt.Sprintf("trace-%d", i),
		)
		require.NoErrorf(t, err, "insert row %d", i)
	}
	require.NoError(t, tx.Commit(ctx))

	select {
	case batch := <-submitted:
		require.Len(t, batch, totalRows,
			"all %d rows should arrive in a single submit call", totalRows)
		assert.Equal(t, "trace-0", batch[0].TraceID, "first row")
		assert.Equal(t, fmt.Sprintf("trace-%d", totalRows-1), batch[totalRows-1].TraceID, "last row")
		seen := make(map[string]struct{}, totalRows)
		for _, j := range batch {
			seen[j.TraceID] = struct{}{}
		}
		assert.Len(t, seen, totalRows, "all trace IDs must be unique")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the large batch")
	}

	select {
	case extra := <-submitted:
		t.Fatalf("unexpected second submit call with %d jobs", len(extra))
	case <-time.After(500 * time.Millisecond):
	}

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 2*time.Second, 20*time.Millisecond, "cursor was not advanced after large batch flush")

	awaitClean(t, cancel, runErr)
}

func TestIntegration_DriverRestartAfterCleanFlush_DoesNotRedeliver(t *testing.T) {
	env := newIntegrationEnv(t)

	// Driver 1: sends row A cleanly and persists the cursor.
	d1 := env.newDriver(t)

	submitted1 := make(chan []courier.Job, 4)
	submit1 := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted1 <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	ctx1, cancel1 := context.WithCancel(t.Context())
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- d1.Run(ctx1, submit1) }()

	env.insertJob(t, "producer-a", "send_email", "trace-A", []byte(`{"id":"A"}`), false)

	select {
	case batch := <-submitted1:
		require.Len(t, batch, 1)
		assert.Equal(t, "trace-A", batch[0].TraceID)
	case <-time.After(10 * time.Second):
		t.Fatal("driver 1 never received row A")
	}

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(t.Context(), fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 2*time.Second, 20*time.Millisecond, "cursor not persisted after row A flush")

	awaitClean(t, cancel1, runErr1)

	require.Eventually(t, func() bool {
		var active bool
		err := env.pool.QueryRow(t.Context(),
			`SELECT active FROM pg_replication_slots WHERE slot_name = $1`,
			env.slot,
		).Scan(&active)
		return err == nil && !active
	}, 5*time.Second, 20*time.Millisecond, "slot still active after driver 1 exit")

	// Driver 2: must not receive again row A but only the newly row B from below
	d2 := env.newDriver(t)

	submitted2 := make(chan []courier.Job, 4)
	submit2 := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted2 <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- d2.Run(ctx2, submit2) }()

	env.insertJob(t, "producer-a", "send_email", "trace-B", []byte(`{"id":"B"}`), false)

	select {
	case batch := <-submitted2:
		require.Len(t, batch, 1, "driver 2 should ship exactly one row")
		assert.Equal(t, "trace-B", batch[0].TraceID,
			"driver 2 must deliver only the new row, not re-deliver trace-A")
	case <-time.After(15 * time.Second):
		t.Fatal("driver 2 never received row B")
	}

	select {
	case extra := <-submitted2:
		ids := make([]string, len(extra))
		for i, j := range extra {
			ids[i] = j.TraceID
		}
		t.Fatalf("unexpected extra submit by driver 2: %v", ids)
	case <-time.After(500 * time.Millisecond):
	}

	awaitClean(t, cancel2, runErr2)
}

func TestIntegration_RowSurvivesDriverRestart(t *testing.T) {
	env := newIntegrationEnv(t)

	// this should get a row and we kill it mid flush before cursor advance
	d1 := env.newDriver(t)

	submitStarted1 := make(chan []courier.Job, 1)
	submit1 := func(ctx context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitStarted1 <- jobs
		// wath the context to cancel so flushBatch gets an error
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx1, cancel1 := context.WithCancel(t.Context())
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- d1.Run(ctx1, submit1) }()

	env.insertJob(t, "producer-a", "send_email", "trace-survives", []byte(`{"i":1}`), false)

	var receivedBy1 []courier.Job
	select {
	case receivedBy1 = <-submitStarted1:
	case <-time.After(10 * time.Second):
		t.Fatal("driver 1 never received the row")
	}
	require.Len(t, receivedBy1, 1)
	assert.Equal(t, "trace-survives", receivedBy1[0].TraceID)

	// kill driver1 -> submit1 unblocks with ctx.Err() -> flushBatch returns error -> cursor never written
	cancel1()
	select {
	case <-runErr1:
	case <-time.After(5 * time.Second):
		t.Fatal("driver 1 did not exit after cancel")
	}

	var cursorCount int
	require.NoError(t, env.pool.QueryRow(t.Context(), fmt.Sprintf(
		"SELECT count(*) FROM %s.%s_cursor",
		env.schema, env.prefix,
	)).Scan(&cursorCount))
	assert.Equal(t, 0, cursorCount, "cursor should not have been persisted by the crashed driver")

	require.Eventually(t, func() bool {
		var active bool
		err := env.pool.QueryRow(t.Context(),
			`SELECT active FROM pg_replication_slots WHERE slot_name = $1`,
			env.slot,
		).Scan(&active)
		return err == nil && !active
	}, 5*time.Second, 20*time.Millisecond, "slot still active after driver 1 exit")

	// this new driver should see the same row
	d2 := env.newDriver(t)

	submitted2 := make(chan []courier.Job, 1)
	submit2 := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted2 <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- d2.Run(ctx2, submit2) }()

	select {
	case batch := <-submitted2:
		require.Len(t, batch, 1, "driver 2 must re-receive exactly one row")
		assert.Equal(t, "trace-survives", batch[0].TraceID, "driver 2 must see the row driver 1 lost")
		assert.Equal(t, []byte(`{"i":1}`), batch[0].Payload)
	case <-time.After(15 * time.Second):
		t.Fatal("driver 2 did not re-receive the row")
	}

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(ctx2, fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 5*time.Second, 50*time.Millisecond, "cursor not advanced after driver 2 flushed")

	awaitClean(t, cancel2, runErr2)
}

func TestIntegration_ShadowFlag_PreservedThroughSubmit(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	submitted := make(chan []courier.Job, 1)
	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	env.insertJob(t, "producer-a", "send_email", "trace-shadow", []byte(`{"i":1}`), true)

	select {
	case batch := <-submitted:
		require.Len(t, batch, 1)
		assert.Equal(t, "trace-shadow", batch[0].TraceID)
		assert.True(t, batch[0].Shadow, "shadow flag must round-trip as true")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for driver to submit the shadow row")
	}

	awaitClean(t, cancel, runErr)
}

func TestIntegration_PayloadRoundtrip_Binary(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	submitted := make(chan []courier.Job, 1)
	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	env.insertJob(t, "producer-a", "send_email", "trace-binary", payload, false)

	select {
	case batch := <-submitted:
		require.Len(t, batch, 1)
		assert.Equal(t, "trace-binary", batch[0].TraceID)
		require.Len(t, batch[0].Payload, len(payload), "payload length must match")
		assert.Equal(t, payload, batch[0].Payload, "binary payload must roundtrip byte-for-byte")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for driver to submit the binary row")
	}

	awaitClean(t, cancel, runErr)
}

func TestIntegration_SubmitErrorRetries(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	var submitCount atomic.Int32
	submitted := make(chan []courier.Job, 4)

	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted <- jobs
		if submitCount.Add(1) == 1 {
			return nil, errors.New("simulated transient error")
		}
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	env.insertJob(t, "producer-a", "send_email", "trace-retry", []byte(`{"x":1}`), false)

	select {
	case batch := <-submitted:
		require.Len(t, batch, 1)
		assert.Equal(t, "trace-retry", batch[0].TraceID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first submit attempt")
	}

	select {
	case batch := <-submitted:
		require.Len(t, batch, 1)
		assert.Equal(t, "trace-retry", batch[0].TraceID, "retry should resubmit the same row")
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for submit retry after reconnect")
	}

	var dlqCount int
	require.NoError(t, env.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s.%s_dlq",
		env.schema, env.prefix,
	)).Scan(&dlqCount))
	assert.Equal(t, 0, dlqCount, "submit error should not produce DLQ rows")

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 5*time.Second, 50*time.Millisecond, "cursor was not advanced after retry succeeded")

	awaitClean(t, cancel, runErr)
}

func TestIntegration_MixedAcceptAndReject_SplitsCorrectly(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	submitted := make(chan []courier.Job, 1)
	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitted <- jobs
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			if j.TraceID == "trace-reject" {
				results[i] = courier.SubmitResult{
					CorrelationID: j.CorrelationID,
					Err:           "payload exceeds max size",
					Reason:        "payload_too_large",
				}
				continue
			}
			results[i] = courier.SubmitResult{
				CorrelationID: j.CorrelationID,
				JobID:         fmt.Sprintf("job-%d", i),
			}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	// Three rows in one transaction so they arrive in a single batch.
	tx, err := env.pool.Begin(ctx)
	require.NoError(t, err)
	rows := []struct {
		traceID string
		payload string
	}{
		{"trace-ok-1", `{"i":1}`},
		{"trace-reject", `{"i":2}`},
		{"trace-ok-3", `{"i":3}`},
	}
	for _, r := range rows {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.%s_jobs (producer_id, job_type, payload, run_at, trace_id, shadow)
			VALUES ($1, $2, $3, NOW(), $4, false)
		`, env.schema, env.prefix),
			"producer-a", "send_email", []byte(r.payload), r.traceID)
		require.NoErrorf(t, err, "insert %s", r.traceID)
	}
	require.NoError(t, tx.Commit(ctx))

	select {
	case batch := <-submitted:
		require.Len(t, batch, 3, "expected one batch of three jobs")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for driver to submit the batch")
	}

	// the rejected row should go in DLQ.
	require.Eventually(t, func() bool {
		var count int
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT count(*) FROM %s.%s_dlq",
			env.schema, env.prefix,
		)).Scan(&count)
		return err == nil && count == 1
	}, 5*time.Second, 20*time.Millisecond, "expected exactly 1 DLQ row")

	var (
		dlqTraceID, dlqReason, dlqErrMsg string
		dlqPayload                       []byte
	)
	err = env.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT trace_id, payload, reason, error_message FROM %s.%s_dlq",
		env.schema, env.prefix,
	)).Scan(&dlqTraceID, &dlqPayload, &dlqReason, &dlqErrMsg)
	require.NoError(t, err)
	assert.Equal(t, "trace-reject", dlqTraceID)
	assert.Equal(t, []byte(`{"i":2}`), dlqPayload)
	assert.Equal(t, "payload_too_large", dlqReason)
	assert.Equal(t, "payload exceeds max size", dlqErrMsg)

	var leakedCount int
	err = env.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s.%s_dlq WHERE trace_id IN ('trace-ok-1', 'trace-ok-3')",
		env.schema, env.prefix,
	)).Scan(&leakedCount)
	require.NoError(t, err)
	assert.Equal(t, 0, leakedCount, "accepted rows should not appear in DLQ")

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 2*time.Second, 20*time.Millisecond, "cursor was not advanced past the mixed batch")

	awaitClean(t, cancel, runErr)
}

func TestIntegration_NoActivity_SlotAdvances(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var initialLSN string
	require.NoError(t, env.pool.QueryRow(ctx,
		`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`,
		env.slot,
	).Scan(&initialLSN))

	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		t.Errorf("submit unexpectedly called with %d jobs: idle test should ship nothing", len(jobs))
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return results, nil
	}

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	// just to force some WAL activity
	_, err := env.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS pglg_noise (id BIGSERIAL PRIMARY KEY)")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = env.pool.Exec(context.Background(), "DROP TABLE IF EXISTS pglg_noise")
	})
	_, err = env.pool.Exec(ctx, "INSERT INTO pglg_noise DEFAULT VALUES")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var advanced bool
		err := env.pool.QueryRow(ctx, `
			SELECT confirmed_flush_lsn > $1::pg_lsn
			FROM pg_replication_slots
			WHERE slot_name = $2
		`, initialLSN, env.slot).Scan(&advanced)
		return err == nil && advanced
	}, 30*time.Second, 500*time.Millisecond,
		"slot's confirmed_flush_lsn did not advance past %s during idle", initialLSN)

	awaitClean(t, cancel, runErr)
}

func TestIntegration_LeaderElection_StandbyTakesOver(t *testing.T) {
	env := newIntegrationEnv(t)
	dA := env.newDriver(t)
	dB := env.newDriver(t)

	submittedA := make(chan []courier.Job, 4)
	submittedB := make(chan []courier.Job, 4)
	okSubmit := func(ch chan<- []courier.Job) courier.SubmitFunc {
		return func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
			ch <- jobs
			results := make([]courier.SubmitResult, len(jobs))
			for i, j := range jobs {
				results[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
			}
			return results, nil
		}
	}

	ctxA, cancelA := context.WithCancel(t.Context())
	defer cancelA()
	runErrA := make(chan error, 1)
	go func() { runErrA <- dA.Run(ctxA, okSubmit(submittedA)) }()

	require.Eventually(t, func() bool {
		return dA.Role() == "leader"
	}, 5*time.Second, 50*time.Millisecond, "driver A never became leader")

	ctxB, cancelB := context.WithCancel(t.Context())
	defer cancelB()
	runErrB := make(chan error, 1)
	go func() { runErrB <- dB.Run(ctxB, okSubmit(submittedB)) }()

	// B should fail to acquire the slot (A holds it), enter the SlotBusy
	// retry path, and report itself as standby.
	require.Eventually(t, func() bool {
		return dB.Role() == "standby"
	}, 5*time.Second, 50*time.Millisecond, "driver B never became standby")
	assert.Equal(t, "leader", dA.Role(), "A should still be leader while B is standby")

	// Kill A. Its replication connection closes, PG marks the slot inactive.
	cancelA()
	select {
	case <-runErrA:
	case <-time.After(5 * time.Second):
		t.Fatal("driver A did not exit after cancel")
	}

	// B's next retry should succeed within SlotBusyRetryDelay + jitter
	// (defaults: 5s ± 1s). Give comfortable headroom for slot release lag.
	require.Eventually(t, func() bool {
		return dB.Role() == "leader"
	}, 15*time.Second, 100*time.Millisecond, "driver B never promoted to leader")

	// Verify B works as the new leader.
	env.insertJob(t, "producer-a", "send_email", "trace-promoted", []byte(`{"who":"B"}`), false)
	select {
	case batch := <-submittedB:
		require.Len(t, batch, 1)
		assert.Equal(t, "trace-promoted", batch[0].TraceID)
	case <-time.After(10 * time.Second):
		t.Fatal("driver B (new leader) never shipped the row")
	}

	// A should never have shipped anything — nothing was inserted while A
	// held the slot.
	select {
	case b := <-submittedA:
		t.Fatalf("driver A should not have shipped any rows: %v", b)
	default:
	}

	awaitClean(t, cancelB, runErrB)
}

func TestIntegration_DLQ_PreservesShadowFlag(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{
				CorrelationID: j.CorrelationID,
				Err:           "payload exceeds max size",
				Reason:        "payload_too_large",
			}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	env.insertJob(t, "producer-a", "send_email", "trace-dlq-shadow", []byte(`{"i":1}`), true)

	require.Eventually(t, func() bool {
		var count int
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT count(*) FROM %s.%s_dlq WHERE trace_id = $1",
			env.schema, env.prefix,
		), "trace-dlq-shadow").Scan(&count)
		return err == nil && count == 1
	}, 5*time.Second, 20*time.Millisecond, "DLQ row not written")

	var shadow bool
	err := env.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT shadow FROM %s.%s_dlq WHERE trace_id = $1",
		env.schema, env.prefix,
	), "trace-dlq-shadow").Scan(&shadow)
	require.NoError(t, err)
	assert.True(t, shadow, "shadow flag must round-trip into the DLQ row")

	awaitClean(t, cancel, runErr)
}

func TestIntegration_RejectedRow_StoredInDLQ(t *testing.T) {
	env := newIntegrationEnv(t)
	d := env.newDriver(t)

	submit := func(_ context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		results := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = courier.SubmitResult{
				CorrelationID: j.CorrelationID,
				Err:           "payload exceeds max size",
				Reason:        "payload_too_large",
			}
		}
		return results, nil
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, submit) }()

	env.insertJob(t, "producer-a", "send_email", "trace-dlq", []byte(`{"big":"row"}`), false)

	require.Eventually(t, func() bool {
		var count int
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT count(*) FROM %s.%s_dlq WHERE trace_id = $1",
			env.schema, env.prefix,
		), "trace-dlq").Scan(&count)
		return err == nil && count == 1
	}, 5*time.Second, 20*time.Millisecond, "DLQ row not written")

	var (
		producerID, jobType, traceID, reason, errMsg string
		payload                                      []byte
		shadow                                       bool
	)
	err := env.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT producer_id, job_type, payload, trace_id, shadow, reason, error_message
		FROM %s.%s_dlq WHERE trace_id = $1
	`, env.schema, env.prefix), "trace-dlq").
		Scan(&producerID, &jobType, &payload, &traceID, &shadow, &reason, &errMsg)
	require.NoError(t, err)
	assert.Equal(t, "producer-a", producerID)
	assert.Equal(t, "send_email", jobType)
	assert.Equal(t, []byte(`{"big":"row"}`), payload)
	assert.Equal(t, "trace-dlq", traceID)
	assert.False(t, shadow)
	assert.Equal(t, "payload_too_large", reason)
	assert.Equal(t, "payload exceeds max size", errMsg)

	require.Eventually(t, func() bool {
		var cursorLSN string
		err := env.pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT last_lsn::text FROM %s.%s_cursor WHERE slot_name = $1",
			env.schema, env.prefix,
		), env.slot).Scan(&cursorLSN)
		return err == nil && cursorLSN != "0/0"
	}, 2*time.Second, 20*time.Millisecond, "cursor did not advance past rejected row")

	awaitClean(t, cancel, runErr)
}
