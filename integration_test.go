//go:build integration

package pglg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

func (e *integrationEnv) reset(t *testing.T) {
	t.Helper()
	ctx := t.Context()

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

func TestIntegration_RowSurvivesDriverRestart(t *testing.T) {
	env := newIntegrationEnv(t)

	d1 := env.newDriver(t)

	submitStarted1 := make(chan []courier.Job, 1)
	submit1 := func(ctx context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		submitStarted1 <- jobs
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
