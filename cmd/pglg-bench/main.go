package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	courier "github.com/clerk/jack-courier-lib"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pglg "github.com/clerk/jack-courier-driver-pglg"
)

func main() {
	cfg := parseFlags()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, cfg.connString)
	if err != nil {
		fatal(fmt.Errorf("connect: %w", err))
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		fatal(fmt.Errorf("ping: %w", err))
	}
	fmt.Printf("pglg-bench: connected; rate=%d/s duration=%s submit-latency=%s payload=%dB\n",
		cfg.rate, cfg.duration, cfg.submitLatency, cfg.payloadSize)

	if err := reset(ctx, pool, cfg); err != nil {
		fatal(fmt.Errorf("reset: %w", err))
	}

	d, err := pglg.New(pglg.Config{
		ConnString:      cfg.connString,
		SlotName:        cfg.slot,
		PublicationName: cfg.publication,
		Schema:          cfg.schema,
		TablePrefix:     cfg.prefix,
		MaxBatchSize:    cfg.batchSize,
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		fatal(fmt.Errorf("new driver: %w", err))
	}

	submit := makeSubmit(cfg.submitLatency)

	driverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(driverCtx, submit) }()

	steady, err := runSteady(ctx, pool, cfg, runErr)
	if err != nil {
		fatal(err)
	}

	verdict := "bounded ✓"
	if !bounded(steady.samples) {
		verdict = "GROWING ✗"
	}
	fmt.Printf("\n=== submit-latency=%s ===\n", cfg.submitLatency)
	fmt.Printf("@%d/s: rate %.0f/s | lag p50 %s p99 %s max %s -> %s\n",
		cfg.rate, steady.achievedRate,
		humanizeBytes(percentile(steady.samples, 50)),
		humanizeBytes(percentile(steady.samples, 99)),
		humanizeBytes(maxInt64(steady.samples)),
		verdict)

	cancel()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		fmt.Fprintln(os.Stderr, "pglg-bench: driver did not stop within 5s")
	}
}

type config struct {
	connString    string
	schema        string
	prefix        string
	publication   string
	slot          string
	rate          int
	duration      time.Duration
	submitLatency time.Duration
	payloadSize   int
	batchSize     int
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.connString, "conn-string",
		"host=127.0.0.1 port=15434 user=pglg password=pglg dbname=pglg sslmode=disable",
		"PostgreSQL connection string")
	flag.StringVar(&c.schema, "schema", "public", "schema for outbox tables")
	flag.StringVar(&c.prefix, "prefix", "bench", "table prefix")
	flag.StringVar(&c.publication, "publication", "bench_pub", "publication name")
	flag.StringVar(&c.slot, "slot", "bench_slot", "replication slot name")
	flag.IntVar(&c.rate, "rate", 2000, "insert rate (jobs/sec); effective minimum is 10/s")
	flag.DurationVar(&c.duration, "duration", 60*time.Second, "run duration")
	flag.DurationVar(&c.submitLatency, "submit-latency", 0, "per-submit sleep in the stub")
	flag.IntVar(&c.payloadSize, "payload-size", 256, "bytes per job payload")
	flag.IntVar(&c.batchSize, "batch-size", 100, "driver MaxBatchSize")
	flag.Parse()
	return c
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "pglg-bench:", err)
	os.Exit(1)
}

func slotLag(ctx context.Context, pool *pgxpool.Pool, slot string) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
		 FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&n)
	return n, err
}

type steadyResult struct {
	achievedRate float64
	samples      []int64
}

func runSteady(ctx context.Context, pool *pgxpool.Pool, cfg config, runErr <-chan error) (steadyResult, error) {
	perTick := cfg.rate / 10
	if perTick < 1 {
		perTick = 1
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var samples []int64
	var inserted int64
	start := time.Now()
	deadline := start.Add(cfg.duration)

	for time.Now().Before(deadline) {
		select {
		case err := <-runErr:
			return steadyResult{}, fmt.Errorf("driver exited: %w", err)
		case <-ticker.C:
		}
		if err := insertJobs(ctx, pool, cfg, perTick); err != nil {
			return steadyResult{}, fmt.Errorf("insert: %w", err)
		}
		inserted += int64(perTick)
		if lag, err := slotLag(ctx, pool, cfg.slot); err == nil {
			samples = append(samples, lag)
		}
	}

	if len(samples) == 0 {
		return steadyResult{}, fmt.Errorf("no slot-lag samples collected")
	}

	return steadyResult{
		achievedRate: float64(inserted) / time.Since(start).Seconds(),
		samples:      samples,
	}, nil
}

func reset(ctx context.Context, pool *pgxpool.Pool, cfg config) error {
	if _, err := pool.Exec(ctx,
		`SELECT pg_drop_replication_slot(slot_name)
		 FROM pg_replication_slots WHERE slot_name = $1`, cfg.slot); err != nil {
		return fmt.Errorf("drop slot: %w", err)
	}
	if _, err := pool.Exec(ctx, "DROP PUBLICATION IF EXISTS "+cfg.publication); err != nil {
		return fmt.Errorf("drop publication: %w", err)
	}
	for _, suffix := range []string{"_jobs", "_cursor", "_dlq"} {
		if _, err := pool.Exec(ctx,
			fmt.Sprintf("TRUNCATE %s.%s%s", cfg.schema, cfg.prefix, suffix)); err != nil {
			return fmt.Errorf("truncate %s%s: %w", cfg.prefix, suffix, err)
		}
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s.%s_jobs WITH (publish = 'insert')",
		cfg.publication, cfg.schema, cfg.prefix)); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	if _, err := pool.Exec(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')", cfg.slot); err != nil {
		return fmt.Errorf("create slot: %w", err)
	}
	return nil
}

func insertJobs(ctx context.Context, pool *pgxpool.Pool, cfg config, n int) error {
	payload := make([]byte, cfg.payloadSize)
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{"bench", "bench_job", payload, nil, "", false}
	}
	_, err := pool.CopyFrom(ctx,
		pgx.Identifier{cfg.schema, cfg.prefix + "_jobs"},
		[]string{"producer_id", "job_type", "payload", "run_at", "trace_id", "shadow"},
		pgx.CopyFromRows(rows))
	return err
}

func makeSubmit(latency time.Duration) courier.SubmitFunc {
	return func(ctx context.Context, jobs []courier.Job) ([]courier.SubmitResult, error) {
		if latency > 0 {
			select {
			case <-time.After(latency):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		res := make([]courier.SubmitResult, len(jobs))
		for i, j := range jobs {
			res[i] = courier.SubmitResult{CorrelationID: j.CorrelationID, JobID: "ok"}
		}
		return res, nil
	}
}
