package pglg

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	courier "github.com/clerk/jack-courier-lib"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds all configuration for the pglg driver.
type Config struct {
	// ConnString is the PostgreSQL connection string (standard libpq format).
	// Used for both replication and DML (cursor, partition management).
	// Required.
	ConnString string

	// SlotName is the logical replication slot name. Required.
	SlotName string

	// PublicationName is the PostgreSQL publication name. Required.
	PublicationName string

	// Schema is the database schema for outbox tables. Default: "public".
	Schema string

	// TablePrefix is prepended to table names (_jobs, _cursor, _partition_meta).
	// Default: "outbox".
	TablePrefix string

	// MaxBatchSize is the maximum number of jobs per submit() call. Default: 100.
	MaxBatchSize int

	// BatchTimeout is the max time to wait for a full batch before flushing.
	// Default: 1s.
	BatchTimeout time.Duration

	// StandbyInterval is the interval between keepalive status updates to Postgres.
	// Default: 10s.
	StandbyInterval time.Duration

	// PartitionInterval is the duration of each partition. Default: 1h.
	PartitionInterval time.Duration

	// PartitionLookAhead is how far ahead to pre-create partitions. Default: 12h.
	PartitionLookAhead time.Duration

	// PartitionRetention is how long to keep old partitions before dropping. Default: 3h.
	PartitionRetention time.Duration

	// PartitionMaintInterval is how often the partition maintenance loop runs. Default: 5m.
	PartitionMaintInterval time.Duration

	// ReconnectInitialDelay is the initial delay before reconnecting after a failure.
	// Default: 1s.
	ReconnectInitialDelay time.Duration

	// ReconnectMaxDelay is the maximum reconnection delay. Default: 30s.
	ReconnectMaxDelay time.Duration

	// Logger is the structured logger. Default: slog.Default().
	Logger *slog.Logger

	// Statsd is the DogStatsD client for metrics. Default: no-op client.
	Statsd statsd.ClientInterface

	// Replicas configures logical replicas that should receive mirrored
	// partition CREATE/DROP DDL. Empty means no replication. Default: empty.
	Replicas []ReplicaConfig
}

type ReplicaConfig struct {
	Name             string
	ConnString       string
	SubscriptionName string
}

func (c *Config) setDefaults() {
	if c.Schema == "" {
		c.Schema = "public"
	}
	if c.TablePrefix == "" {
		c.TablePrefix = "outbox"
	}
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = 100
	}
	if c.BatchTimeout <= 0 {
		c.BatchTimeout = 1 * time.Second
	}
	if c.StandbyInterval <= 0 {
		c.StandbyInterval = 10 * time.Second
	}
	if c.PartitionInterval <= 0 {
		c.PartitionInterval = 1 * time.Hour
	}
	if c.PartitionLookAhead <= 0 {
		c.PartitionLookAhead = 12 * time.Hour
	}
	if c.PartitionRetention <= 0 {
		c.PartitionRetention = 3 * time.Hour
	}
	if c.PartitionMaintInterval <= 0 {
		c.PartitionMaintInterval = 5 * time.Minute
	}
	if c.ReconnectInitialDelay <= 0 {
		c.ReconnectInitialDelay = 1 * time.Second
	}
	if c.ReconnectMaxDelay <= 0 {
		c.ReconnectMaxDelay = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Statsd == nil {
		c.Statsd = &statsd.NoOpClient{}
	}
}

func (c *Config) validate() error {
	if c.ConnString == "" {
		return fmt.Errorf("pglg: ConnString is required")
	}
	if c.SlotName == "" {
		return fmt.Errorf("pglg: SlotName is required")
	}
	if c.PublicationName == "" {
		return fmt.Errorf("pglg: PublicationName is required")
	}

	seen := make(map[string]bool, len(c.Replicas))
	for i, r := range c.Replicas {
		if r.Name == "" {
			return fmt.Errorf("pglg: Replicas[%d]: Name is required", i)
		}
		if r.ConnString == "" {
			return fmt.Errorf("pglg: Replicas[%d] (%q): ConnString is required", i, r.Name)
		}
		if r.SubscriptionName == "" {
			return fmt.Errorf("pglg: Replicas[%d] (%q): SubscriptionName is required", i, r.Name)
		}
		if seen[r.Name] {
			return fmt.Errorf("pglg: duplicate replica name %q", r.Name)
		}
		seen[r.Name] = true
	}

	return nil
}

// jobsTable returns the fully qualified outbox jobs table name.
func (c *Config) jobsTable() string {
	return fmt.Sprintf("%s.%s_jobs", c.Schema, c.TablePrefix)
}

// cursorTable returns the fully qualified cursor table name.
func (c *Config) cursorTable() string {
	return fmt.Sprintf("%s.%s_cursor", c.Schema, c.TablePrefix)
}

// partitionMetaTable returns the fully qualified partition metadata table name.
func (c *Config) partitionMetaTable() string {
	return fmt.Sprintf("%s.%s_partition_meta", c.Schema, c.TablePrefix)
}

// ConfigFromEnv reads configuration from PGLG_* environment variables.
// Required: PGLG_CONN_STRING, PGLG_SLOT_NAME, PGLG_PUBLICATION_NAME.
func ConfigFromEnv() (Config, error) {
	cfg := Config{
		ConnString:      os.Getenv("PGLG_CONN_STRING"),
		SlotName:        os.Getenv("PGLG_SLOT_NAME"),
		PublicationName: os.Getenv("PGLG_PUBLICATION_NAME"),
		Schema:          os.Getenv("PGLG_SCHEMA"),
		TablePrefix:     os.Getenv("PGLG_TABLE_PREFIX"),
	}

	if v := os.Getenv("PGLG_MAX_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("pglg: invalid PGLG_MAX_BATCH_SIZE: %w", err)
		}
		cfg.MaxBatchSize = n
	}
	if v := os.Getenv("PGLG_BATCH_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("pglg: invalid PGLG_BATCH_TIMEOUT: %w", err)
		}
		cfg.BatchTimeout = d
	}
	if v := os.Getenv("PGLG_STANDBY_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("pglg: invalid PGLG_STANDBY_INTERVAL: %w", err)
		}
		cfg.StandbyInterval = d
	}

	if dsn := os.Getenv("PGLG_REPLICA_CONN_STRING"); dsn != "" {
		cfg.Replicas = append(cfg.Replicas, ReplicaConfig{
			Name:             os.Getenv("PGLG_REPLICA_NAME"),
			ConnString:       dsn,
			SubscriptionName: os.Getenv("PGLG_REPLICA_SUBSCRIPTION"),
		})
	}

	return cfg, nil
}

// Driver implements courier.Driver using PostgreSQL logical replication.
type Driver struct {
	cfg          Config
	pool         *pgxpool.Pool
	replicaPools []replicaPool
}

type replicaPool struct {
	name             string
	pool             *pgxpool.Pool
	subscriptionName string
}

// New creates a new pglg Driver.
// Call courier.RegisterDriver(driver) before courier.Main().
func New(cfg Config) (*Driver, error) {
	cfg.setDefaults()

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	pool, err := pgxpool.New(context.Background(), cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("pglg: create pool: %w", err)
	}

	replicaPools := make([]replicaPool, 0, len(cfg.Replicas))
	for _, r := range cfg.Replicas {
		rp, err := pgxpool.New(context.Background(), r.ConnString)
		if err != nil {
			for _, prev := range replicaPools {
				prev.pool.Close()
			}
			pool.Close()
			return nil, fmt.Errorf("pglg: create replica pool %q: %w", r.Name, err)
		}

		replicaPools = append(replicaPools, replicaPool{name: r.Name, pool: rp, subscriptionName: r.SubscriptionName})
	}

	return &Driver{cfg: cfg, pool: pool, replicaPools: replicaPools}, nil
}

// Run implements courier.Driver. It blocks until ctx is cancelled or an
// unrecoverable error occurs.
func (d *Driver) Run(ctx context.Context, submit courier.SubmitFunc) error {
	defer func() {
		d.pool.Close()
		for _, r := range d.replicaPools {
			r.pool.Close()
		}
	}()

	// Start partition maintenance goroutine.
	go d.partitionMaintenance(ctx)

	bo := &backoff{
		initial:    d.cfg.ReconnectInitialDelay,
		max:        d.cfg.ReconnectMaxDelay,
		multiplier: 2.0,
	}

	for {
		err := d.runOnce(ctx, submit)
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}

		_ = d.cfg.Statsd.Incr("jack.courier.wal.reconnect", nil, 1)
		d.cfg.Logger.Error("WAL stream error, will reconnect",
			slog.String("error", err.Error()))

		if waitErr := bo.wait(ctx); waitErr != nil {
			return waitErr
		}
	}
}

// runOnce runs a single WAL streaming session.
func (d *Driver) runOnce(ctx context.Context, submit courier.SubmitFunc) error {
	// 1. Read last cursor LSN.
	startLSN, err := d.readCursor(ctx)
	if err != nil {
		return fmt.Errorf("pglg: read cursor: %w", err)
	}

	// 2. Connect WAL consumer.
	wal := newWALConsumer(d.cfg.SlotName, d.cfg.PublicationName, d.cfg.StandbyInterval)
	if err := wal.connect(ctx, d.cfg.ConnString); err != nil {
		return err
	}
	defer wal.close(ctx)

	// 3. Start streaming.
	if err := wal.startStreaming(ctx, startLSN); err != nil {
		return err
	}

	d.cfg.Logger.Info("WAL streaming started",
		slog.String("slot", d.cfg.SlotName),
		slog.String("start_lsn", startLSN.String()))

	// 4. Receive loop.
	var (
		buf              txBuffer
		pendingJobs      []courier.Job
		pendingCommitLSN = startLSN
		inStream         bool
	)

	batchTimer := time.NewTimer(d.cfg.BatchTimeout)
	defer batchTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		walData, err := wal.receiveMessage(ctx)
		if err == errStandbyTimeout {
			if sendErr := wal.sendStandbyStatus(ctx); sendErr != nil {
				return fmt.Errorf("pglg: standby status: %w", sendErr)
			}
			// Flush any pending jobs on timeout.
			if len(pendingJobs) > 0 {
				if flushErr := d.flushBatch(ctx, submit, wal, pendingJobs, pendingCommitLSN); flushErr != nil {
					return flushErr
				}
				pendingJobs = pendingJobs[:0]
			}
			batchTimer.Reset(d.cfg.BatchTimeout)
			continue
		}
		if err != nil {
			return fmt.Errorf("pglg: receive: %w", err)
		}
		if walData == nil {
			continue
		}

		msg, err := parseWALMessage(walData, inStream)
		if err != nil {
			d.cfg.Logger.Warn("failed to parse WAL message", slog.String("error", err.Error()))
			continue
		}

		switch ev := msg.(type) {
		case *walRelation:
			wal.setRelation(ev.id, ev.name, ev.columns)
		case *walInsert:
			job, parseErr := wal.parseInsert(ev)
			if parseErr != nil {
				_ = d.cfg.Statsd.Incr("jack.courier.parse.error", nil, 1)
				d.cfg.Logger.Error("failed to parse INSERT, row dropped", slog.String("error", parseErr.Error()))
				continue
			}
			if job != nil {
				buf.addInsert(*job)
			}
		case *walCommit:
			for i := range buf.inserts {
				d.recordRowAge(&buf.inserts[i])
				pendingJobs = append(pendingJobs, buf.inserts[i].toJob())
			}
			if ev.commitLSN > pendingCommitLSN {
				pendingCommitLSN = ev.commitLSN
			}
			buf.reset()

			if len(pendingJobs) >= d.cfg.MaxBatchSize {
				if flushErr := d.flushBatch(ctx, submit, wal, pendingJobs, pendingCommitLSN); flushErr != nil {
					return flushErr
				}
				pendingJobs = pendingJobs[:0]
				batchTimer.Reset(d.cfg.BatchTimeout)
			}
		case *walStreamStart:
			inStream = true
		case *walStreamStop:
			inStream = false
		case *walStreamCommit:
			for i := range buf.inserts {
				d.recordRowAge(&buf.inserts[i])
				pendingJobs = append(pendingJobs, buf.inserts[i].toJob())
			}
			if ev.commitLSN > pendingCommitLSN {
				pendingCommitLSN = ev.commitLSN
			}
			buf.reset()
		case *walStreamAbort:
			buf.reset()
		}

		// Check batch timer (non-blocking).
		select {
		case <-batchTimer.C:
			if len(pendingJobs) > 0 {
				if flushErr := d.flushBatch(ctx, submit, wal, pendingJobs, pendingCommitLSN); flushErr != nil {
					return flushErr
				}
				pendingJobs = pendingJobs[:0]
			}
			batchTimer.Reset(d.cfg.BatchTimeout)
		default:
		}
	}
}

// recordRowAge emits the time-since-row-creation as a distribution so
// operators can see how stale rows are when the courier picks them up.
// Approximates CDC lag without a separate WAL-position poll: if the
// courier keeps up, age tracks the courier's batch interval; if it
// falls behind, age grows.
func (d *Driver) recordRowAge(p *parsedInsert) {
	if p.createdAt.IsZero() {
		return
	}
	_ = d.cfg.Statsd.Distribution("jack.courier.row.age", time.Since(p.createdAt).Seconds(),
		[]string{"job_type:" + p.jobType}, 1)
}

// flushBatch submits a batch of jobs and advances the cursor on success.
func (d *Driver) flushBatch(
	ctx context.Context,
	submit courier.SubmitFunc,
	wal *walConsumer,
	jobs []courier.Job,
	commitLSN lsn,
) error {
	if len(jobs) == 0 {
		return nil
	}

	d.cfg.Logger.Debug("submitting batch",
		slog.Int("count", len(jobs)),
		slog.String("commit_lsn", commitLSN.String()))

	_ = d.cfg.Statsd.Distribution("jack.courier.flush.batch_size", float64(len(jobs)), nil, 1)

	flushStart := time.Now()
	results, err := submit(ctx, jobs)
	if err != nil {
		_ = d.cfg.Statsd.Incr("jack.courier.flush.count", []string{"status:error"}, 1)
		return fmt.Errorf("pglg: submit failed: %w", err)
	}

	var failCount int
	for _, r := range results {
		if r.Err != "" {
			failCount++
			d.cfg.Logger.Warn("job submit rejected",
				slog.String("correlation_id", r.CorrelationID),
				slog.String("error", r.Err))
		}
	}

	// Advance cursor even on partial per-job failures (permanent rejections).
	if err := d.writeCursor(ctx, commitLSN); err != nil {
		return fmt.Errorf("pglg: write cursor: %w", err)
	}

	// Send standby status to Postgres after advancing cursor.
	if err := wal.sendStandbyStatus(ctx); err != nil {
		d.cfg.Logger.Warn("failed to send standby status after flush",
			slog.String("error", err.Error()))
	}

	_ = d.cfg.Statsd.Incr("jack.courier.flush.count", []string{"status:success"}, 1)
	_ = d.cfg.Statsd.Distribution("jack.courier.flush.duration", time.Since(flushStart).Seconds(), nil, 1)
	if failCount > 0 {
		_ = d.cfg.Statsd.Distribution("jack.courier.flush.failed_jobs", float64(failCount), nil, 1)
	}

	d.cfg.Logger.Info("batch submitted",
		slog.Int("total", len(jobs)),
		slog.Int("failed", failCount),
		slog.String("cursor_lsn", commitLSN.String()))

	return nil
}
