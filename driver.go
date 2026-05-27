package pglg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"sync/atomic"
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

	// SlotBusyRetryDelay is the base delay between retries when the replication
	// slot is held by another consumer (active-passive leader election). Default: 5s.
	SlotBusyRetryDelay time.Duration

	// SlotBusyRetryJitter is the +/- jitter applied to SlotBusyRetryDelay
	// to avoid thundering herd on simultaneous standby retries. Default: 1s.
	SlotBusyRetryJitter time.Duration

	// LeaderHeartbeatInterval is how often each replica emits the
	// jack.courier.leader.is_leader gauge (1 for leader, 0 for standby). Default: 15s.
	LeaderHeartbeatInterval time.Duration

	// DLQRetention is how long DLQ rows are kept. Default: 30 days.
	DLQRetention time.Duration

	// DLQCleanupInterval is how often the leader prunes old DLQ rows. Default: 1h.
	DLQCleanupInterval time.Duration

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
	if c.SlotBusyRetryDelay <= 0 {
		c.SlotBusyRetryDelay = 5 * time.Second
	}
	if c.SlotBusyRetryJitter <= 0 {
		c.SlotBusyRetryJitter = 1 * time.Second
	}
	if c.LeaderHeartbeatInterval <= 0 {
		c.LeaderHeartbeatInterval = 15 * time.Second
	}
	if c.DLQRetention <= 0 {
		c.DLQRetention = 30 * 24 * time.Hour
	}
	if c.DLQCleanupInterval <= 0 {
		c.DLQCleanupInterval = 1 * time.Hour
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

func (c *Config) dlqTable() string {
	return fmt.Sprintf("%s.%s_dlq", c.Schema, c.TablePrefix)
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
	if v := os.Getenv("PGLG_DLQ_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("pglg: invalid PGLG_DLQ_RETENTION: %w", err)
		}
		cfg.DLQRetention = d
	}
	if v := os.Getenv("PGLG_DLQ_CLEANUP_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("pglg: invalid PGLG_DLQ_CLEANUP_INTERVAL: %w", err)
		}
		cfg.DLQCleanupInterval = d
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

	isLeader atomic.Bool

	maintWake chan struct{}

	standbyLogged bool
}

// Role returns the current role of this driver instance: "leader" or "standby".
func (d *Driver) Role() string {
	if d.isLeader.Load() {
		return "leader"
	}

	return "standby"
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

	return &Driver{
		cfg:          cfg,
		pool:         pool,
		replicaPools: replicaPools,
		maintWake:    make(chan struct{}, 1),
	}, nil
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
	go d.leaderHeartbeat(ctx)
	go d.dlqCleanup(ctx)

	bo := &backoff{
		initial:    d.cfg.ReconnectInitialDelay,
		max:        d.cfg.ReconnectMaxDelay,
		multiplier: 2.0,
	}

	for {
		err := d.runOnce(ctx, submit)
		if err == nil || ctx.Err() != nil {
			d.relinquishLeadership("shutdown")
			return ctx.Err()
		}

		if errors.Is(err, errSlotBusy) {
			d.relinquishLeadership("slot_busy")
			if !d.standbyLogged {
				d.cfg.Logger.Info("standby - slot held by another consumer",
					slog.String("slot", d.cfg.SlotName))
				d.standbyLogged = true
			}
			jitter := time.Duration(rand.Int64N(int64(2*d.cfg.SlotBusyRetryJitter+1))) - d.cfg.SlotBusyRetryJitter
			t := time.NewTimer(d.cfg.SlotBusyRetryDelay + jitter)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
			continue
		}

		_ = d.cfg.Statsd.Incr("jack.courier.wal.reconnect", nil, 1)
		d.cfg.Logger.Error("WAL stream error, will reconnect",
			slog.String("error", err.Error()))

		if waitErr := bo.wait(ctx); waitErr != nil {
			return waitErr
		}
	}
}

func (d *Driver) acquireLeadership() {
	if d.isLeader.CompareAndSwap(false, true) {
		d.standbyLogged = false
		_ = d.cfg.Statsd.Incr("jack.courier.leader.acquired", nil, 1)
		d.emitLeaderGauge()
		d.cfg.Logger.Info("acquired leader role", slog.String("slot", d.cfg.SlotName))
		select {
		case d.maintWake <- struct{}{}:
		default:
		}
	}
}

func (d *Driver) emitLeaderGauge() {
	v := 0.0
	if d.isLeader.Load() {
		v = 1.0
	}
	_ = d.cfg.Statsd.Gauge("jack.courier.leader.is_leader", v, nil, 1)
}

func (d *Driver) leaderHeartbeat(ctx context.Context) {
	d.emitLeaderGauge()
	t := time.NewTicker(d.cfg.LeaderHeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.emitLeaderGauge()
		}
	}
}

func (d *Driver) relinquishLeadership(reason string) {
	if d.isLeader.CompareAndSwap(true, false) {
		_ = d.cfg.Statsd.Incr("jack.courier.leader.relinquished", []string{"reason:" + reason}, 1)
		d.emitLeaderGauge()
		d.cfg.Logger.Info("relinquished leader role", slog.String("reason", reason))
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

	d.acquireLeadership()

	d.cfg.Logger.Info("WAL streaming started",
		slog.String("slot", d.cfg.SlotName),
		slog.String("start_lsn", startLSN.String()))

	// 4. Receive loop.
	var (
		buf              txBuffer
		pendingInserts   []parsedInsert
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
			if len(pendingInserts) > 0 {
				if flushErr := d.flushBatch(ctx, submit, wal, pendingInserts, pendingCommitLSN); flushErr != nil {
					return flushErr
				}
				pendingInserts = pendingInserts[:0]
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
				pendingInserts = append(pendingInserts, buf.inserts[i])
			}
			if ev.commitLSN > pendingCommitLSN {
				pendingCommitLSN = ev.commitLSN
			}
			buf.reset()

			if len(pendingInserts) >= d.cfg.MaxBatchSize {
				if flushErr := d.flushBatch(ctx, submit, wal, pendingInserts, pendingCommitLSN); flushErr != nil {
					return flushErr
				}
				pendingInserts = pendingInserts[:0]
				batchTimer.Reset(d.cfg.BatchTimeout)
			}
		case *walStreamStart:
			inStream = true
		case *walStreamStop:
			inStream = false
		case *walStreamCommit:
			for i := range buf.inserts {
				d.recordRowAge(&buf.inserts[i])
				pendingInserts = append(pendingInserts, buf.inserts[i])
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
			if len(pendingInserts) > 0 {
				if flushErr := d.flushBatch(ctx, submit, wal, pendingInserts, pendingCommitLSN); flushErr != nil {
					return flushErr
				}
				pendingInserts = pendingInserts[:0]
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

// flushBatch submits a batch and advances the cursor. Rejections go to the
// DLQ in the same transaction so no row is ever lost.
func (d *Driver) flushBatch(
	ctx context.Context,
	submit courier.SubmitFunc,
	wal *walConsumer,
	inserts []parsedInsert,
	commitLSN lsn,
) error {
	if len(inserts) == 0 {
		return nil
	}

	jobs := make([]courier.Job, len(inserts))
	for i := range inserts {
		jobs[i] = inserts[i].toJob()
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

	dlqRows, err := mapResults(inserts, results)
	if err != nil {
		_ = d.cfg.Statsd.Incr("jack.courier.flush.count", []string{"status:error"}, 1)
		return fmt.Errorf("pglg: jack response inconsistent: %w", err)
	}

	for _, r := range results {
		if r.Err != "" {
			d.cfg.Logger.Warn("job submit rejected",
				slog.String("correlation_id", r.CorrelationID),
				slog.String("error", r.Err),
				slog.String("reason", r.Reason))
		}
	}

	if err := d.persistFlushResult(ctx, dlqRows, commitLSN); err != nil {
		return err
	}

	if err := wal.sendStandbyStatus(ctx); err != nil {
		d.cfg.Logger.Warn("failed to send standby status after flush",
			slog.String("error", err.Error()))
	}

	for _, row := range dlqRows {
		_ = d.cfg.Statsd.Incr("jack.courier.dlq.write",
			[]string{"job_type:" + row.jobType, "reason:" + row.reason}, 1)
	}

	_ = d.cfg.Statsd.Incr("jack.courier.flush.count", []string{"status:success"}, 1)
	_ = d.cfg.Statsd.Distribution("jack.courier.flush.duration", time.Since(flushStart).Seconds(), nil, 1)
	if len(dlqRows) > 0 {
		_ = d.cfg.Statsd.Distribution("jack.courier.flush.failed_jobs", float64(len(dlqRows)), nil, 1)
	}

	d.cfg.Logger.Info("batch submitted",
		slog.Int("total", len(jobs)),
		slog.Int("failed", len(dlqRows)),
		slog.String("cursor_lsn", commitLSN.String()))

	return nil
}

func (d *Driver) persistFlushResult(ctx context.Context, dlqRows []dlqRow, commitLSN lsn) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pglg: begin flush tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := insertDLQRows(ctx, tx, d.cfg.dlqTable(), dlqRows); err != nil {
		return err
	}
	if err := d.writeCursorTx(ctx, tx, commitLSN); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pglg: commit flush tx: %w", err)
	}
	return nil
}
