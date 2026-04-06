package pglg

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// partitionMaintenance runs in a background goroutine, creating future partitions
// and dropping expired ones on a periodic schedule.
func (d *Driver) partitionMaintenance(ctx context.Context) {
	// Run once immediately on startup.
	d.runPartitionMaintenance(ctx)

	ticker := time.NewTicker(d.cfg.PartitionMaintInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runPartitionMaintenance(ctx)
		}
	}
}

func (d *Driver) runPartitionMaintenance(ctx context.Context) {
	start := time.Now()

	if err := d.createPartitions(ctx); err != nil {
		_ = d.cfg.Statsd.Incr("jack.courier.partition.error", []string{"op:create"}, 1)
		d.cfg.Logger.Error("partition creation failed", slog.String("error", err.Error()))
	}
	if err := d.dropExpiredPartitions(ctx); err != nil {
		_ = d.cfg.Statsd.Incr("jack.courier.partition.error", []string{"op:drop"}, 1)
		d.cfg.Logger.Error("partition cleanup failed", slog.String("error", err.Error()))
	}

	_ = d.cfg.Statsd.Distribution("jack.courier.partition.maintenance.duration", time.Since(start).Seconds(), nil, 1)

	d.emitPartitionMetrics(ctx)
}

// createPartitions ensures partitions exist from (now - 1*interval) to (now + lookAhead).
func (d *Driver) createPartitions(ctx context.Context) error {
	now := time.Now().UTC()
	start := now.Add(-d.cfg.PartitionInterval).Truncate(d.cfg.PartitionInterval)
	end := now.Add(d.cfg.PartitionLookAhead)
	metaTable := d.cfg.partitionMetaTable()
	jobsTable := d.cfg.jobsTable()

	for t := start; t.Before(end); t = t.Add(d.cfg.PartitionInterval) {
		lower := t.UTC()
		upper := t.Add(d.cfg.PartitionInterval).UTC()
		partName := d.partitionName(lower)

		// Check if partition is already registered.
		var exists bool
		err := d.pool.QueryRow(ctx,
			fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE partition_name = $1)", metaTable),
			partName,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("pglg: check partition %s: %w", partName, err)
		}
		if exists {
			continue
		}

		// Create partition + register in meta inside a transaction.
		tx, err := d.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("pglg: begin tx for partition %s: %w", partName, err)
		}

		createSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
			d.cfg.Schema, partName, jobsTable,
			lower.Format(time.RFC3339),
			upper.Format(time.RFC3339),
		)
		if _, err := tx.Exec(ctx, createSQL); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("pglg: create partition %s: %w", partName, err)
		}

		insertMetaSQL := fmt.Sprintf(
			"INSERT INTO %s (partition_name, lower_bound, upper_bound) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
			metaTable,
		)
		if _, err := tx.Exec(ctx, insertMetaSQL, partName, lower, upper); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("pglg: register partition %s: %w", partName, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pglg: commit partition %s: %w", partName, err)
		}

		_ = d.cfg.Statsd.Incr("jack.courier.partition.created", nil, 1)
		d.cfg.Logger.Info("partition created",
			slog.String("name", partName),
			slog.Time("lower", lower),
			slog.Time("upper", upper))
	}
	return nil
}

// dropExpiredPartitions detaches and drops partitions older than the retention window.
func (d *Driver) dropExpiredPartitions(ctx context.Context) error {
	cutoff := time.Now().UTC().Add(-d.cfg.PartitionRetention)
	metaTable := d.cfg.partitionMetaTable()
	jobsTable := d.cfg.jobsTable()

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pglg: begin tx for partition drop: %w", err)
	}
	defer tx.Rollback(ctx)

	// Lock and select expired partitions.
	rows, err := tx.Query(ctx,
		fmt.Sprintf("SELECT partition_name FROM %s WHERE upper_bound <= $1 FOR UPDATE", metaTable),
		cutoff,
	)
	if err != nil {
		return fmt.Errorf("pglg: query expired partitions: %w", err)
	}

	var expired []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("pglg: scan partition name: %w", err)
		}
		expired = append(expired, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("pglg: iterate expired partitions: %w", err)
	}

	for _, name := range expired {
		// Detach partition from parent table.
		detachSQL := fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s.%s",
			jobsTable, d.cfg.Schema, name)
		if _, err := tx.Exec(ctx, detachSQL); err != nil {
			return fmt.Errorf("pglg: detach partition %s: %w", name, err)
		}

		// Drop the detached table.
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", d.cfg.Schema, name)
		if _, err := tx.Exec(ctx, dropSQL); err != nil {
			return fmt.Errorf("pglg: drop partition %s: %w", name, err)
		}

		// Remove from meta.
		deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE partition_name = $1", metaTable)
		if _, err := tx.Exec(ctx, deleteSQL, name); err != nil {
			return fmt.Errorf("pglg: delete meta %s: %w", name, err)
		}

		_ = d.cfg.Statsd.Incr("jack.courier.partition.dropped", nil, 1)
		d.cfg.Logger.Info("partition dropped", slog.String("name", name))
	}

	return tx.Commit(ctx)
}

// emitPartitionMetrics queries partition_meta and emits gauge metrics for
// operational visibility into partition health.
func (d *Driver) emitPartitionMetrics(ctx context.Context) {
	metaTable := d.cfg.partitionMetaTable()
	now := time.Now().UTC()

	// Total partition count.
	var count int
	err := d.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", metaTable),
	).Scan(&count)
	if err != nil {
		d.cfg.Logger.Warn("failed to query partition count", slog.String("error", err.Error()))
		return
	}
	_ = d.cfg.Statsd.Gauge("jack.courier.partition.total_count", float64(count), nil, 1)

	if count == 0 {
		_ = d.cfg.Statsd.Gauge("jack.courier.partition.lookahead_hours", 0, nil, 1)
		_ = d.cfg.Statsd.Gauge("jack.courier.partition.oldest_hours", 0, nil, 1)
		return
	}

	// Furthest upper_bound — how far ahead we have coverage.
	var maxUpper time.Time
	err = d.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT MAX(upper_bound) FROM %s", metaTable),
	).Scan(&maxUpper)
	if err != nil {
		d.cfg.Logger.Warn("failed to query max upper_bound", slog.String("error", err.Error()))
		return
	}
	lookaheadHours := maxUpper.Sub(now).Hours()
	_ = d.cfg.Statsd.Gauge("jack.courier.partition.lookahead_hours", lookaheadHours, nil, 1)

	// Oldest lower_bound — how old the oldest partition is.
	var minLower time.Time
	err = d.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT MIN(lower_bound) FROM %s", metaTable),
	).Scan(&minLower)
	if err != nil {
		d.cfg.Logger.Warn("failed to query min lower_bound", slog.String("error", err.Error()))
		return
	}
	oldestHours := now.Sub(minLower).Hours()
	_ = d.cfg.Statsd.Gauge("jack.courier.partition.oldest_hours", oldestHours, nil, 1)

	d.cfg.Logger.Debug("partition metrics emitted",
		slog.Int("total_count", count),
		slog.Float64("lookahead_hours", lookaheadHours),
		slog.Float64("oldest_hours", oldestHours))
}

// partitionName generates a deterministic partition name from a lower-bound timestamp.
func (d *Driver) partitionName(lower time.Time) string {
	return fmt.Sprintf("%s_jobs_%s", d.cfg.TablePrefix, lower.Format("20060102_1504"))
}
