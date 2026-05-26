package pglg

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	courier "github.com/clerk/jack-courier-lib"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	dlqReasonUnspecified = "unspecified"
	dlqErrorMessageMax   = 4096
)

type dlqRow struct {
	id           int64
	createdAt    time.Time
	producerID   string
	jobType      string
	payload      []byte
	runAt        time.Time
	traceID      string
	shadow       bool
	reason       string
	errorMessage string
}

// mapResults matches results to inserts by CorrelationID because courier-lib
// reorders shadow vs non-shadow jobs. Returns an error on any mismatch so the
// caller skips the cursor advance.
func mapResults(inserts []parsedInsert, results []courier.SubmitResult) ([]dlqRow, error) {
	if len(results) != len(inserts) {
		return nil, fmt.Errorf("pglg: submit returned %d results for %d submitted jobs", len(results), len(inserts))
	}

	insertByID := make(map[string]*parsedInsert, len(inserts))
	for i := range inserts {
		cid := strconv.FormatInt(inserts[i].id, 10)
		if _, dup := insertByID[cid]; dup {
			return nil, fmt.Errorf("pglg: duplicate CorrelationID %q among submitted inserts", cid)
		}
		insertByID[cid] = &inserts[i]
	}

	seen := make(map[string]bool, len(results))
	var rows []dlqRow
	for _, r := range results {
		if seen[r.CorrelationID] {
			return nil, fmt.Errorf("pglg: duplicate CorrelationID %q in submit results", r.CorrelationID)
		}
		seen[r.CorrelationID] = true

		p, ok := insertByID[r.CorrelationID]
		if !ok {
			return nil, fmt.Errorf("pglg: unknown CorrelationID %q in submit results", r.CorrelationID)
		}

		if r.Err == "" {
			continue
		}

		reason := r.Reason
		if reason == "" {
			reason = dlqReasonUnspecified
		}
		rows = append(rows, dlqRow{
			id:           p.id,
			createdAt:    p.createdAt,
			producerID:   p.producerID,
			jobType:      p.jobType,
			payload:      p.payload,
			runAt:        p.runAt,
			traceID:      p.traceID,
			shadow:       p.shadow,
			reason:       reason,
			errorMessage: truncate(r.Err, dlqErrorMessageMax),
		})
	}
	return rows, nil
}

func truncate(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) > max {
		s = s[:max]
	}
	return strings.ToValidUTF8(s, "")
}

func insertDLQRows(ctx context.Context, tx pgx.Tx, table string, rows []dlqRow) error {
	if len(rows) == 0 {
		return nil
	}

	const cols = 10
	args := make([]any, 0, len(rows)*cols)
	placeholders := make([]string, 0, len(rows))
	for i, r := range rows {
		base := i*cols + 1
		placeholders = append(placeholders, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9,
		))
		runAt := any(nil)
		if !r.runAt.IsZero() {
			runAt = r.runAt
		}
		args = append(args,
			r.id, r.createdAt, r.producerID, r.jobType, r.payload,
			runAt, r.traceID, r.shadow, r.reason, r.errorMessage,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, created_at, producer_id, job_type, payload, run_at, trace_id, shadow, reason, error_message)
		VALUES %s
	`, table, strings.Join(placeholders, ","))

	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("pglg: insert dlq: %w", err)
	}
	return nil
}

func (d *Driver) dlqCleanup(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.DLQCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if !d.isLeader.Load() {
			continue
		}
		d.runDLQCleanup(ctx)
	}
}

func (d *Driver) runDLQCleanup(ctx context.Context) {
	start := time.Now()
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE dead_lettered_at < now() - $1",
		d.cfg.dlqTable(),
	)
	interval := pgtype.Interval{
		Microseconds: d.cfg.DLQRetention.Microseconds(),
		Valid:        true,
	}
	res, err := d.pool.Exec(ctx, query, interval)
	if err != nil {
		_ = d.cfg.Statsd.Incr("jack.courier.dlq.cleanup.error", nil, 1)
		d.cfg.Logger.Error("dlq cleanup failed", slog.String("error", err.Error()))
		return
	}
	_ = d.cfg.Statsd.Distribution("jack.courier.dlq.cleanup.deleted", float64(res.RowsAffected()), nil, 1)
	_ = d.cfg.Statsd.Distribution("jack.courier.dlq.cleanup.duration", time.Since(start).Seconds(), nil, 1)
	d.cfg.Logger.Info("dlq cleanup complete",
		slog.Int64("deleted", res.RowsAffected()),
		slog.Duration("took", time.Since(start)))
}
