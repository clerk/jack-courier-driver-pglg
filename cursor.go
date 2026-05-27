package pglg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// readCursor reads the last confirmed LSN from the cursor table.
// Returns 0/0 if no cursor row exists (fresh start).
func (d *Driver) readCursor(ctx context.Context) (lsn, error) {
	query := fmt.Sprintf(
		"SELECT last_lsn::text FROM %s WHERE slot_name = $1",
		d.cfg.cursorTable(),
	)

	var lsnStr string
	err := d.pool.QueryRow(ctx, query, d.cfg.SlotName).Scan(&lsnStr)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("pglg: read cursor: %w", err)
	}

	parsed, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, fmt.Errorf("pglg: parse cursor LSN %q: %w", lsnStr, err)
	}
	return lsn(parsed), nil
}

func (d *Driver) writeCursorTx(ctx context.Context, tx pgx.Tx, l lsn) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (slot_name, last_lsn, updated_at)
		VALUES ($1, $2::pg_lsn, now())
		ON CONFLICT (slot_name)
		DO UPDATE SET last_lsn = EXCLUDED.last_lsn, updated_at = now()
	`, d.cfg.cursorTable())

	if _, err := tx.Exec(ctx, query, d.cfg.SlotName, pglogrepl.LSN(l).String()); err != nil {
		return fmt.Errorf("pglg: write cursor: %w", err)
	}
	return nil
}
