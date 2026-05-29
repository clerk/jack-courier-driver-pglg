package pglg

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestMapStartReplicationError(t *testing.T) {
	t.Run("slot busy (55006) returns errSlotBusy", func(t *testing.T) {
		in := &pgconn.PgError{
			Code:    pgerrcode.ObjectInUse,
			Message: `replication slot "background_jobs_slot_core" is active for PID 12345`,
		}
		got := mapStartReplicationError(in)
		assert.ErrorIs(t, got, errSlotBusy)
	})

	t.Run("wrapped slot busy still classifies", func(t *testing.T) {
		in := fmt.Errorf("outer: %w", &pgconn.PgError{Code: pgerrcode.ObjectInUse})
		got := mapStartReplicationError(in)
		assert.ErrorIs(t, got, errSlotBusy)
	})

	t.Run("other PG error returns wrapped error", func(t *testing.T) {
		in := &pgconn.PgError{Code: pgerrcode.InsufficientPrivilege, Message: "permission denied"}
		got := mapStartReplicationError(in)
		assert.NotErrorIs(t, got, errSlotBusy)
		assert.Contains(t, got.Error(), "pglg: start replication")
		assert.ErrorIs(t, got, in)
	})

	t.Run("non-PG error returns wrapped error", func(t *testing.T) {
		in := errors.New("connection refused")
		got := mapStartReplicationError(in)
		assert.NotErrorIs(t, got, errSlotBusy)
		assert.Contains(t, got.Error(), "pglg: start replication")
		assert.ErrorIs(t, got, in)
	})
}

func TestStandbyStatusUpdateReportsSafeFlushLSN(t *testing.T) {
	wal := &walConsumer{
		clientXLogPos: lsn(0x300),
		flushLSN:      lsn(0x100),
	}
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)

	got := wal.standbyStatusUpdate(now)

	assert.Equal(t, pglogrepl.LSN(0x300), got.WALWritePosition)
	assert.Equal(t, pglogrepl.LSN(0x100), got.WALFlushPosition)
	assert.Equal(t, pglogrepl.LSN(0x100), got.WALApplyPosition)
	assert.Equal(t, now, got.ClientTime)
}

func TestStandbyStatusUpdateDoesNotAdvanceFlushLSNFromZero(t *testing.T) {
	wal := &walConsumer{
		clientXLogPos: lsn(0x300),
	}
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)

	got := wal.standbyStatusUpdate(now)

	assert.Equal(t, pglogrepl.LSN(0), got.WALWritePosition)
	assert.Equal(t, pglogrepl.LSN(0), got.WALFlushPosition)
	assert.Equal(t, pglogrepl.LSN(0), got.WALApplyPosition)
	assert.Equal(t, now, got.ClientTime)
}

func TestStandbyStatusUpdateWriteLSNIsAtLeastFlushLSN(t *testing.T) {
	wal := &walConsumer{
		clientXLogPos: lsn(0x100),
		flushLSN:      lsn(0x300),
	}
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)

	got := wal.standbyStatusUpdate(now)

	assert.Equal(t, pglogrepl.LSN(0x300), got.WALWritePosition)
	assert.Equal(t, pglogrepl.LSN(0x300), got.WALFlushPosition)
	assert.Equal(t, pglogrepl.LSN(0x300), got.WALApplyPosition)
	assert.Equal(t, now, got.ClientTime)
}
