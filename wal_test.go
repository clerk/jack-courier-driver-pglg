package pglg

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
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
