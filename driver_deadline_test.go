package pglg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNextReceiveDeadlineUsesPendingBatchDeadline(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	nextBatchAt := now.Add(time.Second)
	nextStandbyAt := now.Add(10 * time.Second)

	got := nextReceiveDeadline(nextBatchAt, nextStandbyAt)

	assert.Equal(t, nextBatchAt, got)
}

func TestNextReceiveDeadlineIgnoresInactiveBatchDeadline(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	nextStandbyAt := now.Add(10 * time.Second)

	got := nextReceiveDeadline(time.Time{}, nextStandbyAt)

	assert.Equal(t, nextStandbyAt, got)
}

func TestCanAdvanceIdleFlush(t *testing.T) {
	tests := []struct {
		name           string
		pendingInserts []parsedInsert
		buf            txBuffer
		inTx           bool
		want           bool
	}{
		{
			name: "true when no work is in flight",
			want: true,
		},
		{
			name:           "false when committed rows are pending flush",
			pendingInserts: []parsedInsert{{id: 1}},
			want:           false,
		},
		{
			name: "false when the current transaction has buffered rows",
			buf: txBuffer{
				inserts: []parsedInsert{{id: 1}},
			},
			want: false,
		},
		{
			name: "false while a transaction is open",
			inTx: true,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canAdvanceIdleFlush(tt.pendingInserts, tt.buf, tt.inTx)

			assert.Equal(t, tt.want, got)
		})
	}
}
