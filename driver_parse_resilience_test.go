package pglg

import (
	"context"
	"errors"
	"testing"
	"time"

	courier "github.com/clerk/jack-courier-lib"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeConn struct {
	msgs chan pgproto3.BackendMessage
}

func newFakeConn(msgs ...pgproto3.BackendMessage) *fakeConn {
	ch := make(chan pgproto3.BackendMessage, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	return &fakeConn{msgs: ch}
}

func (f *fakeConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	select {
	case m := <-f.msgs:
		return m, nil
	case <-ctx.Done():
		return &pgproto3.CopyData{}, nil
	}
}

func (f *fakeConn) Close(_ context.Context) error { return nil }

func badXLogCopy(walData []byte) *pgproto3.CopyData {
	data := make([]byte, 25+len(walData))
	data[0] = pglogrepl.XLogDataByteID
	copy(data[25:], walData)
	return &pgproto3.CopyData{Data: data}
}

func TestProcessWALStream_LoopSurvivesParseError(t *testing.T) {
	conn := newFakeConn(badXLogCopy([]byte{0xFF, 0x00, 0x00, 0x00}))

	rs := &recordingStatsd{}
	d := newTestDriver(rs)
	d.cfg.BatchTimeout = 10 * time.Minute
	d.cfg.StandbyInterval = time.Hour

	wal := &walConsumer{
		conn:            conn,
		slotName:        "test-slot",
		publicationName: "test-pub",
		relations:       map[uint32]*relationMeta{},
	}

	submit := func(_ context.Context, _ []courier.Job) ([]courier.SubmitResult, error) {
		return nil, errors.New("submit called")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- d.processWALStream(ctx, wal, submit, 0) }()

	require.Eventually(t, func() bool {
		return len(conn.msgs) == 0
	}, time.Second, 5*time.Millisecond)

	cancel()

	select {
	case err := <-runErr:
		assert.True(t, errors.Is(err, context.Canceled), "got: %v", err)
	case <-time.After(time.Second):
		t.Fatal("did not return")
	}

	var parseErrCount int
	for _, c := range rs.calls() {
		if c.name == "jack.courier.wal.parse.error" {
			parseErrCount++
		}
	}
	assert.Equal(t, 1, parseErrCount)
}
