package pglg

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func toBytea(s string) string {
	return `\x` + hex.EncodeToString([]byte(s))
}

func TestParseInsertColumns(t *testing.T) {
	columns := []string{"id", "created_at", "producer_id", "job_type", "payload", "run_at", "trace_id"}
	values := []columnValue{
		{data: "42"},
		{data: "2026-02-16 14:30:00.123456+00"},
		{data: "billing-service"},
		{data: "charge_customer"},
		{data: toBytea(`{"amount":100}`)},
		{data: "2026-02-16 15:00:00+00"},
		{data: "trace-abc-123"},
	}

	row, err := parseInsertColumns(columns, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if row.id != 42 {
		t.Errorf("expected id=42, got %d", row.id)
	}
	if row.producerID != "billing-service" {
		t.Errorf("expected producerID=billing-service, got %s", row.producerID)
	}
	if row.jobType != "charge_customer" {
		t.Errorf("expected jobType=charge_customer, got %s", row.jobType)
	}
	if string(row.payload) != `{"amount":100}` {
		t.Errorf("expected payload={\"amount\":100}, got %s", row.payload)
	}
	if row.traceID != "trace-abc-123" {
		t.Errorf("expected traceID=trace-abc-123, got %s", row.traceID)
	}
	if row.runAt.IsZero() {
		t.Error("expected non-zero runAt")
	}
}

func TestParseInsertColumns_Shadow(t *testing.T) {
	cases := []struct {
		name string
		val  string
		want bool
	}{
		{"true", "t", true},
		{"false", "f", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			columns := []string{"id", "producer_id", "job_type", "shadow"}
			values := []columnValue{{data: "1"}, {data: "svc"}, {data: "t"}, {data: tc.val}}

			row, err := parseInsertColumns(columns, values)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if row.shadow != tc.want {
				t.Errorf("shadow=%v, want %v", row.shadow, tc.want)
			}
		})
	}
}

func TestParseInsertColumns_ShadowDefaultsFalseWhenAbsent(t *testing.T) {
	columns := []string{"id", "producer_id", "job_type"}
	values := []columnValue{{data: "1"}, {data: "svc"}, {data: "t"}}

	row, err := parseInsertColumns(columns, values)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if row.shadow {
		t.Errorf("shadow should default to false when column absent, got true")
	}
}

func TestParseInsertColumns_NullRunAt(t *testing.T) {
	columns := []string{"id", "producer_id", "job_type", "payload", "run_at", "trace_id"}
	values := []columnValue{
		{data: "1"},
		{data: "svc"},
		{data: "do_thing"},
		{data: toBytea(`{}`)},
		{isNull: true},
		{data: ""},
	}

	row, err := parseInsertColumns(columns, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !row.runAt.IsZero() {
		t.Errorf("expected zero runAt for NULL, got %v", row.runAt)
	}
}

func TestParseInsertColumns_MismatchedCounts(t *testing.T) {
	columns := []string{"id", "producer_id"}
	values := []columnValue{{data: "1"}}

	_, err := parseInsertColumns(columns, values)
	if err == nil {
		t.Fatal("expected error for mismatched column/value counts")
	}
}

func TestParsedInsertToJob(t *testing.T) {
	runAt := time.Date(2026, 2, 16, 15, 0, 0, 0, time.UTC)
	p := &parsedInsert{
		id:         99,
		producerID: "prod_abc",
		jobType:    "send_email",
		payload:    []byte(`{"to":"user@example.com"}`),
		runAt:      runAt,
		traceID:    "trace-xyz",
		shadow:     true,
	}

	job := p.toJob()

	if !job.Shadow {
		t.Errorf("expected Shadow=true on courier.Job, got false")
	}
	if job.CorrelationID != "99" {
		t.Errorf("expected CorrelationID=99, got %s", job.CorrelationID)
	}
	if job.ProducerID != "prod_abc" {
		t.Errorf("expected ProducerID=prod_abc, got %s", job.ProducerID)
	}
	if job.JobType != "send_email" {
		t.Errorf("expected JobType=send_email, got %s", job.JobType)
	}
	if string(job.Payload) != `{"to":"user@example.com"}` {
		t.Errorf("unexpected Payload: %s", job.Payload)
	}
	if !job.RunAt.Equal(runAt) {
		t.Errorf("expected RunAt=%v, got %v", runAt, job.RunAt)
	}
	if job.TraceID != "trace-xyz" {
		t.Errorf("expected TraceID=trace-xyz, got %s", job.TraceID)
	}
}

func TestParseWALMessageBegin(t *testing.T) {
	msg := make([]byte, 1+8+8+4)
	msg[0] = 'B'
	binary.BigEndian.PutUint64(msg[1:], 0x10)
	binary.BigEndian.PutUint64(msg[9:], 0)
	binary.BigEndian.PutUint32(msg[17:], 123)

	got, err := parseWALMessage(msg, false)

	require.NoError(t, err)
	assert.IsType(t, &walBegin{}, got)
}

func TestParseWALMessageStreamInsertIncludesXID(t *testing.T) {
	msg := make([]byte, 1+4+4+1+2+1+4+2)
	msg[0] = 'I'
	binary.BigEndian.PutUint32(msg[1:], 77)
	binary.BigEndian.PutUint32(msg[5:], 1234)
	msg[9] = 'N'
	binary.BigEndian.PutUint16(msg[10:], 1)
	msg[12] = 't'
	binary.BigEndian.PutUint32(msg[13:], 2)
	copy(msg[17:], "42")

	got, err := parseWALMessage(msg, true)

	require.NoError(t, err)
	require.IsType(t, &walInsert{}, got)
	insert := got.(*walInsert)
	assert.Equal(t, uint32(77), insert.xid)
	assert.Equal(t, uint32(1234), insert.relationID)
	assert.Equal(t, []columnValue{{data: "42"}}, insert.values)
}

func TestParseWALMessageStreamAbortIncludesXIDs(t *testing.T) {
	msg := make([]byte, 1+4+4)
	msg[0] = 'A'
	binary.BigEndian.PutUint32(msg[1:], 10)
	binary.BigEndian.PutUint32(msg[5:], 20)

	got, err := parseWALMessage(msg, false)

	require.NoError(t, err)
	require.IsType(t, &walStreamAbort{}, got)
	abort := got.(*walStreamAbort)
	assert.Equal(t, uint32(10), abort.xid)
	assert.Equal(t, uint32(20), abort.subXid)
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  time.Time
	}{
		{
			"postgres with microseconds",
			"2026-02-16 14:30:00.123456+00",
			time.Date(2026, 2, 16, 14, 30, 0, 123456000, time.UTC),
		},
		{
			"postgres no microseconds",
			"2026-02-16 14:30:00+00",
			time.Date(2026, 2, 16, 14, 30, 0, 0, time.UTC),
		},
		{
			"RFC3339",
			"2026-02-16T14:30:00Z",
			time.Date(2026, 2, 16, 14, 30, 0, 0, time.UTC),
		},
		{
			"RFC3339Nano",
			"2026-02-16T14:30:00.123456789Z",
			time.Date(2026, 2, 16, 14, 30, 0, 123456789, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTimestamp(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseTimestamp_Invalid(t *testing.T) {
	_, err := parseTimestamp("not-a-timestamp")
	if err == nil {
		t.Fatal("expected error for invalid timestamp")
	}
}

func TestTxBuffer(t *testing.T) {
	var buf txBuffer

	buf.addInsert(parsedInsert{id: 1, jobType: "a"}, 0)
	buf.addInsert(parsedInsert{id: 2, jobType: "b"}, 0)

	if len(buf.inserts) != 2 {
		t.Fatalf("expected 2 inserts, got %d", len(buf.inserts))
	}

	buf.reset()

	if len(buf.inserts) != 0 {
		t.Fatalf("expected 0 inserts after reset, got %d", len(buf.inserts))
	}

	// Ensure the underlying slice is reused (no allocation).
	buf.addInsert(parsedInsert{id: 3, jobType: "c"}, 0)
	if len(buf.inserts) != 1 {
		t.Fatalf("expected 1 insert after re-add, got %d", len(buf.inserts))
	}
}

func TestTxBufferRemoveXIDRemovesOnlyAbortedSubtransactionRows(t *testing.T) {
	var buf txBuffer
	buf.addInsert(parsedInsert{id: 1, jobType: "outer"}, 100)
	buf.addInsert(parsedInsert{id: 2, jobType: "sub"}, 200)
	buf.addInsert(parsedInsert{id: 3, jobType: "outer-later"}, 100)

	buf.removeXID(200)

	require.Len(t, buf.inserts, 2)
	assert.Equal(t, int64(1), buf.inserts[0].row.id)
	assert.Equal(t, int64(3), buf.inserts[1].row.id)
}
