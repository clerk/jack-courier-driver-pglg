package pglg

import (
	"testing"
	"time"
)

func TestParseInsertColumns(t *testing.T) {
	columns := []string{"id", "created_at", "producer_id", "job_type", "payload", "run_at", "trace_id"}
	values := []columnValue{
		{data: "42"},
		{data: "2026-02-16 14:30:00.123456+00"},
		{data: "billing-service"},
		{data: "charge_customer"},
		{data: `{"amount":100}`},
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

func TestParseInsertColumns_NullRunAt(t *testing.T) {
	columns := []string{"id", "producer_id", "job_type", "payload", "run_at", "trace_id"}
	values := []columnValue{
		{data: "1"},
		{data: "svc"},
		{data: "do_thing"},
		{data: "{}"},
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
	}

	job := p.toJob()

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

	buf.addInsert(parsedInsert{id: 1, jobType: "a"})
	buf.addInsert(parsedInsert{id: 2, jobType: "b"})

	if len(buf.inserts) != 2 {
		t.Fatalf("expected 2 inserts, got %d", len(buf.inserts))
	}

	buf.reset()

	if len(buf.inserts) != 0 {
		t.Fatalf("expected 0 inserts after reset, got %d", len(buf.inserts))
	}

	// Ensure the underlying slice is reused (no allocation).
	buf.addInsert(parsedInsert{id: 3, jobType: "c"})
	if len(buf.inserts) != 1 {
		t.Fatalf("expected 1 insert after re-add, got %d", len(buf.inserts))
	}
}
