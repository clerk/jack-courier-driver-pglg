package pglg

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	courier "github.com/clerk/jack-courier-lib"
	"github.com/jackc/pglogrepl"
)

// WAL message types used as internal representations after parsing.

type walRelation struct {
	id      uint32
	name    string
	columns []string
}

type walInsert struct {
	relationID uint32
	values     []columnValue
}

type columnValue struct {
	isNull bool
	data   string
}

type walCommit struct {
	commitLSN lsn
}

type walBegin struct{}

// parseWALMessage parses a raw pgoutput v2 WAL message into an internal type.
// The driver does not negotiate streaming, so pgoutput never emits stream
// messages and we don't decode them.
func parseWALMessage(walData []byte) (any, error) {
	msg, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		return nil, fmt.Errorf("pglg: parse WAL message: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		cols := make([]string, len(m.Columns))
		for i, c := range m.Columns {
			cols[i] = c.Name
		}
		return &walRelation{id: m.RelationID, name: m.RelationName, columns: cols}, nil

	case *pglogrepl.RelationMessage:
		cols := make([]string, len(m.Columns))
		for i, c := range m.Columns {
			cols[i] = c.Name
		}
		return &walRelation{id: m.RelationID, name: m.RelationName, columns: cols}, nil

	case *pglogrepl.InsertMessageV2:
		vals := make([]columnValue, len(m.Tuple.Columns))
		for i, col := range m.Tuple.Columns {
			vals[i] = columnValue{
				isNull: col.DataType == pglogrepl.TupleDataTypeNull,
				data:   string(col.Data),
			}
		}
		return &walInsert{relationID: m.RelationID, values: vals}, nil

	case *pglogrepl.CommitMessage:
		return &walCommit{commitLSN: m.TransactionEndLSN}, nil

	case *pglogrepl.BeginMessage:
		return &walBegin{}, nil

	case *pglogrepl.StreamStartMessageV2,
		*pglogrepl.StreamStopMessageV2,
		*pglogrepl.StreamCommitMessageV2,
		*pglogrepl.StreamAbortMessageV2:
		return nil, fmt.Errorf("pglg: received streaming WAL message %T but the driver does not negotiate streaming: check StartReplication PluginArgs", m)

	default:
		return nil, nil // ignore unhandled message types
	}
}

// parsedInsert holds fields extracted from an outbox row INSERT.
type parsedInsert struct {
	id         int64
	createdAt  time.Time
	producerID string
	jobType    string
	payload    []byte
	runAt      time.Time
	traceID    string
	shadow     bool
}

// toJob converts a parsedInsert to a courier.Job.
func (p *parsedInsert) toJob() courier.Job {
	return courier.Job{
		CorrelationID: strconv.FormatInt(p.id, 10),
		ProducerID:    p.producerID,
		JobType:       p.jobType,
		Payload:       p.payload,
		RunAt:         p.runAt,
		TraceID:       p.traceID,
		Shadow:        p.shadow,
	}
}

// parseInsertColumns extracts outbox fields from column names and values.
func parseInsertColumns(columns []string, values []columnValue) (*parsedInsert, error) {
	if len(columns) != len(values) {
		return nil, fmt.Errorf("pglg: column count %d != value count %d", len(columns), len(values))
	}

	row := &parsedInsert{}
	for i, colName := range columns {
		v := values[i]
		if v.isNull {
			continue
		}

		switch colName {
		case "id":
			n, err := strconv.ParseInt(v.data, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("pglg: parse id: %w", err)
			}
			row.id = n
		case "created_at":
			t, err := parseTimestamp(v.data)
			if err != nil {
				return nil, fmt.Errorf("pglg: parse created_at: %w", err)
			}
			row.createdAt = t
		case "producer_id":
			row.producerID = v.data
		case "job_type":
			row.jobType = v.data
		case "payload":
			b, err := decodeBytea(v.data)
			if err != nil {
				return nil, fmt.Errorf("pglg: decode payload: %w", err)
			}
			row.payload = b
		case "run_at":
			t, err := parseTimestamp(v.data)
			if err != nil {
				return nil, fmt.Errorf("pglg: parse run_at: %w", err)
			}
			row.runAt = t
		case "trace_id":
			row.traceID = v.data
		case "shadow":
			row.shadow = v.data == "t"
		}
	}

	return row, nil
}

// decodeBytea decodes pgoutput's text-mode BYTEA wire form: "\x" + hex.
// Inputs without the "\x" prefix pass through unchanged.
func decodeBytea(s string) ([]byte, error) {
	if len(s) >= 2 && s[0] == '\\' && s[1] == 'x' {
		return hex.DecodeString(s[2:])
	}
	return []byte(s), nil
}

// parseTimestamp parses a Postgres timestamp string.
func parseTimestamp(s string) (time.Time, error) {
	// Try common Postgres timestamp formats.
	formats := []string{
		"2006-01-02 15:04:05.999999+00",
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999Z",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05+00",
		"2006-01-02 15:04:05-07",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("pglg: unrecognized timestamp format: %q", s)
}

// txBuffer accumulates parsed inserts within a single WAL transaction.
type txBuffer struct {
	inserts []parsedInsert
}

func (tb *txBuffer) reset() {
	tb.inserts = tb.inserts[:0]
}

func (tb *txBuffer) empty() bool {
	return len(tb.inserts) == 0
}

func (tb *txBuffer) addInsert(p parsedInsert) {
	tb.inserts = append(tb.inserts, p)
}
