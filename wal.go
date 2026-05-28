package pglg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// lsn is a type alias for pglogrepl.LSN used throughout the driver.
type lsn = pglogrepl.LSN

// walCon is a minimal subset of *pgconn.PgConn so tests can inject a fake.
// Production always holds a *pgconn.PgConn.
type walConn interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	Close(ctx context.Context) error
}

var (
	errStandbyTimeout = errors.New("standby timeout")
	errSlotBusy       = errors.New("pglg: replication slot in use by another consumer")
)

func mapStartReplicationError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ObjectInUse {
		return errSlotBusy
	}

	return fmt.Errorf("pglg: start replication: %w", err)
}

// walConsumer manages a logical replication connection to PostgreSQL.
type walConsumer struct {
	conn            walConn
	slotName        string
	publicationName string

	// relations maps relation OID to metadata for parsing INSERT messages.
	relations map[uint32]*relationMeta

	// clientXLogPos tracks the position we've received up to.
	clientXLogPos lsn

	// flushLSN tracks the position we've durably processed and persisted.
	flushLSN lsn
}

type relationMeta struct {
	name    string
	columns []string
}

func newWALConsumer(slotName, publicationName string) *walConsumer {
	return &walConsumer{
		slotName:        slotName,
		publicationName: publicationName,
		relations:       make(map[uint32]*relationMeta),
	}
}

// connect establishes a replication-mode connection to PostgreSQL.
func (w *walConsumer) connect(ctx context.Context, connString string) error {
	// pgconn requires the replication parameter in the connection string.
	replConn := connString
	if replConn != "" && replConn[len(replConn)-1] != ' ' {
		replConn += " "
	}
	replConn += "replication=database"

	conn, err := pgconn.Connect(ctx, replConn)
	if err != nil {
		return fmt.Errorf("pglg: replication connect: %w", err)
	}
	w.conn = conn
	return nil
}

// startStreaming begins logical replication from the given LSN.
func (w *walConsumer) startStreaming(ctx context.Context, startLSN lsn) error {
	pgConn, ok := w.conn.(*pgconn.PgConn)
	if !ok { // this won't happen in production,
		return fmt.Errorf("pglg: invalid connection type")
	}

	if _, err := pglogrepl.IdentifySystem(ctx, pgConn); err != nil {
		return fmt.Errorf("pglg: identify system: %w", err)
	}

	err := pglogrepl.StartReplication(ctx, pgConn, w.slotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				fmt.Sprintf("publication_names '%s'", w.publicationName),
			},
		})
	if err != nil {
		return mapStartReplicationError(err)
	}

	w.clientXLogPos = startLSN
	w.flushLSN = startLSN
	return nil
}

// receiveMessage receives the next WAL data payload. Returns nil data for
// keepalive-only messages. Returns errStandbyTimeout when the deadline elapses
// with no message.
func (w *walConsumer) receiveMessage(ctx context.Context, deadline time.Time) ([]byte, error) {
	receiveCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	rawMsg, err := w.conn.ReceiveMessage(receiveCtx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil, errStandbyTimeout
		}
		return nil, fmt.Errorf("pglg: receive message: %w", err)
	}

	copyData, ok := rawMsg.(*pgproto3.CopyData)
	if !ok || len(copyData.Data) < 1 {
		return nil, nil
	}

	switch copyData.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
		if err != nil {
			return nil, fmt.Errorf("pglg: parse keepalive: %w", err)
		}
		if lsn(pkm.ServerWALEnd) > w.clientXLogPos {
			w.clientXLogPos = lsn(pkm.ServerWALEnd)
		}
		if pkm.ReplyRequested {
			if sendErr := w.sendStandbyStatus(ctx); sendErr != nil {
				return nil, sendErr
			}
		}
		return nil, nil

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			return nil, fmt.Errorf("pglg: parse xlog data: %w", err)
		}
		endPos := xld.WALStart + lsn(len(xld.WALData))
		if endPos > w.clientXLogPos {
			w.clientXLogPos = endPos
		}
		return xld.WALData, nil
	}

	return nil, nil
}

// sendStandbyStatus sends a status update to PostgreSQL.
func (w *walConsumer) sendStandbyStatus(ctx context.Context) error {
	// Use a separate timeout context to avoid losing the status update
	// if the parent context is being cancelled during shutdown.
	statusCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	return pglogrepl.SendStandbyStatusUpdate(statusCtx, w.conn.(*pgconn.PgConn), w.standbyStatusUpdate(time.Now().UTC()))
}

func (w *walConsumer) standbyStatusUpdate(now time.Time) pglogrepl.StandbyStatusUpdate {
	writeLSN := w.clientXLogPos
	if w.flushLSN == 0 {
		// flushLSN is 0 only when the cursor table has no row for this slot
		// and we have not flushed anything yet in this session. This is the
		// first-ever run on a fresh(the initial) deployment.
		//
		// pglogrepl replaces a zero flush/apply with the write position. If
		// we sent {Write: clientXLogPos, Flush: 0, Apply: 0}, it would put
		// clientXLogPos in all three fields. That would tell PG we durably
		// flushed everything we received, which is a lie, and PG would
		// reclaim WAL we have not handled.
		//
		// Send all zeros instead. PG does not advance confirmed_flush_lsn,
		// and no WAL is reclaimed until ther first real flush sets a
		// non zero flushLSN.
		writeLSN = 0
	} else if writeLSN < w.flushLSN {
		writeLSN = w.flushLSN
	}

	return pglogrepl.StandbyStatusUpdate{
		WALWritePosition: writeLSN,
		WALFlushPosition: w.flushLSN,
		WALApplyPosition: w.flushLSN,
		ClientTime:       now,
	}
}

func (w *walConsumer) setFlushLSN(l lsn) {
	if l > w.flushLSN {
		w.flushLSN = l
	}
}

func (w *walConsumer) writePos() lsn {
	return w.clientXLogPos
}

func (w *walConsumer) receivedPastFlush() bool {
	return w.clientXLogPos > w.flushLSN
}

// close terminates the replication connection.
func (w *walConsumer) close(ctx context.Context) error {
	if w.conn != nil {
		return w.conn.Close(ctx)
	}
	return nil
}

// setRelation stores relation metadata for a given OID.
func (w *walConsumer) setRelation(id uint32, name string, columns []string) {
	w.relations[id] = &relationMeta{name: name, columns: columns}
}

// parseInsert converts a walInsert event into a parsedInsert using stored
// relation metadata. Returns nil if the relation is unknown or not the outbox table.
func (w *walConsumer) parseInsert(ins *walInsert) (*parsedInsert, error) {
	rel, ok := w.relations[ins.relationID]
	if !ok {
		return nil, nil // unknown relation, skip
	}
	return parseInsertColumns(rel.columns, ins.values)
}
