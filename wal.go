package pglg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// lsn is a type alias for pglogrepl.LSN used throughout the driver.
type lsn = pglogrepl.LSN

var errStandbyTimeout = errors.New("standby timeout")

// walConsumer manages a logical replication connection to PostgreSQL.
type walConsumer struct {
	conn            *pgconn.PgConn
	slotName        string
	publicationName string
	standbyInterval time.Duration

	// relations maps relation OID to metadata for parsing INSERT messages.
	relations map[uint32]*relationMeta

	// clientXLogPos tracks the position we've received up to.
	clientXLogPos lsn
}

type relationMeta struct {
	name    string
	columns []string
}

func newWALConsumer(slotName, publicationName string, standbyInterval time.Duration) *walConsumer {
	return &walConsumer{
		slotName:        slotName,
		publicationName: publicationName,
		standbyInterval: standbyInterval,
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
	if _, err := pglogrepl.IdentifySystem(ctx, w.conn); err != nil {
		return fmt.Errorf("pglg: identify system: %w", err)
	}

	err := pglogrepl.StartReplication(ctx, w.conn, w.slotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				fmt.Sprintf("publication_names '%s'", w.publicationName),
			},
		})
	if err != nil {
		return fmt.Errorf("pglg: start replication: %w", err)
	}

	w.clientXLogPos = startLSN
	return nil
}

// receiveMessage receives the next WAL data payload. Returns nil data for
// keepalive-only messages. Returns errStandbyTimeout when the standby interval
// elapses with no message.
func (w *walConsumer) receiveMessage(ctx context.Context) ([]byte, error) {
	deadline := time.Now().Add(w.standbyInterval)
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
	return pglogrepl.SendStandbyStatusUpdate(statusCtx, w.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: w.clientXLogPos,
			WALFlushPosition: w.clientXLogPos,
			WALApplyPosition: w.clientXLogPos,
			ClientTime:       time.Now().UTC(),
		})
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
