package source

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/mujib77/rift/internal/config"
)

type Event struct {
	Table     string
	Operation string
	Data      map[string]interface{}
	OldData   map[string]interface{}
	LSN       string
	Timestamp time.Time
}

type PostgresSource struct {
	cfg       config.SourceConfig
	conn      *pgconn.PgConn
	relations map[uint32]*pglogrepl.RelationMessage
}

func New(cfg config.SourceConfig) *PostgresSource {
	return &PostgresSource{
		cfg:       cfg,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}
}

func (p *PostgresSource) Connect(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, p.cfg.URL)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	p.conn = conn
	fmt.Println("  connected to postgres")
	return nil
}

func (p *PostgresSource) Setup(ctx context.Context) error {
	_, err := p.conn.Exec(ctx, fmt.Sprintf(
		`CREATE PUBLICATION %s FOR ALL TABLES`,
		p.cfg.Publication,
	)).ReadAll()
	if err != nil {
		fmt.Println("  publication may already exist, continuing...")
	}

	_, err = pglogrepl.CreateReplicationSlot(
		ctx,
		p.conn,
		p.cfg.Slot,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{},
	)
	if err != nil {
		fmt.Println("  slot may already exist, continuing...")
	}

	fmt.Println("  replication slot ready:", p.cfg.Slot)
	return nil
}

func (p *PostgresSource) Start(ctx context.Context) error {
	err := pglogrepl.StartReplication(
		ctx,
		p.conn,
		p.cfg.Slot,
		0,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", p.cfg.Publication),
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}
	fmt.Println("  replication started")
	return nil
}


func (p *PostgresSource) NextEvent(ctx context.Context) (*Event, error) {
	rawMsg, err := p.conn.ReceiveMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to receive message: %w", err)
	}


	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil, nil
	}

	if len(msg.Data) == 0 {
		return nil, nil
	}


	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return nil, nil
		}
		if pka.ReplyRequested {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, p.conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: pka.ServerWALEnd,
					ClientTime:       time.Now(),
				})
			if err != nil {
				return nil, fmt.Errorf("failed to send keepalive: %w", err)
			}
		}
		return nil, nil

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return nil, nil
		}

		logMsg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return nil, nil
		}

		return p.handleMessage(logMsg, xld.WALStart)
	}

	return nil, nil
}


func (p *PostgresSource) handleMessage(
	msg pglogrepl.Message,
	lsn pglogrepl.LSN,
) (*Event, error) {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		p.relations[m.RelationID] = m
		return nil, nil

	case *pglogrepl.InsertMessage:
		rel, ok := p.relations[m.RelationID]
		if !ok {
			return nil, nil
		}
		return &Event{
			Table:     rel.RelationName,
			Operation: "INSERT",
			Data:      decodeColumns(rel, m.Tuple),
			LSN:       lsn.String(),
			Timestamp: time.Now(),
		}, nil

	case *pglogrepl.UpdateMessage:
		rel, ok := p.relations[m.RelationID]
		if !ok {
			return nil, nil
		}
		event := &Event{
			Table:     rel.RelationName,
			Operation: "UPDATE",
			Data:      decodeColumns(rel, m.NewTuple),
			LSN:       lsn.String(),
			Timestamp: time.Now(),
		}
		if m.OldTuple != nil {
			event.OldData = decodeColumns(rel, m.OldTuple)
		}
		return event, nil

	case *pglogrepl.DeleteMessage:
		rel, ok := p.relations[m.RelationID]
		if !ok {
			return nil, nil
		}
		event := &Event{
			Table:     rel.RelationName,
			Operation: "DELETE",
			LSN:       lsn.String(),
			Timestamp: time.Now(),
		}
		if m.OldTuple != nil {
			event.OldData = decodeColumns(rel, m.OldTuple)
		}
		return event, nil
	}

	return nil, nil
}


func (p *PostgresSource) Close(ctx context.Context) {
	if p.conn != nil {
		p.conn.Close(ctx)
	}
}


func decodeColumns(
	rel *pglogrepl.RelationMessage,
	tuple *pglogrepl.TupleData,
) map[string]interface{} {
	data := make(map[string]interface{})
	if tuple == nil {
		return data
	}
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n':
			data[colName] = nil
		case 't':
			data[colName] = string(col.Data)
		}
	}
	return data
}
