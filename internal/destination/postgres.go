package destination

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/mujib77/rift/internal/config"
	"github.com/mujib77/rift/internal/source"
)

type PostgresDestination struct {
	cfg  config.DestinationConfig
	conn *pgx.Conn
}

func NewPostgres(cfg config.DestinationConfig) *PostgresDestination {
	return &PostgresDestination{cfg: cfg}
}

func (p *PostgresDestination) Name() string {
	return p.cfg.Name
}

func (p *PostgresDestination) Connect(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, p.cfg.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to destination postgres: %w", err)
	}
	p.conn = conn
	fmt.Printf("  connected to postgres destination: %s\n", p.cfg.Name)
	return nil
}

func (p *PostgresDestination) Send(ctx context.Context, event *source.Event) error {
	if p.conn == nil {
		return fmt.Errorf("not connected to destination")
	}

	switch event.Operation {
	case "INSERT":
		return p.handleInsert(ctx, event)
	case "UPDATE":
		return p.handleUpdate(ctx, event)
	case "DELETE":
		return p.handleDelete(ctx, event)
	}
	return nil
}

func (p *PostgresDestination) handleInsert(ctx context.Context, event *source.Event) error {
	if len(event.Data) == 0 {
		return nil
	}

	cols := []string{}
	vals := []interface{}{}
	placeholders := []string{}
	i := 1

	for k, v := range event.Data {
		cols = append(cols, k)
		vals = append(vals, v)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		i++
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		event.Table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := p.conn.Exec(ctx, query, vals...)
	return err
}

func (p *PostgresDestination) handleUpdate(ctx context.Context, event *source.Event) error {
	if len(event.Data) == 0 {
		return nil
	}

	sets := []string{}
	vals := []interface{}{}
	i := 1

	for k, v := range event.Data {
		sets = append(sets, fmt.Sprintf("%s = $%d", k, i))
		vals = append(vals, v)
		i++
	}

	id, ok := event.Data["id"]
	if !ok {
		return fmt.Errorf("no id field found for UPDATE")
	}

	vals = append(vals, id)
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE id = $%d",
		event.Table,
		strings.Join(sets, ", "),
		i,
	)

	_, err := p.conn.Exec(ctx, query, vals...)
	return err
}

func (p *PostgresDestination) handleDelete(ctx context.Context, event *source.Event) error {
	id, ok := event.OldData["id"]
	if !ok {
		return fmt.Errorf("no id field found for DELETE")
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", event.Table)
	_, err := p.conn.Exec(ctx, query, id)
	return err
}

func (p *PostgresDestination) Close() error {
	if p.conn != nil {
		p.conn.Close(context.Background())
	}
	return nil
}