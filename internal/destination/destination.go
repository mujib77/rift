package destination

import (
	"context"
	"fmt"
    "github.com/mujib77/rift/internal/config"
	"github.com/mujib77/rift/internal/source"
)

type Destination interface {
	Name() string
	Send(ctx context.Context, event *source.Event) error
	Close() error
}

func New(cfg config.DestinationConfig) (Destination, error) {
	switch cfg.Type {
	case "webhook":
		return NewWebhook(cfg), nil
	case "http":
		return NewHTTP(cfg), nil
	case "postgres":
		dest := NewPostgres(cfg)
		if err := dest.Connect(context.Background()); err != nil {
			return nil, err
		}
		return dest, nil

	case "redis":
    return NewRedis(cfg)
	
	default:
		return nil, fmt.Errorf("unknown destination type: %s", cfg.Type)
	}
}