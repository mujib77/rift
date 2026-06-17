package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/mujib77/rift/internal/config"
	"github.com/mujib77/rift/internal/destination"
	"github.com/mujib77/rift/internal/source"
)

type Engine struct {
	cfg          *config.Config
	source       *source.PostgresSource
	destinations []destination.Destination
}

func New(cfg *config.Config) (*Engine, error) {
	src := source.New(cfg.Source)

	dests := []destination.Destination{}
	for _, destCfg := range cfg.Destinations {
		dest, err := destination.New(destCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create destination %s: %w", destCfg.Name, err)
		}
		dests = append(dests, dest)
		fmt.Printf("  destination ready: %s (%s)\n", destCfg.Name, destCfg.Type)
	}

	return &Engine{
		cfg:          cfg,
		source:       src,
		destinations: dests,
	}, nil
}

func (e *Engine) Start(ctx context.Context) error {
	fmt.Println("\n  ◆ RIFT — starting CDC pipeline")

	if err := e.source.Connect(ctx); err != nil {
		return err
	}
	defer e.source.Close(ctx)

	if err := e.source.Setup(ctx); err != nil {
		return err
	}

	if err := e.source.Start(ctx); err != nil {
		return err
	}

	fmt.Println("\n  ◆ listening for changes...")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n  shutting down...")
			return nil
		default:
			event, err := e.source.NextEvent(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				fmt.Println("  error:", err)
				time.Sleep(time.Second)
				continue
			}

			if event == nil {
				continue
			}

			fmt.Printf("  [%s] %s\n", event.Operation, event.Table)

			e.sendToDestinations(ctx, event)
		}
	}
}

func (e *Engine) sendToDestinations(ctx context.Context, event *source.Event) {
	for _, dest := range e.destinations {
		err := dest.Send(ctx, event)
		if err != nil {
			fmt.Printf("  error sending to %s: %v\n", dest.Name(), err)
			continue
		}
		fmt.Printf("  ✓ sent to %s\n", dest.Name())
	}
}