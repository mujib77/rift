package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mujib77/rift/internal/config"
	"github.com/mujib77/rift/internal/destination"
	"github.com/mujib77/rift/internal/queue"
	"github.com/mujib77/rift/internal/source"
)

type Engine struct {
	cfg          *config.Config
	source       *source.PostgresSource
	destinations []destination.Destination
	queue        *queue.Queue
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

	eng := &Engine{
		cfg:          cfg,
		source:       src,
		destinations: dests,
	}

	if cfg.Queue.Enabled {
		q, err := queue.New(cfg.Queue.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to create queue: %w", err)
		}
		eng.queue = q
		fmt.Printf("  disk queue enabled: %s\n", cfg.Queue.Path)
	}

	return eng, nil
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

	if e.queue != nil {
		go e.drainQueue(ctx)
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

			if e.queue != nil {
				err := e.queue.Push(event)
				if err != nil {
					fmt.Println("  error queuing event:", err)
				}
			} else {
				e.sendToDestinations(ctx, event)
			}
		}
	}
}

func (e *Engine) drainQueue(ctx context.Context) {
	fmt.Println("  queue drainer started")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if e.queue.Len() == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			queued, err := e.queue.Pop()
			if err != nil {
				fmt.Println("  error popping from queue:", err)
				time.Sleep(time.Second)
				continue
			}

			if queued == nil {
				continue
			}

			var event source.Event
			err = json.Unmarshal(queued.Payload, &event)
			if err != nil {
				fmt.Println("  error unmarshaling event:", err)
				continue
			}

			success := true
			for _, dest := range e.destinations {
				err := dest.Send(ctx, &event)
				if err != nil {
					fmt.Printf("  error sending to %s: %v — requeueing\n", dest.Name(), err)
					e.queue.Push(event)
					success = false
					time.Sleep(5 * time.Second)
					break
				}
			}

			if success {
				fmt.Printf("  ✔ drained event [%s] %s\n", event.Operation, event.Table)
			}
		}
	}
}

func (e *Engine) sendToDestinations(ctx context.Context, event *source.Event) {
	for _, dest := range e.destinations {
		err := dest.Send(ctx, event)
		if err != nil {
			fmt.Printf("  error sending to %s: %v\n", dest.Name(), err)
			if e.queue != nil {
				e.queue.Push(event)
				fmt.Println("  event queued for retry")
			}
			continue
		}
		fmt.Printf("  ✓ sent to %s\n", dest.Name())
	}
}