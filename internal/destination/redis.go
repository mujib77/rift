package destination

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mujib77/rift/internal/config"
	"github.com/mujib77/rift/internal/source"
	"github.com/redis/go-redis/v9"
)

type RedisDestination struct {
	cfg    config.DestinationConfig
	client *redis.Client
}

func NewRedis(cfg config.DestinationConfig) (*RedisDestination, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	fmt.Printf("  connected to redis destination: %s\n", cfg.Name)

	return &RedisDestination{
		cfg:    cfg,
		client: client,
	}, nil
}

func (r *RedisDestination) Name() string {
	return r.cfg.Name
}

func (r *RedisDestination) Send(ctx context.Context, event *source.Event) error {
	payload := map[string]interface{}{
		"table":     event.Table,
		"operation": event.Operation,
		"data":      event.Data,
		"old_data":  event.OldData,
		"lsn":       event.LSN,
		"timestamp": event.Timestamp.UTC().Format(time.RFC3339),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	channel := fmt.Sprintf("rift:%s:%s", event.Table, event.Operation)
	err = r.client.Publish(ctx, channel, data).Err()
	if err != nil {
		return fmt.Errorf("failed to publish to redis: %w", err)
	}

	listKey := fmt.Sprintf("rift:events:%s", event.Table)
	err = r.client.LPush(ctx, listKey, data).Err()
	if err != nil {
		return fmt.Errorf("failed to push to redis list: %w", err)
	}

	r.client.LTrim(ctx, listKey, 0, 999)

	return nil
}

func (r *RedisDestination) Close() error {
	return r.client.Close()
}