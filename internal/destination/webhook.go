package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/mujib77/rift/internal/config"
	"github.com/mujib77/rift/internal/source"
)

type WebhookDestination struct {
	cfg    config.DestinationConfig
	client *http.Client
}

func NewWebhook(cfg config.DestinationConfig) *WebhookDestination {
	return &WebhookDestination{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (w *WebhookDestination) Name() string {
	return w.cfg.Name
}

type WebhookPayload struct {
	Table     string  `json:"table"`
	Operation string  `json:"operation"`
	Data      map[string]interface{} `json:"data"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
	LSN       string   `json:"lsn"`
	Timestamp string   `json:"timestamp"`
}

func (w *WebhookDestination) Send(ctx context.Context, event *source.Event) error {
	payload := WebhookPayload{
		Table:     event.Table,
		Operation: event.Operation,
		Data:      event.Data,
		OldData:   event.OldData,
		LSN:       event.LSN,
		Timestamp: event.Timestamp.UTC().Format(time.RFC3339),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx, "POST", w.cfg.URL,
		bytes.NewBuffer(body),
	)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Rift-Event", event.Operation)
	req.Header.Set("X-Rift-Table", event.Table)

	for key, val := range w.cfg.Headers {
		req.Header.Set(key, val)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}

	return nil
}

func (w *WebhookDestination) Close() error {
	return nil
}