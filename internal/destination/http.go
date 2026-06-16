package destination

import (
	"context"
	"github.com/mujib77/rift/internal/config"
	"github.com/mujib77/rift/internal/source"
)

type HTTPDestination struct {
	webhook *WebhookDestination
}

func NewHTTP(cfg config.DestinationConfig) *HTTPDestination {
	return &HTTPDestination{
		webhook: NewWebhook(cfg),
	}
}

func (h *HTTPDestination) Name() string {
	return h.webhook.Name()
}

func (h *HTTPDestination) Send(ctx context.Context, event *source.Event) error {
	return h.webhook.Send(ctx, event)
}

func (h *HTTPDestination) Close() error {
	return nil
}