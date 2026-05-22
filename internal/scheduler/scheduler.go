package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/observability"
	"github.com/FernandoCendretti/webhook-delivery/internal/queue"
)

// deliveryStore is the subset of store.DeliveryStore used by the scheduler.
type deliveryStore interface {
	ClaimReady(ctx context.Context, batch int, leaseUntil time.Time) ([]domain.Delivery, error)
}

// kafkaMessage is the payload published to the Kafka delivery topic.
type kafkaMessage struct {
	DeliveryID uuid.UUID `json:"delivery_id"`
	EndpointID uuid.UUID `json:"endpoint_id"`
}

// Config holds the dependencies and tuning parameters for a Scheduler.
type Config struct {
	DeliveryStore deliveryStore
	Publisher     *queue.Publisher
	BatchSize     int
	LeaseDuration time.Duration
	Metrics       *observability.Metrics
	Logger        *slog.Logger
}

// Scheduler claims batches of ready deliveries from the store and publishes
// them to Kafka for workers to process.
type Scheduler struct {
	cfg Config
}

// New constructs a Scheduler; zero BatchSize defaults to 100 and zero
// LeaseDuration defaults to 60 s.
func New(cfg Config) *Scheduler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 60 * time.Second
	}
	return &Scheduler{cfg: cfg}
}

// Tick claims one batch of ready deliveries and publishes each to Kafka.
func (s *Scheduler) Tick(ctx context.Context) error {
	leaseUntil := time.Now().Add(s.cfg.LeaseDuration)
	deliveries, err := s.cfg.DeliveryStore.ClaimReady(ctx, s.cfg.BatchSize, leaseUntil)
	if err != nil {
		return fmt.Errorf("claim ready: %w", err)
	}
	for _, d := range deliveries {
		msg, err := json.Marshal(kafkaMessage{DeliveryID: d.ID, EndpointID: d.EndpointID})
		if err != nil {
			return fmt.Errorf("marshal kafka message: %w", err)
		}
		if err := s.cfg.Publisher.Publish(ctx, []byte(d.EndpointID.String()), msg); err != nil {
			return fmt.Errorf("publish delivery %s: %w", d.ID, err)
		}
	}

	if m := s.cfg.Metrics; m != nil && len(deliveries) > 0 {
		m.SchedulerClaimed.Add(float64(len(deliveries)))
	}
	s.cfg.Logger.InfoContext(ctx, "scheduler tick", "claimed", len(deliveries))
	return nil
}

// Run loops until ctx is cancelled, calling Tick every tickInterval.
func (s *Scheduler) Run(ctx context.Context, tickInterval time.Duration) error {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil {
				s.cfg.Logger.ErrorContext(ctx, "scheduler tick error", "err", err)
			}
		}
	}
}
