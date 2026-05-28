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

// circuitStore is the subset of store.CircuitStore used by the scheduler.
type circuitStore interface {
	OrphanedHalfOpenEndpoints(ctx context.Context) ([]uuid.UUID, error)
	ProcessExpiredSuspensions(ctx context.Context) (halfOpenIDs []uuid.UUID, closedIDs []uuid.UUID, err error)
	SetProbeDelivery(ctx context.Context, endpointID uuid.UUID) error
}

// kafkaMessage is the payload published to the Kafka delivery topic.
type kafkaMessage struct {
	DeliveryID uuid.UUID `json:"delivery_id"`
	EndpointID uuid.UUID `json:"endpoint_id"`
}

// Config holds the dependencies and tuning parameters for a Scheduler.
type Config struct {
	DeliveryStore deliveryStore
	CircuitStore  circuitStore // optional; when nil circuit-breaker steps are skipped
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
// Steps 0a and 0b run first when a CircuitStore is configured.
func (s *Scheduler) Tick(ctx context.Context) error {
	if cs := s.cfg.CircuitStore; cs != nil {
		// Step 0a: recover orphaned half_open endpoints (scheduler crash guard).
		orphans, err := cs.OrphanedHalfOpenEndpoints(ctx)
		if err != nil {
			s.cfg.Logger.WarnContext(ctx, "find orphaned half_open", "err", err)
		} else {
			for _, id := range orphans {
				if err := cs.SetProbeDelivery(ctx, id); err != nil {
					s.cfg.Logger.WarnContext(ctx, "set probe delivery (recovery)", "endpoint", id, "err", err)
				}
			}
		}

		// Step 0b: transition expired open circuits and assign probes.
		halfOpenIDs, _, err := cs.ProcessExpiredSuspensions(ctx)
		if err != nil {
			s.cfg.Logger.WarnContext(ctx, "process expired suspensions", "err", err)
		} else {
			for _, id := range halfOpenIDs {
				if err := cs.SetProbeDelivery(ctx, id); err != nil {
					s.cfg.Logger.WarnContext(ctx, "set probe delivery", "endpoint", id, "err", err)
				}
			}
		}
	}

	// Step 1: claim eligible deliveries and publish to Kafka.
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
