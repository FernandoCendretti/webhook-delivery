package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/FernandoCendretti/webhook-delivery/internal/observability"
)

// leaseResurrector is the subset of DeliveryStore used by the Reaper.
type leaseResurrector interface {
	ResurrectExpiredLeases(ctx context.Context) (int, error)
}

// Config holds the dependencies and tuning parameters for a Reaper.
type Config struct {
	Store    leaseResurrector
	Interval time.Duration
	Metrics  *observability.Metrics
	Logger   *slog.Logger
}

// Reaper periodically resets in_flight deliveries whose lease has expired so
// they become eligible for re-processing by the scheduler.
type Reaper struct {
	cfg Config
}

// New constructs a Reaper; zero Interval defaults to 60 s.
func New(cfg Config) *Reaper {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	return &Reaper{cfg: cfg}
}

// Tick runs one reaper pass: resurrects all expired in_flight deliveries.
func (r *Reaper) Tick(ctx context.Context) error {
	n, err := r.cfg.Store.ResurrectExpiredLeases(ctx)
	if err != nil {
		return fmt.Errorf("reaper tick: %w", err)
	}
	if n > 0 {
		r.cfg.Logger.InfoContext(ctx, "reaper resurrected deliveries", "count", n)
		if r.cfg.Metrics != nil {
			r.cfg.Metrics.DeliveryLeaseResurrected.Add(float64(n))
		}
	}
	return nil
}

// Run loops until ctx is cancelled, calling Tick every cfg.Interval.
func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.Tick(ctx); err != nil {
				r.cfg.Logger.ErrorContext(ctx, "reaper error", "err", err)
			}
		}
	}
}
