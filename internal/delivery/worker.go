package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/config"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/observability"
	"github.com/FernandoCendretti/webhook-delivery/internal/queue"
	"github.com/FernandoCendretti/webhook-delivery/internal/signing"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

type deliveryStore interface {
	LoadForWorker(ctx context.Context, deliveryID uuid.UUID) (*store.WorkerDelivery, error)
	MarkDelivered(ctx context.Context, id uuid.UUID) error
	MarkPermanentlyFailed(ctx context.Context, id uuid.UUID) error
	Reschedule(ctx context.Context, id uuid.UUID, nextAt time.Time) error
}

type attemptStore interface {
	InsertStarted(ctx context.Context, deliveryID uuid.UUID, sequence int) (uuid.UUID, error)
	UpdateOutcome(ctx context.Context, id uuid.UUID, outcome domain.AttemptOutcome, statusCode *int, errorReason *string) error
}

// workerCircuitStore is the subset of store.CircuitStore used by the worker.
type workerCircuitStore interface {
	HandleSuccess(ctx context.Context, endpointID uuid.UUID) error
	HandleTransientFailure(ctx context.Context, endpointID uuid.UUID, cfg config.CircuitConfig) error
	HandleProbePermanentFailure(ctx context.Context, endpointID uuid.UUID, cfg config.CircuitConfig) error
}

type scheduledMessage struct {
	DeliveryID uuid.UUID `json:"delivery_id"`
	EndpointID uuid.UUID `json:"endpoint_id"`
}

// WorkerConfig holds the dependencies required to construct a Worker.
type WorkerConfig struct {
	DeliveryStore deliveryStore
	AttemptStore  attemptStore
	Consumer      *queue.Consumer
	Pool          *pgxpool.Pool
	CircuitStore  workerCircuitStore   // optional; when nil circuit-breaker calls are skipped
	CircuitCfg    config.CircuitConfig // used only when CircuitStore is non-nil
	HTTPClient    *http.Client
	Metrics       *observability.Metrics
	Logger        *slog.Logger
}

// Worker consumes delivery messages from Kafka, makes the outbound HTTP call,
// and persists the attempt outcome.
type Worker struct {
	cfg WorkerConfig
}

// NewWorker constructs a Worker; nil Logger and HTTPClient are replaced with
// sensible defaults.
func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = NewHTTPClient()
	}
	return &Worker{cfg: cfg}
}

// ProcessOne fetches one Kafka message, delivers the webhook, persists the
// outcome, and commits the offset — in that order. Offset is committed only
// after the Postgres transaction commits (at-least-once guarantee).
func (w *Worker) ProcessOne(ctx context.Context) error {
	msg, err := w.cfg.Consumer.FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("fetch message: %w", err)
	}

	var sm scheduledMessage
	if err := json.Unmarshal(msg.Value, &sm); err != nil {
		// Poison pill: commit and skip.
		_ = w.cfg.Consumer.Commit(ctx, msg)
		return fmt.Errorf("unmarshal kafka message: %w", err)
	}

	if err := w.process(ctx, sm.DeliveryID); err != nil {
		w.cfg.Logger.ErrorContext(ctx, "process delivery failed",
			"delivery_id", sm.DeliveryID, "err", err)
		return err
	}

	return w.cfg.Consumer.Commit(ctx, msg)
}

func (w *Worker) process(ctx context.Context, deliveryID uuid.UUID) error {
	wd, err := w.cfg.DeliveryStore.LoadForWorker(ctx, deliveryID)
	if err != nil {
		return fmt.Errorf("load delivery %s: %w", deliveryID, err)
	}

	attemptSeq := wd.Delivery.AttemptCount + 1
	attemptID, err := w.cfg.AttemptStore.InsertStarted(ctx, deliveryID, attemptSeq)
	if err != nil {
		return fmt.Errorf("insert attempt: %w", err)
	}

	startedAt := time.Now()
	resp, httpErr := w.doHTTP(ctx, wd.EndpointURL, wd.Payload, wd.SigningSecret)
	elapsed := time.Since(startedAt)

	outcome := Classify(resp, httpErr)

	var statusCode *int
	var errorReason *string
	if resp != nil {
		c := resp.StatusCode
		statusCode = &c
		_ = resp.Body.Close()
	}
	if httpErr != nil {
		s := httpErr.Error()
		errorReason = &s
	}

	if err := w.cfg.AttemptStore.UpdateOutcome(ctx, attemptID, outcome, statusCode, errorReason); err != nil {
		w.cfg.Logger.ErrorContext(ctx, "update attempt outcome failed", "attempt_id", attemptID, "err", err)
	}

	epID := wd.Delivery.EndpointID.String()
	if m := w.cfg.Metrics; m != nil {
		m.DeliveryAttempts.WithLabelValues(epID, string(outcome)).Inc()
		m.DeliveryAttemptDuration.WithLabelValues(epID).Observe(elapsed.Seconds())
	}

	endpointID := wd.Delivery.EndpointID

	// Circuit breaker updates (FR-010–FR-018).
	if cs := w.cfg.CircuitStore; cs != nil {
		switch outcome {
		case domain.OutcomeSuccess:
			if err := cs.HandleSuccess(ctx, endpointID); err != nil {
				w.cfg.Logger.WarnContext(ctx, "circuit HandleSuccess", "endpoint", endpointID, "err", err)
			}
		case domain.OutcomePermanentFailure:
			// Permanent failures only update the circuit when the endpoint is in half_open
			// (probe failure); in closed state the counter is NOT incremented (FR-011).
			if wd.EndpointCircuitState == "half_open" {
				if err := cs.HandleProbePermanentFailure(ctx, endpointID, w.cfg.CircuitCfg); err != nil {
					w.cfg.Logger.WarnContext(ctx, "circuit HandleProbePermanentFailure", "endpoint", endpointID, "err", err)
				}
			}
		default: // transient_failure or timeout
			if err := cs.HandleTransientFailure(ctx, endpointID, w.cfg.CircuitCfg); err != nil {
				w.cfg.Logger.WarnContext(ctx, "circuit HandleTransientFailure", "endpoint", endpointID, "err", err)
			}
		}
	}

	// Delivery state transitions.
	switch outcome {
	case domain.OutcomeSuccess:
		if m := w.cfg.Metrics; m != nil {
			m.EndpointFailureStreak.WithLabelValues(epID).Set(0)
		}
		return w.cfg.DeliveryStore.MarkDelivered(ctx, deliveryID)

	case domain.OutcomePermanentFailure:
		return w.cfg.DeliveryStore.MarkPermanentlyFailed(ctx, deliveryID)

	default: // transient_failure or timeout
		if m := w.cfg.Metrics; m != nil {
			m.EndpointFailureStreak.WithLabelValues(epID).Inc()
		}
		// attemptSeq is the 1-indexed number of the attempt we just ran.
		// If we've exhausted the budget, permanently fail.
		if attemptSeq >= domain.MaxAttempts {
			return w.cfg.DeliveryStore.MarkPermanentlyFailed(ctx, deliveryID)
		}
		delay := domain.Delay(attemptSeq + 1)
		return w.cfg.DeliveryStore.Reschedule(ctx, deliveryID, time.Now().Add(delay))
	}
}

func (w *Worker) doHTTP(ctx context.Context, url string, payload []byte, signingSecret []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	ts := time.Now().Unix()
	sig := signing.Sign(signingSecret, ts, payload)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Webhook-Signature", sig)
	return w.cfg.HTTPClient.Do(req)
}

// Run processes messages in a loop until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := w.ProcessOne(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.cfg.Logger.ErrorContext(ctx, "worker error", "err", err)
		}
	}
}
