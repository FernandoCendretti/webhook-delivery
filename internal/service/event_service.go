package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// EventService handles event submission, creating both the event and its
// initial delivery record inside a single database transaction.
type EventService struct {
	pool      *pgxpool.Pool
	endpoints *EndpointService
}

// NewEventService constructs an EventService that uses pool for transactional
// writes and endpoints for endpoint validation.
func NewEventService(pool *pgxpool.Pool, endpoints *EndpointService) *EventService {
	return &EventService{pool: pool, endpoints: endpoints}
}

// Submit validates the endpoint, applies idempotency deduplication when
// idempotencyKey is non-empty, then atomically inserts an event and a
// scheduled delivery.
//
// rawBody is the unmodified request body used to compute the payload hash when
// idempotencyKey is set. Duplicate submissions (same key, same payload hash)
// return the original delivery without creating new rows. Same key with a
// different payload returns domain.ErrConflict.
func (s *EventService) Submit(ctx context.Context, endpointID uuid.UUID, payload json.RawMessage, idempotencyKey string, rawBody []byte) (*domain.Delivery, error) {
	if _, err := s.endpoints.Get(ctx, endpointID); err != nil {
		return nil, err
	}

	var d *domain.Delivery
	err := store.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		idem := store.NewIdempotencyTxStore(tx)

		if idempotencyKey != "" {
			if err := idem.AcquireAdvisoryLock(ctx, endpointID, idempotencyKey); err != nil {
				return err
			}
			rec, err := idem.Lookup(ctx, endpointID, idempotencyKey)
			if err != nil {
				return err
			}
			if rec != nil && rec.Complete {
				payloadHash := store.PayloadHash(rawBody)
				if rec.PayloadHash != payloadHash {
					return domain.ErrConflict
				}
				// Return cached result — no writes needed; signal to skip the commit.
				d = &domain.Delivery{ID: rec.DeliveryID, EventID: rec.EventID, EndpointID: endpointID}
				return errCachedResult
			}
			payloadHash := store.PayloadHash(rawBody)
			if err := idem.Claim(ctx, endpointID, idempotencyKey, payloadHash); err != nil {
				return err
			}
		}

		var eventID uuid.UUID
		const insertEvent = `
			INSERT INTO events (endpoint_id, payload) VALUES ($1, $2) RETURNING id`
		if err := tx.QueryRow(ctx, insertEvent, endpointID, payload).Scan(&eventID); err != nil {
			return fmt.Errorf("insert event: %w", err)
		}

		const insertDelivery = `
			INSERT INTO deliveries (event_id, endpoint_id, status, attempt_count, next_attempt_at)
			VALUES ($1, $2, 'scheduled', 0, NOW())
			RETURNING id, event_id, endpoint_id, status::text, attempt_count,
			          next_attempt_at, in_flight_lease_until, created_at, updated_at`
		var tmp domain.Delivery
		if err := tx.QueryRow(ctx, insertDelivery, eventID, endpointID).Scan(
			&tmp.ID, &tmp.EventID, &tmp.EndpointID, &tmp.Status, &tmp.AttemptCount,
			&tmp.NextAttemptAt, &tmp.InFlightLeaseUntil, &tmp.CreatedAt, &tmp.UpdatedAt,
		); err != nil {
			return fmt.Errorf("insert delivery: %w", err)
		}
		d = &tmp

		if idempotencyKey != "" {
			if err := idem.Complete(ctx, endpointID, idempotencyKey, eventID, tmp.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if err == errCachedResult {
			return d, nil
		}
		return nil, fmt.Errorf("submit event: %w", err)
	}
	return d, nil
}

// errCachedResult is a sentinel returned from within InTx to signal that a
// completed idempotency record was found. InTx rolls back (no writes are
// needed) and Submit returns the cached delivery.
var errCachedResult = fmt.Errorf("idempotency: cached result")
