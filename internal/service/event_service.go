package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// ErrTenantEndpointMismatch is returned when the supplied tenant_id does not match
// the endpoint's tenant (US2, FR-007).
// Reuse the sentinel already declared in endpoint_service.go — same package.

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

// Submit validates the endpoint + tenant, applies idempotency deduplication when
// idempotencyKey is non-empty, then atomically inserts an event and a scheduled
// delivery. tenantID must match the endpoint's tenant (FR-007).
//
// rawBody is the unmodified request body used to compute the payload hash when
// idempotencyKey is set. Duplicate submissions (same key, same payload hash)
// return the original delivery without creating new rows. Same key with a
// different payload returns domain.ErrConflict.
func (s *EventService) Submit(ctx context.Context, endpointID uuid.UUID, payload json.RawMessage, idempotencyKey string, rawBody []byte, tenantID uuid.UUID) (*domain.Delivery, error) {
	ep, err := s.endpoints.Get(ctx, endpointID)
	if err != nil {
		return nil, err
	}

	// Validate tenant exists.
	if s.endpoints.tenants != nil {
		if _, err := s.endpoints.tenants.GetByID(ctx, tenantID); err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, ErrTenantNotFound
			}
			return nil, fmt.Errorf("validate tenant: %w", err)
		}
	}

	// Validate tenant matches endpoint.
	if ep.TenantID != tenantID {
		return nil, ErrTenantEndpointMismatch
	}

	var d *domain.Delivery
	err = store.InTx(ctx, s.pool, func(tx pgx.Tx) error {
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
			INSERT INTO events (endpoint_id, tenant_id, payload) VALUES ($1, $2, $3) RETURNING id`
		if err := tx.QueryRow(ctx, insertEvent, endpointID, tenantID, payload).Scan(&eventID); err != nil {
			return fmt.Errorf("insert event: %w", err)
		}

		const insertDelivery = `
			INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at)
			VALUES ($1, $2, $3, 'scheduled', 0, NOW())
			RETURNING id, event_id, endpoint_id, status::text, attempt_count,
			          next_attempt_at, in_flight_lease_until, created_at, updated_at`
		var tmp domain.Delivery
		if err := tx.QueryRow(ctx, insertDelivery, eventID, endpointID, tenantID).Scan(
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
