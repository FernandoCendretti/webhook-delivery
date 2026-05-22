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

// SubmitResult carries the identifiers created by a successful Submit call.
type SubmitResult struct {
	DeliveryID uuid.UUID
	EventID    uuid.UUID
}

// Submit validates the endpoint exists, then atomically inserts an event and a
// scheduled delivery inside a single transaction.
func (s *EventService) Submit(ctx context.Context, endpointID uuid.UUID, payload json.RawMessage) (*domain.Delivery, error) {
	if _, err := s.endpoints.Get(ctx, endpointID); err != nil {
		return nil, err
	}

	var d *domain.Delivery
	err := store.InTx(ctx, s.pool, func(tx pgx.Tx) error {
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
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("submit event: %w", err)
	}
	return d, nil
}
