package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// AttemptStore provides persistence for delivery attempt records.
type AttemptStore struct {
	pool *pgxpool.Pool
}

// NewAttemptStore constructs an AttemptStore backed by the given connection pool.
func NewAttemptStore(pool *pgxpool.Pool) *AttemptStore {
	return &AttemptStore{pool: pool}
}

// InsertStarted records the start of a delivery attempt. outcome is set to a
// placeholder value ('transient_failure') that UpdateOutcome overwrites on
// completion. This satisfies the NOT NULL constraint while allowing the worker
// to capture the start timestamp before the HTTP call is made.
func (s *AttemptStore) InsertStarted(ctx context.Context, deliveryID uuid.UUID, sequence int) (uuid.UUID, error) {
	const q = `
		INSERT INTO attempts (delivery_id, sequence, started_at, outcome)
		VALUES ($1, $2, NOW(), 'transient_failure')
		RETURNING id`
	var id uuid.UUID
	if err := s.pool.QueryRow(ctx, q, deliveryID, sequence).Scan(&id); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// ListByDelivery returns all attempt records for the given delivery, ordered by
// sequence ascending.
func (s *AttemptStore) ListByDelivery(ctx context.Context, deliveryID uuid.UUID) ([]domain.Attempt, error) {
	const q = `
		SELECT id, delivery_id, sequence, started_at, completed_at,
		       response_status_code, outcome::text, error_reason
		FROM attempts WHERE delivery_id = $1 ORDER BY sequence ASC`
	rows, err := s.pool.Query(ctx, q, deliveryID)
	if err != nil {
		return nil, fmt.Errorf("list attempts by delivery: %w", err)
	}
	defer rows.Close()

	var out []domain.Attempt
	for rows.Next() {
		var a domain.Attempt
		var outcome string
		if err := rows.Scan(
			&a.ID, &a.DeliveryID, &a.Sequence, &a.StartedAt, &a.CompletedAt,
			&a.ResponseStatusCode, &outcome, &a.ErrorReason,
		); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		a.Outcome = domain.AttemptOutcome(outcome)
		out = append(out, a)
	}
	return out, rows.Err()
}

// UpdateOutcome finalises an attempt record after the HTTP call completes.
func (s *AttemptStore) UpdateOutcome(
	ctx context.Context,
	id uuid.UUID,
	outcome domain.AttemptOutcome,
	statusCode *int,
	errorReason *string,
) error {
	now := time.Now()
	const q = `
		UPDATE attempts
		SET outcome = $2, completed_at = $3,
		    response_status_code = $4, error_reason = $5
		WHERE id = $1`
	_, err := s.pool.Exec(ctx, q, id, string(outcome), now, statusCode, errorReason)
	return err
}
