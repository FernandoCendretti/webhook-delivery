package store

import (
	"context"
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
