package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/config"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// CircuitStore manages circuit breaker state persisted in PostgreSQL.
type CircuitStore struct {
	pool *pgxpool.Pool
}

// NewCircuitStore constructs a CircuitStore backed by the given connection pool.
func NewCircuitStore(pool *pgxpool.Pool) *CircuitStore {
	return &CircuitStore{pool: pool}
}

// HandleSuccess records a successful delivery outcome.
// Transitions closed→closed (resets counter) or half_open→closed (enables
// sensitive recovery). No-op on open circuits.
func (s *CircuitStore) HandleSuccess(ctx context.Context, endpointID uuid.UUID) error {
	const q = `
		UPDATE endpoints SET
			circuit_failure_count    = 0,
			circuit_state            = 'closed',
			circuit_suspended_until  = NULL,
			circuit_probe_delivery_id = NULL,
			circuit_sensitive_recovery = CASE
				WHEN circuit_state = 'half_open' THEN TRUE
				ELSE FALSE
			END
		WHERE id = $1 AND circuit_state IN ('closed', 'half_open')`
	_, err := s.pool.Exec(ctx, q, endpointID)
	return err
}

// HandleTransientFailure records a transient failure (5xx or timeout).
// Increments the consecutive failure counter and opens the circuit when the
// threshold is reached, or if in half_open state, or if sensitive_recovery is true.
// No-op on already-open circuits.
func (s *CircuitStore) HandleTransientFailure(ctx context.Context, endpointID uuid.UUID, cfg config.CircuitConfig) error {
	const q = `
		UPDATE endpoints SET
			circuit_failure_count = circuit_failure_count + 1,
			circuit_state = CASE
				WHEN circuit_state = 'half_open'           THEN 'open'::circuit_state
				WHEN circuit_sensitive_recovery = TRUE     THEN 'open'::circuit_state
				WHEN circuit_failure_count + 1 >= $1       THEN 'open'::circuit_state
				ELSE 'closed'::circuit_state
			END,
			circuit_suspended_until = CASE
				WHEN circuit_state = 'half_open'           THEN NOW() + ($2 * INTERVAL '1 second')
				WHEN circuit_sensitive_recovery = TRUE     THEN NOW() + ($2 * INTERVAL '1 second')
				WHEN circuit_failure_count + 1 >= $1       THEN NOW() + ($2 * INTERVAL '1 second')
				ELSE circuit_suspended_until
			END,
			circuit_sensitive_recovery = FALSE,
			circuit_probe_delivery_id = CASE
				WHEN circuit_state = 'half_open' THEN NULL
				ELSE circuit_probe_delivery_id
			END
		WHERE id = $3 AND circuit_state IN ('closed', 'half_open')`
	_, err := s.pool.Exec(ctx, q, cfg.Threshold, cfg.SuspensionSeconds, endpointID)
	return err
}

// HandleProbePermanentFailure records a permanent failure for the probe delivery
// while the circuit is half_open. Reopens the circuit for a full suspension period.
// The failure counter is NOT incremented (FR-011).
func (s *CircuitStore) HandleProbePermanentFailure(ctx context.Context, endpointID uuid.UUID, cfg config.CircuitConfig) error {
	const q = `
		UPDATE endpoints SET
			circuit_state             = 'open',
			circuit_suspended_until   = NOW() + ($1 * INTERVAL '1 second'),
			circuit_probe_delivery_id = NULL
		WHERE id = $2 AND circuit_state = 'half_open'`
	_, err := s.pool.Exec(ctx, q, cfg.SuspensionSeconds, endpointID)
	return err
}

// ProcessExpiredSuspensions advances open circuits whose suspended_until is in the
// past. Transitions to half_open when non-terminal deliveries exist, or directly
// to closed (with counter reset) when the queue is empty (FR-024).
// Returns the IDs of endpoints transitioned to half_open and to closed respectively.
func (s *CircuitStore) ProcessExpiredSuspensions(ctx context.Context) (halfOpenIDs []uuid.UUID, closedIDs []uuid.UUID, err error) {
	const q = `
		UPDATE endpoints SET
			circuit_state = CASE
				WHEN EXISTS (
					SELECT 1 FROM deliveries d
					WHERE d.endpoint_id = endpoints.id
					  AND d.status NOT IN ('delivered', 'permanently_failed')
				) THEN 'half_open'::circuit_state
				ELSE 'closed'::circuit_state
			END,
			circuit_failure_count = CASE
				WHEN EXISTS (
					SELECT 1 FROM deliveries d
					WHERE d.endpoint_id = endpoints.id
					  AND d.status NOT IN ('delivered', 'permanently_failed')
				) THEN circuit_failure_count
				ELSE 0
			END,
			circuit_suspended_until = NULL
		WHERE circuit_state = 'open' AND circuit_suspended_until <= NOW()
		RETURNING id, circuit_state::text`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, nil, fmt.Errorf("process expired suspensions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		var state string
		if err := rows.Scan(&id, &state); err != nil {
			return nil, nil, fmt.Errorf("scan expired suspension: %w", err)
		}
		switch state {
		case "half_open":
			halfOpenIDs = append(halfOpenIDs, id)
		case "closed":
			closedIDs = append(closedIDs, id)
		}
	}
	return halfOpenIDs, closedIDs, rows.Err()
}

// SetProbeDelivery picks the oldest non-terminal delivery for a half_open endpoint
// and marks it as the probe delivery. If the queue is empty it transitions the
// endpoint back to closed (FR-024 empty-queue fallback).
func (s *CircuitStore) SetProbeDelivery(ctx context.Context, endpointID uuid.UUID) error {
	var probeID uuid.UUID
	err := s.pool.QueryRow(ctx,
		`SELECT id FROM deliveries
		 WHERE endpoint_id = $1 AND status NOT IN ('delivered', 'permanently_failed')
		 ORDER BY created_at ASC LIMIT 1`,
		endpointID).Scan(&probeID)

	if errors.Is(err, pgx.ErrNoRows) {
		// Empty queue: close the circuit (FR-024 fallback).
		_, execErr := s.pool.Exec(ctx,
			`UPDATE endpoints SET
				circuit_state = 'closed',
				circuit_failure_count = 0,
				circuit_probe_delivery_id = NULL
			 WHERE id = $1 AND circuit_state = 'half_open'`,
			endpointID)
		return execErr
	}
	if err != nil {
		return fmt.Errorf("select probe delivery: %w", err)
	}

	// Set the probe delivery ID on the endpoint.
	if _, err := s.pool.Exec(ctx,
		`UPDATE endpoints SET circuit_probe_delivery_id = $1 WHERE id = $2 AND circuit_state = 'half_open'`,
		probeID, endpointID); err != nil {
		return fmt.Errorf("set probe delivery id: %w", err)
	}

	// Reset next_attempt_at to NOW() if it's in the future (dispatch probe immediately).
	if _, err := s.pool.Exec(ctx,
		`UPDATE deliveries SET next_attempt_at = NOW()
		 WHERE id = $1 AND next_attempt_at > NOW()`,
		probeID); err != nil {
		return fmt.Errorf("reset probe next_attempt_at: %w", err)
	}
	return nil
}

// OrphanedHalfOpenEndpoints returns IDs of endpoints that are in half_open state
// with no probe delivery assigned (circuit_probe_delivery_id IS NULL). These are
// endpoints that were transitioned to half_open by the scheduler but lost their
// probe assignment due to a crash (Step 0a recovery guard).
func (s *CircuitStore) OrphanedHalfOpenEndpoints(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM endpoints
		 WHERE circuit_state = 'half_open' AND circuit_probe_delivery_id IS NULL`)
	if err != nil {
		return nil, fmt.Errorf("find orphaned half_open: %w", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan orphaned half_open id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// GetState returns the current circuit breaker state for the given endpoint.
// Returns domain.ErrNotFound if the endpoint does not exist.
func (s *CircuitStore) GetState(ctx context.Context, endpointID uuid.UUID) (*domain.CircuitBreakerInfo, error) {
	var info domain.CircuitBreakerInfo
	var state string
	info.EndpointID = endpointID
	err := s.pool.QueryRow(ctx,
		`SELECT circuit_state::text, circuit_failure_count, circuit_suspended_until
		 FROM endpoints WHERE id = $1`,
		endpointID).Scan(&state, &info.ConsecutiveFailures, &info.SuspendedUntil)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get circuit state: %w", err)
	}
	info.State = domain.CircuitState(state)
	return &info, nil
}
