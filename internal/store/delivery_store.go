package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// DeliveryStore provides persistence and state-machine operations for deliveries.
type DeliveryStore struct {
	pool *pgxpool.Pool
}

// NewDeliveryStore constructs a DeliveryStore backed by the given connection pool.
func NewDeliveryStore(pool *pgxpool.Pool) *DeliveryStore {
	return &DeliveryStore{pool: pool}
}

// Create inserts a new delivery in the 'scheduled' status and returns the
// fully populated record.
func (s *DeliveryStore) Create(ctx context.Context, eventID, endpointID uuid.UUID) (*domain.Delivery, error) {
	const q = `
		INSERT INTO deliveries (event_id, endpoint_id, status, attempt_count, next_attempt_at)
		VALUES ($1, $2, 'scheduled', 0, NOW())
		RETURNING id, event_id, endpoint_id, status::text, attempt_count,
		          next_attempt_at, in_flight_lease_until, created_at, updated_at`
	var d domain.Delivery
	if err := scanDelivery(s.pool.QueryRow(ctx, q, eventID, endpointID), &d); err != nil {
		return nil, fmt.Errorf("create delivery: %w", err)
	}
	return &d, nil
}

// GetByID returns the delivery with the given id, or domain.ErrNotFound if it
// does not exist.
func (s *DeliveryStore) GetByID(ctx context.Context, id uuid.UUID) (*domain.Delivery, error) {
	const q = `
		SELECT id, event_id, endpoint_id, status::text, attempt_count,
		       next_attempt_at, in_flight_lease_until, created_at, updated_at
		FROM deliveries WHERE id = $1`
	var d domain.Delivery
	if err := scanDelivery(s.pool.QueryRow(ctx, q, id), &d); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get delivery: %w", err)
	}
	return &d, nil
}

// ClaimReady atomically selects up to batch scheduled deliveries that are due,
// marks them in_flight with the given lease, and returns them.
func (s *DeliveryStore) ClaimReady(ctx context.Context, batch int, leaseUntil time.Time) ([]domain.Delivery, error) {
	const q = `
		WITH claimed AS (
			SELECT id FROM deliveries
			WHERE status = 'scheduled' AND next_attempt_at <= NOW()
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE deliveries d
		SET status = 'in_flight',
		    in_flight_lease_until = $2,
		    updated_at = NOW()
		FROM claimed
		WHERE d.id = claimed.id
		RETURNING d.id, d.event_id, d.endpoint_id, d.status::text, d.attempt_count,
		          d.next_attempt_at, d.in_flight_lease_until, d.created_at, d.updated_at`

	rows, err := s.pool.Query(ctx, q, batch, leaseUntil)
	if err != nil {
		return nil, fmt.Errorf("claim ready deliveries: %w", err)
	}
	defer rows.Close()

	var out []domain.Delivery
	for rows.Next() {
		var d domain.Delivery
		if err := rows.Scan(
			&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount,
			&d.NextAttemptAt, &d.InFlightLeaseUntil, &d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan claimed delivery: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MarkDelivered transitions a delivery to the 'delivered' status and
// increments its attempt count.
func (s *DeliveryStore) MarkDelivered(ctx context.Context, id uuid.UUID) error {
	const q = `
		UPDATE deliveries
		SET status = 'delivered', in_flight_lease_until = NULL,
		    attempt_count = attempt_count + 1, updated_at = NOW()
		WHERE id = $1`
	_, err := s.pool.Exec(ctx, q, id)
	return err
}

// MarkPermanentlyFailed transitions a delivery to the 'permanently_failed'
// status and increments its attempt count.
func (s *DeliveryStore) MarkPermanentlyFailed(ctx context.Context, id uuid.UUID) error {
	const q = `
		UPDATE deliveries
		SET status = 'permanently_failed', in_flight_lease_until = NULL,
		    attempt_count = attempt_count + 1, updated_at = NOW()
		WHERE id = $1`
	_, err := s.pool.Exec(ctx, q, id)
	return err
}

// MarkInFlight transitions a delivery to the 'in_flight' status and sets the
// lease expiry.
func (s *DeliveryStore) MarkInFlight(ctx context.Context, id uuid.UUID, leaseUntil time.Time) error {
	const q = `
		UPDATE deliveries
		SET status = 'in_flight', in_flight_lease_until = $2, updated_at = NOW()
		WHERE id = $1`
	_, err := s.pool.Exec(ctx, q, id, leaseUntil)
	return err
}

// Reschedule transitions a delivery back to 'scheduled' with the given next
// attempt time and increments the attempt count.
func (s *DeliveryStore) Reschedule(ctx context.Context, id uuid.UUID, nextAt time.Time) error {
	const q = `
		UPDATE deliveries
		SET status = 'scheduled', next_attempt_at = $2, in_flight_lease_until = NULL,
		    attempt_count = attempt_count + 1, updated_at = NOW()
		WHERE id = $1`
	_, err := s.pool.Exec(ctx, q, id, nextAt)
	return err
}

// LoadForWorker fetches the delivery joined with its endpoint URL and event payload
// in a single query — used by the worker to avoid extra round-trips.
type WorkerDelivery struct {
	Delivery    domain.Delivery
	EndpointURL string
	Payload     []byte
}

func (s *DeliveryStore) LoadForWorker(ctx context.Context, deliveryID uuid.UUID) (*WorkerDelivery, error) {
	const q = `
		SELECT d.id, d.event_id, d.endpoint_id, d.status::text, d.attempt_count,
		       d.next_attempt_at, d.in_flight_lease_until, d.created_at, d.updated_at,
		       e.url, ev.payload
		FROM deliveries d
		JOIN endpoints e  ON e.id = d.endpoint_id
		JOIN events    ev ON ev.id = d.event_id
		WHERE d.id = $1`

	var wd WorkerDelivery
	err := s.pool.QueryRow(ctx, q, deliveryID).Scan(
		&wd.Delivery.ID, &wd.Delivery.EventID, &wd.Delivery.EndpointID,
		&wd.Delivery.Status, &wd.Delivery.AttemptCount,
		&wd.Delivery.NextAttemptAt, &wd.Delivery.InFlightLeaseUntil,
		&wd.Delivery.CreatedAt, &wd.Delivery.UpdatedAt,
		&wd.EndpointURL, &wd.Payload,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("load for worker: %w", err)
	}
	return &wd, nil
}

// GetByIDWithAttempts fetches a delivery and all its attempt records in a single
// query (LEFT JOIN), with attempts ordered by sequence ascending.
func (s *DeliveryStore) GetByIDWithAttempts(ctx context.Context, id uuid.UUID) (*domain.Delivery, []domain.Attempt, error) {
	const q = `
		SELECT d.id, d.event_id, d.endpoint_id, d.status::text, d.attempt_count,
		       d.next_attempt_at, d.in_flight_lease_until, d.created_at, d.updated_at,
		       a.id, a.delivery_id, a.sequence, a.started_at, a.completed_at,
		       a.response_status_code, a.outcome::text, a.error_reason
		FROM deliveries d
		LEFT JOIN attempts a ON a.delivery_id = d.id
		WHERE d.id = $1
		ORDER BY a.sequence ASC`

	rows, err := s.pool.Query(ctx, q, id)
	if err != nil {
		return nil, nil, fmt.Errorf("get delivery with attempts: %w", err)
	}
	defer rows.Close()

	var (
		d        domain.Delivery
		attempts []domain.Attempt
		found    bool
	)
	for rows.Next() {
		var (
			aID         *uuid.UUID
			aDeliveryID *uuid.UUID
			aSeq        *int
			aStartedAt  *time.Time
			aCompletedAt *time.Time
			aStatusCode  *int
			aOutcome    *string
			aErrReason  *string
		)
		if err := rows.Scan(
			&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount,
			&d.NextAttemptAt, &d.InFlightLeaseUntil, &d.CreatedAt, &d.UpdatedAt,
			&aID, &aDeliveryID, &aSeq, &aStartedAt, &aCompletedAt,
			&aStatusCode, &aOutcome, &aErrReason,
		); err != nil {
			return nil, nil, fmt.Errorf("scan delivery+attempt: %w", err)
		}
		found = true
		if aID != nil {
			attempts = append(attempts, domain.Attempt{
				ID:                 *aID,
				DeliveryID:         *aDeliveryID,
				Sequence:           *aSeq,
				StartedAt:          *aStartedAt,
				CompletedAt:        aCompletedAt,
				ResponseStatusCode: aStatusCode,
				Outcome:            domain.AttemptOutcome(*aOutcome),
				ErrorReason:        aErrReason,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, nil, domain.ErrNotFound
	}
	return &d, attempts, nil
}

// ResurrectExpiredLeases resets in_flight deliveries whose lease has expired
// back to scheduled, making them eligible for re-claim by the scheduler.
// Returns the number of rows resurrected.
func (s *DeliveryStore) ResurrectExpiredLeases(ctx context.Context) (int, error) {
	const q = `
		UPDATE deliveries
		SET status = 'scheduled', in_flight_lease_until = NULL, updated_at = NOW()
		WHERE status = 'in_flight' AND in_flight_lease_until < NOW()`
	tag, err := s.pool.Exec(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("resurrect expired leases: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func scanDelivery(row pgx.Row, d *domain.Delivery) error {
	return row.Scan(
		&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount,
		&d.NextAttemptAt, &d.InFlightLeaseUntil, &d.CreatedAt, &d.UpdatedAt,
	)
}
