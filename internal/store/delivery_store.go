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
// fully populated record. sourceDeliveryID may be nil for original deliveries.
func (s *DeliveryStore) Create(ctx context.Context, eventID, endpointID uuid.UUID, sourceDeliveryID *uuid.UUID) (*domain.Delivery, error) {
	const q = `
		INSERT INTO deliveries (event_id, endpoint_id, status, attempt_count, next_attempt_at, source_delivery_id)
		VALUES ($1, $2, 'scheduled', 0, NOW(), $3)
		RETURNING id, event_id, endpoint_id, status::text, attempt_count,
		          next_attempt_at, in_flight_lease_until, created_at, updated_at`
	var d domain.Delivery
	if err := scanDelivery(s.pool.QueryRow(ctx, q, eventID, endpointID, sourceDeliveryID), &d); err != nil {
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
//
// Per-tenant ordering (FR-008): a delivery D is only eligible if there is no
// earlier delivery D' for the same tenant that is non-terminal (i.e., D' is
// still in scheduled or in_flight state). This uses idx_deliveries_tenant_ordering.
//
// Circuit breaker (FR-014): only deliveries whose endpoint is 'closed', or is
// 'half_open' and has designated this delivery as the probe, are eligible.
func (s *DeliveryStore) ClaimReady(ctx context.Context, batch int, leaseUntil time.Time) ([]domain.Delivery, error) {
	const q = `
		WITH claimed AS (
			SELECT d.id FROM deliveries d
			JOIN endpoints e ON e.id = d.endpoint_id
			WHERE d.status = 'scheduled' AND d.next_attempt_at <= NOW()
			  AND NOT EXISTS (
			      SELECT 1 FROM deliveries d2
			      WHERE d2.tenant_id = d.tenant_id
			        AND d2.id != d.id
			        AND d2.status NOT IN ('delivered', 'permanently_failed')
			        AND d2.created_at < d.created_at
			  )
			  AND (
			      e.circuit_state = 'closed'
			      OR (e.circuit_state = 'half_open' AND e.circuit_probe_delivery_id = d.id)
			  )
			ORDER BY d.next_attempt_at
			LIMIT $1
			FOR UPDATE OF d SKIP LOCKED
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

// LoadForWorker fetches the delivery joined with its endpoint URL, signing secret,
// circuit state, and event payload in a single query — used by the worker to avoid
// extra round-trips.
type WorkerDelivery struct {
	Delivery             domain.Delivery
	EndpointURL          string
	Payload              []byte
	SigningSecret        []byte
	EndpointCircuitState string
}

func (s *DeliveryStore) LoadForWorker(ctx context.Context, deliveryID uuid.UUID) (*WorkerDelivery, error) {
	const q = `
		SELECT d.id, d.event_id, d.endpoint_id, d.status::text, d.attempt_count,
		       d.next_attempt_at, d.in_flight_lease_until, d.created_at, d.updated_at,
		       e.url, e.signing_secret, ev.payload, e.circuit_state::text
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
		&wd.EndpointURL, &wd.SigningSecret, &wd.Payload, &wd.EndpointCircuitState,
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

// ListPermanentlyFailed returns a paginated slice of domain.DLQEntry rows for
// deliveries with status='permanently_failed', ordered by updated_at DESC.
// The caller passes limit+1 to detect HasNext.
func (s *DeliveryStore) ListPermanentlyFailed(ctx context.Context, filter domain.DLQFilter, page, limit int) ([]domain.DLQEntry, error) {
	offset := (page - 1) * limit
	args := []any{limit, offset}
	where := "WHERE d.status = 'permanently_failed'"
	if filter.TenantID != nil {
		args = append(args, *filter.TenantID)
		where += fmt.Sprintf(" AND d.tenant_id = $%d", len(args))
	}
	if filter.EndpointID != nil {
		args = append(args, *filter.EndpointID)
		where += fmt.Sprintf(" AND d.endpoint_id = $%d", len(args))
	}

	q := fmt.Sprintf(`
		SELECT d.id, d.event_id, d.endpoint_id, d.tenant_id, d.attempt_count,
		       COALESCE(lat.failed_at, d.updated_at) AS failed_at
		FROM deliveries d
		LEFT JOIN LATERAL (
		    SELECT MAX(completed_at) AS failed_at
		    FROM attempts
		    WHERE delivery_id = d.id
		) lat ON TRUE
		%s
		ORDER BY d.updated_at DESC
		LIMIT $1 OFFSET $2`, where)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list permanently failed: %w", err)
	}
	defer rows.Close()

	var out []domain.DLQEntry
	for rows.Next() {
		var e domain.DLQEntry
		if err := rows.Scan(&e.DeliveryID, &e.EventID, &e.EndpointID, &e.TenantID, &e.AttemptCount, &e.FailedAt); err != nil {
			return nil, fmt.Errorf("scan dlq entry: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
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

// PurgeExpiredIdempotencyRecords deletes idempotency_records whose window has
// elapsed. Strict < (not <=) preserves records at the exact 24-hour boundary
// per FR-009 ("expires_at >= NOW() → still within window").
// Returns the number of rows deleted.
func (s *DeliveryStore) PurgeExpiredIdempotencyRecords(ctx context.Context) (int, error) {
	const q = `DELETE FROM idempotency_records WHERE expires_at < NOW()`
	tag, err := s.pool.Exec(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("purge idempotency records: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func scanDelivery(row pgx.Row, d *domain.Delivery) error {
	return row.Scan(
		&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount,
		&d.NextAttemptAt, &d.InFlightLeaseUntil, &d.CreatedAt, &d.UpdatedAt,
	)
}
