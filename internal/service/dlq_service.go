package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// DLQDetail is the full detail response (metadata + attempt history).
type DLQDetail struct {
	domain.DLQEntry
	Attempts []domain.Attempt
}

// Pagination carries paging metadata returned by List.
type Pagination struct {
	Page    int
	Limit   int
	HasNext bool
}

// DLQService exposes all DLQ inspection and replay operations.
type DLQService interface {
	List(ctx context.Context, filter domain.DLQFilter, page, limit int) ([]domain.DLQEntry, Pagination, error)
	Detail(ctx context.Context, deliveryID uuid.UUID) (*DLQDetail, error)
	Replay(ctx context.Context, deliveryID uuid.UUID) (*domain.Delivery, error)
	BulkReplay(ctx context.Context, filter domain.DLQFilter) (int, error)
}

// dlqDeliveryStore is the delivery-store surface used by dlqService.
type dlqDeliveryStore interface {
	ListPermanentlyFailed(ctx context.Context, filter domain.DLQFilter, page, limit int) ([]domain.DLQEntry, error)
	GetPermanentlyFailed(ctx context.Context, id uuid.UUID) (*domain.Delivery, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Delivery, error)
	CreateReplay(ctx context.Context, eventID, endpointID, sourceID uuid.UUID) (*domain.Delivery, error)
}

// dlqEndpointStore is the endpoint-store surface used by dlqService.
type dlqEndpointStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Endpoint, error)
}

// dlqAttemptStore is the attempt-store surface used by dlqService.
type dlqAttemptStore interface {
	ListByDelivery(ctx context.Context, deliveryID uuid.UUID) ([]domain.Attempt, error)
}

// dlqService is the concrete implementation of DLQService.
type dlqService struct {
	deliveries dlqDeliveryStore
	endpoints  dlqEndpointStore
	attempts   dlqAttemptStore
}

// NewDLQService constructs a dlqService backed by the given stores.
func NewDLQService(deliveries dlqDeliveryStore, endpoints dlqEndpointStore, attempts dlqAttemptStore) DLQService {
	return &dlqService{deliveries: deliveries, endpoints: endpoints, attempts: attempts}
}

func (s *dlqService) List(ctx context.Context, filter domain.DLQFilter, page, limit int) ([]domain.DLQEntry, Pagination, error) {
	if page < 1 {
		return nil, Pagination{}, fmt.Errorf("page must be >= 1")
	}
	if limit < 1 || limit > 100 {
		return nil, Pagination{}, fmt.Errorf("limit must be between 1 and 100")
	}

	rows, err := s.deliveries.ListPermanentlyFailed(ctx, filter, page, limit+1)
	if err != nil {
		return nil, Pagination{}, fmt.Errorf("list dlq: %w", err)
	}

	hasNext := len(rows) > limit
	if hasNext {
		rows = rows[:limit]
	}

	return rows, Pagination{Page: page, Limit: limit, HasNext: hasNext}, nil
}

func (s *dlqService) Detail(ctx context.Context, deliveryID uuid.UUID) (*DLQDetail, error) {
	delivery, err := s.deliveries.GetPermanentlyFailed(ctx, deliveryID)
	if err != nil {
		return nil, err
	}

	endpoint, err := s.endpoints.GetByID(ctx, delivery.EndpointID)
	if err != nil {
		return nil, fmt.Errorf("get endpoint for dlq detail: %w", err)
	}

	attempts, err := s.attempts.ListByDelivery(ctx, deliveryID)
	if err != nil {
		return nil, fmt.Errorf("list attempts for dlq detail: %w", err)
	}

	var failedAt time.Time
	for _, a := range attempts {
		if a.CompletedAt != nil && a.CompletedAt.After(failedAt) {
			failedAt = *a.CompletedAt
		}
	}

	return &DLQDetail{
		DLQEntry: domain.DLQEntry{
			DeliveryID:   delivery.ID,
			EventID:      delivery.EventID,
			EndpointID:   delivery.EndpointID,
			TenantID:     endpoint.TenantID,
			AttemptCount: delivery.AttemptCount,
			FailedAt:     failedAt,
		},
		Attempts: attempts,
	}, nil
}

func (s *dlqService) Replay(ctx context.Context, deliveryID uuid.UUID) (*domain.Delivery, error) {
	// Use the generic GetByID so we can distinguish a missing delivery (404)
	// from one that exists but is not permanently_failed (409).
	delivery, err := s.deliveries.GetByID(ctx, deliveryID)
	if err != nil {
		return nil, err // domain.ErrNotFound propagates to a 404
	}
	if delivery.Status != domain.StatusPermanentlyFailed {
		return nil, fmt.Errorf("replay delivery %s: status is %s: %w", deliveryID, delivery.Status, domain.ErrConflict)
	}

	if _, err := s.endpoints.GetByID(ctx, delivery.EndpointID); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("replay delivery %s: endpoint %s no longer exists: %w", deliveryID, delivery.EndpointID, domain.ErrUnprocessable)
		}
		return nil, fmt.Errorf("get endpoint for replay: %w", err)
	}

	newDelivery, err := s.deliveries.CreateReplay(ctx, delivery.EventID, delivery.EndpointID, delivery.ID)
	if err != nil {
		// A unique-violation on idx_deliveries_one_active_replay means a
		// non-terminal replay already exists for this delivery (SC-005).
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, fmt.Errorf("replay delivery %s: a non-terminal replay already exists: %w", deliveryID, domain.ErrConflict)
		}
		return nil, fmt.Errorf("create replay for delivery %s: %w", deliveryID, err)
	}
	return newDelivery, nil
}

func (s *dlqService) BulkReplay(_ context.Context, _ domain.DLQFilter) (int, error) {
	panic("not implemented")
}

