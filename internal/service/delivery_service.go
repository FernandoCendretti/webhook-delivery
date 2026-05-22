package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// deliveryReader is the consumer-side interface for the delivery query used by
// DeliveryService.
type deliveryReader interface {
	GetByIDWithAttempts(ctx context.Context, id uuid.UUID) (*domain.Delivery, []domain.Attempt, error)
}

// DeliveryView is the aggregated read model returned to the handler.
type DeliveryView struct {
	Delivery *domain.Delivery
	Attempts []domain.Attempt
}

// DeliveryService provides read access to delivery lifecycle state and attempt history.
type DeliveryService struct {
	store deliveryReader
}

// NewDeliveryService constructs a DeliveryService backed by the given store.
func NewDeliveryService(s deliveryReader) *DeliveryService {
	return &DeliveryService{store: s}
}

// Get returns the delivery with its full attempt history.
// Returns domain.ErrNotFound if no delivery exists for the given id.
func (s *DeliveryService) Get(ctx context.Context, id uuid.UUID) (*DeliveryView, error) {
	d, attempts, err := s.store.GetByIDWithAttempts(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get delivery: %w", err)
	}
	return &DeliveryView{Delivery: d, Attempts: attempts}, nil
}
