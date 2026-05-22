package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// endpointStore is the consumer-side interface (per plan.md "Structure
// Decision") covering the store operations EndpointService relies on. The
// concrete store.EndpointStore satisfies it.
type endpointStore interface {
	Insert(ctx context.Context, e *domain.Endpoint) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Endpoint, error)
}

// EndpointService manages the lifecycle of registered webhook endpoints.
type EndpointService struct {
	store endpointStore
}

// NewEndpointService constructs an EndpointService backed by the given store.
func NewEndpointService(s endpointStore) *EndpointService {
	return &EndpointService{store: s}
}

// Register validates the URL and persists a new endpoint. Returns the
// populated domain.Endpoint (with ID and CreatedAt filled by the store) on
// success, or a domain.ErrInvalidURL-wrapped error if validation fails.
func (s *EndpointService) Register(ctx context.Context, url string) (*domain.Endpoint, error) {
	if err := domain.ValidateURL(url); err != nil {
		return nil, err
	}
	e := domain.Endpoint{URL: url}
	if err := s.store.Insert(ctx, &e); err != nil {
		return nil, fmt.Errorf("register endpoint: %w", err)
	}
	return &e, nil
}

// Get returns the endpoint with the given id. Surfaces domain.ErrNotFound
// directly so callers can map it to a 404.
func (s *EndpointService) Get(ctx context.Context, id uuid.UUID) (*domain.Endpoint, error) {
	return s.store.GetByID(ctx, id)
}
