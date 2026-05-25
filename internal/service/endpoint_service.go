package service

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// endpointStore is the consumer-side interface covering the store operations
// EndpointService relies on. The concrete store.EndpointStore satisfies it.
type endpointStore interface {
	Insert(ctx context.Context, e *domain.Endpoint) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Endpoint, error)
	UpdateSecret(ctx context.Context, id uuid.UUID, newSecret []byte) error
}

// EndpointService manages the lifecycle of registered webhook endpoints.
type EndpointService struct {
	store endpointStore
}

// NewEndpointService constructs an EndpointService backed by the given store.
func NewEndpointService(s endpointStore) *EndpointService {
	return &EndpointService{store: s}
}

// Register validates the URL, generates a cryptographic signing secret, and
// persists a new endpoint. The returned Endpoint has SigningSecret populated —
// the only time the raw secret is returned to the caller.
func (s *EndpointService) Register(ctx context.Context, url string) (*domain.Endpoint, error) {
	if err := domain.ValidateURL(url); err != nil {
		return nil, err
	}
	rawSecret := make([]byte, 32)
	if _, err := rand.Read(rawSecret); err != nil {
		return nil, fmt.Errorf("generate signing secret: %w", err)
	}
	e := domain.Endpoint{URL: url, SigningSecret: rawSecret}
	if err := s.store.Insert(ctx, &e); err != nil {
		return nil, fmt.Errorf("register endpoint: %w", err)
	}
	return &e, nil
}

// Get returns the endpoint with the given id. Surfaces domain.ErrNotFound
// directly so callers can map it to a 404. SigningSecret is nil on the result.
func (s *EndpointService) Get(ctx context.Context, id uuid.UUID) (*domain.Endpoint, error) {
	return s.store.GetByID(ctx, id)
}

// RotateSecret generates a new 32-byte signing secret, persists it, and
// returns the raw bytes. Returns domain.ErrNotFound if id does not exist.
func (s *EndpointService) RotateSecret(ctx context.Context, id uuid.UUID) ([]byte, error) {
	newSecret := make([]byte, 32)
	if _, err := rand.Read(newSecret); err != nil {
		return nil, fmt.Errorf("generate signing secret: %w", err)
	}
	if err := s.store.UpdateSecret(ctx, id, newSecret); err != nil {
		return nil, err
	}
	return newSecret, nil
}
