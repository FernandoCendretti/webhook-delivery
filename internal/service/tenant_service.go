package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// tenantStore is the store interface consumed by TenantService.
type tenantStore interface {
	Insert(ctx context.Context, t *domain.Tenant) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error)
}

// TenantService manages the lifecycle of tenant records.
type TenantService struct {
	store tenantStore
}

// NewTenantService constructs a TenantService backed by the given store.
func NewTenantService(s tenantStore) *TenantService {
	return &TenantService{store: s}
}

// Create inserts a new tenant with the optional name and returns the persisted record.
func (s *TenantService) Create(ctx context.Context, name *string) (*domain.Tenant, error) {
	t := &domain.Tenant{Name: name}
	if err := s.store.Insert(ctx, t); err != nil {
		return nil, fmt.Errorf("tenant service create: %w", err)
	}
	return t, nil
}

// GetByID returns the tenant with the given id.
// Returns domain.ErrNotFound when no such tenant exists.
func (s *TenantService) GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error) {
	return s.store.GetByID(ctx, id)
}
