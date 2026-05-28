package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// TenantStore provides persistence for tenant records.
type TenantStore struct {
	pool *pgxpool.Pool
}

// NewTenantStore constructs a TenantStore backed by the given connection pool.
func NewTenantStore(pool *pgxpool.Pool) *TenantStore {
	return &TenantStore{pool: pool}
}

// Insert persists t and fills the database-generated ID and CreatedAt in place.
func (s *TenantStore) Insert(ctx context.Context, t *domain.Tenant) error {
	const q = `
		INSERT INTO tenants (name) VALUES ($1)
		RETURNING id, name, created_at`
	err := s.pool.QueryRow(ctx, q, t.Name).Scan(&t.ID, &t.Name, &t.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert tenant: %w", err)
	}
	return nil
}

// GetByID returns the tenant with the given id, or domain.ErrNotFound if it
// does not exist.
func (s *TenantStore) GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error) {
	const q = `SELECT id, name, created_at FROM tenants WHERE id = $1`
	var t domain.Tenant
	if err := s.pool.QueryRow(ctx, q, id).Scan(&t.ID, &t.Name, &t.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	return &t, nil
}
