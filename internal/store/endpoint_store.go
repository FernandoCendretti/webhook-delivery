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

// EndpointStore provides persistence for webhook endpoint records.
type EndpointStore struct {
	pool *pgxpool.Pool
}

// NewEndpointStore constructs an EndpointStore backed by the given connection pool.
func NewEndpointStore(pool *pgxpool.Pool) *EndpointStore {
	return &EndpointStore{pool: pool}
}

// Insert persists e and fills the database-generated ID, CreatedAt, and the
// echoed SigningSecret fields in place. The caller must set e.URL, e.TenantID,
// and e.SigningSecret before calling.
func (s *EndpointStore) Insert(ctx context.Context, e *domain.Endpoint) error {
	const q = `
		INSERT INTO endpoints (url, tenant_id, signing_secret) VALUES ($1, $2, $3)
		RETURNING id, tenant_id, created_at, signing_secret`
	if err := s.pool.QueryRow(ctx, q, e.URL, e.TenantID, e.SigningSecret).
		Scan(&e.ID, &e.TenantID, &e.CreatedAt, &e.SigningSecret); err != nil {
		return fmt.Errorf("insert endpoint: %w", err)
	}
	return nil
}

// GetByID returns the endpoint with the given id, or domain.ErrNotFound if
// no such row exists. signing_secret is intentionally excluded (FR-002).
func (s *EndpointStore) GetByID(ctx context.Context, id uuid.UUID) (*domain.Endpoint, error) {
	const q = `SELECT id, url, tenant_id, created_at FROM endpoints WHERE id = $1`
	var e domain.Endpoint
	if err := s.pool.QueryRow(ctx, q, id).Scan(&e.ID, &e.URL, &e.TenantID, &e.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get endpoint: %w", err)
	}
	return &e, nil
}

// UpdateSecret replaces the signing secret for the given endpoint id.
// Returns domain.ErrNotFound when no row matches.
func (s *EndpointStore) UpdateSecret(ctx context.Context, id uuid.UUID, newSecret []byte) error {
	const q = `UPDATE endpoints SET signing_secret = $1 WHERE id = $2 RETURNING id`
	var dummy uuid.UUID
	err := s.pool.QueryRow(ctx, q, newSecret, id).Scan(&dummy)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("update secret: %w", err)
	}
	return nil
}
