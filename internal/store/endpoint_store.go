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

// Insert persists e and fills the database-generated ID and CreatedAt fields
// in place. e.URL is the only input field the caller is expected to set.
func (s *EndpointStore) Insert(ctx context.Context, e *domain.Endpoint) error {
	const q = `INSERT INTO endpoints (url) VALUES ($1) RETURNING id, created_at`
	if err := s.pool.QueryRow(ctx, q, e.URL).Scan(&e.ID, &e.CreatedAt); err != nil {
		return fmt.Errorf("insert endpoint: %w", err)
	}
	return nil
}

// GetByID returns the endpoint with the given id, or domain.ErrNotFound if
// no such row exists.
func (s *EndpointStore) GetByID(ctx context.Context, id uuid.UUID) (*domain.Endpoint, error) {
	const q = `SELECT id, url, created_at FROM endpoints WHERE id = $1`
	var e domain.Endpoint
	if err := s.pool.QueryRow(ctx, q, id).Scan(&e.ID, &e.URL, &e.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get endpoint: %w", err)
	}
	return &e, nil
}
