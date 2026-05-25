package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// IdempotencyRecord holds the fields returned by Lookup.
type IdempotencyRecord struct {
	PayloadHash string
	EventID     uuid.UUID // zero if incomplete (event_id IS NULL)
	DeliveryID  uuid.UUID // zero if incomplete
	ExpiresAt   time.Time
	Complete    bool // true when EventID and DeliveryID are populated
}

// IdempotencyTxStore executes idempotency operations within a single Postgres
// transaction. All methods must be called before the transaction commits or
// rolls back.
type IdempotencyTxStore struct {
	tx pgx.Tx
}

// NewIdempotencyTxStore wraps an open transaction. The caller retains ownership
// of the transaction lifecycle.
func NewIdempotencyTxStore(tx pgx.Tx) *IdempotencyTxStore {
	return &IdempotencyTxStore{tx: tx}
}

// lockKey derives a deterministic int64 advisory lock key from an endpoint ID
// and idempotency key string using FNV-64a.
func lockKey(endpointID uuid.UUID, key string) int64 {
	h := fnv.New64a()
	h.Write(endpointID[:])
	h.Write([]byte(key))
	return int64(h.Sum64())
}

// PayloadHash returns the hex-encoded SHA-256 digest of rawBody.
func PayloadHash(rawBody []byte) string {
	sum := sha256.Sum256(rawBody)
	return hex.EncodeToString(sum[:])
}

// AcquireAdvisoryLock takes a PostgreSQL transaction-scoped advisory lock for
// (endpointID, key). Must be called within the transaction held by this store.
func (s *IdempotencyTxStore) AcquireAdvisoryLock(ctx context.Context, endpointID uuid.UUID, key string) error {
	_, err := s.tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockKey(endpointID, key))
	if err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	return nil
}

// Lookup returns the non-expired record for (endpointID, key), or (nil, nil) if
// none exists. Uses expires_at >= NOW() so records at the exact boundary are
// still considered valid (AS5).
func (s *IdempotencyTxStore) Lookup(ctx context.Context, endpointID uuid.UUID, key string) (*IdempotencyRecord, error) {
	const q = `
		SELECT payload_hash,
		       COALESCE(event_id, '00000000-0000-0000-0000-000000000000'),
		       COALESCE(delivery_id, '00000000-0000-0000-0000-000000000000'),
		       expires_at,
		       event_id IS NOT NULL AND delivery_id IS NOT NULL
		FROM idempotency_records
		WHERE endpoint_id = $1
		  AND idempotency_key = $2
		  AND expires_at >= NOW()`
	var r IdempotencyRecord
	err := s.tx.QueryRow(ctx, q, endpointID, key).Scan(
		&r.PayloadHash, &r.EventID, &r.DeliveryID, &r.ExpiresAt, &r.Complete,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("idempotency lookup: %w", err)
	}
	return &r, nil
}

// Claim inserts a partial idempotency record with expires_at = NOW() + 24h.
// If an expired record exists for the same (endpoint_id, idempotency_key), it
// is overwritten (ON CONFLICT DO UPDATE … WHERE expires_at < NOW()).
func (s *IdempotencyTxStore) Claim(ctx context.Context, endpointID uuid.UUID, key, payloadHash string) error {
	const q = `
		INSERT INTO idempotency_records
		       (endpoint_id, idempotency_key, payload_hash, expires_at)
		VALUES ($1, $2, $3, NOW() + interval '24 hours')
		ON CONFLICT (endpoint_id, idempotency_key)
		DO UPDATE SET payload_hash = EXCLUDED.payload_hash,
		              expires_at   = EXCLUDED.expires_at,
		              event_id     = NULL,
		              delivery_id  = NULL
		WHERE idempotency_records.expires_at < NOW()`
	if _, err := s.tx.Exec(ctx, q, endpointID, key, payloadHash); err != nil {
		return fmt.Errorf("idempotency claim: %w", err)
	}
	return nil
}

// Complete sets event_id and delivery_id on the existing record for (endpointID, key).
func (s *IdempotencyTxStore) Complete(ctx context.Context, endpointID uuid.UUID, key string, eventID, deliveryID uuid.UUID) error {
	const q = `
		UPDATE idempotency_records
		SET event_id = $1, delivery_id = $2
		WHERE endpoint_id = $3 AND idempotency_key = $4`
	if _, err := s.tx.Exec(ctx, q, eventID, deliveryID, endpointID, key); err != nil {
		return fmt.Errorf("idempotency complete: %w", err)
	}
	return nil
}
