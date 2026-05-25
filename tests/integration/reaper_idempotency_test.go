//go:build integration

package integration_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/FernandoCendretti/webhook-delivery/internal/recovery"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// TestReaper_PurgesExpiredIdempotencyRecords — T041
//
// Verifies that reaper.Tick() deletes idempotency_records whose expires_at is in
// the past while leaving still-valid records untouched (Flow E from plan.md).
func TestReaper_PurgesExpiredIdempotencyRecords(t *testing.T) {
	ctx := context.Background()
	_, pool := setupAPI(t)

	ds := store.NewDeliveryStore(pool)
	silentLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	reap := recovery.New(recovery.Config{
		Store:  ds,
		Logger: silentLog,
	})

	// Create a real endpoint to satisfy the FK constraint on idempotency_records.
	epID, err := seedEndpoint(ctx, pool, "https://reaper-test.example.com/wh")
	if err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}

	// Insert an expired record (expires_at in the past).
	_, err = pool.Exec(ctx, `
		INSERT INTO idempotency_records
			(endpoint_id, idempotency_key, payload_hash, expires_at)
		VALUES ($1, $2, 'aabbcc', NOW() - interval '1 second')`,
		epID, "expired-key",
	)
	if err != nil {
		t.Fatalf("insert expired record: %v", err)
	}

	// Insert a still-valid record (expires_at in the future).
	_, err = pool.Exec(ctx, `
		INSERT INTO idempotency_records
			(endpoint_id, idempotency_key, payload_hash, expires_at)
		VALUES ($1, $2, 'ddeeff', NOW() + interval '24 hours')`,
		epID, "valid-key",
	)
	if err != nil {
		t.Fatalf("insert valid record: %v", err)
	}

	// Run the reaper tick.
	if err := reap.Tick(ctx); err != nil {
		t.Fatalf("reaper tick: %v", err)
	}

	// Expired record must be gone.
	var expiredCount int
	pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM idempotency_records
		WHERE endpoint_id=$1 AND idempotency_key='expired-key'`,
		epID,
	).Scan(&expiredCount) //nolint:errcheck
	if expiredCount != 0 {
		t.Errorf("expired record still present after reaper tick; want 0, got %d", expiredCount)
	}

	// Valid record must survive.
	var validCount int
	pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM idempotency_records
		WHERE endpoint_id=$1 AND idempotency_key='valid-key'`,
		epID,
	).Scan(&validCount) //nolint:errcheck
	if validCount != 1 {
		t.Errorf("valid record was incorrectly purged; want 1, got %d", validCount)
	}
}
