//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func TestScheduler_ClaimReady_NoDuplicates(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	// Seed 100 independent tenants, each with one endpoint and one delivery.
	// With per-tenant ordering each delivery is immediately eligible (no earlier
	// non-terminal delivery for the same tenant), allowing all 100 to be claimed
	// concurrently.
	const n = 100
	for i := 0; i < n; i++ {
		var tenantID uuid.UUID
		if err := pool.QueryRow(ctx,
			`INSERT INTO tenants (name) VALUES ($1) RETURNING id`,
			fmt.Sprintf("sched-tenant-%d", i)).Scan(&tenantID); err != nil {
			t.Fatalf("seed tenant %d: %v", i, err)
		}
		var endpointID uuid.UUID
		if err := pool.QueryRow(ctx,
			`INSERT INTO endpoints (url, signing_secret, tenant_id) VALUES ($1, gen_random_bytes(32), $2) RETURNING id`,
			fmt.Sprintf("https://example.com/sched/%d", i), tenantID).Scan(&endpointID); err != nil {
			t.Fatalf("seed endpoint %d: %v", i, err)
		}
		var eventID uuid.UUID
		if err := pool.QueryRow(ctx,
			`INSERT INTO events (endpoint_id, tenant_id, payload) VALUES ($1, $2, $3) RETURNING id`,
			endpointID, tenantID, `{"n":1}`).Scan(&eventID); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at)
			 VALUES ($1, $2, $3, 'scheduled', 0, NOW() - INTERVAL '1 second')`,
			eventID, endpointID, tenantID); err != nil {
			t.Fatalf("seed delivery %d: %v", i, err)
		}
	}

	ds := store.NewDeliveryStore(pool)
	leaseUntil := time.Now().Add(30 * time.Second)
	var mu sync.Mutex
	seen := make(map[uuid.UUID]int)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := ds.ClaimReady(ctx, 100, leaseUntil)
			if err != nil {
				t.Errorf("ClaimReady: %v", err)
				return
			}
			mu.Lock()
			for _, d := range claimed {
				seen[d.ID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	for id, count := range seen {
		if count > 1 {
			t.Errorf("delivery %s claimed %d times (want 1)", id, count)
		}
	}
	if len(seen) != n {
		t.Errorf("total claimed: got %d, want %d", len(seen), n)
	}
}
