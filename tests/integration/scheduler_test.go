//go:build integration

package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func TestScheduler_ClaimReady_NoDuplicates(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	// Seed one endpoint.
	var endpointID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO endpoints (url) VALUES ($1) RETURNING id`,
		"https://example.com/sched").Scan(&endpointID); err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}

	// Seed 100 deliveries, all with next_attempt_at in the past.
	for i := 0; i < 100; i++ {
		var eventID uuid.UUID
		if err := pool.QueryRow(ctx,
			`INSERT INTO events (endpoint_id, payload) VALUES ($1, $2) RETURNING id`,
			endpointID, `{"n":`+string(rune('0'+i%10))+`}`).Scan(&eventID); err != nil {
			t.Fatalf("seed event: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO deliveries (event_id, endpoint_id, status, attempt_count, next_attempt_at)
			 VALUES ($1, $2, 'scheduled', 0, NOW() - INTERVAL '1 second')`,
			eventID, endpointID); err != nil {
			t.Fatalf("seed delivery: %v", err)
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
	if len(seen) != 100 {
		t.Errorf("total claimed: got %d, want 100", len(seen))
	}
}
