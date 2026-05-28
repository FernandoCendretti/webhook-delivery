//go:build integration

// Integration tests for per-tenant FIFO delivery ordering (T020).
//
// Deliveries are seeded directly via SQL to avoid depending on the (not-yet-updated)
// EventService.Submit signature. ClaimReady is called directly on DeliveryStore.
package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// seedOrderingTenant inserts a tenant and one endpoint under it, then inserts n
// deliveries in sequence with created_at spaced 1 ms apart. All deliveries start
// in 'scheduled' status with next_attempt_at = NOW(). Returns deliveryIDs oldest-first.
func seedOrderingTenant(t *testing.T, pool *pgxpool.Pool, n int) (tenantID, endpointID uuid.UUID, deliveryIDs []uuid.UUID) {
	t.Helper()
	ctx := context.Background()

	if err := pool.QueryRow(ctx, `INSERT INTO tenants DEFAULT VALUES RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, signing_secret, tenant_id) VALUES ('https://example.com/cb', gen_random_bytes(32), $1) RETURNING id`,
		tenantID).Scan(&endpointID); err != nil {
		t.Fatalf("insert endpoint: %v", err)
	}

	base := time.Now().UTC().Add(-time.Duration(n) * time.Millisecond)
	for i := range n {
		createdAt := base.Add(time.Duration(i) * time.Millisecond)

		var evID uuid.UUID
		if err := pool.QueryRow(ctx,
			`INSERT INTO events (endpoint_id, tenant_id, payload) VALUES ($1, $2, '{}') RETURNING id`,
			endpointID, tenantID).Scan(&evID); err != nil {
			t.Fatalf("insert event %d: %v", i, err)
		}
		var dID uuid.UUID
		if err := pool.QueryRow(ctx,
			`INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at, created_at)
			 VALUES ($1, $2, $3, 'scheduled', 0, NOW(), $4) RETURNING id`,
			evID, endpointID, tenantID, createdAt).Scan(&dID); err != nil {
			t.Fatalf("insert delivery %d: %v", i, err)
		}
		deliveryIDs = append(deliveryIDs, dID)
	}
	return
}

// deliveryIDs extracts the ID from each WorkerDelivery for logging.
func deliveryIDList(ds []domain.Delivery) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.ID.String()
	}
	return out
}

// TestOrdering_SameTenant_E2BlockedWhileE1NonTerminal asserts that ClaimReady does
// not return D2 while D1 (created earlier, same tenant) is non-terminal (AS4, FR-008).
func TestOrdering_SameTenant_E2BlockedWhileE1NonTerminal(t *testing.T) {
	ctx := context.Background()
	pool := setup003Pool(t)
	ds := store.NewDeliveryStore(pool)

	_, _, deliveryIDs := seedOrderingTenant(t, pool, 2)
	d1ID, d2ID := deliveryIDs[0], deliveryIDs[1]

	// Both deliveries are scheduled. ClaimReady must return only D1.
	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimReady returned %d deliveries, want 1; ids=%v", len(claimed), deliveryIDList(claimed))
	}
	if claimed[0].ID != d1ID {
		t.Errorf("ClaimReady returned %v, want D1=%v; D2=%v", claimed[0].ID, d1ID, d2ID)
	}
}

// TestOrdering_SameTenant_E1Delivered_UnblocksE2 asserts that once D1 reaches
// 'delivered', ClaimReady returns D2 (FR-008).
func TestOrdering_SameTenant_E1Delivered_UnblocksE2(t *testing.T) {
	ctx := context.Background()
	pool := setup003Pool(t)
	ds := store.NewDeliveryStore(pool)

	_, _, deliveryIDs := seedOrderingTenant(t, pool, 2)
	d1ID, d2ID := deliveryIDs[0], deliveryIDs[1]

	if _, err := pool.Exec(ctx,
		`UPDATE deliveries SET status='delivered', updated_at=NOW() WHERE id=$1`, d1ID); err != nil {
		t.Fatalf("mark D1 delivered: %v", err)
	}

	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("after D1 delivered: ClaimReady returned %d, want 1 (D2); ids=%v",
			len(claimed), deliveryIDList(claimed))
	}
	if claimed[0].ID != d2ID {
		t.Errorf("returned %v, want D2=%v", claimed[0].ID, d2ID)
	}
}

// TestOrdering_SameTenant_E1PermanentlyFailed_UnblocksE2 asserts that
// 'permanently_failed' is also a terminal state that unblocks the queue (FR-008).
func TestOrdering_SameTenant_E1PermanentlyFailed_UnblocksE2(t *testing.T) {
	ctx := context.Background()
	pool := setup003Pool(t)
	ds := store.NewDeliveryStore(pool)

	_, _, deliveryIDs := seedOrderingTenant(t, pool, 2)
	d1ID, d2ID := deliveryIDs[0], deliveryIDs[1]

	if _, err := pool.Exec(ctx,
		`UPDATE deliveries SET status='permanently_failed', updated_at=NOW() WHERE id=$1`, d1ID); err != nil {
		t.Fatalf("mark D1 permanently_failed: %v", err)
	}

	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != d2ID {
		t.Errorf("after permanently_failed: got %d deliveries, want 1 (D2=%v); ids=%v",
			len(claimed), d2ID, deliveryIDList(claimed))
	}
}

// TestOrdering_CrossTenant_Independent asserts that D1 under T1 does not block
// D2 under T2 (FR-009).
func TestOrdering_CrossTenant_Independent(t *testing.T) {
	ctx := context.Background()
	pool := setup003Pool(t)
	ds := store.NewDeliveryStore(pool)

	_, _, t1Deliveries := seedOrderingTenant(t, pool, 1)
	_, _, t2Deliveries := seedOrderingTenant(t, pool, 1)
	d1ID := t1Deliveries[0]
	d2ID := t2Deliveries[0]

	// Force D1 to in_flight so it's non-terminal but not claimable via scheduling.
	if _, err := pool.Exec(ctx,
		`UPDATE deliveries SET status='in_flight', in_flight_lease_until=NOW()+interval'30s', updated_at=NOW() WHERE id=$1`, d1ID); err != nil {
		t.Fatalf("mark D1 in_flight: %v", err)
	}

	// D2 belongs to a different tenant — must be claimable.
	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != d2ID {
		t.Errorf("cross-tenant: got %d deliveries, want 1 (D2=%v); ids=%v",
			len(claimed), d2ID, deliveryIDList(claimed))
	}
}

// TestOrdering_SameTenant_InFlight_BlocksE2 asserts that in_flight (non-terminal)
// also blocks a later same-tenant delivery from being claimed.
func TestOrdering_SameTenant_InFlight_BlocksE2(t *testing.T) {
	ctx := context.Background()
	pool := setup003Pool(t)
	ds := store.NewDeliveryStore(pool)

	_, _, deliveryIDs := seedOrderingTenant(t, pool, 2)
	d1ID, _ := deliveryIDs[0], deliveryIDs[1]

	// Move D1 to in_flight (claimed by an earlier scheduler run).
	if _, err := pool.Exec(ctx,
		`UPDATE deliveries SET status='in_flight', in_flight_lease_until=NOW()+interval'30s', updated_at=NOW() WHERE id=$1`, d1ID); err != nil {
		t.Fatalf("mark D1 in_flight: %v", err)
	}

	// D2 (same tenant, later created_at) must NOT be returned.
	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if len(claimed) != 0 {
		t.Errorf("in_flight D1 must block D2; ClaimReady returned %d: %v",
			len(claimed), deliveryIDList(claimed))
	}
}

// Ensure deliveryIDList is used to satisfy the compiler; suppress unused warning.
var _ = fmt.Sprint
