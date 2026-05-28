//go:build integration

// Integration tests for the circuit breaker store (T026–T039b).
package integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/config"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

var cbCfg = config.CircuitConfig{Threshold: 5, SuspensionSeconds: 60}

// seedCBEndpoint creates a tenant + endpoint for circuit-breaker tests.
func seedCBEndpoint(t *testing.T, pool *pgxpool.Pool) (tenantID, endpointID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	if err := pool.QueryRow(ctx,
		`INSERT INTO tenants (name) VALUES ($1) RETURNING id`,
		"cb-tenant-"+uuid.NewString()[:8]).Scan(&tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, signing_secret, tenant_id) VALUES ($1, gen_random_bytes(32), $2) RETURNING id`,
		"https://example.com/cb/"+uuid.NewString()[:8], tenantID).Scan(&endpointID); err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	return
}

// seedCBDelivery inserts a scheduled delivery for an endpoint.
// nextAttemptAt is the absolute timestamp for next_attempt_at.
func seedCBDelivery(t *testing.T, pool *pgxpool.Pool, tenantID, endpointID uuid.UUID, nextAttemptAt time.Time) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	var eventID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO events (endpoint_id, tenant_id, payload) VALUES ($1, $2, $3) RETURNING id`,
		endpointID, tenantID, `{"cb":1}`).Scan(&eventID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	var deliveryID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at)
		 VALUES ($1, $2, $3, 'scheduled', 0, $4) RETURNING id`,
		eventID, endpointID, tenantID, nextAttemptAt).Scan(&deliveryID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	return deliveryID
}

// cbRow returns (state, failureCount, suspendedUntil, probeDelveryID) for an endpoint.
func cbRow(t *testing.T, pool *pgxpool.Pool, endpointID uuid.UUID) (state string, count int, suspended *time.Time, probeID *uuid.UUID) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT circuit_state::text, circuit_failure_count, circuit_suspended_until, circuit_probe_delivery_id
		 FROM endpoints WHERE id = $1`, endpointID).
		Scan(&state, &count, &suspended, &probeID)
	if err != nil {
		t.Fatalf("read circuit row: %v", err)
	}
	return
}

// T026 — 5 transient failures open the circuit; 6th is a no-op; GetState is consistent.
func TestCircuitBreaker_OpenAfterThreshold(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	for i := 0; i < 5; i++ {
		if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}

	state, count, suspended, _ := cbRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if count != 5 {
		t.Errorf("count: got %d, want 5", count)
	}
	if suspended == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}

	// 6th failure — no-op.
	if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
		t.Fatalf("6th call: %v", err)
	}
	_, count2, _, _ := cbRow(t, pool, epID)
	if count2 != 5 {
		t.Errorf("count after 6th: got %d, want 5 (no-op)", count2)
	}

	// GetState reflects the open state.
	info, err := cs.GetState(ctx, epID)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if info == nil {
		t.Fatal("GetState: returned nil info without error")
	}
	if info.State != domain.CircuitOpen {
		t.Errorf("GetState.State: got %q, want open", info.State)
	}
	if info.ConsecutiveFailures != 5 {
		t.Errorf("GetState.ConsecutiveFailures: got %d, want 5", info.ConsecutiveFailures)
	}
	if info.SuspendedUntil == nil {
		t.Error("GetState.SuspendedUntil: want non-nil, got nil")
	}
}

// T027 — permanent failure does NOT increment the counter (FR-011).
func TestCircuitBreaker_PermanentFailureNoCounter(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	// 4 transient failures — count=4, circuit still closed.
	for i := 0; i < 4; i++ {
		if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
			t.Fatalf("transient %d: %v", i+1, err)
		}
	}
	// Permanent failure: worker calls HandleProbePermanentFailure ONLY in half_open.
	// In closed state the worker simply marks permanently_failed — counter untouched.
	_, count, _, _ := cbRow(t, pool, epID)
	if count != 4 {
		t.Errorf("count after 4 transient + 1 permanent: got %d, want 4", count)
	}

	// One more transient → 5 total → circuit opens.
	if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
		t.Fatalf("5th transient: %v", err)
	}
	state, count2, _, _ := cbRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if count2 != 5 {
		t.Errorf("count: got %d, want 5", count2)
	}
}

// T028 — ProcessExpiredSuspensions with a non-terminal delivery → half_open.
// SetProbeDelivery sets circuit_probe_delivery_id and resets delivery next_attempt_at.
func TestCircuitBreaker_ProcessExpired_NonTerminalDelivery(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	tenantID, epID := seedCBEndpoint(t, pool)

	// Seed a scheduled delivery with next_attempt_at in the future.
	dID := seedCBDelivery(t, pool, tenantID, epID, time.Now().Add(10*time.Minute))

	// Force endpoint to open with expired suspension.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='open', circuit_failure_count=5,
		 circuit_suspended_until=NOW()-INTERVAL '1 second' WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force open: %v", err)
	}

	halfOpenIDs, closedIDs, err := cs.ProcessExpiredSuspensions(ctx)
	if err != nil {
		t.Fatalf("ProcessExpiredSuspensions: %v", err)
	}
	if len(closedIDs) != 0 {
		t.Errorf("closedIDs: want 0, got %d", len(closedIDs))
	}
	if len(halfOpenIDs) != 1 || halfOpenIDs[0] != epID {
		t.Errorf("halfOpenIDs: want [%v], got %v", epID, halfOpenIDs)
	}

	state, _, _, _ := cbRow(t, pool, epID)
	if state != "half_open" {
		t.Errorf("state after ProcessExpired: got %q, want half_open", state)
	}

	// SetProbeDelivery picks the delivery.
	if err := cs.SetProbeDelivery(ctx, epID); err != nil {
		t.Fatalf("SetProbeDelivery: %v", err)
	}
	_, _, _, probeID := cbRow(t, pool, epID)
	if probeID == nil || *probeID != dID {
		t.Errorf("probe_delivery_id: want %v, got %v", dID, probeID)
	}

	// next_attempt_at must have been reset to NOW() (i.e. not in the future any more).
	var nextAt time.Time
	if err := pool.QueryRow(ctx,
		`SELECT next_attempt_at FROM deliveries WHERE id=$1`, dID).Scan(&nextAt); err != nil {
		t.Fatalf("read next_attempt_at: %v", err)
	}
	if nextAt.After(time.Now().Add(5 * time.Second)) {
		t.Errorf("next_attempt_at: want past/now, got %v (still in future)", nextAt)
	}
}

// T029 — ProcessExpiredSuspensions with empty queue → closed directly (FR-024).
func TestCircuitBreaker_ProcessExpired_EmptyQueue_ClosedDirectly(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	// No deliveries for this endpoint — force open with expired suspension.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='open', circuit_failure_count=5,
		 circuit_suspended_until=NOW()-INTERVAL '1 second' WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force open: %v", err)
	}

	halfOpenIDs, closedIDs, err := cs.ProcessExpiredSuspensions(ctx)
	if err != nil {
		t.Fatalf("ProcessExpiredSuspensions: %v", err)
	}
	if len(halfOpenIDs) != 0 {
		t.Errorf("halfOpenIDs: want 0, got %d", len(halfOpenIDs))
	}
	if len(closedIDs) != 1 || closedIDs[0] != epID {
		t.Errorf("closedIDs: want [%v], got %v", epID, closedIDs)
	}

	state, count, suspended, _ := cbRow(t, pool, epID)
	if state != "closed" {
		t.Errorf("state: got %q, want closed", state)
	}
	if count != 0 {
		t.Errorf("count: got %d, want 0 (reset)", count)
	}
	if suspended != nil {
		t.Errorf("suspended_until: want nil, got %v", suspended)
	}
}

// T030 — Scheduler crash recovery (Step 0a):
// half_open endpoint with probe_delivery_id=NULL → SetProbeDelivery fills it in.
func TestCircuitBreaker_CrashRecovery_HalfOpenNullProbe(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	tenantID, epID := seedCBEndpoint(t, pool)

	dID := seedCBDelivery(t, pool, tenantID, epID, time.Now().Add(-time.Second))

	// Simulate crash: half_open but no probe_delivery_id.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5,
		 circuit_probe_delivery_id=NULL WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	if err := cs.SetProbeDelivery(ctx, epID); err != nil {
		t.Fatalf("SetProbeDelivery: %v", err)
	}
	_, _, _, probeID := cbRow(t, pool, epID)
	if probeID == nil || *probeID != dID {
		t.Errorf("probe_delivery_id: want %v, got %v", dID, probeID)
	}

	// Delivery should now be returned by ClaimReady.
	ds := store.NewDeliveryStore(pool)
	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	found := false
	for _, d := range claimed {
		if d.ID == dID {
			found = true
		}
	}
	if !found {
		t.Errorf("probe delivery %v not returned by ClaimReady", dID)
	}
}

// T031 — SetProbeDelivery empty-queue race (FR-024 fallback):
// half_open, last delivery marked delivered before SetProbeDelivery → endpoint closes.
func TestCircuitBreaker_SetProbeDelivery_EmptyQueueFallback(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	tenantID, epID := seedCBEndpoint(t, pool)

	dID := seedCBDelivery(t, pool, tenantID, epID, time.Now().Add(-time.Second))

	// Force half_open.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5 WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	// Delivery is marked delivered before SetProbeDelivery is called (race).
	_, err = pool.Exec(ctx,
		`UPDATE deliveries SET status='delivered' WHERE id=$1`, dID)
	if err != nil {
		t.Fatalf("mark delivered: %v", err)
	}

	if err := cs.SetProbeDelivery(ctx, epID); err != nil {
		t.Fatalf("SetProbeDelivery: %v", err)
	}
	state, count, _, probeID := cbRow(t, pool, epID)
	if state != "closed" {
		t.Errorf("state: got %q, want closed (empty-queue fallback)", state)
	}
	if count != 0 {
		t.Errorf("count: got %d, want 0", count)
	}
	if probeID != nil {
		t.Errorf("probe_delivery_id: want nil, got %v", probeID)
	}
}

// T032 — Probe success: half_open + HandleSuccess → closed, sensitive_recovery=TRUE.
func TestCircuitBreaker_ProbeSuccess(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5 WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	if err := cs.HandleSuccess(ctx, epID); err != nil {
		t.Fatalf("HandleSuccess: %v", err)
	}

	state, count, _, probeID := cbRow(t, pool, epID)
	if state != "closed" {
		t.Errorf("state: got %q, want closed", state)
	}
	if count != 0 {
		t.Errorf("count: got %d, want 0", count)
	}
	if probeID != nil {
		t.Errorf("probe_delivery_id: want nil, got %v", probeID)
	}

	var sensitive bool
	if err := pool.QueryRow(ctx,
		`SELECT circuit_sensitive_recovery FROM endpoints WHERE id=$1`, epID).Scan(&sensitive); err != nil {
		t.Fatalf("read sensitive: %v", err)
	}
	if !sensitive {
		t.Error("sensitive_recovery: want TRUE after half_open→closed, got FALSE")
	}
}

// T033 — Probe transient failure: half_open + HandleTransientFailure → open, probe cleared.
func TestCircuitBreaker_ProbeTransientFailure(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5 WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
		t.Fatalf("HandleTransientFailure: %v", err)
	}

	state, _, suspended, probeID := cbRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if suspended == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}
	if probeID != nil {
		t.Errorf("probe_delivery_id: want nil, got %v", probeID)
	}
}

// T034 — HandleProbePermanentFailure: half_open + permanent failure → open, suspended.
func TestCircuitBreaker_ProbePermanentFailure(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5 WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	if err := cs.HandleProbePermanentFailure(ctx, epID, cbCfg); err != nil {
		t.Fatalf("HandleProbePermanentFailure: %v", err)
	}

	state, count, suspended, probeID := cbRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if suspended == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}
	if probeID != nil {
		t.Errorf("probe_delivery_id: want nil, got %v", probeID)
	}
	// Counter must NOT be incremented (FR-011).
	if count != 5 {
		t.Errorf("count: got %d, want 5 (not incremented)", count)
	}
}

// T035 — FR-019 sensitive recovery: one failure opens immediately when sensitive=TRUE.
func TestCircuitBreaker_SensitiveRecovery_SingleFailureOpens(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_sensitive_recovery=TRUE WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("set sensitive: %v", err)
	}

	if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
		t.Fatalf("HandleTransientFailure: %v", err)
	}

	state, _, suspended, _ := cbRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if suspended == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}
	var sensitive bool
	if err := pool.QueryRow(ctx,
		`SELECT circuit_sensitive_recovery FROM endpoints WHERE id=$1`, epID).Scan(&sensitive); err != nil {
		t.Fatalf("read sensitive: %v", err)
	}
	if sensitive {
		t.Error("sensitive_recovery: want FALSE after opening, got TRUE")
	}
}

// T036 — FR-019 reset: HandleSuccess after closed resets sensitive_recovery; next
// HandleTransientFailure does NOT open immediately (threshold applies).
func TestCircuitBreaker_SensitiveRecovery_Reset(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	// half_open → HandleSuccess → sensitive=TRUE.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5 WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}
	if err := cs.HandleSuccess(ctx, epID); err != nil {
		t.Fatalf("first HandleSuccess (probe): %v", err)
	}

	// Second HandleSuccess (simulates one subsequent successful delivery).
	if err := cs.HandleSuccess(ctx, epID); err != nil {
		t.Fatalf("second HandleSuccess: %v", err)
	}
	var sensitive bool
	if err := pool.QueryRow(ctx,
		`SELECT circuit_sensitive_recovery FROM endpoints WHERE id=$1`, epID).Scan(&sensitive); err != nil {
		t.Fatalf("read sensitive: %v", err)
	}
	if sensitive {
		t.Error("sensitive_recovery: want FALSE after second success, got TRUE")
	}

	// Single failure should NOT open (threshold=5 applies normally).
	if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
		t.Fatalf("HandleTransientFailure: %v", err)
	}
	state, _, _, _ := cbRow(t, pool, epID)
	if state != "closed" {
		t.Errorf("state: got %q, want closed (threshold not reached)", state)
	}
}

// T037 — Restart durability (FR-013, SC-007): circuit state survives reconnect.
func TestCircuitBreaker_RestartDurability(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	// Open the circuit.
	for i := 0; i < 5; i++ {
		if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
			t.Fatalf("transient %d: %v", i+1, err)
		}
	}

	// Create a new CircuitStore (simulates reconnect / fresh process).
	cs2 := store.NewCircuitStore(pool)
	info, err := cs2.GetState(ctx, epID)
	if err != nil {
		t.Fatalf("GetState from new store: %v", err)
	}
	if info.State != domain.CircuitOpen {
		t.Errorf("state: got %q, want open", info.State)
	}
}

// T038 — Multi-instance concurrency: two goroutines call HandleTransientFailure for
// the 5th failure; exactly one wins; count must not exceed threshold.
func TestCircuitBreaker_ConcurrentTransientFailure(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	_, epID := seedCBEndpoint(t, pool)

	// 4 failures first.
	for i := 0; i < 4; i++ {
		if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
			t.Fatalf("transient %d: %v", i+1, err)
		}
	}

	// Two goroutines both try to be the 5th failure.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cs.HandleTransientFailure(ctx, epID, cbCfg); err != nil {
				t.Errorf("concurrent HandleTransientFailure: %v", err)
			}
		}()
	}
	wg.Wait()

	state, count, _, _ := cbRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if count > cbCfg.Threshold {
		t.Errorf("count: got %d, must not exceed threshold %d", count, cbCfg.Threshold)
	}
}

// T039 — FR-020 overdue retry: closing the circuit (via SQL) makes previously blocked
// delivery visible to ClaimReady.
func TestCircuitBreaker_CircuitClose_UnblocksDelivery(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	tenantID, epID := seedCBEndpoint(t, pool)

	dID := seedCBDelivery(t, pool, tenantID, epID, time.Now().Add(-time.Second))

	// Open circuit — delivery should NOT be returned by ClaimReady.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='open', circuit_failure_count=5,
		 circuit_suspended_until=NOW()+INTERVAL '60 seconds' WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force open: %v", err)
	}

	ds := store.NewDeliveryStore(pool)
	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady (open): %v", err)
	}
	for _, d := range claimed {
		if d.ID == dID {
			t.Errorf("delivery %v claimed while circuit is open", dID)
		}
	}

	// Close the circuit via HandleSuccess (simulates a successful probe outcome).
	_ = cs.HandleSuccess(ctx, epID) // from half_open — force via SQL
	_, err = pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='closed', circuit_failure_count=0,
		 circuit_suspended_until=NULL, circuit_probe_delivery_id=NULL WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force close: %v", err)
	}

	claimed2, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady (closed): %v", err)
	}
	found := false
	for _, d := range claimed2 {
		if d.ID == dID {
			found = true
		}
	}
	if !found {
		t.Errorf("delivery %v not returned by ClaimReady after circuit close", dID)
	}
}

// T039a — AS8 cross-tenant circuit isolation: open circuit on T1 does NOT block T2's
// endpoint delivery.
func TestCircuitBreaker_CrossTenantIsolation(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	// T1: open circuit.
	tenantID1, epID1 := seedCBEndpoint(t, pool)
	_ = tenantID1
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='open', circuit_failure_count=5,
		 circuit_suspended_until=NOW()+INTERVAL '60 seconds' WHERE id=$1`, epID1)
	if err != nil {
		t.Fatalf("force open T1: %v", err)
	}

	// T2: closed circuit with a ready delivery.
	tenantID2, epID2 := seedCBEndpoint(t, pool)
	dID2 := seedCBDelivery(t, pool, tenantID2, epID2, time.Now().Add(-time.Second))

	ds := store.NewDeliveryStore(pool)
	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	found := false
	for _, d := range claimed {
		if d.ID == dID2 {
			found = true
		}
	}
	if !found {
		t.Errorf("T2 delivery %v not returned by ClaimReady (blocked by T1 circuit?)", dID2)
	}
}

// T039b — SC-005 queue drain: probe succeeds → circuit closes → D2 and D3 delivered in order.
func TestCircuitBreaker_QueueDrainAfterProbeSuccess(t *testing.T) {
	_, pool := setupAPI(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	tenantID, epID := seedCBEndpoint(t, pool)

	// Seed D1 (probe), D2, D3 in order (created_at spaced 1ms).
	d1 := seedCBDelivery(t, pool, tenantID, epID, time.Now().Add(-3*time.Second))
	time.Sleep(2 * time.Millisecond)
	d2 := seedCBDelivery(t, pool, tenantID, epID, time.Now().Add(-2*time.Second))
	time.Sleep(2 * time.Millisecond)
	d3 := seedCBDelivery(t, pool, tenantID, epID, time.Now().Add(-time.Second))

	// Force half_open with D1 as probe.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5,
		 circuit_probe_delivery_id=$1 WHERE id=$2`, d1, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	// Claim D1 (the probe) — only D1 should be eligible.
	ds := store.NewDeliveryStore(pool)
	claimed, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady (probe): %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != d1 {
		t.Fatalf("expected only probe D1, got %v", claimed)
	}

	// Probe succeeds: D1 marked delivered.
	if err := cs.HandleSuccess(ctx, epID); err != nil {
		t.Fatalf("HandleSuccess: %v", err)
	}
	if err := ds.MarkDelivered(ctx, d1); err != nil {
		t.Fatalf("MarkDelivered D1: %v", err)
	}

	// Now D2 is the oldest non-terminal; D3 is blocked until D2 is terminal.
	claimed2, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady (D2): %v", err)
	}
	if len(claimed2) != 1 || claimed2[0].ID != d2 {
		t.Fatalf("expected D2 next, got %v", claimed2)
	}

	// D2 delivered → D3 becomes eligible.
	if err := ds.MarkDelivered(ctx, d2); err != nil {
		t.Fatalf("MarkDelivered D2: %v", err)
	}
	claimed3, err := ds.ClaimReady(ctx, 10, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatalf("ClaimReady (D3): %v", err)
	}
	if len(claimed3) != 1 || claimed3[0].ID != d3 {
		t.Fatalf("expected D3 next, got %v", claimed3)
	}
}
