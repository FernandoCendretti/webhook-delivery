//go:build integration

package store_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/FernandoCendretti/webhook-delivery/internal/config"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func setupCircuitPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pgCtr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("webhookd_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pgCtr.Terminate(context.Background()) })

	connStr, err := pgCtr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres conn string: %v", err)
	}
	pool, err := store.NewPool(ctx, store.PoolConfig{DatabaseURL: connStr, MaxConns: 5})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	_, file, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(file), "..", "..")
	migrations := []string{
		"001_init.sql",
		"002_signing_secret.sql",
		"003_idempotency.sql",
		"004_tenants.sql",
		"005_tenant_columns.sql",
		"006_circuit_breaker.sql",
	}
	for _, name := range migrations {
		raw, readErr := os.ReadFile(filepath.Join(repoRoot, "internal/store/migrations", name))
		if readErr != nil {
			t.Fatalf("read migration %s: %v", name, readErr)
		}
		s := string(raw)
		upIdx := strings.Index(s, "-- +goose Up")
		downIdx := strings.Index(s, "-- +goose Down")
		if upIdx < 0 || downIdx < 0 {
			t.Fatalf("goose markers not found in %s", name)
		}
		if _, execErr := pool.Exec(ctx, s[upIdx:downIdx]); execErr != nil {
			t.Fatalf("apply migration %s: %v", name, execErr)
		}
	}
	return pool
}

// seedEndpoint inserts a tenant + endpoint and returns the endpoint UUID.
func seedEndpointCircuit(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	var tenantID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO tenants (name) VALUES ($1) RETURNING id`, "circuit-test-tenant").Scan(&tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	var endpointID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, signing_secret, tenant_id) VALUES ($1, gen_random_bytes(32), $2) RETURNING id`,
		"https://example.com/cb", tenantID).Scan(&endpointID); err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	return endpointID
}

// circuitRow returns (state, failureCount, suspendedUntil, sensitiveRecovery) for an endpoint.
func circuitRow(t *testing.T, pool *pgxpool.Pool, endpointID uuid.UUID) (state string, count int, suspended *time.Time, sensitive bool) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT circuit_state::text, circuit_failure_count, circuit_suspended_until, circuit_sensitive_recovery
		 FROM endpoints WHERE id = $1`, endpointID).
		Scan(&state, &count, &suspended, &sensitive)
	if err != nil {
		t.Fatalf("read circuit row: %v", err)
	}
	return
}

var defaultCfg = config.CircuitConfig{Threshold: 5, SuspensionSeconds: 60}

// T025 — HandleTransientFailure state machine transitions.

func TestCircuit_HandleTransientFailure_BelowThreshold(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	// 4 failures — still closed, count=4.
	for i := 0; i < 4; i++ {
		if err := cs.HandleTransientFailure(ctx, epID, defaultCfg); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}
	state, count, suspended, _ := circuitRow(t, pool, epID)
	if state != "closed" {
		t.Errorf("state: got %q, want closed", state)
	}
	if count != 4 {
		t.Errorf("count: got %d, want 4", count)
	}
	if suspended != nil {
		t.Errorf("suspended_until: want nil, got %v", suspended)
	}
}

func TestCircuit_HandleTransientFailure_AtThresholdOpens(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	// 5 failures — circuit opens on the 5th.
	for i := 0; i < 5; i++ {
		if err := cs.HandleTransientFailure(ctx, epID, defaultCfg); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}
	state, count, suspended, _ := circuitRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if count != 5 {
		t.Errorf("count: got %d, want 5", count)
	}
	if suspended == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}
}

func TestCircuit_HandleTransientFailure_OpenIsNoOp(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	// Force circuit open via SQL.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='open', circuit_failure_count=5,
		 circuit_suspended_until=NOW()+INTERVAL '60 seconds' WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force open: %v", err)
	}

	// 6th failure must be a no-op (WHERE clause skips open state).
	if err := cs.HandleTransientFailure(ctx, epID, defaultCfg); err != nil {
		t.Fatalf("HandleTransientFailure: %v", err)
	}
	_, count, _, _ := circuitRow(t, pool, epID)
	if count != 5 {
		t.Errorf("count: got %d, want 5 (no-op)", count)
	}
}

func TestCircuit_HandleTransientFailure_SensitiveRecoveryOpensImmediately(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	// Set sensitive_recovery = TRUE (as would happen after a successful probe).
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_sensitive_recovery=TRUE WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("set sensitive: %v", err)
	}

	// Single failure opens the circuit immediately.
	if err := cs.HandleTransientFailure(ctx, epID, defaultCfg); err != nil {
		t.Fatalf("HandleTransientFailure: %v", err)
	}
	state, count, suspended, sensitive := circuitRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if count != 1 {
		t.Errorf("count: got %d, want 1", count)
	}
	if suspended == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}
	if sensitive {
		t.Error("sensitive_recovery: want FALSE after opening, got TRUE")
	}
}

func TestCircuit_HandleTransientFailure_HalfOpenOpensImmediately(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	// Force circuit to half_open.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5 WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	// Any failure while half_open reopens immediately.
	if err := cs.HandleTransientFailure(ctx, epID, defaultCfg); err != nil {
		t.Fatalf("HandleTransientFailure: %v", err)
	}
	state, _, suspended, _ := circuitRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open", state)
	}
	if suspended == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}
}

// T025 — HandleSuccess state machine transitions.

func TestCircuit_HandleSuccess_ClosedResetsCounter(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	// Accumulate 3 failures without opening, then succeed.
	for i := 0; i < 3; i++ {
		_ = cs.HandleTransientFailure(ctx, epID, defaultCfg)
	}
	if err := cs.HandleSuccess(ctx, epID); err != nil {
		t.Fatalf("HandleSuccess: %v", err)
	}
	state, count, _, sensitive := circuitRow(t, pool, epID)
	if state != "closed" {
		t.Errorf("state: got %q, want closed", state)
	}
	if count != 0 {
		t.Errorf("count: got %d, want 0", count)
	}
	if sensitive {
		t.Error("sensitive_recovery: want FALSE after closed→closed success, got TRUE")
	}
}

func TestCircuit_HandleSuccess_HalfOpenCloses(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	// Force half_open with a probe delivery id (simulates probe dispatch).
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5 WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	if err := cs.HandleSuccess(ctx, epID); err != nil {
		t.Fatalf("HandleSuccess: %v", err)
	}
	state, count, _, sensitive := circuitRow(t, pool, epID)
	if state != "closed" {
		t.Errorf("state: got %q, want closed", state)
	}
	if count != 0 {
		t.Errorf("count: got %d, want 0", count)
	}
	if !sensitive {
		t.Error("sensitive_recovery: want TRUE after half_open→closed, got FALSE")
	}
}

func TestCircuit_HandleSuccess_OpenIsNoOp(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	// Force open.
	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='open', circuit_failure_count=5,
		 circuit_suspended_until=NOW()+INTERVAL '60 seconds' WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force open: %v", err)
	}

	if err := cs.HandleSuccess(ctx, epID); err != nil {
		t.Fatalf("HandleSuccess: %v", err)
	}
	state, count, _, _ := circuitRow(t, pool, epID)
	if state != "open" {
		t.Errorf("state: got %q, want open (no-op)", state)
	}
	if count != 5 {
		t.Errorf("count: got %d, want 5 (no-op)", count)
	}
}

// T025 — GetState

func TestCircuit_GetState_NotFound(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)

	_, err := cs.GetState(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("GetState: expected error, got nil")
	}
	if !isNotFound(err) {
		t.Errorf("GetState: got %v, want domain.ErrNotFound", err)
	}
}

func TestCircuit_GetState_Closed(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	info, err := cs.GetState(ctx, epID)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if info == nil {
		t.Fatal("GetState: returned nil info without error")
	}
	if info.State != domain.CircuitClosed {
		t.Errorf("state: got %q, want closed", info.State)
	}
	if info.ConsecutiveFailures != 0 {
		t.Errorf("failures: got %d, want 0", info.ConsecutiveFailures)
	}
	if info.SuspendedUntil != nil {
		t.Errorf("suspended_until: want nil, got %v", info.SuspendedUntil)
	}
	if info.EndpointID != epID {
		t.Errorf("endpoint_id: got %v, want %v", info.EndpointID, epID)
	}
}

func TestCircuit_GetState_Open(t *testing.T) {
	pool := setupCircuitPool(t)
	cs := store.NewCircuitStore(pool)
	ctx := context.Background()
	epID := seedEndpointCircuit(t, pool)

	_, err := pool.Exec(ctx,
		`UPDATE endpoints SET circuit_state='open', circuit_failure_count=5,
		 circuit_suspended_until=NOW()+INTERVAL '60 seconds' WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force open: %v", err)
	}

	info, err := cs.GetState(ctx, epID)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if info == nil {
		t.Fatal("GetState: returned nil info without error")
	}
	if info.State != domain.CircuitOpen {
		t.Errorf("state: got %q, want open", info.State)
	}
	if info.ConsecutiveFailures != 5 {
		t.Errorf("failures: got %d, want 5", info.ConsecutiveFailures)
	}
	if info.SuspendedUntil == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}
}

// isNotFound returns true when the error wraps domain.ErrNotFound.
func isNotFound(err error) bool {
	return errors.Is(err, domain.ErrNotFound)
}
