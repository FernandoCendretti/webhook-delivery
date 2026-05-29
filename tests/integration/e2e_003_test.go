//go:build integration

// E2E test covering ordering + circuit breaker working together (T048).
//
// Scenario (SC-002, SC-003, SC-005, SC-006):
//
//   1. Register tenant, endpoint E_A (always 503 for 6 calls, then 200) and
//      endpoint E_B (always 200) under the same tenant.
//   2. Submit delivery D1 → E_A and D2 → E_B.
//   3. Five transient failures on E_A open the circuit (threshold = 5).
//   4. While the circuit is open, D1 is excluded from ClaimReady;
//      D2 is also excluded because D1 (created earlier, same tenant) is still
//      non-terminal — per-tenant FIFO ordering (FR-008).
//   5. Advance suspended_until to the past. The scheduler transitions E_A to
//      half_open and designates D1 as the probe. D1 fires → 503 → circuit
//      reopens (probe failure).
//   6. Advance suspended_until again. New probe fires → 200 → HandleSuccess →
//      circuit closed, D1 delivered.
//   7. D2 is now unblocked (D1 terminal, E_B circuit closed) and is delivered.
//   8. GET /v1/endpoints/{id}/circuit-breaker reflects state = "closed".
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/config"
	"github.com/FernandoCendretti/webhook-delivery/internal/delivery"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/queue"
	"github.com/FernandoCendretti/webhook-delivery/internal/scheduler"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func TestE2E_OrderingAndCircuitBreaker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	// Full API (all routes, including circuit-breaker) + Postgres container.
	handler, pool := setupAPI(t)
	apiSrv := httptest.NewServer(handler)
	t.Cleanup(apiSrv.Close)

	// ── Destination servers ──────────────────────────────────────────────────
	// eAServer: first 6 calls return 503, call 7+ returns 200.
	// Call breakdown: 5 (open circuit) + 1 (probe fail) + 1 (probe succeed) = 7.
	var eACallCount atomic.Int32
	eAServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n := eACallCount.Add(1); n <= 6 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(eAServer.Close)

	eBServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(eBServer.Close)

	// ── Create tenant ────────────────────────────────────────────────────────
	tenantRes, err := http.Post(apiSrv.URL+"/v1/tenants", "application/json",
		bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("POST /v1/tenants: %v", err)
	}
	var tenantResp struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(tenantRes.Body).Decode(&tenantResp); err != nil {
		t.Fatalf("decode tenant: %v", err)
	}
	tenantRes.Body.Close()
	tenantID := tenantResp.ID

	// ── Register endpoints ───────────────────────────────────────────────────
	mustRegisterEndpoint := func(url string) uuid.UUID {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"url": url, "tenant_id": tenantID})
		res, err := http.Post(apiSrv.URL+"/v1/endpoints", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST /v1/endpoints: %v", err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusCreated {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("register endpoint status %d: %s", res.StatusCode, b)
		}
		var ep struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(res.Body).Decode(&ep); err != nil {
			t.Fatalf("decode endpoint: %v", err)
		}
		id, _ := uuid.Parse(ep.ID)
		return id
	}

	eAID := mustRegisterEndpoint(eAServer.URL)
	eBID := mustRegisterEndpoint(eBServer.URL)
	_ = eBID // registered; ordering logic uses tenant+created_at, not endpoint identity

	// ── Submit deliveries ────────────────────────────────────────────────────
	mustSubmitEvent := func(endpointID uuid.UUID) uuid.UUID {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"endpoint_id": endpointID.String(),
			"tenant_id":   tenantID,
			"payload":     json.RawMessage(`{"e2e":true}`),
		})
		res, err := http.Post(apiSrv.URL+"/v1/events", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST /v1/events: %v", err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusAccepted {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("submit event status %d: %s", res.StatusCode, b)
		}
		var accepted struct {
			DeliveryID string `json:"delivery_id"`
		}
		if err := json.NewDecoder(res.Body).Decode(&accepted); err != nil {
			t.Fatalf("decode accepted: %v", err)
		}
		id, _ := uuid.Parse(accepted.DeliveryID)
		return id
	}

	// D1 must be submitted before D2 so created_at ordering puts D1 first.
	d1ID := mustSubmitEvent(eAID)
	d2ID := mustSubmitEvent(eBID)

	// ── Pipeline setup ───────────────────────────────────────────────────────
	cbCfgE2E := config.CircuitConfig{Threshold: 5, SuspensionSeconds: 60}
	cs := store.NewCircuitStore(pool)
	ds := store.NewDeliveryStore(pool)
	as := store.NewAttemptStore(pool)

	brokers := testKafkaBrokers(t)
	topic := "e2e-003-" + uuid.NewString()[:8]
	pub := queue.NewPublisher(queue.PublisherConfig{Brokers: brokers, Topic: topic})
	t.Cleanup(func() { _ = pub.Close() })

	cons := queue.NewConsumer(queue.ConsumerConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "e2e-003-" + uuid.NewString()[:8],
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = cons.Close() })

	sched := scheduler.New(scheduler.Config{
		DeliveryStore: ds,
		CircuitStore:  cs,
		Publisher:     pub,
		BatchSize:     10,
		LeaseDuration: 60 * time.Second,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	w := delivery.NewWorker(delivery.WorkerConfig{
		DeliveryStore: ds,
		AttemptStore:  as,
		Consumer:      cons,
		Pool:          pool,
		CircuitStore:  cs,
		CircuitCfg:    cbCfgE2E,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	// ── Helpers ──────────────────────────────────────────────────────────────
	tickOnce := func() {
		t.Helper()
		if err := sched.Tick(ctx); err != nil {
			t.Fatalf("scheduler tick: %v", err)
		}
	}
	processOne := func() {
		t.Helper()
		if err := w.ProcessOne(ctx); err != nil {
			t.Fatalf("worker ProcessOne: %v", err)
		}
	}
	makeReady := func(delivID uuid.UUID) {
		t.Helper()
		_, err := pool.Exec(ctx,
			`UPDATE deliveries SET next_attempt_at = NOW() - INTERVAL '1 second' WHERE id = $1`, delivID)
		if err != nil {
			t.Fatalf("make delivery ready: %v", err)
		}
	}
	advanceSuspension := func(epID uuid.UUID) {
		t.Helper()
		_, err := pool.Exec(ctx,
			`UPDATE endpoints SET circuit_suspended_until = NOW() - INTERVAL '1 second' WHERE id = $1`, epID)
		if err != nil {
			t.Fatalf("advance suspension: %v", err)
		}
	}
	getStatus := func(delivID uuid.UUID) domain.DeliveryStatus {
		t.Helper()
		d, err := ds.GetByID(ctx, delivID)
		if err != nil {
			t.Fatalf("get delivery %s: %v", delivID, err)
		}
		return d.Status
	}

	// ── Phase 1: 5 transient failures → circuit opens ────────────────────────
	// Each iteration: claim D1, get 503, reschedule, reset next_attempt_at.
	for i := 0; i < 5; i++ {
		tickOnce()
		processOne()
		makeReady(d1ID)
	}

	// Assert circuit opened after 5 failures.
	state, count, _, _ := cbRow(t, pool, eAID)
	if state != "open" {
		t.Fatalf("phase 1: circuit state = %q, want open", state)
	}
	if count != 5 {
		t.Fatalf("phase 1: failure_count = %d, want 5", count)
	}

	// Assert D1 and D2 are both scheduled (D2 has not been claimed yet).
	if s := getStatus(d1ID); s != domain.StatusScheduled {
		t.Errorf("phase 1: D1 status = %q, want scheduled", s)
	}
	if s := getStatus(d2ID); s != domain.StatusScheduled {
		t.Errorf("phase 1: D2 status = %q, want scheduled", s)
	}

	// ── Phase 1b: verify both deliveries remain blocked ───────────────────────
	// Circuit is open → D1 excluded. D1 is non-terminal → ordering blocks D2.
	tickOnce()
	if s := getStatus(d1ID); s != domain.StatusScheduled {
		t.Errorf("phase 1b: D1 should still be scheduled (circuit open), got %q", s)
	}
	if s := getStatus(d2ID); s != domain.StatusScheduled {
		t.Errorf("phase 1b: D2 should still be scheduled (ordering blocked), got %q", s)
	}

	// ── Phase 2: probe fires → 503 → circuit reopens ─────────────────────────
	advanceSuspension(eAID)
	// Tick: Step 0b transitions E_A to half_open, designates D1 as probe,
	//       resets D1.next_attempt_at → NOW(); Step 1 claims D1 and publishes.
	tickOnce()
	// Worker processes probe: 503 → HandleTransientFailure(half_open) → open.
	processOne()

	state, _, _, probeID := cbRow(t, pool, eAID)
	if state != "open" {
		t.Fatalf("phase 2: circuit should have reopened after probe 503, got %q", state)
	}
	if probeID != nil {
		t.Errorf("phase 2: probe_delivery_id should be cleared after reopen, got %v", probeID)
	}

	// ── Phase 3: second probe → 200 → circuit closed, D1 delivered ───────────
	advanceSuspension(eAID)
	// Tick: half_open again; D1 designated probe; claimed and published.
	tickOnce()
	// Worker processes probe: 200 → HandleSuccess → circuit closed.
	processOne()

	state, count, _, _ = cbRow(t, pool, eAID)
	if state != "closed" {
		t.Fatalf("phase 3: circuit should be closed after probe success, got %q", state)
	}
	if count != 0 {
		t.Errorf("phase 3: failure_count = %d, want 0 after close", count)
	}
	if s := getStatus(d1ID); s != domain.StatusDelivered {
		t.Errorf("phase 3: D1 status = %q, want delivered", s)
	}

	// ── Phase 4: D2 unblocked → delivered ────────────────────────────────────
	// D1 is terminal; E_A circuit closed; E_B circuit closed.
	// ClaimReady should now pick D2.
	tickOnce()
	processOne()

	if s := getStatus(d2ID); s != domain.StatusDelivered {
		t.Errorf("phase 4: D2 status = %q, want delivered", s)
	}

	// ── Phase 5: circuit-breaker API reflects final state ─────────────────────
	res, err := http.Get(apiSrv.URL + "/v1/endpoints/" + eAID.String() + "/circuit-breaker")
	if err != nil {
		t.Fatalf("GET /v1/endpoints/%s/circuit-breaker: %v", eAID, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("circuit-breaker API status %d: %s", res.StatusCode, b)
	}
	var cbAPIResp struct {
		State               string  `json:"state"`
		ConsecutiveFailures int     `json:"consecutive_failures"`
		SuspendedUntil      *string `json:"suspended_until"`
	}
	if err := json.NewDecoder(res.Body).Decode(&cbAPIResp); err != nil {
		t.Fatalf("decode circuit-breaker response: %v", err)
	}
	if cbAPIResp.State != "closed" {
		t.Errorf("phase 5: circuit-breaker API state = %q, want closed", cbAPIResp.State)
	}
	if cbAPIResp.ConsecutiveFailures != 0 {
		t.Errorf("phase 5: consecutive_failures = %d, want 0", cbAPIResp.ConsecutiveFailures)
	}
	if cbAPIResp.SuspendedUntil != nil {
		t.Errorf("phase 5: suspended_until should be absent, got %v", cbAPIResp.SuspendedUntil)
	}
}
