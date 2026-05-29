//go:build integration

// Integration tests for GET /v1/dlq (T006 — US1).
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/api"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// setupDLQHandler builds an http.Handler with DLQ routes registered.
func setupDLQHandler(t *testing.T, pool *pgxpool.Pool) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := api.NewServer(api.ServerConfig{APIAddr: ":0", Logger: logger})
	tenantSvc := service.NewTenantService(store.NewTenantStore(pool))
	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool), tenantSvc)
	s.RegisterTenants(tenantSvc)
	s.RegisterEndpoints(endpointSvc)
	s.RegisterEvents(service.NewEventService(pool, endpointSvc))
	s.RegisterDeliveries(service.NewDeliveryService(store.NewDeliveryStore(pool)))
	dlqSvc := service.NewDLQService(
		store.NewDeliveryStore(pool),
		store.NewEndpointStore(pool),
		store.NewAttemptStore(pool),
	)
	s.RegisterDLQ(dlqSvc)
	return s.Mux()
}

// insertPermanentlyFailed seeds a permanently_failed delivery directly in DB.
func insertPermanentlyFailed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, endpointID, tenantID uuid.UUID) uuid.UUID {
	t.Helper()
	evtID := uuid.New()
	if err := pool.QueryRow(ctx,
		`INSERT INTO events (id, endpoint_id, tenant_id, payload) VALUES ($1, $2, $3, '{}') RETURNING id`,
		evtID, endpointID, tenantID).Scan(&evtID); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	var id uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at)
		 VALUES ($1, $2, $3, 'permanently_failed', 3, NOW()) RETURNING id`,
		evtID, endpointID, tenantID).Scan(&id); err != nil {
		t.Fatalf("insert permanently_failed delivery: %v", err)
	}
	return id
}

func TestDLQList_HappyPathSingleItem(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, err := seedEndpoint(ctx, pool, "http://dlq1.example.com")
	if err != nil {
		t.Fatalf("seedEndpoint: %v", err)
	}
	tenantID := uuid.MustParse(systemDefaultTenantID)
	deliveryID := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(ts.URL + "/v1/dlq")
	if err != nil {
		t.Fatalf("GET /v1/dlq: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 200; body=%s", res.StatusCode, b)
	}

	var resp struct {
		Items []struct {
			DeliveryID   string `json:"delivery_id"`
			EndpointID   string `json:"endpoint_id"`
			TenantID     string `json:"tenant_id"`
			AttemptCount int    `json:"attempt_count"`
			FailedAt     string `json:"failed_at"`
		} `json:"items"`
		Pagination struct {
			Page    int  `json:"page"`
			HasNext bool `json:"has_next"`
		} `json:"pagination"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items len: got %d, want 1", len(resp.Items))
	}
	if resp.Items[0].DeliveryID != deliveryID.String() {
		t.Errorf("delivery_id: got %q, want %q", resp.Items[0].DeliveryID, deliveryID)
	}
	if resp.Items[0].EndpointID != endpointID.String() {
		t.Errorf("endpoint_id: got %q, want %q", resp.Items[0].EndpointID, endpointID)
	}
	if resp.Items[0].AttemptCount != 3 {
		t.Errorf("attempt_count: got %d, want 3", resp.Items[0].AttemptCount)
	}
	if resp.Items[0].FailedAt == "" {
		t.Error("failed_at must be present")
	}
}

func TestDLQList_EmptyList(t *testing.T) {
	_, pool := setupAPI(t)
	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(ts.URL + "/v1/dlq")
	if err != nil {
		t.Fatalf("GET /v1/dlq: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 200; body=%s", res.StatusCode, b)
	}

	var resp struct {
		Items []json.RawMessage `json:"items"`
	}
	json.NewDecoder(res.Body).Decode(&resp)
	if len(resp.Items) != 0 {
		t.Errorf("items: got %d, want 0", len(resp.Items))
	}
}

func TestDLQList_Pagination(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://page.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)
	for i := 0; i < 5; i++ {
		insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)
	}

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	// Page 1 with limit 3.
	res1, err := http.Get(fmt.Sprintf("%s/v1/dlq?page=1&limit=3", ts.URL))
	if err != nil {
		t.Fatalf("GET page 1: %v", err)
	}
	defer res1.Body.Close()

	var p1 struct {
		Items      []json.RawMessage `json:"items"`
		Pagination struct {
			Page    int  `json:"page"`
			HasNext bool `json:"has_next"`
		} `json:"pagination"`
	}
	json.NewDecoder(res1.Body).Decode(&p1)
	if len(p1.Items) != 3 {
		t.Errorf("page1 items: got %d, want 3", len(p1.Items))
	}
	if !p1.Pagination.HasNext {
		t.Error("page1: has_next should be true")
	}

	// Page 2 with limit 3 — 2 remaining items, no next page.
	res2, err := http.Get(fmt.Sprintf("%s/v1/dlq?page=2&limit=3", ts.URL))
	if err != nil {
		t.Fatalf("GET page 2: %v", err)
	}
	defer res2.Body.Close()

	var p2 struct {
		Items      []json.RawMessage `json:"items"`
		Pagination struct {
			HasNext bool `json:"has_next"`
		} `json:"pagination"`
	}
	json.NewDecoder(res2.Body).Decode(&p2)
	if len(p2.Items) != 2 {
		t.Errorf("page2 items: got %d, want 2", len(p2.Items))
	}
	if p2.Pagination.HasNext {
		t.Error("page2: has_next should be false")
	}
}

func TestDLQList_FilterByTenantID(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	var tenant2ID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO tenants DEFAULT VALUES RETURNING id`).Scan(&tenant2ID); err != nil {
		t.Fatalf("insert tenant2: %v", err)
	}

	ep1, _ := seedEndpoint(ctx, pool, "http://t1.dlq.example.com")
	var ep2 uuid.UUID
	pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, signing_secret, tenant_id) VALUES ($1, gen_random_bytes(32), $2) RETURNING id`,
		"http://t2.dlq.example.com", tenant2ID).Scan(&ep2)

	insertPermanentlyFailed(t, ctx, pool, ep1, uuid.MustParse(systemDefaultTenantID))
	insertPermanentlyFailed(t, ctx, pool, ep2, tenant2ID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(fmt.Sprintf("%s/v1/dlq?tenant_id=%s", ts.URL, tenant2ID))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	var resp struct {
		Items []struct {
			TenantID string `json:"tenant_id"`
		} `json:"items"`
	}
	json.NewDecoder(res.Body).Decode(&resp)
	if len(resp.Items) != 1 {
		t.Fatalf("items: got %d, want 1", len(resp.Items))
	}
	if resp.Items[0].TenantID != tenant2ID.String() {
		t.Errorf("tenant_id: got %q, want %q", resp.Items[0].TenantID, tenant2ID)
	}
}

func TestDLQList_FilterByEndpointID(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()
	tenantID := uuid.MustParse(systemDefaultTenantID)

	ep1, _ := seedEndpoint(ctx, pool, "http://ep1.dlq.example.com")
	ep2, _ := seedEndpoint(ctx, pool, "http://ep2.dlq.example.com")

	insertPermanentlyFailed(t, ctx, pool, ep1, tenantID)
	insertPermanentlyFailed(t, ctx, pool, ep2, tenantID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(fmt.Sprintf("%s/v1/dlq?endpoint_id=%s", ts.URL, ep1))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	var resp struct {
		Items []struct {
			EndpointID string `json:"endpoint_id"`
		} `json:"items"`
	}
	json.NewDecoder(res.Body).Decode(&resp)
	if len(resp.Items) != 1 {
		t.Fatalf("items: got %d, want 1", len(resp.Items))
	}
	if resp.Items[0].EndpointID != ep1.String() {
		t.Errorf("endpoint_id: got %q, want %q", resp.Items[0].EndpointID, ep1)
	}
}

func TestDLQList_InvalidUUIDQueryParam(t *testing.T) {
	_, pool := setupAPI(t)
	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	for _, tc := range []struct{ param, val string }{
		{"tenant_id", "not-a-uuid"},
		{"endpoint_id", "not-a-uuid"},
	} {
		t.Run(tc.param, func(t *testing.T) {
			res, err := http.Get(fmt.Sprintf("%s/v1/dlq?%s=%s", ts.URL, tc.param, tc.val))
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			res.Body.Close()
			if res.StatusCode != http.StatusBadRequest {
				t.Errorf("status: got %d, want 400", res.StatusCode)
			}
		})
	}
}

// TestDLQList_SC001_Freshness verifies a newly permanently-failed delivery appears < 1s.
func TestDLQList_SC001_Freshness(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://fresh.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	start := time.Now()
	insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	res, err := http.Get(ts.URL + "/v1/dlq")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()
	elapsed := time.Since(start)

	var resp struct {
		Items []json.RawMessage `json:"items"`
	}
	json.NewDecoder(res.Body).Decode(&resp)

	if len(resp.Items) == 0 {
		t.Error("SC-001: delivery not found in listing")
	}
	if elapsed >= time.Second {
		t.Errorf("SC-001: took %v, want < 1s", elapsed)
	}
}

// TestDLQList_SC004_Performance seeds 1000+ records and asserts first page < 1s.
func TestDLQList_SC004_Performance(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://perf.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)

	for i := 0; i < 1010; i++ {
		evtID := uuid.New()
		pool.QueryRow(ctx,
			`INSERT INTO events (id, endpoint_id, tenant_id, payload) VALUES ($1, $2, $3, '{}') RETURNING id`,
			evtID, endpointID, tenantID).Scan(&evtID)
		pool.QueryRow(ctx,
			`INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at)
			 VALUES ($1, $2, $3, 'permanently_failed', 3, NOW()) RETURNING id`,
			evtID, endpointID, tenantID).Scan(new(uuid.UUID))
	}

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	start := time.Now()
	res, err := http.Get(ts.URL + "/v1/dlq?limit=20")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()
	elapsed := time.Since(start)

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d; body=%s", res.StatusCode, b)
	}
	if elapsed >= time.Second {
		t.Errorf("SC-004: first page took %v, want < 1s", elapsed)
	}
}
