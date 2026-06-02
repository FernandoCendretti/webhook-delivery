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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/api"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
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

// --- US2: GET /v1/dlq/{delivery_id} ---

// insertAttempt seeds an attempt record for a delivery directly in DB.
func insertAttempt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, deliveryID uuid.UUID, seq int, outcome string, statusCode *int) {
	t.Helper()
	_, err := pool.Exec(ctx,
		`INSERT INTO attempts (delivery_id, sequence, started_at, completed_at, outcome, response_status_code)
		 VALUES ($1, $2, NOW() - interval '10 seconds', NOW(), $3, $4)`,
		deliveryID, seq, outcome, statusCode)
	if err != nil {
		t.Fatalf("insert attempt: %v", err)
	}
}

func TestDLQDetail_HappyPath(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://detail.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)
	deliveryID := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	sc503 := 503
	insertAttempt(t, ctx, pool, deliveryID, 1, "transient_failure", &sc503)
	insertAttempt(t, ctx, pool, deliveryID, 2, "transient_failure", &sc503)
	insertAttempt(t, ctx, pool, deliveryID, 3, "permanent_failure", &sc503)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(fmt.Sprintf("%s/v1/dlq/%s", ts.URL, deliveryID))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 200; body=%s", res.StatusCode, b)
	}

	var resp struct {
		DeliveryID   string `json:"delivery_id"`
		EndpointID   string `json:"endpoint_id"`
		TenantID     string `json:"tenant_id"`
		AttemptCount int    `json:"attempt_count"`
		FailedAt     string `json:"failed_at"`
		Attempts     []struct {
			Sequence           int    `json:"sequence"`
			Outcome            string `json:"outcome"`
			ResponseStatusCode *int   `json:"response_status_code"`
		} `json:"attempts"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.DeliveryID != deliveryID.String() {
		t.Errorf("delivery_id: got %q, want %q", resp.DeliveryID, deliveryID)
	}
	if resp.EndpointID != endpointID.String() {
		t.Errorf("endpoint_id: got %q, want %q", resp.EndpointID, endpointID)
	}
	if len(resp.Attempts) != 3 {
		t.Fatalf("attempts len: got %d, want 3", len(resp.Attempts))
	}
	// Verify sorted by sequence ASC
	for i, a := range resp.Attempts {
		if a.Sequence != i+1 {
			t.Errorf("attempt[%d].sequence: got %d, want %d", i, a.Sequence, i+1)
		}
	}
	if resp.FailedAt == "" {
		t.Error("failed_at must be present")
	}
}

func TestDLQDetail_NotFound(t *testing.T) {
	_, pool := setupAPI(t)
	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(fmt.Sprintf("%s/v1/dlq/%s", ts.URL, uuid.New()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", res.StatusCode)
	}
}

func TestDLQDetail_WrongStatus(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://wrongstatus.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)

	// Insert a delivery with status 'scheduled' (not permanently_failed)
	evtID := uuid.New()
	pool.QueryRow(ctx, `INSERT INTO events (id, endpoint_id, tenant_id, payload) VALUES ($1, $2, $3, '{}') RETURNING id`,
		evtID, endpointID, tenantID).Scan(&evtID)
	var scheduledID uuid.UUID
	pool.QueryRow(ctx,
		`INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at)
		 VALUES ($1, $2, $3, 'scheduled', 0, NOW()) RETURNING id`,
		evtID, endpointID, tenantID).Scan(&scheduledID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(fmt.Sprintf("%s/v1/dlq/%s", ts.URL, scheduledID))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", res.StatusCode)
	}
}

func TestDLQDetail_HTTPErrorAttempt(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://httperr.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)
	deliveryID := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	sc503 := 503
	insertAttempt(t, ctx, pool, deliveryID, 1, "transient_failure", &sc503)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(fmt.Sprintf("%s/v1/dlq/%s", ts.URL, deliveryID))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	var resp struct {
		Attempts []struct {
			Outcome            string `json:"outcome"`
			ResponseStatusCode *int   `json:"response_status_code"`
		} `json:"attempts"`
	}
	json.NewDecoder(res.Body).Decode(&resp)
	if len(resp.Attempts) != 1 {
		t.Fatalf("attempts: got %d, want 1", len(resp.Attempts))
	}
	if resp.Attempts[0].ResponseStatusCode == nil || *resp.Attempts[0].ResponseStatusCode != 503 {
		t.Errorf("response_status_code: got %v, want 503", resp.Attempts[0].ResponseStatusCode)
	}
}

func TestDLQDetail_TimeoutAttempt(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://timeout.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)
	deliveryID := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	// timeout attempt: no response_status_code
	insertAttempt(t, ctx, pool, deliveryID, 1, "timeout", nil)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(fmt.Sprintf("%s/v1/dlq/%s", ts.URL, deliveryID))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	var resp struct {
		Attempts []struct {
			Outcome            string `json:"outcome"`
			ResponseStatusCode *int   `json:"response_status_code"`
		} `json:"attempts"`
	}
	json.NewDecoder(res.Body).Decode(&resp)
	if len(resp.Attempts) != 1 {
		t.Fatalf("attempts: got %d, want 1", len(resp.Attempts))
	}
	if resp.Attempts[0].Outcome != "timeout" {
		t.Errorf("outcome: got %q, want timeout", resp.Attempts[0].Outcome)
	}
	if resp.Attempts[0].ResponseStatusCode != nil {
		t.Errorf("response_status_code: got %v, want null", resp.Attempts[0].ResponseStatusCode)
	}
}

func TestDLQDetail_InvalidUUID(t *testing.T) {
	_, pool := setupAPI(t)
	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Get(ts.URL + "/v1/dlq/not-a-uuid")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", res.StatusCode)
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

// --- US3: POST /v1/dlq/{delivery_id}/replay ---

// replayURL builds the replay endpoint URL for a delivery.
func replayURL(base string, deliveryID uuid.UUID) string {
	return fmt.Sprintf("%s/v1/dlq/%s/replay", base, deliveryID)
}

// insertPermanentlyFailedOrphanEndpoint seeds a permanently_failed delivery whose
// endpoint_id references a non-existent endpoint. Because deliveries.endpoint_id
// has a foreign key to endpoints, the insert temporarily disables the table's
// triggers (which include the FK constraint check) inside a single transaction.
func insertPermanentlyFailedOrphanEndpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID uuid.UUID) uuid.UUID {
	t.Helper()
	// A real endpoint + event satisfies the events.endpoint_id FK; the delivery
	// then points at a different, non-existent endpoint id.
	realEP, err := seedEndpoint(ctx, pool, "http://orphan-real.dlq.example.com")
	if err != nil {
		t.Fatalf("seedEndpoint: %v", err)
	}
	evtID := uuid.New()
	if err := pool.QueryRow(ctx,
		`INSERT INTO events (id, endpoint_id, tenant_id, payload) VALUES ($1, $2, $3, '{}') RETURNING id`,
		evtID, realEP, tenantID).Scan(&evtID); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	missingEP := uuid.New()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `ALTER TABLE deliveries DISABLE TRIGGER ALL`); err != nil {
		t.Fatalf("disable triggers: %v", err)
	}
	var id uuid.UUID
	if err := tx.QueryRow(ctx,
		`INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at)
		 VALUES ($1, $2, $3, 'permanently_failed', 3, NOW()) RETURNING id`,
		evtID, missingEP, tenantID).Scan(&id); err != nil {
		t.Fatalf("insert orphan delivery: %v", err)
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE deliveries ENABLE TRIGGER ALL`); err != nil {
		t.Fatalf("enable triggers: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
	return id
}

func TestDLQReplay_HappyPath(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://replay-happy.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)
	deliveryID := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Post(replayURL(ts.URL, deliveryID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 202; body=%s", res.StatusCode, b)
	}

	var resp struct {
		DeliveryID string `json:"delivery_id"`
		Status     string `json:"status"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.DeliveryID == deliveryID.String() {
		t.Error("replay must return a new delivery_id, got the original")
	}
	if resp.Status != "scheduled" {
		t.Errorf("status: got %q, want scheduled", resp.Status)
	}

	// The original delivery must remain permanently_failed.
	var origStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM deliveries WHERE id = $1`, deliveryID).Scan(&origStatus); err != nil {
		t.Fatalf("query original status: %v", err)
	}
	if origStatus != "permanently_failed" {
		t.Errorf("original status: got %q, want permanently_failed", origStatus)
	}

	// The new delivery must reference the original via source_delivery_id.
	newID := uuid.MustParse(resp.DeliveryID)
	var src uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT source_delivery_id FROM deliveries WHERE id = $1`, newID).Scan(&src); err != nil {
		t.Fatalf("query source_delivery_id: %v", err)
	}
	if src != deliveryID {
		t.Errorf("source_delivery_id: got %s, want %s", src, deliveryID)
	}
}

// TestDLQReplay_SC002_Latency asserts the replay responds in < 500 ms with a
// non-trivial (< 10 000) number of permanently-failed records in the DB.
func TestDLQReplay_SC002_Latency(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://replay-latency.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)
	for i := 0; i < 200; i++ {
		insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)
	}
	target := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	start := time.Now()
	res, err := http.Post(replayURL(ts.URL, target), "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer res.Body.Close()
	elapsed := time.Since(start)

	if res.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 202; body=%s", res.StatusCode, b)
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("SC-002: replay took %v, want < 500ms", elapsed)
	}
}

// TestDLQReplay_SC003_ReachesDelivered runs the full pipeline and verifies the
// replayed delivery reaches 'delivered' against a healthy endpoint.
func TestDLQReplay_SC003_ReachesDelivered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	_, pool := setupAPI(t)
	brokers := testKafkaBrokers(t)

	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dst.Close)

	endpointID, _ := seedEndpoint(ctx, pool, dst.URL)
	tenantID := uuid.MustParse(systemDefaultTenantID)
	deliveryID := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	pipe := startPipeline(ctx, t, pool, brokers, 30)
	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Post(replayURL(ts.URL, deliveryID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 202; body=%s", res.StatusCode, b)
	}
	var resp struct {
		DeliveryID string `json:"delivery_id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	newID := uuid.MustParse(resp.DeliveryID)

	if err := waitForDeliveryStatus(ctx, pipe.DS, newID, domain.StatusDelivered); err != nil {
		t.Fatalf("SC-003: %v", err)
	}
}

func TestDLQReplay_NotFound(t *testing.T) {
	_, pool := setupAPI(t)
	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Post(replayURL(ts.URL, uuid.New()), "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", res.StatusCode)
	}
}

func TestDLQReplay_WrongStatusConflict(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://replay-wrongstatus.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)

	evtID := uuid.New()
	pool.QueryRow(ctx, `INSERT INTO events (id, endpoint_id, tenant_id, payload) VALUES ($1, $2, $3, '{}') RETURNING id`,
		evtID, endpointID, tenantID).Scan(&evtID)
	var scheduledID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO deliveries (event_id, endpoint_id, tenant_id, status, attempt_count, next_attempt_at)
		 VALUES ($1, $2, $3, 'scheduled', 0, NOW()) RETURNING id`,
		evtID, endpointID, tenantID).Scan(&scheduledID); err != nil {
		t.Fatalf("insert scheduled delivery: %v", err)
	}

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Post(replayURL(ts.URL, scheduledID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", res.StatusCode)
	}
}

// TestDLQReplay_ConcurrentDuplicateConflict (SC-005): two concurrent replays of
// the same delivery yield exactly one 202 and one 409, and exactly one new
// delivery is created.
func TestDLQReplay_ConcurrentDuplicateConflict(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://replay-concurrent.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)
	deliveryID := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)
	url := replayURL(ts.URL, deliveryID)

	const n = 2
	codes := make([]int, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			res, err := http.Post(url, "application/json", nil)
			if err != nil {
				codes[idx] = -1
				return
			}
			res.Body.Close()
			codes[idx] = res.StatusCode
		}(i)
	}
	close(start)
	wg.Wait()

	var accepted, conflict int
	for _, c := range codes {
		switch c {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflict++
		}
	}
	if accepted != 1 || conflict != 1 {
		t.Errorf("SC-005: got codes %v, want exactly one 202 and one 409", codes)
	}

	var created int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deliveries WHERE source_delivery_id = $1`, deliveryID).Scan(&created); err != nil {
		t.Fatalf("count replays: %v", err)
	}
	if created != 1 {
		t.Errorf("SC-005: replay deliveries created: got %d, want 1", created)
	}
}

// TestDLQReplay_DeletedEndpointUnprocessable (US3-AS5): replaying a delivery whose
// endpoint no longer exists returns 422 and creates no new delivery.
func TestDLQReplay_DeletedEndpointUnprocessable(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()
	tenantID := uuid.MustParse(systemDefaultTenantID)

	deliveryID := insertPermanentlyFailedOrphanEndpoint(t, ctx, pool, tenantID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	res, err := http.Post(replayURL(ts.URL, deliveryID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("status: got %d, want 422", res.StatusCode)
	}

	var created int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM deliveries WHERE source_delivery_id = $1`, deliveryID).Scan(&created); err != nil {
		t.Fatalf("count replays: %v", err)
	}
	if created != 0 {
		t.Errorf("no replay should be created on 422; got %d", created)
	}
}

// TestDLQReplay_ChainAllowed (US3-AS6): replaying a delivery that is itself a
// replay returns 202.
func TestDLQReplay_ChainAllowed(t *testing.T) {
	_, pool := setupAPI(t)
	ctx := context.Background()

	endpointID, _ := seedEndpoint(ctx, pool, "http://replay-chain.dlq.example.com")
	tenantID := uuid.MustParse(systemDefaultTenantID)
	original := insertPermanentlyFailed(t, ctx, pool, endpointID, tenantID)

	ts := httptest.NewServer(setupDLQHandler(t, pool))
	t.Cleanup(ts.Close)

	// First replay of the original.
	res1, err := http.Post(replayURL(ts.URL, original), "application/json", nil)
	if err != nil {
		t.Fatalf("POST first replay: %v", err)
	}
	defer res1.Body.Close()
	if res1.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res1.Body)
		t.Fatalf("first replay status: got %d, want 202; body=%s", res1.StatusCode, b)
	}
	var first struct {
		DeliveryID string `json:"delivery_id"`
	}
	json.NewDecoder(res1.Body).Decode(&first)
	firstReplayID := uuid.MustParse(first.DeliveryID)

	// Drive the first replay into permanently_failed (as if it had exhausted retries).
	if _, err := pool.Exec(ctx,
		`UPDATE deliveries SET status = 'permanently_failed', updated_at = NOW() WHERE id = $1`,
		firstReplayID); err != nil {
		t.Fatalf("mark first replay failed: %v", err)
	}

	// Replay the replay — chains are allowed.
	res2, err := http.Post(replayURL(ts.URL, firstReplayID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST chained replay: %v", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res2.Body)
		t.Fatalf("chained replay status: got %d, want 202; body=%s", res2.StatusCode, b)
	}
}
