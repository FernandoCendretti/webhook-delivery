//go:build integration

// Integration tests for the endpoints API (T021).
//
// Run: `make test-integration` (which passes `-tags integration`). Docker is
// required for the postgres testcontainer.
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/FernandoCendretti/webhook-delivery/internal/api"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func setupAPI(t *testing.T) (http.Handler, *pgxpool.Pool) {
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
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgCtr.Terminate(context.Background()) })

	connStr, err := pgCtr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	pool, err := store.NewPool(ctx, store.PoolConfig{DatabaseURL: connStr, MaxConns: 5})
	if err != nil {
		t.Fatalf("pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, loadMigrationUp(t)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	s := api.NewServer(api.ServerConfig{
		APIAddr: ":0",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	tenantStore := store.NewTenantStore(pool)
	tenantSvc := service.NewTenantService(tenantStore)
	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool), tenantSvc)
	circuitStore := store.NewCircuitStore(pool)
	s.RegisterTenants(tenantSvc)
	s.RegisterEndpoints(endpointSvc)
	s.RegisterEvents(service.NewEventService(pool, endpointSvc))
	s.RegisterDeliveries(service.NewDeliveryService(store.NewDeliveryStore(pool)))
	s.RegisterCircuitBreaker(circuitStore)
	return s.Mux(), pool
}

// loadMigrationUp returns the combined Up SQL for all six migrations,
// suitable for direct execution against a test database.
func loadMigrationUp(t *testing.T) string {
	t.Helper()
	var combined string
	for _, name := range allMigrations {
		combined += loadMigrationFile(t, name) + "\n"
	}
	return combined
}

func TestEndpointsAPI_Register_Valid(t *testing.T) {
	handler, _ := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	// Create a tenant first (required by FR-007).
	tenantRes, err := http.Post(ts.URL+"/v1/tenants", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST /v1/tenants: %v", err)
	}
	var tenant struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(tenantRes.Body).Decode(&tenant); err != nil {
		t.Fatalf("decode tenant: %v", err)
	}
	tenantRes.Body.Close()

	body := `{"url":"https://example.com/webhook","tenant_id":"` + tenant.ID + `"}`
	res, err := http.Post(ts.URL+"/v1/endpoints", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/endpoints: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 201; body=%s", res.StatusCode, string(b))
	}

	var resp struct {
		ID        string `json:"id"`
		URL       string `json:"url"`
		TenantID  string `json:"tenant_id"`
		CreatedAt string `json:"created_at"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, err := uuid.Parse(resp.ID); err != nil {
		t.Errorf("id is not a uuid: %q (%v)", resp.ID, err)
	}
	if resp.URL != "https://example.com/webhook" {
		t.Errorf("url: got %q, want %q", resp.URL, "https://example.com/webhook")
	}
	if resp.TenantID != tenant.ID {
		t.Errorf("tenant_id: got %q, want %q", resp.TenantID, tenant.ID)
	}
	if resp.CreatedAt == "" {
		t.Error("created_at is empty")
	}
}

func TestEndpointsAPI_Register_Invalid(t *testing.T) {
	handler, _ := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res, err := http.Post(ts.URL+"/v1/endpoints", "application/json",
		strings.NewReader(`{"url":"ftp://example.com/file"}`))
	if err != nil {
		t.Fatalf("POST /v1/endpoints: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, string(b))
	}
}

func TestEndpointsAPI_Get_Existing(t *testing.T) {
	handler, pool := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	ctx := context.Background()
	// Migration 005 inserts system-default-tenant; use it for direct SQL seeds.
	const systemTenant = "00000000-0000-0000-0000-000000000001"
	var id uuid.UUID
	err := pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, signing_secret, tenant_id) VALUES ($1, gen_random_bytes(32), $2) RETURNING id`,
		"https://example.com/seeded", systemTenant).Scan(&id)
	if err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}

	res, err := http.Get(ts.URL + "/v1/endpoints/" + id.String())
	if err != nil {
		t.Fatalf("GET /v1/endpoints/%s: %v", id, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 200; body=%s", res.StatusCode, string(b))
	}
	var resp struct {
		ID  string `json:"id"`
		URL string `json:"url"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID != id.String() {
		t.Errorf("id: got %q, want %q", resp.ID, id.String())
	}
	if resp.URL != "https://example.com/seeded" {
		t.Errorf("url: got %q, want %q", resp.URL, "https://example.com/seeded")
	}
}

func TestEndpointsAPI_Get_Unknown(t *testing.T) {
	handler, _ := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res, err := http.Get(ts.URL + "/v1/endpoints/" + uuid.NewString())
	if err != nil {
		t.Fatalf("GET /v1/endpoints/<unknown>: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 404; body=%s", res.StatusCode, string(b))
	}
	// Default ServeMux 404 returns text/plain. Asserting the JSON shape from
	// the plan ({ "error": "endpoint_not_found" }) keeps this test red until
	// the real handler is wired by T025/T026.
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if resp.Error != "endpoint_not_found" {
		t.Errorf("error code: got %q, want %q", resp.Error, "endpoint_not_found")
	}
}
