//go:build integration

package integration_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/FernandoCendretti/webhook-delivery/internal/api"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// allMigrations returns the names of all migration files in apply order.
var allMigrations = []string{
	"001_init.sql",
	"002_signing_secret.sql",
	"003_idempotency.sql",
	"004_tenants.sql",
	"005_tenant_columns.sql",
	"006_circuit_breaker.sql",
}

// setup003Pool starts a Postgres testcontainer and applies all six migrations.
func setup003Pool(t *testing.T) *pgxpool.Pool {
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
	pool, err := store.NewPool(ctx, store.PoolConfig{DatabaseURL: connStr, MaxConns: 10})
	if err != nil {
		t.Fatalf("pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	for _, name := range allMigrations {
		sql := loadMigrationFile(t, name)
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
	return pool
}

// setup003API wires the full API (all routes including tenants) against a pool
// that has all six migrations applied.
func setup003API(t *testing.T) (http.Handler, *pgxpool.Pool) {
	t.Helper()
	pool := setup003Pool(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s := api.NewServer(api.ServerConfig{APIAddr: ":0", Logger: logger})
	endpointStore := store.NewEndpointStore(pool)
	tenantStore := store.NewTenantStore(pool)
	tenantSvc := service.NewTenantService(tenantStore)
	endpointSvc := service.NewEndpointService(endpointStore, tenantSvc)
	s.RegisterTenants(tenantSvc)
	s.RegisterEndpoints(endpointSvc)
	s.RegisterEvents(service.NewEventService(pool, endpointSvc))
	s.RegisterDeliveries(service.NewDeliveryService(store.NewDeliveryStore(pool)))
	return s.Mux(), pool
}
