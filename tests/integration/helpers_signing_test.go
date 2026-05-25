//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// loadMigrationFile reads a migration file by name from internal/store/migrations/
// and returns the SQL for the Up section, suitable for direct execution.
func loadMigrationFile(t *testing.T, filename string) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(file), "..", "..")
	raw, err := os.ReadFile(filepath.Join(repoRoot, "internal/store/migrations", filename))
	if err != nil {
		t.Fatalf("read migration %s: %v", filename, err)
	}
	s := string(raw)
	upIdx := strings.Index(s, "-- +goose Up")
	downIdx := strings.Index(s, "-- +goose Down")
	if upIdx < 0 || downIdx < 0 {
		t.Fatalf("goose markers not found in %s", filename)
	}
	return s[upIdx:downIdx]
}

// setupSigningPool starts a Postgres container and applies all three migrations.
// Returns the connection pool.
func setupSigningPool(t *testing.T) *pgxpool.Pool {
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
	pool, err := store.NewPool(ctx, store.PoolConfig{DatabaseURL: connStr, MaxConns: 10})
	if err != nil {
		t.Fatalf("pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	for _, name := range []string{"001_init.sql", "002_signing_secret.sql", "003_idempotency.sql"} {
		sql := loadMigrationFile(t, name)
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}

	return pool
}

// setupSigningAPIWithPool creates a Postgres container with all three migrations
// and wires the full HTTP handler (endpoints + events + deliveries).
func setupSigningAPIWithPool(t *testing.T) (http.Handler, *pgxpool.Pool) {
	t.Helper()
	pool := setupSigningPool(t)
	return setupFullAPI(t, pool), pool
}
