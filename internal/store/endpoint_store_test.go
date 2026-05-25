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

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func setupStorePool(t *testing.T) *pgxpool.Pool {
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
	for _, name := range []string{"001_init.sql", "002_signing_secret.sql", "003_idempotency.sql"} {
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

// T038: UpdateSecret returns domain.ErrNotFound for a non-existent endpoint.
func TestUpdateSecret_NotFound(t *testing.T) {
	pool := setupStorePool(t)
	es := store.NewEndpointStore(pool)

	secret := make([]byte, 32)
	err := es.UpdateSecret(context.Background(), uuid.New(), secret)
	if err == nil {
		t.Fatal("UpdateSecret: expected error, got nil")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("UpdateSecret: got %v, want domain.ErrNotFound", err)
	}
}
