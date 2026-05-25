//go:build integration

// SC-007 E2E load test (T062):
// Submit 1 000 events at ~50 events/sec to a healthy endpoint, restart all
// pipeline components mid-run, then assert every delivery reaches 'delivered'
// (at-least-once: duplicate HTTP calls to the destination are acceptable).
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FernandoCendretti/webhook-delivery/internal/delivery"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/queue"
	"github.com/FernandoCendretti/webhook-delivery/internal/scheduler"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

const (
	sc007EventCount = 1_000
	sc007RatePerSec = 50
	sc007Concurrency = 8
)

func TestSC007_AtLeastOnceDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("SC-007 is a long-running load test; skipped in short mode")
	}

	// Generous timeout: 1000/50 = 20 s submission + processing overhead.
	outerCtx, outerCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(outerCancel)

	_, pool := setupAPI(t)
	brokers := testKafkaBrokers(t)
	silentLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	topic := "wh.sc007." + uuid.NewString()[:8]

	// Destination counts received calls; always 200.
	var received atomic.Int64
	dstURL := startDestinationServer(t, func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	epID, err := seedEndpoint(outerCtx, pool, dstURL)
	if err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}

	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool))
	eventSvc := service.NewEventService(pool, endpointSvc)

	// Submit 1 000 events at 50/sec; collect delivery IDs.
	deliveryIDs := sc007Submit(outerCtx, t, eventSvc, epID)
	t.Logf("submitted %d events", len(deliveryIDs))

	// --- Phase 1: run pipeline for roughly the first half. ---
	phase1Ctx, phase1Cancel := context.WithCancel(outerCtx)
	sc007StartPipeline(t, pool, brokers, topic, sc007Concurrency, silentLog, phase1Ctx)

	sc007WaitFraction(outerCtx, t, pool, deliveryIDs, 0.45)
	phase1Cancel() // simulate crash / restart
	time.Sleep(300 * time.Millisecond)

	// --- Phase 2: restart with fresh goroutines. ---
	sc007StartPipeline(t, pool, brokers, topic, sc007Concurrency, silentLog, outerCtx)

	// Wait for all deliveries to reach a terminal state.
	sc007WaitAllTerminal(outerCtx, t, pool, deliveryIDs)

	// Assert: no data loss.
	failed := sc007CountByStatus(outerCtx, pool, deliveryIDs, domain.StatusPermanentlyFailed)
	delivered := sc007CountByStatus(outerCtx, pool, deliveryIDs, domain.StatusDelivered)

	if failed != 0 {
		t.Errorf("SC-007: %d permanently_failed (data loss), want 0", failed)
	}
	if delivered != sc007EventCount {
		t.Errorf("SC-007: delivered %d, want %d", delivered, sc007EventCount)
	}
	t.Logf("SC-007 passed: %d delivered, destination received %d HTTP calls (at-least-once duplicates: %d)",
		delivered, received.Load(), received.Load()-int64(delivered))
}

// sc007Submit sends count events at ratePerSec and returns their delivery IDs.
func sc007Submit(ctx context.Context, t *testing.T, svc *service.EventService, epID uuid.UUID) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, 0, sc007EventCount)
	payload, _ := json.Marshal(map[string]string{"sc007": "payload"})
	ticker := time.NewTicker(time.Second / time.Duration(sc007RatePerSec))
	defer ticker.Stop()
	for i := 0; i < sc007EventCount; i++ {
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled after %d/%d submissions", i, sc007EventCount)
		case <-ticker.C:
		}
		d, err := svc.Submit(ctx, epID, payload, "", payload)
		if err != nil {
			t.Fatalf("submit event %d: %v", i, err)
		}
		ids = append(ids, d.ID)
	}
	return ids
}

// sc007StartPipeline launches a scheduler + concurrency workers under pipeCtx.
func sc007StartPipeline(
	t *testing.T,
	pool *pgxpool.Pool,
	brokers []string,
	topic string,
	concurrency int,
	log *slog.Logger,
	pipeCtx context.Context,
) {
	t.Helper()
	ds := store.NewDeliveryStore(pool)
	as := store.NewAttemptStore(pool)

	pub := queue.NewPublisher(queue.PublisherConfig{Brokers: brokers, Topic: topic, Logger: log})
	t.Cleanup(func() { _ = pub.Close() })

	sched := scheduler.New(scheduler.Config{
		DeliveryStore: ds,
		Publisher:     pub,
		BatchSize:     100,
		LeaseDuration: 60 * time.Second,
		Logger:        log,
	})
	go func() { _ = sched.Run(pipeCtx, 20*time.Millisecond) }()

	for i := 0; i < concurrency; i++ {
		cons := queue.NewConsumer(queue.ConsumerConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: "sc007-" + uuid.NewString()[:6],
			Logger:  log,
		})
		t.Cleanup(func() { _ = cons.Close() })
		w := delivery.NewWorker(delivery.WorkerConfig{
			DeliveryStore: ds,
			AttemptStore:  as,
			Consumer:      cons,
			Pool:          pool,
			Logger:        log,
		})
		go func() { _ = w.Run(pipeCtx) }()
	}
}

func sc007WaitFraction(ctx context.Context, t *testing.T, pool *pgxpool.Pool, ids []uuid.UUID, fraction float64) {
	t.Helper()
	target := int(float64(len(ids)) * fraction)
	for {
		if ctx.Err() != nil {
			return
		}
		var n int
		_ = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM deliveries
			 WHERE id = ANY($1::uuid[]) AND status IN ('delivered','permanently_failed')`,
			sc007IDsToArray(ids)).Scan(&n)
		if n >= target {
			t.Logf("phase 1 done: %d/%d terminal", n, len(ids))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func sc007WaitAllTerminal(ctx context.Context, t *testing.T, pool *pgxpool.Pool, ids []uuid.UUID) {
	t.Helper()
	for {
		if ctx.Err() != nil {
			t.Error("timeout: not all deliveries reached terminal state")
			return
		}
		var pending int
		_ = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM deliveries
			 WHERE id = ANY($1::uuid[]) AND status NOT IN ('delivered','permanently_failed')`,
			sc007IDsToArray(ids)).Scan(&pending)
		if pending == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func sc007CountByStatus(ctx context.Context, pool *pgxpool.Pool, ids []uuid.UUID, status domain.DeliveryStatus) int {
	var n int
	_ = pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM deliveries WHERE id = ANY($1::uuid[]) AND status = $2`,
		sc007IDsToArray(ids), string(status)).Scan(&n)
	return n
}

// sc007IDsToArray converts uuid slice to a pgx-compatible []string for ANY().
func sc007IDsToArray(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

func startDestinationServer(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	ln, err := listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String()
}
