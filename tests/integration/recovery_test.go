//go:build integration

package integration_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/FernandoCendretti/webhook-delivery/internal/delivery"
	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/queue"
	"github.com/FernandoCendretti/webhook-delivery/internal/recovery"
	"github.com/FernandoCendretti/webhook-delivery/internal/scheduler"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// TestRecovery_ReaperResurrectsExpiredLease — T050
//
// Flow:
//  1. Submit event → delivery scheduled.
//  2. Scheduler claims it with a very short lease (1 s).
//  3. Worker 1's context is cancelled before it can fetch the message,
//     simulating a crash (the delivery remains in_flight with an expiring lease).
//  4. After the lease expires the reaper resets delivery → scheduled.
//  5. Scheduler re-claims; Worker 2 delivers successfully.
func TestRecovery_ReaperResurrectsExpiredLease(t *testing.T) {
	outerCtx, outerCancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(outerCancel)

	_, pool := setupAPI(t)
	brokers := testKafkaBrokers(t)

	const leaseSeconds = 1
	silentLog := slog.New(slog.NewTextHandler(io.Discard, nil))

	topic := "wh.recovery." + "t050"
	ds := store.NewDeliveryStore(pool)
	as := store.NewAttemptStore(pool)
	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool))
	eventSvc := service.NewEventService(pool, endpointSvc)

	// Destination always succeeds.
	dstURL := newFlakeyServer(t, 0, http.StatusOK, http.StatusOK)
	epID, err := seedEndpoint(outerCtx, pool, dstURL)
	if err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	deliveryID, err := submitEvent(outerCtx, eventSvc, epID)
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}

	pub := queue.NewPublisher(queue.PublisherConfig{Brokers: brokers, Topic: topic, Logger: silentLog})
	t.Cleanup(func() { _ = pub.Close() })

	// -- Phase 1: scheduler claims the delivery with a short lease.
	sched := scheduler.New(scheduler.Config{
		DeliveryStore: ds,
		Publisher:     pub,
		BatchSize:     1,
		LeaseDuration: leaseSeconds * time.Second,
		Logger:        silentLog,
	})
	if err := sched.Tick(outerCtx); err != nil {
		t.Fatalf("scheduler tick 1: %v", err)
	}

	d, _ := ds.GetByID(outerCtx, deliveryID)
	if d.Status != domain.StatusInFlight {
		t.Fatalf("expected in_flight, got %q", d.Status)
	}

	// -- Phase 2: worker 1 has its context cancelled (crash simulation).
	// The Kafka message stays unconsumed; delivery remains in_flight until lease expires.
	crashCtx, crashCancel := context.WithCancel(outerCtx)
	crashCancel() // cancel immediately
	cons1 := queue.NewConsumer(queue.ConsumerConfig{
		Brokers: brokers, Topic: topic, GroupID: "wkr-crash", Logger: silentLog,
	})
	t.Cleanup(func() { _ = cons1.Close() })
	w1 := delivery.NewWorker(delivery.WorkerConfig{
		DeliveryStore: ds, AttemptStore: as, Consumer: cons1, Pool: pool, Logger: silentLog,
	})
	_ = w1.ProcessOne(crashCtx) // expected to fail — that's the point

	// -- Phase 3: wait for the lease to expire, then run the reaper.
	time.Sleep(time.Duration(leaseSeconds)*time.Second + 300*time.Millisecond)

	reap := recovery.New(recovery.Config{
		Store: ds, Interval: 50 * time.Millisecond, Logger: silentLog,
	})
	if err := reap.Tick(outerCtx); err != nil {
		t.Fatalf("reaper tick: %v", err)
	}

	d, _ = ds.GetByID(outerCtx, deliveryID)
	if d.Status != domain.StatusScheduled {
		t.Fatalf("expected scheduled after reap, got %q", d.Status)
	}

	// -- Phase 4: re-claim via scheduler and deliver with worker 2.
	if err := sched.Tick(outerCtx); err != nil {
		t.Fatalf("scheduler tick 2: %v", err)
	}

	cons2 := queue.NewConsumer(queue.ConsumerConfig{
		Brokers: brokers, Topic: topic, GroupID: "wkr-ok", Logger: silentLog,
	})
	t.Cleanup(func() { _ = cons2.Close() })
	w2 := delivery.NewWorker(delivery.WorkerConfig{
		DeliveryStore: ds, AttemptStore: as, Consumer: cons2, Pool: pool, Logger: silentLog,
	})
	if err := w2.ProcessOne(outerCtx); err != nil {
		t.Fatalf("worker 2 process: %v", err)
	}

	d, _ = ds.GetByID(outerCtx, deliveryID)
	if d.Status != domain.StatusDelivered {
		t.Errorf("final status: got %q, want %q", d.Status, domain.StatusDelivered)
	}
}
