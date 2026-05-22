//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/queue"
	"github.com/FernandoCendretti/webhook-delivery/internal/scheduler"
	"github.com/FernandoCendretti/webhook-delivery/internal/delivery"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func TestPipeline_HappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	handler, pool := setupFullAPIWithPool(t)
	apiServer := httptest.NewServer(handler)
	t.Cleanup(apiServer.Close)

	// Destination server that captures the request body.
	var (
		receivedBody atomic.Value
		receivedCT   atomic.Value
	)
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedBody.Store(b)
		receivedCT.Store(r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dst.Close)

	// Register destination endpoint via API.
	epBody, _ := json.Marshal(map[string]string{"url": dst.URL})
	res, err := http.Post(apiServer.URL+"/v1/endpoints", "application/json", bytes.NewReader(epBody))
	if err != nil {
		t.Fatalf("register endpoint: %v", err)
	}
	defer res.Body.Close()
	var ep struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&ep); err != nil {
		t.Fatalf("decode endpoint: %v", err)
	}

	// Submit event.
	payload := map[string]string{"hello": "pipeline"}
	payloadBytes, _ := json.Marshal(payload)
	evtBody, _ := json.Marshal(map[string]any{
		"endpoint_id": ep.ID,
		"payload":     json.RawMessage(payloadBytes),
	})
	res2, err := http.Post(apiServer.URL+"/v1/events", "application/json", bytes.NewReader(evtBody))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res2.Body)
		t.Fatalf("submit status %d: %s", res2.StatusCode, b)
	}
	var accepted struct {
		DeliveryID string `json:"delivery_id"`
	}
	if err := json.NewDecoder(res2.Body).Decode(&accepted); err != nil {
		t.Fatalf("decode accepted: %v", err)
	}
	deliveryID, _ := uuid.Parse(accepted.DeliveryID)

	// Start in-process Kafka (via testcontainers) — use the shared broker from
	// the test environment or skip if unavailable.
	brokers := testKafkaBrokers(t)

	topic := "webhook.deliveries.test." + uuid.NewString()[:8]
	pub := queue.NewPublisher(queue.PublisherConfig{Brokers: brokers, Topic: topic})
	t.Cleanup(func() { _ = pub.Close() })

	ds := store.NewDeliveryStore(pool)
	as := store.NewAttemptStore(pool)

	// Run scheduler for one tick to claim the delivery and publish to Kafka.
	sched := scheduler.New(scheduler.Config{
		DeliveryStore: ds,
		Publisher:     pub,
		BatchSize:     10,
		LeaseDuration: 30 * time.Second,
	})
	if err := sched.Tick(ctx); err != nil {
		t.Fatalf("scheduler tick: %v", err)
	}

	// Run worker for one message.
	cons := queue.NewConsumer(queue.ConsumerConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "test-worker-" + uuid.NewString()[:8],
	})
	t.Cleanup(func() { _ = cons.Close() })

	w := delivery.NewWorker(delivery.WorkerConfig{
		DeliveryStore: ds,
		AttemptStore:  as,
		Consumer:      cons,
		Pool:          pool,
	})
	if err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("worker process: %v", err)
	}

	// Assert delivery status = delivered.
	d, err := ds.GetByID(ctx, deliveryID)
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if d.Status != domain.StatusDelivered {
		t.Errorf("status: got %q, want %q", d.Status, domain.StatusDelivered)
	}

	// Assert destination received the payload.
	body, ok := receivedBody.Load().([]byte)
	if !ok || len(body) == 0 {
		t.Fatal("destination did not receive any body")
	}
	if !bytes.Equal(bytes.TrimSpace(body), payloadBytes) {
		t.Errorf("body mismatch:\n got  %s\n want %s", body, payloadBytes)
	}
	ct, _ := receivedCT.Load().(string)
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}
}
