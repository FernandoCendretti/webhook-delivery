//go:build integration

// E2E test covering the full pipeline with signing + idempotency together (T042).
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/signing"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

// TestE2E_SigningAndIdempotency verifies end-to-end that:
//   - An outgoing delivery POST carries correct signing headers.
//   - Resubmitting with the same Idempotency-Key returns the same event/delivery IDs.
//   - Exactly one event row, one delivery row, and one idempotency record exist.
func TestE2E_SigningAndIdempotency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pool := setupSigningPool(t)

	ch := make(chan webhookCapture, 2)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ch <- webhookCapture{headers: r.Header.Clone(), body: body}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.Close)

	// Register endpoint and capture signing secret.
	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool))
	ep, err := endpointSvc.Register(ctx, receiver.URL)
	if err != nil {
		t.Fatalf("register endpoint: %v", err)
	}
	if ep.SigningSecret == nil {
		t.Fatal("endpoint.SigningSecret is nil after Register")
	}
	signingSecret := ep.SigningSecret

	// Start pipeline.
	brokers := testKafkaBrokers(t)
	tp := startPipeline(ctx, t, pool, brokers, 30)

	// First submission with an idempotency key.
	const idemKey = "e2e-test-key-001"
	payload := json.RawMessage(`{"e2e":true}`)
	d1, err := tp.EventSvc.Submit(ctx, ep.ID, payload, idemKey, payload)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}

	// Wait for delivery to complete and capture the outgoing POST.
	if err := waitForDeliveryStatus(ctx, tp.DS, d1.ID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	var cap webhookCapture
	select {
	case cap = <-ch:
	case <-ctx.Done():
		t.Fatal("timed out waiting for webhook delivery")
	}

	// Verify signing headers.
	tsStr := cap.headers.Get("X-Webhook-Timestamp")
	sigStr := cap.headers.Get("X-Webhook-Signature")
	if tsStr == "" || sigStr == "" {
		t.Fatalf("signing headers missing: ts=%q sig=%q", tsStr, sigStr)
	}
	tsVal, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		t.Fatalf("X-Webhook-Timestamp not int64: %q", tsStr)
	}
	if got := signing.Sign(signingSecret, tsVal, cap.body); got != sigStr {
		t.Errorf("signature verification failed: computed %q, header %q", got, sigStr)
	}

	// Second submission with the same key and identical payload — must be idempotent.
	d2, err := tp.EventSvc.Submit(ctx, ep.ID, payload, idemKey, payload)
	if err != nil {
		t.Fatalf("second (idempotent) submit: %v", err)
	}
	if d2.ID != d1.ID {
		t.Errorf("idempotent resubmission returned different delivery_id: first=%v second=%v", d1.ID, d2.ID)
	}

	// Assert DB invariants: exactly 1 event, 1 delivery, 1 idempotency record.
	var eventCount int
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM events WHERE id=$1`, d1.EventID).Scan(&eventCount) //nolint:errcheck
	if eventCount != 1 {
		t.Errorf("events count: got %d, want 1", eventCount)
	}

	var deliveryCount int
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM deliveries WHERE id=$1`, d1.ID).Scan(&deliveryCount) //nolint:errcheck
	if deliveryCount != 1 {
		t.Errorf("deliveries count: got %d, want 1", deliveryCount)
	}

	var idemCount int
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM idempotency_records WHERE endpoint_id=$1 AND idempotency_key=$2`,
		ep.ID, idemKey).Scan(&idemCount) //nolint:errcheck
	if idemCount != 1 {
		t.Errorf("idempotency_records count: got %d, want 1", idemCount)
	}
}
