//go:build integration

// Integration tests for US1: outgoing delivery POST carries signing headers (T009-T011).
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
	"github.com/FernandoCendretti/webhook-delivery/internal/signing"
	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

type webhookCapture struct {
	headers http.Header
	body    []byte
}

// TestWorkerSigning_HeadersPresentAndValid asserts that every outgoing delivery POST
// carries X-Webhook-Timestamp and X-Webhook-Signature, and that the consumer
// verification procedure produces the correct value (FR-003, FR-004, SC-001, SC-002).
func TestWorkerSigning_HeadersPresentAndValid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pool := setupSigningPool(t)

	ch := make(chan webhookCapture, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ch <- webhookCapture{headers: r.Header.Clone(), body: body}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.Close)

	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool))
	ep, err := endpointSvc.Register(ctx, receiver.URL, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("register endpoint: %v", err)
	}
	if ep.SigningSecret == nil {
		t.Fatal("endpoint.SigningSecret is nil after Register — T015 not implemented")
	}

	brokers := testKafkaBrokers(t)
	tp := startPipeline(ctx, t, pool, brokers, 30)

	payload := json.RawMessage(`{"test":"signing"}`)
	d, err := tp.EventSvc.Submit(ctx, ep.ID, payload, "", payload, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}

	if err := waitForDeliveryStatus(ctx, tp.DS, d.ID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}

	var cap webhookCapture
	select {
	case cap = <-ch:
	case <-ctx.Done():
		t.Fatal("timed out waiting for webhook delivery")
	}

	tsStr := cap.headers.Get("X-Webhook-Timestamp")
	sigStr := cap.headers.Get("X-Webhook-Signature")
	if tsStr == "" {
		t.Fatal("X-Webhook-Timestamp header missing")
	}
	if sigStr == "" {
		t.Fatal("X-Webhook-Signature header missing")
	}
	if len(sigStr) != 64 {
		t.Errorf("X-Webhook-Signature len = %d, want 64", len(sigStr))
	}

	// Consumer verification procedure (spec §Signing Scheme Contract).
	tsVal, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		t.Fatalf("X-Webhook-Timestamp is not a valid int64: %q", tsStr)
	}
	expected := signing.Sign(ep.SigningSecret, tsVal, cap.body)
	if expected != sigStr {
		t.Errorf("signature mismatch:\n got  %q\n want %q", sigStr, expected)
	}
}

// TestWorkerSigning_RetryHasNewTimestamp asserts that a retry delivery attempt uses a
// freshly computed X-Webhook-Timestamp that differs from the first attempt (FR-015).
func TestWorkerSigning_RetryHasNewTimestamp(t *testing.T) {
	restore := domain.UseShortScheduleForTests()
	t.Cleanup(restore)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pool := setupSigningPool(t)

	ch := make(chan webhookCapture, 2)
	var count atomic.Int32
	// Sleep 1.1 s on the first attempt so the retry lands in a different Unix second.
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ch <- webhookCapture{headers: r.Header.Clone(), body: body}
		if count.Add(1) == 1 {
			time.Sleep(1100 * time.Millisecond)
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(receiver.Close)

	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool))
	ep, err := endpointSvc.Register(ctx, receiver.URL, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("register endpoint: %v", err)
	}
	if ep.SigningSecret == nil {
		t.Fatal("endpoint.SigningSecret is nil after Register — T015 not implemented")
	}

	brokers := testKafkaBrokers(t)
	tp := startPipeline(ctx, t, pool, brokers, 30)

	payload := json.RawMessage(`{"test":"retry-signing"}`)
	d, err := tp.EventSvc.Submit(ctx, ep.ID, payload, "", payload, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}

	if err := waitForDeliveryStatus(ctx, tp.DS, d.ID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}

	var first, second webhookCapture
	select {
	case first = <-ch:
	case <-ctx.Done():
		t.Fatal("timed out waiting for first attempt")
	}
	select {
	case second = <-ch:
	case <-ctx.Done():
		t.Fatal("timed out waiting for retry attempt")
	}

	ts1 := first.headers.Get("X-Webhook-Timestamp")
	ts2 := second.headers.Get("X-Webhook-Timestamp")
	if ts1 == "" || ts2 == "" {
		t.Fatalf("X-Webhook-Timestamp missing: first=%q second=%q", ts1, ts2)
	}
	if ts1 == ts2 {
		t.Errorf("X-Webhook-Timestamp must differ between attempts; both=%q", ts1)
	}

	// Retry signature must verify against its own timestamp.
	ts2Val, _ := strconv.ParseInt(ts2, 10, 64)
	expected := signing.Sign(ep.SigningSecret, ts2Val, second.body)
	if expected != second.headers.Get("X-Webhook-Signature") {
		t.Error("retry X-Webhook-Signature does not verify against current secret")
	}
}

// TestWorkerSigning_EmptyPayload asserts that signing headers are present and valid
// even when the event payload is an empty JSON object (FR-003, FR-004).
func TestWorkerSigning_EmptyPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pool := setupSigningPool(t)

	ch := make(chan webhookCapture, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ch <- webhookCapture{headers: r.Header.Clone(), body: body}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.Close)

	endpointSvc := service.NewEndpointService(store.NewEndpointStore(pool))
	ep, err := endpointSvc.Register(ctx, receiver.URL, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("register endpoint: %v", err)
	}
	if ep.SigningSecret == nil {
		t.Fatal("endpoint.SigningSecret is nil after Register — T015 not implemented")
	}

	brokers := testKafkaBrokers(t)
	tp := startPipeline(ctx, t, pool, brokers, 30)

	// Empty JSON object as event payload.
	payload := json.RawMessage(`{}`)
	d, err := tp.EventSvc.Submit(ctx, ep.ID, payload, "", payload, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}

	if err := waitForDeliveryStatus(ctx, tp.DS, d.ID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}

	var cap webhookCapture
	select {
	case cap = <-ch:
	case <-ctx.Done():
		t.Fatal("timed out waiting for webhook delivery")
	}

	tsStr := cap.headers.Get("X-Webhook-Timestamp")
	sigStr := cap.headers.Get("X-Webhook-Signature")
	if tsStr == "" {
		t.Fatal("X-Webhook-Timestamp missing for empty payload delivery")
	}
	if sigStr == "" {
		t.Fatal("X-Webhook-Signature missing for empty payload delivery")
	}

	tsVal, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		t.Fatalf("X-Webhook-Timestamp is not a valid int64: %q", tsStr)
	}
	expected := signing.Sign(ep.SigningSecret, tsVal, cap.body)
	if expected != sigStr {
		t.Errorf("empty-payload signature mismatch:\n got  %q\n want %q", sigStr, expected)
	}
}
