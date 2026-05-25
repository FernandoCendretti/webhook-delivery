//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// TestRetry_TransientThenSuccess — T047
// Destination returns 503 three times, then 200.
// Expects delivery to be 'delivered' with exactly 4 attempts.
func TestRetry_TransientThenSuccess(t *testing.T) {
	restore := domain.UseShortScheduleForTests()
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	_, pool := setupAPI(t)
	brokers := testKafkaBrokers(t)
	pipe := startPipeline(ctx, t, pool, brokers, 30)

	dstURL := newFlakeyServer(t, 3, http.StatusServiceUnavailable, http.StatusOK)

	epID, err := seedEndpoint(ctx, pool, dstURL)
	if err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	deliveryID, err := submitEvent(ctx, pipe.EventSvc, epID)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if err := waitForDeliveryStatus(ctx, pipe.DS, deliveryID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}

	n, err := countAttempts(ctx, pool, deliveryID)
	if err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if n != 4 {
		t.Errorf("attempt count: got %d, want 4", n)
	}
}

// TestRetry_PermanentFailureOn400 — T048
// Destination returns 400 → permanently_failed after 1 attempt (no retry).
func TestRetry_PermanentFailureOn400(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	_, pool := setupAPI(t)
	brokers := testKafkaBrokers(t)
	pipe := startPipeline(ctx, t, pool, brokers, 30)

	dstURL := newFlakeyServer(t, 0, http.StatusOK, http.StatusBadRequest) // always 400
	epID, _ := seedEndpoint(ctx, pool, dstURL)
	deliveryID, _ := submitEvent(ctx, pipe.EventSvc, epID)

	if err := waitForDeliveryStatus(ctx, pipe.DS, deliveryID, domain.StatusPermanentlyFailed); err != nil {
		t.Fatal(err)
	}
	n, _ := countAttempts(ctx, pool, deliveryID)
	if n != 1 {
		t.Errorf("attempt count: got %d, want 1 (400 is permanent, no retry)", n)
	}
}

// TestRetry_429IsRetried — T048
// Destination returns 429 (rate-limit) → treated as transient, retried.
func TestRetry_429IsRetried(t *testing.T) {
	restore := domain.UseShortScheduleForTests()
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	_, pool := setupAPI(t)
	brokers := testKafkaBrokers(t)
	pipe := startPipeline(ctx, t, pool, brokers, 30)

	// 429 twice then 200
	dstURL := newFlakeyServer(t, 2, http.StatusTooManyRequests, http.StatusOK)
	epID, _ := seedEndpoint(ctx, pool, dstURL)
	deliveryID, _ := submitEvent(ctx, pipe.EventSvc, epID)

	if err := waitForDeliveryStatus(ctx, pipe.DS, deliveryID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	n, _ := countAttempts(ctx, pool, deliveryID)
	if n != 3 {
		t.Errorf("attempt count: got %d, want 3", n)
	}
}

// TestRetry_ExhaustsAllAttempts — T049
// Destination returns 503 forever → permanently_failed after MaxAttempts (9).
func TestRetry_ExhaustsAllAttempts(t *testing.T) {
	restore := domain.UseShortScheduleForTests()
	defer restore()

	// Long timeout: 8 retries × max jitter of short schedule ≈ (10+20+40+80+160+320+640+1280)ms ≈ 2.6s
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	_, pool := setupAPI(t)
	brokers := testKafkaBrokers(t)
	pipe := startPipeline(ctx, t, pool, brokers, 30)

	dstURL := newFlakeyServer(t, 999, http.StatusServiceUnavailable, http.StatusOK) // always 503
	epID, _ := seedEndpoint(ctx, pool, dstURL)
	deliveryID, _ := submitEvent(ctx, pipe.EventSvc, epID)

	if err := waitForDeliveryStatus(ctx, pipe.DS, deliveryID, domain.StatusPermanentlyFailed); err != nil {
		t.Fatal(err)
	}
	n, _ := countAttempts(ctx, pool, deliveryID)
	if n != domain.MaxAttempts {
		t.Errorf("attempt count: got %d, want %d", n, domain.MaxAttempts)
	}
}

// newFlakeyServer returns the URL of an HTTP server that responds with failCode
// for the first failN requests, then successCode for all subsequent requests.
func newFlakeyServer(t *testing.T, failN int, failCode, successCode int) string {
	t.Helper()
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		if n <= failN {
			w.WriteHeader(failCode)
			return
		}
		w.WriteHeader(successCode)
	})
	ln, err := listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String()
}
