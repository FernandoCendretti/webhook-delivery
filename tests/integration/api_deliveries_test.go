//go:build integration

// Integration tests for GET /v1/deliveries/{id} (T056).
package integration_test

import (
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
)

func TestDeliveriesAPI_Get_UnknownID(t *testing.T) {
	handler, _ := setupFullAPIWithPool(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res, err := http.Get(ts.URL + "/v1/deliveries/" + uuid.NewString())
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 404; body=%s", res.StatusCode, b)
	}
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "delivery_not_found" {
		t.Errorf("error code: got %q, want %q", resp.Error, "delivery_not_found")
	}
}

func TestDeliveriesAPI_Get_WithAttempts(t *testing.T) {
	// Use a short retry schedule so the test completes quickly.
	restore := domain.UseShortScheduleForTests()
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	_, pool := setupAPI(t)
	brokers := testKafkaBrokers(t)

	// Flaky destination: 503 twice then 200.
	var calls atomic.Int32
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if int(calls.Add(1)) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dst.Close)

	// Start the full pipeline (API + scheduler + worker).
	pipe := startPipeline(ctx, t, pool, brokers, 30)
	apiHandler := setupFullAPI(t, pool)
	ts := httptest.NewServer(apiHandler)
	t.Cleanup(ts.Close)

	// Register destination endpoint using the system-default tenant.
	body, _ := json.Marshal(map[string]string{"url": dst.URL, "tenant_id": systemDefaultTenantID})
	res, err := http.Post(ts.URL+"/v1/endpoints", "application/json",
		strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("register endpoint: %v", err)
	}
	var ep struct{ ID string `json:"id"` }
	json.NewDecoder(res.Body).Decode(&ep)
	res.Body.Close()

	// Submit event with matching tenant_id.
	evtBody, _ := json.Marshal(map[string]any{
		"endpoint_id": ep.ID,
		"tenant_id":   systemDefaultTenantID,
		"payload":     map[string]string{"k": "v"},
	})
	res2, err := http.Post(ts.URL+"/v1/events", "application/json",
		strings.NewReader(string(evtBody)))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}
	var accepted struct {
		DeliveryID string `json:"delivery_id"`
	}
	json.NewDecoder(res2.Body).Decode(&accepted)
	res2.Body.Close()
	deliveryID, _ := uuid.Parse(accepted.DeliveryID)

	// Wait for the delivery to be delivered.
	if err := waitForDeliveryStatus(ctx, pipe.DS, deliveryID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}

	// Fetch via GET /v1/deliveries/{id}.
	res3, err := http.Get(ts.URL + "/v1/deliveries/" + accepted.DeliveryID)
	if err != nil {
		t.Fatalf("GET delivery: %v", err)
	}
	defer res3.Body.Close()

	if res3.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res3.Body)
		t.Fatalf("status: got %d, want 200; body=%s", res3.StatusCode, b)
	}

	var dr struct {
		ID           string `json:"id"`
		Status       string `json:"status"`
		AttemptCount int    `json:"attempt_count"`
		Attempts     []struct {
			Sequence           int     `json:"sequence"`
			Outcome            string  `json:"outcome"`
			ResponseStatusCode *int    `json:"response_status_code"`
			ErrorReason        *string `json:"error_reason"`
			StartedAt          string  `json:"started_at"`
			CompletedAt        *string `json:"completed_at"`
		} `json:"attempts"`
	}
	if err := json.NewDecoder(res3.Body).Decode(&dr); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if dr.ID != accepted.DeliveryID {
		t.Errorf("id: got %q, want %q", dr.ID, accepted.DeliveryID)
	}
	if dr.Status != "delivered" {
		t.Errorf("status: got %q, want delivered", dr.Status)
	}
	if dr.AttemptCount != 3 {
		t.Errorf("attempt_count: got %d, want 3", dr.AttemptCount)
	}
	if len(dr.Attempts) != 3 {
		t.Fatalf("attempts len: got %d, want 3", len(dr.Attempts))
	}
	// Verify attempts are ordered by sequence.
	for i, a := range dr.Attempts {
		if a.Sequence != i+1 {
			t.Errorf("attempts[%d].sequence: got %d, want %d", i, a.Sequence, i+1)
		}
	}
	// First two attempts must be transient_failure (503).
	if dr.Attempts[0].Outcome != "transient_failure" {
		t.Errorf("attempts[0].outcome: got %q, want transient_failure", dr.Attempts[0].Outcome)
	}
	// Last attempt must be success (200).
	if dr.Attempts[2].Outcome != "success" {
		t.Errorf("attempts[2].outcome: got %q, want success", dr.Attempts[2].Outcome)
	}
}

