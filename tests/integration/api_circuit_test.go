//go:build integration

// Integration tests for GET /v1/endpoints/{id}/circuit-breaker (T044).
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestCircuitAPI_Closed_200(t *testing.T) {
	handler, pool := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	_, epID := seedCBEndpoint(t, pool)

	res, err := http.Get(ts.URL + "/v1/endpoints/" + epID.String() + "/circuit-breaker")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 200; body=%s", res.StatusCode, b)
	}

	var resp struct {
		State               string  `json:"state"`
		ConsecutiveFailures int     `json:"consecutive_failures"`
		SuspendedUntil      *string `json:"suspended_until"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.State != "closed" {
		t.Errorf("state: got %q, want closed", resp.State)
	}
	if resp.ConsecutiveFailures != 0 {
		t.Errorf("consecutive_failures: got %d, want 0", resp.ConsecutiveFailures)
	}
	if resp.SuspendedUntil != nil {
		t.Errorf("suspended_until: want nil/absent, got %v", resp.SuspendedUntil)
	}
}

func TestCircuitAPI_Open_200(t *testing.T) {
	handler, pool := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	_, epID := seedCBEndpoint(t, pool)

	// Force circuit open via SQL.
	_, err := pool.Exec(context.Background(),
		`UPDATE endpoints SET circuit_state='open', circuit_failure_count=5,
		 circuit_suspended_until=NOW()+INTERVAL '60 seconds' WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force open: %v", err)
	}

	res, err := http.Get(ts.URL + "/v1/endpoints/" + epID.String() + "/circuit-breaker")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 200; body=%s", res.StatusCode, b)
	}

	var resp struct {
		State               string  `json:"state"`
		ConsecutiveFailures int     `json:"consecutive_failures"`
		SuspendedUntil      *string `json:"suspended_until"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.State != "open" {
		t.Errorf("state: got %q, want open", resp.State)
	}
	if resp.ConsecutiveFailures != 5 {
		t.Errorf("consecutive_failures: got %d, want 5", resp.ConsecutiveFailures)
	}
	if resp.SuspendedUntil == nil {
		t.Error("suspended_until: want non-nil, got nil")
	}
}

func TestCircuitAPI_HalfOpen_200(t *testing.T) {
	handler, pool := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	_, epID := seedCBEndpoint(t, pool)

	_, err := pool.Exec(context.Background(),
		`UPDATE endpoints SET circuit_state='half_open', circuit_failure_count=5 WHERE id=$1`, epID)
	if err != nil {
		t.Fatalf("force half_open: %v", err)
	}

	res, err := http.Get(ts.URL + "/v1/endpoints/" + epID.String() + "/circuit-breaker")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 200; body=%s", res.StatusCode, b)
	}

	var resp struct {
		State               string  `json:"state"`
		ConsecutiveFailures int     `json:"consecutive_failures"`
		SuspendedUntil      *string `json:"suspended_until"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.State != "half-open" {
		t.Errorf("state: got %q, want half-open (hyphen)", resp.State)
	}
	if resp.ConsecutiveFailures != 5 {
		t.Errorf("consecutive_failures: got %d, want 5", resp.ConsecutiveFailures)
	}
	if resp.SuspendedUntil != nil {
		t.Errorf("suspended_until: want nil for half-open, got %v", resp.SuspendedUntil)
	}
}

func TestCircuitAPI_NotFound_404(t *testing.T) {
	handler, _ := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res, err := http.Get(ts.URL + "/v1/endpoints/" + uuid.NewString() + "/circuit-breaker")
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
	if resp.Error != "endpoint_not_found" {
		t.Errorf("error code: got %q, want endpoint_not_found", resp.Error)
	}
}

func TestCircuitAPI_InvalidUUID_400(t *testing.T) {
	handler, _ := setupAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res, err := http.Get(ts.URL + "/v1/endpoints/not-a-uuid/circuit-breaker")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, b)
	}
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(resp.Error, "invalid") {
		t.Errorf("error code: got %q, want to contain 'invalid'", resp.Error)
	}
}
