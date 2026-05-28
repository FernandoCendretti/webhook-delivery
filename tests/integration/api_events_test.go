//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/store"
)

func setupEventsAPI(t *testing.T) (http.Handler, *store.DeliveryStore) {
	t.Helper()
	handler, pool := setupFullAPIWithPool(t)
	return handler, store.NewDeliveryStore(pool)
}

func TestEventsAPI_Submit_Valid(t *testing.T) {
	handler, _ := setupEventsAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	// Register an endpoint first using the system-default tenant.
	res, err := http.Post(ts.URL+"/v1/endpoints", "application/json",
		strings.NewReader(`{"url":"https://example.com/hook","tenant_id":"`+systemDefaultTenantID+`"}`))
	if err != nil {
		t.Fatalf("register endpoint: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("register endpoint status %d: %s", res.StatusCode, b)
	}
	var ep struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&ep); err != nil {
		t.Fatalf("decode endpoint: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"endpoint_id": ep.ID,
		"tenant_id":   systemDefaultTenantID,
		"payload":     map[string]string{"hello": "world"},
	})
	res2, err := http.Post(ts.URL+"/v1/events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res2.Body)
		t.Fatalf("status: got %d, want 202; body=%s", res2.StatusCode, b)
	}
	var accepted struct {
		DeliveryID string `json:"delivery_id"`
		EventID    string `json:"event_id"`
	}
	if err := json.NewDecoder(res2.Body).Decode(&accepted); err != nil {
		t.Fatalf("decode accepted: %v", err)
	}
	if _, err := uuid.Parse(accepted.DeliveryID); err != nil {
		t.Errorf("delivery_id not a uuid: %q", accepted.DeliveryID)
	}
	if _, err := uuid.Parse(accepted.EventID); err != nil {
		t.Errorf("event_id not a uuid: %q", accepted.EventID)
	}
}

func TestEventsAPI_Submit_UnknownEndpoint(t *testing.T) {
	handler, _ := setupEventsAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	body, _ := json.Marshal(map[string]any{
		"endpoint_id": uuid.NewString(),
		"tenant_id":   systemDefaultTenantID,
		"payload":     map[string]string{"x": "1"},
	})
	res, err := http.Post(ts.URL+"/v1/events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 404; body=%s", res.StatusCode, b)
	}
}

func TestEventsAPI_Submit_TooLarge(t *testing.T) {
	handler, _ := setupEventsAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	payload := strings.Repeat("x", 1<<20+1)
	body := `{"endpoint_id":"` + uuid.NewString() + `","payload":{"data":"` + payload + `"}}`
	res, err := http.Post(ts.URL+"/v1/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 413; body=%s", res.StatusCode, b)
	}
}

func TestEventsAPI_Submit_MalformedJSON(t *testing.T) {
	handler, _ := setupEventsAPI(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res, err := http.Post(ts.URL+"/v1/events", "application/json",
		strings.NewReader(`{not json`))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, b)
	}
}
