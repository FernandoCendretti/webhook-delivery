//go:build integration

// Integration tests for POST /v1/events with tenant_id validation (T019).
package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// createEndpointForTenant posts to /v1/endpoints with the given tenant_id.
func createEndpointForTenant(t *testing.T, ts *httptest.Server, tenantID string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{
		"url":       "https://example.com/hook",
		"tenant_id": tenantID,
	})
	res, err := http.Post(ts.URL+"/v1/endpoints", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/endpoints: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("create endpoint status %d: %s", res.StatusCode, b)
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode endpoint: %v", err)
	}
	return resp.ID
}

// postEvent posts to /v1/events and returns the response.
func postEvent(t *testing.T, ts *httptest.Server, body map[string]interface{}) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	res, err := http.Post(ts.URL+"/v1/events", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST /v1/events: %v", err)
	}
	return res
}

// TestEventsAPI_Submit_MissingTenantID_400 asserts that submitting an event
// without tenant_id in the body returns 400 missing_tenant_id.
func TestEventsAPI_Submit_MissingTenantID_400(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	tenantID := createTenant(t, ts, "t1")
	endpointID := createEndpointForTenant(t, ts, tenantID)

	res := postEvent(t, ts, map[string]interface{}{
		"endpoint_id": endpointID,
		"payload":     json.RawMessage(`{"x":1}`),
		// no tenant_id field
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "missing_tenant_id")
}

// TestEventsAPI_Submit_NonExistentTenant_422 asserts that supplying a tenant_id
// that does not exist returns 422 tenant_not_found.
func TestEventsAPI_Submit_NonExistentTenant_422(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	tenantID := createTenant(t, ts, "t2")
	endpointID := createEndpointForTenant(t, ts, tenantID)

	res := postEvent(t, ts, map[string]interface{}{
		"endpoint_id": endpointID,
		"tenant_id":   uuid.NewString(), // non-existent tenant
		"payload":     json.RawMessage(`{"x":1}`),
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnprocessableEntity {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 422; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "tenant_not_found")
}

// TestEventsAPI_Submit_TenantEndpointMismatch_422 asserts that supplying a tenant_id
// that does not match the endpoint's tenant returns 422 tenant_endpoint_mismatch.
func TestEventsAPI_Submit_TenantEndpointMismatch_422(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	tenant1 := createTenant(t, ts, "t-owner")
	tenant2 := createTenant(t, ts, "t-other")
	endpointID := createEndpointForTenant(t, ts, tenant1)

	res := postEvent(t, ts, map[string]interface{}{
		"endpoint_id": endpointID,
		"tenant_id":   tenant2, // wrong tenant
		"payload":     json.RawMessage(`{"x":1}`),
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnprocessableEntity {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 422; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "tenant_endpoint_mismatch")
}

// TestEventsAPI_Submit_ValidTenant_202 asserts that supplying a tenant_id that
// matches the endpoint's tenant returns 202 Accepted.
func TestEventsAPI_Submit_ValidTenant_202(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	tenantID := createTenant(t, ts, "t-valid")
	endpointID := createEndpointForTenant(t, ts, tenantID)

	res := postEvent(t, ts, map[string]interface{}{
		"endpoint_id": endpointID,
		"tenant_id":   tenantID,
		"payload":     json.RawMessage(`{"x":1}`),
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 202; body=%s", res.StatusCode, b)
	}
	var resp struct {
		DeliveryID string `json:"delivery_id"`
		EventID    string `json:"event_id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode 202 body: %v", err)
	}
	if resp.DeliveryID == "" || resp.EventID == "" {
		t.Error("delivery_id or event_id absent from 202 response")
	}
}
