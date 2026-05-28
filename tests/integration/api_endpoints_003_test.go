//go:build integration

// Integration tests for POST /v1/endpoints with the feature-003 tenant_id field (T009).
//
// Run: `make test-integration`. Uses real Postgres with migrations 1–6.
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
)

// createTenant POSTs to /v1/tenants and returns the created tenant_id.
func createTenant(t *testing.T, ts *httptest.Server, name string) string {
	t.Helper()
	var body string
	if name == "" {
		body = `{}`
	} else {
		body = `{"name":"` + name + `"}`
	}
	res, err := http.Post(ts.URL+"/v1/tenants", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/tenants: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("create tenant: got %d, want 201; body=%s", res.StatusCode, b)
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode tenant response: %v", err)
	}
	return resp.ID
}

func TestEndpoints003_Create_WithoutTenantID_400(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	body, _ := json.Marshal(map[string]string{"url": "https://example.com/hook"})
	res, err := http.Post(ts.URL+"/v1/endpoints", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/endpoints: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "missing_tenant_id")
}

func TestEndpoints003_Create_NonExistentTenant_422(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	body, _ := json.Marshal(map[string]string{
		"url":       "https://example.com/hook",
		"tenant_id": uuid.NewString(),
	})
	res, err := http.Post(ts.URL+"/v1/endpoints", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/endpoints: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnprocessableEntity {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 422; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "tenant_not_found")
}

func TestEndpoints003_Create_ValidTenant_201(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	tenantID := createTenant(t, ts, "my-tenant")

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
		t.Fatalf("status: got %d, want 201; body=%s", res.StatusCode, b)
	}
	var resp struct {
		ID       string `json:"id"`
		TenantID string `json:"tenant_id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TenantID != tenantID {
		t.Errorf("tenant_id: got %q, want %q", resp.TenantID, tenantID)
	}
	if _, err := uuid.Parse(resp.ID); err != nil {
		t.Errorf("id is not a UUID: %q", resp.ID)
	}
}

func TestEndpoints003_Get_IncludesTenantID(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	tenantID := createTenant(t, ts, "tenant-for-get")
	body, _ := json.Marshal(map[string]string{
		"url":       "https://example.com/hook2",
		"tenant_id": tenantID,
	})
	res, err := http.Post(ts.URL+"/v1/endpoints", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/endpoints: %v", err)
	}
	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("create endpoint: %d %s", res.StatusCode, b)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	res.Body.Close()

	res2, err := http.Get(ts.URL + "/v1/endpoints/" + created.ID)
	if err != nil {
		t.Fatalf("GET /v1/endpoints/%s: %v", created.ID, err)
	}
	defer res2.Body.Close()

	if res2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res2.Body)
		t.Fatalf("get status: got %d, want 200; body=%s", res2.StatusCode, b)
	}
	var got struct {
		TenantID string `json:"tenant_id"`
	}
	if err := json.NewDecoder(res2.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.TenantID != tenantID {
		t.Errorf("tenant_id: got %q, want %q", got.TenantID, tenantID)
	}
}
