//go:build integration

// Integration tests for POST /v1/tenants and GET /v1/tenants/{id} (T008).
//
// Run: `make test-integration`. All scenarios use real Postgres with migrations 1–6.
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

// --- helpers ---

func postTenant(t *testing.T, ts *httptest.Server, body string) *http.Response {
	t.Helper()
	res, err := http.Post(ts.URL+"/v1/tenants", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/tenants: %v", err)
	}
	return res
}

func getTenant(t *testing.T, ts *httptest.Server, id string) *http.Response {
	t.Helper()
	res, err := http.Get(ts.URL + "/v1/tenants/" + id)
	if err != nil {
		t.Fatalf("GET /v1/tenants/%s: %v", id, err)
	}
	return res
}

// --- POST /v1/tenants ---

func TestTenantsAPI_Create_NoName(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res := postTenant(t, ts, `{}`)
	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 201; body=%s", res.StatusCode, b)
	}
	var resp struct {
		ID        string  `json:"id"`
		Name      *string `json:"name"`
		CreatedAt string  `json:"created_at"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, err := uuid.Parse(resp.ID); err != nil {
		t.Errorf("id is not a UUID: %q", resp.ID)
	}
	if resp.Name != nil {
		t.Errorf("name should be absent, got %q", *resp.Name)
	}
	if resp.CreatedAt == "" {
		t.Error("created_at is empty")
	}
}

func TestTenantsAPI_Create_WithValidName(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res := postTenant(t, ts, `{"name":"acme-corp"}`)
	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 201; body=%s", res.StatusCode, b)
	}
	var resp struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Name != "acme-corp" {
		t.Errorf("name: got %q, want %q", resp.Name, "acme-corp")
	}
}

func TestTenantsAPI_Create_EmptyName_400(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res := postTenant(t, ts, `{"name":""}`)
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "invalid_name")
}

func TestTenantsAPI_Create_TooLongName_400(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	body, _ := json.Marshal(map[string]string{"name": strings.Repeat("a", 256)})
	res, err := http.Post(ts.URL+"/v1/tenants", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "invalid_name")
}

func TestTenantsAPI_Create_ControlChar_400(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	// json.Marshal encodes the NUL byte as  (valid JSON) so the decoder succeeds
	// and the validator sees the actual rune.
	b, _ := json.Marshal(map[string]string{"name": "a\x00b"})
	body := string(b)
	res, err := http.Post(ts.URL+"/v1/tenants", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "invalid_name")
}

// --- GET /v1/tenants/{id} ---

func TestTenantsAPI_Get_WithName(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res := postTenant(t, ts, `{"name":"test-tenant"}`)
	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("create tenant: %d %s", res.StatusCode, b)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	res.Body.Close()

	res2 := getTenant(t, ts, created.ID)
	defer res2.Body.Close()

	if res2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res2.Body)
		t.Fatalf("get status: got %d, want 200; body=%s", res2.StatusCode, b)
	}
	var got struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(res2.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("id: got %q, want %q", got.ID, created.ID)
	}
	if got.Name != "test-tenant" {
		t.Errorf("name: got %q, want %q", got.Name, "test-tenant")
	}
}

func TestTenantsAPI_Get_WithoutName(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res := postTenant(t, ts, `{}`)
	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("create: %d %s", res.StatusCode, b)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	res.Body.Close()

	res2 := getTenant(t, ts, created.ID)
	defer res2.Body.Close()

	if res2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res2.Body)
		t.Fatalf("get status: got %d, want 200; body=%s", res2.StatusCode, b)
	}
	// Decode into a raw map to verify the "name" key is absent entirely
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(res2.Body).Decode(&raw); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if _, ok := raw["name"]; ok {
		t.Error("name key should be absent from response when tenant has no name")
	}
}

func TestTenantsAPI_Get_NotFound(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res := getTenant(t, ts, uuid.NewString())
	defer res.Body.Close()

	if res.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 404; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "tenant_not_found")
}

func TestTenantsAPI_Get_InvalidUUID(t *testing.T) {
	handler, _ := setup003API(t)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	res := getTenant(t, ts, "not-a-uuid")
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 400; body=%s", res.StatusCode, b)
	}
	assertErrorCode(t, res.Body, "invalid_tenant_id")
}

// assertErrorCode decodes the response body and asserts the "error" field.
func assertErrorCode(t *testing.T, body io.Reader, want string) {
	t.Helper()
	var resp struct {
		Error string `json:"error"`
	}
	b, _ := io.ReadAll(body)
	if err := json.Unmarshal(b, &resp); err != nil {
		t.Fatalf("decode error body: %v (body=%s)", err, b)
	}
	if resp.Error != want {
		t.Errorf("error code: got %q, want %q", resp.Error, want)
	}
}
