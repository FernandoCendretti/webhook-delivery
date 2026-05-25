//go:build integration

// Integration tests for US1: signing_secret in endpoint create/read responses (T008).
package integration_test

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestEndpoint_CreateReturnsSigningSecret asserts that POST /v1/endpoints 201 includes
// a non-empty 64-char lowercase hex signing_secret, and that GET /v1/endpoints/{id}
// does NOT include that field (FR-001, FR-002, SC-005).
func TestEndpoint_CreateReturnsSigningSecret(t *testing.T) {
	handler, _ := setupSigningAPIWithPool(t)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	res, err := http.Post(srv.URL+"/v1/endpoints", "application/json",
		strings.NewReader(`{"url":"https://example.com/webhook"}`))
	if err != nil {
		t.Fatalf("POST /v1/endpoints: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d, want 201; body=%s", res.StatusCode, b)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decode 201 response: %v", err)
	}

	secretVal, ok := resp["signing_secret"]
	if !ok {
		t.Fatal("201 response missing signing_secret field")
	}
	secret, ok := secretVal.(string)
	if !ok {
		t.Fatalf("signing_secret is not a string: %T", secretVal)
	}
	if len(secret) != 64 {
		t.Errorf("signing_secret len = %d, want 64", len(secret))
	}
	if strings.ToLower(secret) != secret {
		t.Errorf("signing_secret is not lowercase: %q", secret)
	}
	if _, err := hex.DecodeString(secret); err != nil {
		t.Errorf("signing_secret is not valid hex: %v", err)
	}

	// GET /v1/endpoints/{id} must NOT expose the secret.
	id, _ := resp["id"].(string)
	getRes, err := http.Get(srv.URL + "/v1/endpoints/" + id)
	if err != nil {
		t.Fatalf("GET /v1/endpoints/%s: %v", id, err)
	}
	defer getRes.Body.Close()

	if getRes.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(getRes.Body)
		t.Fatalf("GET status: got %d, want 200; body=%s", getRes.StatusCode, b)
	}
	var getResp map[string]interface{}
	if err := json.NewDecoder(getRes.Body).Decode(&getResp); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}
	if _, hasSecret := getResp["signing_secret"]; hasSecret {
		t.Error("GET /v1/endpoints/{id} must not include signing_secret (FR-002, SC-005)")
	}
}
