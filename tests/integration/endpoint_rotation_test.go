//go:build integration

// Integration tests for US3: POST /v1/endpoints/{id}/rotate-secret (T033-T037).
package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/signing"
)

type rotateResponse struct {
	SigningSecret string `json:"signing_secret"`
}

// rotateSecretHTTP calls POST /v1/endpoints/{id}/rotate-secret and asserts 200.
func rotateSecretHTTP(t *testing.T, srv *httptest.Server, endpointID uuid.UUID) rotateResponse {
	t.Helper()
	resp, err := http.Post(srv.URL+"/v1/endpoints/"+endpointID.String()+"/rotate-secret",
		"application/json", nil)
	if err != nil {
		t.Fatalf("POST rotate-secret: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("rotate-secret: got %d, want 200; body=%s", resp.StatusCode, b)
	}
	var r rotateResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode rotate-secret response: %v", err)
	}
	return r
}

// waitCapture waits up to 30 s for a delivery to arrive on ch.
func waitCapture(t *testing.T, ch <-chan webhookCapture) webhookCapture {
	t.Helper()
	select {
	case cap := <-ch:
		return cap
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for outgoing webhook delivery")
		return webhookCapture{}
	}
}

// T033: rotate → submit → signature verifies with new secret, not old (SC-004).
func TestRotateSecret_SuccessfulRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pool := setupSigningPool(t)

	ch := make(chan webhookCapture, 1)
	dstSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ch <- webhookCapture{headers: r.Header.Clone(), body: body}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dstSrv.Close)

	// Wire the API handler using the same pool so HTTP calls share the same DB.
	handler := setupFullAPI(t, pool)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	// Register endpoint via HTTP (captures old secret from 201 response).
	createBody, _ := json.Marshal(map[string]string{"url": dstSrv.URL, "tenant_id": systemDefaultTenantID})
	createResp, err := http.Post(srv.URL+"/v1/endpoints", "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	defer createResp.Body.Close()
	var created struct {
		ID            uuid.UUID `json:"id"`
		SigningSecret string    `json:"signing_secret"`
	}
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	oldSecret, _ := hex.DecodeString(created.SigningSecret)

	// Rotate via HTTP.
	rot := rotateSecretHTTP(t, srv, created.ID)
	if rot.SigningSecret == created.SigningSecret {
		t.Error("rotate-secret returned the same secret as before")
	}
	if len(rot.SigningSecret) != 64 {
		t.Errorf("new signing_secret len = %d, want 64", len(rot.SigningSecret))
	}
	if strings.ToLower(rot.SigningSecret) != rot.SigningSecret {
		t.Errorf("new signing_secret is not lowercase hex: %q", rot.SigningSecret)
	}
	newSecret, _ := hex.DecodeString(rot.SigningSecret)

	// Start pipeline and submit event directly via service (same pattern as
	// TestWorkerSigning_* tests which are proven to work with this setup).
	brokers := testKafkaBrokers(t)
	tp := startPipeline(ctx, t, pool, brokers, 30)

	payload := json.RawMessage(`{"x":1}`)
	d, err := tp.EventSvc.Submit(ctx, created.ID, payload, "", payload, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}
	if err := waitForDeliveryStatus(ctx, tp.DS, d.ID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}

	cap := waitCapture(t, ch)

	tsStr := cap.headers.Get("X-Webhook-Timestamp")
	sigStr := cap.headers.Get("X-Webhook-Signature")
	if tsStr == "" || sigStr == "" {
		t.Fatalf("signing headers missing: ts=%q sig=%q", tsStr, sigStr)
	}
	tsVal, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		t.Fatalf("X-Webhook-Timestamp not int64: %q", tsStr)
	}

	if signing.Sign(newSecret, tsVal, cap.body) != sigStr {
		t.Errorf("signature does not verify with new secret (SC-004 violated)")
	}
	if signing.Sign(oldSecret, tsVal, cap.body) == sigStr {
		t.Error("signature still verifies with old secret after rotation (SC-004 violated)")
	}
}

// T034: three sequential rotations — signature verifies only against the third secret.
func TestRotateSecret_SequentialRotations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pool := setupSigningPool(t)

	ch := make(chan webhookCapture, 1)
	dstSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ch <- webhookCapture{headers: r.Header.Clone(), body: body}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dstSrv.Close)

	handler := setupFullAPI(t, pool)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	createBody, _ := json.Marshal(map[string]string{"url": dstSrv.URL, "tenant_id": systemDefaultTenantID})
	createResp, err := http.Post(srv.URL+"/v1/endpoints", "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	defer createResp.Body.Close()
	var created struct{ ID uuid.UUID `json:"id"` }
	json.NewDecoder(createResp.Body).Decode(&created) //nolint:errcheck

	rot1 := rotateSecretHTTP(t, srv, created.ID)
	rot2 := rotateSecretHTTP(t, srv, created.ID)
	rot3 := rotateSecretHTTP(t, srv, created.ID)

	if rot1.SigningSecret == rot2.SigningSecret || rot2.SigningSecret == rot3.SigningSecret {
		t.Error("sequential rotations should produce distinct secrets")
	}

	secret3, _ := hex.DecodeString(rot3.SigningSecret)
	secret1, _ := hex.DecodeString(rot1.SigningSecret)
	secret2, _ := hex.DecodeString(rot2.SigningSecret)

	brokers := testKafkaBrokers(t)
	tp := startPipeline(ctx, t, pool, brokers, 30)

	payload := json.RawMessage(`{"x":2}`)
	d, err := tp.EventSvc.Submit(ctx, created.ID, payload, "", payload, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}
	if err := waitForDeliveryStatus(ctx, tp.DS, d.ID, domain.StatusDelivered); err != nil {
		t.Fatal(err)
	}

	cap := waitCapture(t, ch)

	tsVal, err := strconv.ParseInt(cap.headers.Get("X-Webhook-Timestamp"), 10, 64)
	if err != nil {
		t.Fatalf("parse timestamp: %v", err)
	}
	sigStr := cap.headers.Get("X-Webhook-Signature")

	if signing.Sign(secret3, tsVal, cap.body) != sigStr {
		t.Error("signature does not verify against secret3 (latest rotation)")
	}
	if signing.Sign(secret1, tsVal, cap.body) == sigStr {
		t.Error("signature still verifies against secret1")
	}
	if signing.Sign(secret2, tsVal, cap.body) == sigStr {
		t.Error("signature still verifies against secret2")
	}
}

// T035: rotate non-existent endpoint → 404.
func TestRotateSecret_NotFoundIntegration(t *testing.T) {
	handler, _ := setupSigningAPIWithPool(t)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL+"/v1/endpoints/"+uuid.NewString()+"/rotate-secret",
		"application/json", nil)
	if err != nil {
		t.Fatalf("rotate non-existent: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 404; body=%s", resp.StatusCode, b)
	}
	var errResp struct{ Error string `json:"error"` }
	json.NewDecoder(resp.Body).Decode(&errResp) //nolint:errcheck
	if errResp.Error != "endpoint_not_found" {
		t.Errorf("error: got %q, want %q", errResp.Error, "endpoint_not_found")
	}
}

// T036: rotation after a failed delivery — retry uses the new secret (FR-012, FR-016).
func TestRotateSecret_AfterFailedDelivery(t *testing.T) {
	restore := domain.UseShortScheduleForTests()
	t.Cleanup(restore)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	pool := setupSigningPool(t)

	ch := make(chan webhookCapture, 2)
	var callCount int
	var mu sync.Mutex

	dstSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ch <- webhookCapture{headers: r.Header.Clone(), body: body}
		mu.Lock()
		n := callCount
		callCount++
		mu.Unlock()
		if n == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(dstSrv.Close)

	handler := setupFullAPI(t, pool)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	createBody, _ := json.Marshal(map[string]string{"url": dstSrv.URL, "tenant_id": systemDefaultTenantID})
	createResp, err := http.Post(srv.URL+"/v1/endpoints", "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	defer createResp.Body.Close()
	var created struct{ ID uuid.UUID `json:"id"` }
	json.NewDecoder(createResp.Body).Decode(&created) //nolint:errcheck

	brokers := testKafkaBrokers(t)
	tp := startPipeline(ctx, t, pool, brokers, 30)

	// Submit event directly (proven pattern from TestWorkerSigning_* tests).
	payload := json.RawMessage(`{"x":3}`)
	d, err := tp.EventSvc.Submit(ctx, created.ID, payload, "", payload, uuid.MustParse(systemDefaultTenantID))
	if err != nil {
		t.Fatalf("submit event: %v", err)
	}
	_ = d

	// Wait for the first (failing) delivery attempt.
	// Pipeline startup (consumer group join) takes ~40s, so use a generous timeout.
	select {
	case <-ch:
	case <-time.After(90 * time.Second):
		t.Fatal("timed out waiting for first delivery attempt")
	}

	// Rotate while delivery is in retry backoff.
	rot := rotateSecretHTTP(t, srv, created.ID)
	newSecret, _ := hex.DecodeString(rot.SigningSecret)

	// Wait for retry.
	var second webhookCapture
	select {
	case second = <-ch:
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for retry delivery attempt")
	}

	tsVal, err := strconv.ParseInt(second.headers.Get("X-Webhook-Timestamp"), 10, 64)
	if err != nil {
		t.Fatalf("parse retry timestamp: %v", err)
	}
	sig2 := second.headers.Get("X-Webhook-Signature")

	if signing.Sign(newSecret, tsVal, second.body) != sig2 {
		t.Error("retry signature does not verify with new secret after rotation")
	}
}

// T037: concurrent rotation — both get 200; DB ends up with one of the two returned values.
func TestRotateSecret_Concurrent(t *testing.T) {
	handler, pool := setupSigningAPIWithPool(t)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	ctx := context.Background()

	createBody, _ := json.Marshal(map[string]string{"url": "https://example.com/wh", "tenant_id": systemDefaultTenantID})
	createResp, err := http.Post(srv.URL+"/v1/endpoints", "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	defer createResp.Body.Close()
	var created struct{ ID uuid.UUID `json:"id"` }
	json.NewDecoder(createResp.Body).Decode(&created) //nolint:errcheck

	results := make([]rotateResponse, 2)
	statuses := make([]int, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/v1/endpoints/"+created.ID.String()+"/rotate-secret",
				"application/json", nil)
			if err != nil {
				t.Errorf("goroutine %d: %v", idx, err)
				return
			}
			defer resp.Body.Close()
			statuses[idx] = resp.StatusCode
			if resp.StatusCode == http.StatusOK {
				json.NewDecoder(resp.Body).Decode(&results[idx]) //nolint:errcheck
			}
		}(i)
	}
	wg.Wait()

	for i, s := range statuses {
		if s != http.StatusOK {
			t.Errorf("goroutine %d: status %d, want 200", i, s)
		}
	}

	var dbSecret []byte
	pool.QueryRow(ctx, `SELECT signing_secret FROM endpoints WHERE id=$1`, created.ID).Scan(&dbSecret) //nolint:errcheck
	dbSecretHex := hex.EncodeToString(dbSecret)

	if dbSecretHex != results[0].SigningSecret && dbSecretHex != results[1].SigningSecret {
		t.Errorf("DB secret %q matches neither response (%q, %q)",
			dbSecretHex, results[0].SigningSecret, results[1].SigningSecret)
	}
}
