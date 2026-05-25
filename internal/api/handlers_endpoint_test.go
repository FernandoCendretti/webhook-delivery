package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
)

// stubEndpointStore satisfies the unexported endpointStore interface consumed by
// service.EndpointService. UpdateSecret always returns ErrNotFound.
type stubEndpointStore struct{}

func (s *stubEndpointStore) Insert(_ context.Context, _ *domain.Endpoint) error { return nil }
func (s *stubEndpointStore) GetByID(_ context.Context, _ uuid.UUID) (*domain.Endpoint, error) {
	return nil, domain.ErrNotFound
}
func (s *stubEndpointStore) UpdateSecret(_ context.Context, _ uuid.UUID, _ []byte) error {
	return domain.ErrNotFound
}

func endpointHandlerUnderTest() http.Handler {
	svc := service.NewEndpointService(&stubEndpointStore{})
	h := newEndpointHandler(svc, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/endpoints/{id}/rotate-secret", h.RotateSecret)
	return mux
}

// T039: POST /v1/endpoints/{random-uuid}/rotate-secret → 404 with "endpoint_not_found".
// Exercises the full path: handler → service.RotateSecret → store.UpdateSecret →
// domain.ErrNotFound → writeError(404).
func TestRotateSecret_HandlerNotFound(t *testing.T) {
	handler := endpointHandlerUnderTest()

	req := httptest.NewRequest(http.MethodPost,
		"/v1/endpoints/"+uuid.NewString()+"/rotate-secret", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", rec.Code)
	}
	var resp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "endpoint_not_found" {
		t.Errorf("error: got %q, want %q", resp.Error, "endpoint_not_found")
	}
}

// TestRotateSecret_InvalidUUID asserts that a malformed UUID in the path returns 400.
func TestRotateSecret_InvalidUUID(t *testing.T) {
	handler := endpointHandlerUnderTest()

	req := httptest.NewRequest(http.MethodPost, "/v1/endpoints/not-a-uuid/rotate-secret", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", rec.Code)
	}
	var resp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "invalid_endpoint_id" {
		t.Errorf("error: got %q, want %q", resp.Error, "invalid_endpoint_id")
	}
}
