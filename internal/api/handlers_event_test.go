package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// stubSubmitter satisfies eventSubmitter for unit tests; always succeeds.
type stubSubmitter struct{}

func (s *stubSubmitter) Submit(_ context.Context, _ uuid.UUID, _ json.RawMessage) (*domain.Delivery, error) {
	return &domain.Delivery{ID: uuid.New(), EventID: uuid.New(), EndpointID: uuid.New()}, nil
}

func handlerUnderTest(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	h := newEventHandler(&stubSubmitter{}, nil)
	mux.HandleFunc("POST /v1/events", h.Submit)
	return mux
}

func TestEventHandler_PayloadTooLarge(t *testing.T) {
	handler := handlerUnderTest(t)

	payload := strings.Repeat("a", 1<<20+1)
	body := `{"endpoint_id":"` + uuid.NewString() + `","payload":{"x":"` + payload + `"}}`

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status: got %d, want 413", rec.Code)
	}
}

func TestEventHandler_MalformedJSON(t *testing.T) {
	handler := handlerUnderTest(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(`{not json`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", rec.Code)
	}
	var resp ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if resp.Error != "bad_request" {
		t.Errorf("error: got %q, want %q", resp.Error, "bad_request")
	}
}
