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

func (s *stubSubmitter) Submit(_ context.Context, _ uuid.UUID, _ json.RawMessage, _ string, _ []byte) (*domain.Delivery, error) {
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

// validEventBody returns a minimal valid POST /v1/events body.
func validEventBody() string {
	return `{"endpoint_id":"` + uuid.NewString() + `","payload":{"k":"v"}}`
}

func TestIdempotencyKey_NoHeader_Accepted(t *testing.T) {
	handler := handlerUnderTest(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(validEventBody()))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Errorf("no header: status %d, want 202", rec.Code)
	}
}

func TestIdempotencyKey_ValidKeys_Accepted(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"1-char", "x"},
		{"255-char", strings.Repeat("k", 255)},
		{"boundary-bang", "!"},
		{"boundary-tilde", "~"},
		{"mixed-printable", "abc-123_XYZ"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := handlerUnderTest(t)
			req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(validEventBody()))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", tc.key)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				t.Errorf("key %q: status %d, want 202", tc.key, rec.Code)
			}
		})
	}
}

func TestIdempotencyKey_InvalidKeys_Rejected(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"empty", ""},
		{"256-char", strings.Repeat("k", 256)},
		{"space", "has space"},
		{"del", "has\x7fchar"},
		{"non-ascii", "caf\xc3\xa9"},
		{"null-byte", "has\x00byte"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := handlerUnderTest(t)
			req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(validEventBody()))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", tc.key)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("key %q: status %d, want 400", tc.key, rec.Code)
			}
			var resp ErrorResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("decode error body: %v", err)
			}
			if resp.Error != "invalid_idempotency_key" {
				t.Errorf("error code: got %q, want %q", resp.Error, "invalid_idempotency_key")
			}
		})
	}
}
