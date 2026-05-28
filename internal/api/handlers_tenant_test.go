package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// alwaysSucceedsTenantSvc is a stub service that returns a valid tenant on Create
// and ErrNotFound on GetByID, used to isolate handler-layer validation tests.
type alwaysSucceedsTenantSvc struct{}

func (s *alwaysSucceedsTenantSvc) Create(_ context.Context, name *string) (*domain.Tenant, error) {
	return &domain.Tenant{ID: uuid.New(), Name: name, CreatedAt: time.Now()}, nil
}

func (s *alwaysSucceedsTenantSvc) GetByID(_ context.Context, _ uuid.UUID) (*domain.Tenant, error) {
	return nil, domain.ErrNotFound
}

func tenantHandlerUnderTest() http.Handler {
	h := newTenantHandler(&alwaysSucceedsTenantSvc{}, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tenants", h.Create)
	return mux
}

// marshalNameBody returns a JSON body {"name": <name>} using json.Marshal so that
// control characters in name are encoded as \uXXXX — valid JSON that the decoder
// can parse. This lets validateTenantName see the actual rune and reject it.
func marshalNameBody(name string) string {
	b, _ := json.Marshal(map[string]string{"name": name})
	return string(b)
}

// TestCreateTenant_NameValidation verifies that the handler accepts valid names
// and rejects names that violate FR-002 constraints (empty, >255 chars, Cc chars).
func TestCreateTenant_NameValidation(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantStatus int
		wantError  string
	}{
		// Absent / null name are valid (treated as "no name provided")
		{name: "no_name_field", body: `{}`, wantStatus: http.StatusCreated},
		{name: "null_name", body: `{"name": null}`, wantStatus: http.StatusCreated},

		// Valid non-null names
		{name: "one_char", body: `{"name": "a"}`, wantStatus: http.StatusCreated},
		{name: "255_chars", body: `{"name": "` + strings.Repeat("a", 255) + `"}`, wantStatus: http.StatusCreated},
		{name: "emoji_not_cc", body: `{"name": "🎉 event"}`, wantStatus: http.StatusCreated},

		// Invalid: empty string
		{name: "empty_string", body: `{"name": ""}`, wantStatus: http.StatusBadRequest, wantError: "invalid_name"},

		// Invalid: exceeds 255 chars
		{name: "256_chars", body: `{"name": "` + strings.Repeat("a", 256) + `"}`, wantStatus: http.StatusBadRequest, wantError: "invalid_name"},

		// Invalid: Unicode Cc characters. json.Marshal encodes them as \uXXXX so the body
		// is valid JSON; the decoder decodes the escape back to the rune and validateTenantName rejects it.
		{name: "nul_byte", body: marshalNameBody("a\x00b"), wantStatus: http.StatusBadRequest, wantError: "invalid_name"},
		{name: "ctrl_0x01", body: marshalNameBody("a\x01b"), wantStatus: http.StatusBadRequest, wantError: "invalid_name"},
		{name: "del_0x7F", body: marshalNameBody("a\x7fb"), wantStatus: http.StatusBadRequest, wantError: "invalid_name"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := tenantHandlerUnderTest()
			req := httptest.NewRequest(http.MethodPost, "/v1/tenants", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status: got %d, want %d; body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantError != "" {
				var resp ErrorResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("decode error body: %v (body=%s)", err, rec.Body.String())
				}
				if resp.Error != tc.wantError {
					t.Errorf("error code: got %q, want %q", resp.Error, tc.wantError)
				}
			}
		})
	}
}
