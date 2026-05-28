package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
)

// endpointSvc is the service interface consumed by endpointHandler.
type endpointSvc interface {
	Register(ctx context.Context, url string, tenantID uuid.UUID) (*domain.Endpoint, error)
	Get(ctx context.Context, id uuid.UUID) (*domain.Endpoint, error)
	RotateSecret(ctx context.Context, id uuid.UUID) ([]byte, error)
}

type endpointHandler struct {
	svc endpointSvc
	log *slog.Logger
}

func newEndpointHandler(svc endpointSvc, log *slog.Logger) *endpointHandler {
	if log == nil {
		log = slog.Default()
	}
	return &endpointHandler{svc: svc, log: log}
}

// Create handles POST /v1/endpoints. Requires tenant_id in the body (FR-007).
// The 201 response includes signing_secret as a hex string — the only time it is
// exposed (FR-001).
func (h *endpointHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req EndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "malformed JSON body")
		return
	}
	if req.TenantID == nil {
		writeError(w, http.StatusBadRequest, "missing_tenant_id", "")
		return
	}
	e, err := h.svc.Register(r.Context(), req.URL, *req.TenantID)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidURL) {
			writeError(w, http.StatusBadRequest, "invalid_url", err.Error())
			return
		}
		if errors.Is(err, service.ErrTenantNotFound) {
			writeError(w, http.StatusUnprocessableEntity, "tenant_not_found", "")
			return
		}
		h.log.ErrorContext(r.Context(), "register endpoint failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "")
		return
	}
	writeJSON(w, http.StatusCreated, EndpointCreatedResponse{
		ID:            e.ID,
		URL:           e.URL,
		TenantID:      e.TenantID,
		CreatedAt:     e.CreatedAt,
		SigningSecret: hex.EncodeToString(e.SigningSecret),
	})
}

// RotateSecret handles POST /v1/endpoints/{id}/rotate-secret.
func (h *endpointHandler) RotateSecret(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_endpoint_id", "")
		return
	}
	newSecret, err := h.svc.RotateSecret(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "endpoint_not_found", "")
			return
		}
		h.log.ErrorContext(r.Context(), "rotate secret failed", "err", err, "id", id)
		writeError(w, http.StatusInternalServerError, "internal_error", "")
		return
	}
	writeJSON(w, http.StatusOK, RotateSecretResponse{
		SigningSecret: hex.EncodeToString(newSecret),
	})
}

// Get handles GET /v1/endpoints/{id}.
func (h *endpointHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "endpoint_not_found", "")
		return
	}
	e, err := h.svc.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "endpoint_not_found", "")
			return
		}
		h.log.ErrorContext(r.Context(), "get endpoint failed", "err", err, "id", id)
		writeError(w, http.StatusInternalServerError, "internal_error", "")
		return
	}
	writeJSON(w, http.StatusOK, NewEndpointResponse(*e))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, ErrorResponse{Error: code, Detail: detail})
}
