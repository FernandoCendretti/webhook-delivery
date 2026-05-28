package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"unicode"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// tenantSvc is the service interface consumed by tenantHandler.
type tenantSvc interface {
	Create(ctx context.Context, name *string) (*domain.Tenant, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error)
}

type tenantHandler struct {
	svc tenantSvc
	log *slog.Logger
}

func newTenantHandler(svc tenantSvc, log *slog.Logger) *tenantHandler {
	if log == nil {
		log = slog.Default()
	}
	return &tenantHandler{svc: svc, log: log}
}

// Create handles POST /v1/tenants (FR-001, FR-002).
func (h *tenantHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "malformed JSON body")
		return
	}

	if req.Name != nil {
		if err := validateTenantName(*req.Name); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_name", err.Error())
			return
		}
	}

	t, err := h.svc.Create(r.Context(), req.Name)
	if err != nil {
		h.log.ErrorContext(r.Context(), "create tenant failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "")
		return
	}
	writeJSON(w, http.StatusCreated, NewTenantResponse(*t))
}

// GetByID handles GET /v1/tenants/{id} (FR-003).
func (h *tenantHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_tenant_id", "")
		return
	}
	t, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "tenant_not_found", "")
			return
		}
		h.log.ErrorContext(r.Context(), "get tenant failed", "err", err, "id", id)
		writeError(w, http.StatusInternalServerError, "internal_error", "")
		return
	}
	writeJSON(w, http.StatusOK, NewTenantResponse(*t))
}

// validateTenantName enforces FR-002: non-empty, ≤255 chars, no Unicode Cc characters.
func validateTenantName(name string) error {
	if len(name) == 0 {
		return errors.New("name must not be empty")
	}
	runes := []rune(name)
	if len(runes) > 255 {
		return errors.New("name must not exceed 255 characters")
	}
	for _, r := range runes {
		if unicode.Is(unicode.Cc, r) {
			return errors.New("name must not contain control characters (Unicode Cc)")
		}
	}
	return nil
}
