package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// circuitStateGetter is the subset of store.CircuitStore used by circuitHandler.
type circuitStateGetter interface {
	GetState(ctx context.Context, endpointID uuid.UUID) (*domain.CircuitBreakerInfo, error)
}

type circuitHandler struct {
	store circuitStateGetter
	log   *slog.Logger
}

func newCircuitHandler(store circuitStateGetter, log *slog.Logger) *circuitHandler {
	if log == nil {
		log = slog.Default()
	}
	return &circuitHandler{store: store, log: log}
}

// GetState handles GET /v1/endpoints/{id}/circuit-breaker.
func (h *circuitHandler) GetState(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_endpoint_id", "")
		return
	}

	info, err := h.store.GetState(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "endpoint_not_found", "")
			return
		}
		h.log.ErrorContext(r.Context(), "get circuit state", "endpoint", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "")
		return
	}

	// Translate internal "half_open" → API "half-open" per plan.md §API contracts.
	state := string(info.State)
	if state == "half_open" {
		state = "half-open"
	}

	resp := CircuitBreakerResponse{
		EndpointID:          id,
		State:               state,
		ConsecutiveFailures: info.ConsecutiveFailures,
	}
	if info.State == domain.CircuitOpen {
		resp.SuspendedUntil = info.SuspendedUntil
	}

	writeJSON(w, http.StatusOK, resp)
}
