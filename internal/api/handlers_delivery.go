package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
)

// deliveryGetter is the service interface consumed by the delivery handler.
type deliveryGetter interface {
	Get(ctx context.Context, id uuid.UUID) (*service.DeliveryView, error)
}

type deliveryHandler struct {
	svc deliveryGetter
	log *slog.Logger
}

func newDeliveryHandler(svc deliveryGetter, log *slog.Logger) *deliveryHandler {
	if log == nil {
		log = slog.Default()
	}
	return &deliveryHandler{svc: svc, log: log}
}

// Get handles GET /v1/deliveries/{id}.
func (h *deliveryHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "delivery_not_found", "")
		return
	}

	view, err := h.svc.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "delivery_not_found", "")
			return
		}
		h.log.ErrorContext(r.Context(), "get delivery failed", "err", err, "id", id)
		writeError(w, http.StatusInternalServerError, "internal_error", "")
		return
	}

	writeJSON(w, http.StatusOK, NewDeliveryResponse(*view.Delivery, view.Attempts))
}
