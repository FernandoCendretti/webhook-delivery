package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/observability"
)

// eventSubmitter is the service interface consumed by the event handler.
type eventSubmitter interface {
	Submit(ctx context.Context, endpointID uuid.UUID, payload json.RawMessage) (*domain.Delivery, error)
}

type eventHandler struct {
	svc     eventSubmitter
	log     *slog.Logger
	metrics *observability.Metrics
}

func newEventHandler(svc eventSubmitter, log *slog.Logger) *eventHandler {
	if log == nil {
		log = slog.Default()
	}
	return &eventHandler{svc: svc, log: log}
}

func newEventHandlerWithMetrics(svc eventSubmitter, log *slog.Logger, m *observability.Metrics) *eventHandler {
	h := newEventHandler(svc, log)
	h.metrics = m
	return h
}

// Submit handles POST /v1/events.
func (h *eventHandler) Submit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB

	var req EventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			if h.metrics != nil {
				h.metrics.EventsRejected.WithLabelValues("payload_too_large").Inc()
			}
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds 1 MiB")
			return
		}
		if h.metrics != nil {
			h.metrics.EventsRejected.WithLabelValues("bad_request").Inc()
		}
		writeError(w, http.StatusBadRequest, "bad_request", "malformed JSON body")
		return
	}

	d, err := h.svc.Submit(r.Context(), req.EndpointID, req.Payload)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			if h.metrics != nil {
				h.metrics.EventsRejected.WithLabelValues("endpoint_not_found").Inc()
			}
			writeError(w, http.StatusNotFound, "endpoint_not_found", "")
			return
		}
		h.log.ErrorContext(r.Context(), "submit event failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "")
		return
	}

	if h.metrics != nil {
		h.metrics.EventsSubmitted.WithLabelValues(req.EndpointID.String()).Inc()
	}
	writeJSON(w, http.StatusAccepted, EventAcceptedResponse{
		DeliveryID: d.ID,
		EventID:    d.EventID,
	})
}
