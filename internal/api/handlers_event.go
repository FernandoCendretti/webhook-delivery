package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/observability"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
)

// eventSubmitter is the service interface consumed by the event handler.
type eventSubmitter interface {
	Submit(ctx context.Context, endpointID uuid.UUID, payload json.RawMessage, idempotencyKey string, rawBody []byte, tenantID uuid.UUID) (*domain.Delivery, error)
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

// Submit handles POST /v1/events. Requires tenant_id in the body (FR-007).
func (h *eventHandler) Submit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			if h.metrics != nil {
				h.metrics.EventsRejected.WithLabelValues("payload_too_large").Inc()
			}
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds 1 MiB")
			return
		}
		writeError(w, http.StatusBadRequest, "bad_request", "failed to read body")
		return
	}

	var req EventRequest
	if err := json.Unmarshal(rawBody, &req); err != nil {
		if h.metrics != nil {
			h.metrics.EventsRejected.WithLabelValues("bad_request").Inc()
		}
		writeError(w, http.StatusBadRequest, "bad_request", "malformed JSON body")
		return
	}

	if req.TenantID == nil {
		writeError(w, http.StatusBadRequest, "missing_tenant_id", "")
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if _, present := r.Header["Idempotency-Key"]; present {
		if err := validateIdempotencyKey(idempotencyKey); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_idempotency_key", err.Error())
			return
		}
	}

	d, err := h.svc.Submit(r.Context(), req.EndpointID, req.Payload, idempotencyKey, rawBody, *req.TenantID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			if h.metrics != nil {
				h.metrics.EventsRejected.WithLabelValues("endpoint_not_found").Inc()
			}
			writeError(w, http.StatusNotFound, "endpoint_not_found", "")
			return
		}
		if errors.Is(err, service.ErrTenantNotFound) {
			writeError(w, http.StatusUnprocessableEntity, "tenant_not_found", "")
			return
		}
		if errors.Is(err, service.ErrTenantEndpointMismatch) {
			writeError(w, http.StatusUnprocessableEntity, "tenant_endpoint_mismatch", "")
			return
		}
		if errors.Is(err, domain.ErrConflict) {
			writeError(w, http.StatusConflict, "idempotency_conflict", "payload hash differs from original submission")
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

// validateIdempotencyKey checks that key is 1–255 bytes of printable ASCII
// (bytes in [0x21, 0x7E] inclusive).
func validateIdempotencyKey(key string) error {
	if len(key) == 0 || len(key) > 255 {
		return errors.New("Idempotency-Key must be 1-255 printable ASCII characters")
	}
	for i := range len(key) {
		b := key[i]
		if b < 0x21 || b > 0x7E {
			return errors.New("Idempotency-Key contains invalid character")
		}
	}
	return nil
}
