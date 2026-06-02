package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
	"github.com/FernandoCendretti/webhook-delivery/internal/service"
)

type dlqHandler struct {
	svc    service.DLQService
	logger *slog.Logger
}

func newDLQHandler(svc service.DLQService, logger *slog.Logger) *dlqHandler {
	return &dlqHandler{svc: svc, logger: logger}
}

func (h *dlqHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	var filter domain.DLQFilter
	if raw := q.Get("tenant_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_tenant_id", "tenant_id must be a valid UUID")
			return
		}
		filter.TenantID = &id
	}
	if raw := q.Get("endpoint_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_endpoint_id", "endpoint_id must be a valid UUID")
			return
		}
		filter.EndpointID = &id
	}

	page := 1
	if raw := q.Get("page"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			writeError(w, http.StatusBadRequest, "invalid_page", "page must be an integer >= 1")
			return
		}
		page = v
	}
	limit := 20
	if raw := q.Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > 100 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be an integer between 1 and 100")
			return
		}
		limit = v
	}

	entries, pg, err := h.svc.List(r.Context(), filter, page, limit)
	if err != nil {
		h.logger.ErrorContext(r.Context(), "list dlq", "err", err)
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected error")
		return
	}

	items := make([]DLQItemResponse, len(entries))
	for i, e := range entries {
		items[i] = DLQItemResponse{
			DeliveryID:   e.DeliveryID,
			EventID:      e.EventID,
			EndpointID:   e.EndpointID,
			TenantID:     e.TenantID,
			AttemptCount: e.AttemptCount,
			FailedAt:     e.FailedAt,
		}
	}

	resp := DLQListResponse{
		Items: items,
		Pagination: PaginationResponse{
			Page:    pg.Page,
			Limit:   pg.Limit,
			HasNext: pg.HasNext,
		},
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *dlqHandler) Detail(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("delivery_id")
	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_delivery_id", "delivery_id must be a valid UUID")
		return
	}

	detail, err := h.svc.Detail(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "delivery not found")
			return
		}
		h.logger.ErrorContext(r.Context(), "dlq detail", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected error")
		return
	}

	attempts := make([]AttemptResponse, len(detail.Attempts))
	for i, a := range detail.Attempts {
		attempts[i] = AttemptResponse{
			Sequence:           a.Sequence,
			StartedAt:          a.StartedAt,
			CompletedAt:        a.CompletedAt,
			Outcome:            string(a.Outcome),
			ResponseStatusCode: a.ResponseStatusCode,
			ErrorReason:        a.ErrorReason,
		}
	}

	resp := DLQDetailResponse{
		DeliveryID:   detail.DeliveryID,
		EventID:      detail.EventID,
		EndpointID:   detail.EndpointID,
		TenantID:     detail.TenantID,
		AttemptCount: detail.AttemptCount,
		FailedAt:     detail.FailedAt,
		Attempts:     attempts,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *dlqHandler) BulkReplay(w http.ResponseWriter, r *http.Request) {
	var req BulkReplayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if req.TenantID == nil && req.EndpointID == nil {
		writeError(w, http.StatusBadRequest, "missing_filter", "at least one of tenant_id or endpoint_id must be provided")
		return
	}

	count, err := h.svc.BulkReplay(r.Context(), domain.DLQFilter{TenantID: req.TenantID, EndpointID: req.EndpointID})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrUnprocessable):
			writeError(w, http.StatusUnprocessableEntity, "unprocessable", err.Error())
		default:
			h.logger.ErrorContext(r.Context(), "dlq bulk replay", "err", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "unexpected error")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, BulkReplayResponse{Replayed: count})
}

func (h *dlqHandler) Replay(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("delivery_id")
	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_delivery_id", "delivery_id must be a valid UUID")
		return
	}

	newDelivery, err := h.svc.Replay(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrNotFound):
			writeError(w, http.StatusNotFound, "not_found", "delivery not found")
		case errors.Is(err, domain.ErrConflict):
			writeError(w, http.StatusConflict, "conflict", "delivery is not permanently_failed or a non-terminal replay already exists")
		case errors.Is(err, domain.ErrUnprocessable):
			writeError(w, http.StatusUnprocessableEntity, "unprocessable", "the endpoint referenced by the delivery no longer exists")
		default:
			h.logger.ErrorContext(r.Context(), "dlq replay", "err", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "unexpected error")
		}
		return
	}

	writeJSON(w, http.StatusAccepted, ReplayResponse{
		DeliveryID: newDelivery.ID,
		Status:     string(newDelivery.Status),
	})
}

