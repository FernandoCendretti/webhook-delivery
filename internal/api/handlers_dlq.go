package api

import (
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

