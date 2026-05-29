package api

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/FernandoCendretti/webhook-delivery/internal/domain"
)

// ErrorResponse is the JSON body returned on any non-2xx response.
type ErrorResponse struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

// CreateTenantRequest is the body for POST /v1/tenants.
// Name is optional; absent and null are equivalent (treated as no name).
type CreateTenantRequest struct {
	Name *string `json:"name"`
}

// TenantResponse is the JSON body for POST /v1/tenants (201) and GET /v1/tenants/{id} (200).
// Name is omitted from the response when the tenant has no name.
type TenantResponse struct {
	ID        uuid.UUID `json:"id"`
	Name      *string   `json:"name,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// NewTenantResponse builds a TenantResponse from a domain.Tenant.
func NewTenantResponse(t domain.Tenant) TenantResponse {
	return TenantResponse{ID: t.ID, Name: t.Name, CreatedAt: t.CreatedAt}
}

// EndpointRequest is the body for POST /v1/endpoints.
type EndpointRequest struct {
	URL      string     `json:"url"`
	TenantID *uuid.UUID `json:"tenant_id"`
}

// EndpointResponse is the JSON body returned for read operations (GET).
// signing_secret is intentionally absent (FR-002, SC-005).
type EndpointResponse struct {
	ID        uuid.UUID `json:"id"`
	URL       string    `json:"url"`
	TenantID  uuid.UUID `json:"tenant_id"`
	CreatedAt time.Time `json:"created_at"`
}

// NewEndpointResponse builds an EndpointResponse from a domain.Endpoint.
func NewEndpointResponse(e domain.Endpoint) EndpointResponse {
	return EndpointResponse{ID: e.ID, URL: e.URL, TenantID: e.TenantID, CreatedAt: e.CreatedAt}
}

// EndpointCreatedResponse is the JSON body returned on 201 Created.
// It includes signing_secret once — the only time the caller can obtain it.
type EndpointCreatedResponse struct {
	ID            uuid.UUID `json:"id"`
	URL           string    `json:"url"`
	TenantID      uuid.UUID `json:"tenant_id"`
	CreatedAt     time.Time `json:"created_at"`
	SigningSecret string    `json:"signing_secret"`
}

// RotateSecretResponse is the JSON body returned by POST …/rotate-secret.
type RotateSecretResponse struct {
	SigningSecret string `json:"signing_secret"`
}

// EventRequest is the body for POST /v1/events.
type EventRequest struct {
	EndpointID uuid.UUID       `json:"endpoint_id"`
	TenantID   *uuid.UUID      `json:"tenant_id"`
	Payload    json.RawMessage `json:"payload"`
}

// EventAcceptedResponse is the body returned on 202 Accepted.
type EventAcceptedResponse struct {
	DeliveryID uuid.UUID `json:"delivery_id"`
	EventID    uuid.UUID `json:"event_id"`
}

// AttemptResponse represents a single delivery attempt in GET /v1/deliveries/{id}.
type AttemptResponse struct {
	Sequence           int            `json:"sequence"`
	StartedAt          time.Time      `json:"started_at"`
	CompletedAt        *time.Time     `json:"completed_at,omitempty"`
	Outcome            string         `json:"outcome"`
	ResponseStatusCode *int           `json:"response_status_code,omitempty"`
	ErrorReason        *string        `json:"error_reason,omitempty"`
}

// DeliveryResponse is the body for GET /v1/deliveries/{id}.
type DeliveryResponse struct {
	ID            uuid.UUID         `json:"id"`
	EndpointID    uuid.UUID         `json:"endpoint_id"`
	EventID       uuid.UUID         `json:"event_id"`
	Status        string            `json:"status"`
	AttemptCount  int               `json:"attempt_count"`
	NextAttemptAt *time.Time        `json:"next_attempt_at,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Attempts      []AttemptResponse `json:"attempts"`
}

// DLQItemResponse is a single entry in the DLQ listing.
type DLQItemResponse struct {
	DeliveryID   uuid.UUID `json:"delivery_id"`
	EventID      uuid.UUID `json:"event_id"`
	EndpointID   uuid.UUID `json:"endpoint_id"`
	TenantID     uuid.UUID `json:"tenant_id"`
	AttemptCount int       `json:"attempt_count"`
	FailedAt     time.Time `json:"failed_at"`
}

// PaginationResponse carries pagination metadata in the DLQ listing response.
type PaginationResponse struct {
	Page    int  `json:"page"`
	Limit   int  `json:"limit"`
	HasNext bool `json:"has_next"`
}

// DLQListResponse is the body for GET /v1/dlq.
type DLQListResponse struct {
	Items      []DLQItemResponse  `json:"items"`
	Pagination PaginationResponse `json:"pagination"`
}

// CircuitBreakerResponse is the body for GET /v1/endpoints/{id}/circuit-breaker.
// SuspendedUntil is omitted unless state is "open". The internal half_open state
// is rendered as "half-open" (hyphen) in the API.
type CircuitBreakerResponse struct {
	EndpointID          uuid.UUID  `json:"endpoint_id"`
	State               string     `json:"state"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	SuspendedUntil      *time.Time `json:"suspended_until,omitempty"`
}

// NewDeliveryResponse builds a DeliveryResponse from the domain objects.
// NextAttemptAt is only included when status is 'scheduled'.
func NewDeliveryResponse(d domain.Delivery, attempts []domain.Attempt) DeliveryResponse {
	var nextAt *time.Time
	if d.Status == domain.StatusScheduled {
		t := d.NextAttemptAt
		nextAt = &t
	}

	ar := make([]AttemptResponse, len(attempts))
	for i, a := range attempts {
		ar[i] = AttemptResponse{
			Sequence:           a.Sequence,
			StartedAt:          a.StartedAt,
			CompletedAt:        a.CompletedAt,
			Outcome:            string(a.Outcome),
			ResponseStatusCode: a.ResponseStatusCode,
			ErrorReason:        a.ErrorReason,
		}
	}

	return DeliveryResponse{
		ID:            d.ID,
		EndpointID:    d.EndpointID,
		EventID:       d.EventID,
		Status:        string(d.Status),
		AttemptCount:  d.AttemptCount,
		NextAttemptAt: nextAt,
		CreatedAt:     d.CreatedAt,
		UpdatedAt:     d.UpdatedAt,
		Attempts:      ar,
	}
}
