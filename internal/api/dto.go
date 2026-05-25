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

// EndpointRequest is the body for POST /v1/endpoints.
type EndpointRequest struct {
	URL string `json:"url"`
}

// ToDomain produces the partial domain.Endpoint carrying the user-supplied
// fields. ID and CreatedAt are populated downstream by the store.
func (r EndpointRequest) ToDomain() domain.Endpoint {
	return domain.Endpoint{URL: r.URL}
}

// EndpointResponse is the JSON body returned for read operations (GET).
// signing_secret is intentionally absent (FR-002, SC-005).
type EndpointResponse struct {
	ID        uuid.UUID `json:"id"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"created_at"`
}

// NewEndpointResponse builds an EndpointResponse from a domain.Endpoint.
func NewEndpointResponse(e domain.Endpoint) EndpointResponse {
	return EndpointResponse{ID: e.ID, URL: e.URL, CreatedAt: e.CreatedAt}
}

// EndpointCreatedResponse is the JSON body returned on 201 Created.
// It includes signing_secret once — the only time the caller can obtain it.
type EndpointCreatedResponse struct {
	ID            uuid.UUID `json:"id"`
	URL           string    `json:"url"`
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
