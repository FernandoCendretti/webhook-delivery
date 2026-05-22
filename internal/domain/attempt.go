package domain

import (
	"time"

	"github.com/google/uuid"
)

// AttemptOutcome is the result of a single delivery attempt.
type AttemptOutcome string

const (
	// OutcomeSuccess indicates the target returned a 2xx or 3xx response.
	OutcomeSuccess AttemptOutcome = "success"
	// OutcomeTransientFailure indicates a retryable error (5xx, 429, network).
	OutcomeTransientFailure AttemptOutcome = "transient_failure"
	// OutcomePermanentFailure indicates a non-retryable error (4xx except 429).
	OutcomePermanentFailure AttemptOutcome = "permanent_failure"
	// OutcomeTimeout indicates the HTTP call exceeded its deadline.
	OutcomeTimeout AttemptOutcome = "timeout"
)

// IsTerminal reports whether the outcome ends the delivery lifecycle.
func (o AttemptOutcome) IsTerminal() bool {
	return o == OutcomeSuccess || o == OutcomePermanentFailure
}

// IsRetryable reports whether the outcome should trigger a retry.
func (o AttemptOutcome) IsRetryable() bool {
	return o == OutcomeTransientFailure || o == OutcomeTimeout
}

// Attempt records a single HTTP delivery attempt for a delivery.
type Attempt struct {
	ID                 uuid.UUID
	DeliveryID         uuid.UUID
	Sequence           int
	StartedAt          time.Time
	CompletedAt        *time.Time
	ResponseStatusCode *int
	Outcome            AttemptOutcome
	ErrorReason        *string
}
