package domain

import (
	"time"

	"github.com/google/uuid"
)

// CircuitState represents the current state of an endpoint's circuit breaker.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

// CircuitBreakerInfo is the read model returned by GET /v1/endpoints/{id}/circuit-breaker.
// SuspendedUntil is non-nil only when State == CircuitOpen.
type CircuitBreakerInfo struct {
	EndpointID          uuid.UUID
	State               CircuitState
	ConsecutiveFailures int
	SuspendedUntil      *time.Time
}
