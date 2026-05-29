package domain

import (
	"time"

	"github.com/google/uuid"
)

// DLQFilter holds the optional filters used by DLQ listing and bulk replay.
type DLQFilter struct {
	TenantID   *uuid.UUID
	EndpointID *uuid.UUID
}

// DLQEntry is a single row in the DLQ listing response.
type DLQEntry struct {
	DeliveryID   uuid.UUID
	EventID      uuid.UUID
	EndpointID   uuid.UUID
	TenantID     uuid.UUID
	AttemptCount int
	FailedAt     time.Time
}
