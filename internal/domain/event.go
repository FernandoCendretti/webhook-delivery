package domain

import (
	"time"

	"github.com/google/uuid"
)

// Event represents an inbound webhook payload submitted by a caller.
type Event struct {
	ID          uuid.UUID
	EndpointID  uuid.UUID
	Payload     []byte
	SubmittedAt time.Time
}
