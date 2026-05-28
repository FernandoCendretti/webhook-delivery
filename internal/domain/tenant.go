package domain

import (
	"time"

	"github.com/google/uuid"
)

// Tenant is a logical grouping of endpoints owned by the same producer.
// Name is nil when the producer did not provide one at registration time.
type Tenant struct {
	ID        uuid.UUID
	Name      *string
	CreatedAt time.Time
}
