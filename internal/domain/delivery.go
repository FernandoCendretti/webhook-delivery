package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// DeliveryStatus is the lifecycle state of a delivery.
type DeliveryStatus string

const (
	// StatusScheduled means the delivery is waiting to be picked up by the scheduler.
	StatusScheduled DeliveryStatus = "scheduled"
	// StatusInFlight means the scheduler has claimed the delivery and a worker is processing it.
	StatusInFlight DeliveryStatus = "in_flight"
	// StatusDelivered means the target acknowledged the webhook successfully.
	StatusDelivered DeliveryStatus = "delivered"
	// StatusPermanentlyFailed means all attempts were exhausted or a non-retryable error occurred.
	StatusPermanentlyFailed DeliveryStatus = "permanently_failed"
)

// ErrInvalidTransition is returned when a state transition is not allowed.
var ErrInvalidTransition = errors.New("invalid delivery status transition")

// Delivery represents a single webhook delivery lifecycle tied to an event and endpoint.
type Delivery struct {
	ID                 uuid.UUID
	EventID            uuid.UUID
	EndpointID         uuid.UUID
	Status             DeliveryStatus
	AttemptCount       int
	NextAttemptAt      time.Time
	InFlightLeaseUntil *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// MarkInFlight transitions the delivery from scheduled to in_flight and sets
// the lease expiry.
func (d *Delivery) MarkInFlight(leaseUntil time.Time) error {
	if d.Status != StatusScheduled {
		return fmt.Errorf("%w: %s -> in_flight", ErrInvalidTransition, d.Status)
	}
	d.Status = StatusInFlight
	d.InFlightLeaseUntil = &leaseUntil
	return nil
}

// MarkDelivered transitions the delivery from in_flight to delivered.
func (d *Delivery) MarkDelivered() error {
	if d.Status != StatusInFlight {
		return fmt.Errorf("%w: %s -> delivered", ErrInvalidTransition, d.Status)
	}
	d.Status = StatusDelivered
	d.InFlightLeaseUntil = nil
	d.AttemptCount++
	return nil
}

// MarkPermanentlyFailed transitions the delivery from in_flight to permanently_failed.
func (d *Delivery) MarkPermanentlyFailed() error {
	if d.Status != StatusInFlight {
		return fmt.Errorf("%w: %s -> permanently_failed", ErrInvalidTransition, d.Status)
	}
	d.Status = StatusPermanentlyFailed
	d.InFlightLeaseUntil = nil
	d.AttemptCount++
	return nil
}

// RescheduleAfter transitions the delivery back to scheduled, advancing
// NextAttemptAt by the given delay relative to now.
func (d *Delivery) RescheduleAfter(delay time.Duration, now time.Time) error {
	if d.Status != StatusInFlight {
		return fmt.Errorf("%w: %s -> scheduled (reschedule)", ErrInvalidTransition, d.Status)
	}
	d.Status = StatusScheduled
	d.InFlightLeaseUntil = nil
	d.NextAttemptAt = now.Add(delay)
	d.AttemptCount++
	return nil
}
