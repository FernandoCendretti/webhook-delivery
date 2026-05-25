package domain

import "errors"

// ErrNotFound is the cross-entity sentinel returned by stores when a row is
// missing. Callers map it to the appropriate "<entity>_not_found" error code
// at their layer (e.g. handlers translate it to a 404 with a JSON body).
var ErrNotFound = errors.New("not found")

// ErrConflict is returned by event_service when an Idempotency-Key is
// reused with a different payload (FR-007).
var ErrConflict = errors.New("conflict")
