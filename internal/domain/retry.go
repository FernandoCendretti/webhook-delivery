package domain

import (
	"math/rand"
	"sync"
	"time"
)

// productionSchedule encodes the retry wait sequence per FR-015. Values are
// locked by retry_test.go; do not edit without updating spec.md FR-015 and the
// US3 acceptance scenarios in the same change.
var productionSchedule = []time.Duration{
	1 * time.Second,
	5 * time.Second,
	30 * time.Second,
	5 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
	8 * time.Hour,
	24 * time.Hour,
}

// MaxAttempts is the total attempt budget per FR-015 (1 initial + 8 retries).
const MaxAttempts = 1 + 8

var (
	scheduleMu     sync.RWMutex
	activeSchedule = productionSchedule
)

// UseShortScheduleForTests swaps the production schedule for a fast one suitable
// for integration tests. Returns a restore function. NOT safe for production use.
func UseShortScheduleForTests() func() {
	scheduleMu.Lock()
	defer scheduleMu.Unlock()
	prev := activeSchedule
	activeSchedule = []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
		320 * time.Millisecond,
		640 * time.Millisecond,
		1280 * time.Millisecond,
	}
	return func() {
		scheduleMu.Lock()
		defer scheduleMu.Unlock()
		activeSchedule = prev
	}
}

// Delay returns the wait duration before attempt n (1-indexed). Delay(1) and
// Delay(n > MaxAttempts) return zero — no wait is applicable.
func Delay(attemptNumber int) time.Duration {
	if attemptNumber < 2 || attemptNumber > MaxAttempts {
		return 0
	}
	scheduleMu.RLock()
	sched := activeSchedule
	scheduleMu.RUnlock()

	idx := attemptNumber - 2
	if idx >= len(sched) {
		return 0
	}
	return applyJitter(sched[idx])
}

func applyJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	span := int64(base) * 30 / 100
	if span <= 0 {
		return base
	}
	delta := rand.Int63n(span) - span/2
	return base + time.Duration(delta)
}
