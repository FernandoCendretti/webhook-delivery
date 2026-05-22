package domain

import (
	"testing"
	"time"
)

// TestDelay_Boundaries asserts Delay(1) and Delay(MaxAttempts+1) are both zero (T045).
func TestDelay_Boundaries(t *testing.T) {
	if d := Delay(1); d != 0 {
		t.Errorf("Delay(1) = %v, want 0 (no wait before first attempt)", d)
	}
	if d := Delay(MaxAttempts + 1); d != 0 {
		t.Errorf("Delay(%d) = %v, want 0 (beyond budget)", MaxAttempts+1, d)
	}
}

// TestDelay_Jitter verifies each scheduled delay falls within ±15% of its base (T045).
func TestDelay_Jitter(t *testing.T) {
	const iterations = 300
	for i, base := range productionSchedule {
		attemptNum := i + 2 // attempt numbers 2..9 map to schedule[0..7]
		lo := time.Duration(float64(base) * 0.85)
		hi := time.Duration(float64(base) * 1.15)
		for j := 0; j < iterations; j++ {
			got := Delay(attemptNum)
			if got < lo || got > hi {
				t.Errorf("Delay(%d) iter %d = %v, want [%v, %v]",
					attemptNum, j, got, lo, hi)
			}
		}
	}
}

// TestProductionScheduleLock guards FR-015. The retry schedule is a product
// decision, not an implementation detail. Any change here must be made
// deliberately together with spec.md FR-015 and US3 acceptance scenarios.
func TestProductionScheduleLock(t *testing.T) {
	expected := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		30 * time.Second,
		5 * time.Minute,
		30 * time.Minute,
		2 * time.Hour,
		8 * time.Hour,
		24 * time.Hour,
	}
	if len(productionSchedule) != len(expected) {
		t.Fatalf("schedule length: got %d, want %d (FR-015 lock — update spec.md before editing this test)",
			len(productionSchedule), len(expected))
	}
	for i, want := range expected {
		if productionSchedule[i] != want {
			t.Errorf("schedule[%d]: got %v, want %v (FR-015 lock — update spec.md before editing this test)",
				i, productionSchedule[i], want)
		}
	}
}

// TestMaxAttemptsLock guards FR-015's "9 attempts (1 initial + 8 retries)" guarantee.
func TestMaxAttemptsLock(t *testing.T) {
	if MaxAttempts != 9 {
		t.Errorf("MaxAttempts: got %d, want 9 (FR-015 lock — update spec.md before editing)", MaxAttempts)
	}
	if MaxAttempts != 1+len(productionSchedule) {
		t.Errorf("MaxAttempts inconsistent with schedule length: MaxAttempts=%d, schedule=%d intervals",
			MaxAttempts, len(productionSchedule))
	}
}
