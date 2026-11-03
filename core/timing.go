// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"fmt"
	"time"
)

// Timing holds the session timings. Each member measures them on its own monotonic clock.
type Timing struct {
	// LeaseDuration is how long another holder must observe a session unchanged before it
	// may take that session's shards.
	LeaseDuration time.Duration

	// RenewDeadline sets the session deadline: the send time of the last successful
	// renewal plus RenewDeadline.
	RenewDeadline time.Duration

	// RetryPeriod is the interval of session renewals and acquisition retries.
	RetryPeriod time.Duration

	// SlotGrace is how long a member must observe a slot vacant before the slot drops out
	// of the assignment, until it is claimed again.
	SlotGrace time.Duration
}

// DefaultTiming is client-go's leader-election timing (15 s, 10 s, 2 s) with a 60 s
// SlotGrace. It tolerates clock-rate error strictly below 20%. To change a field, copy
// DefaultTiming and change the copy.
var DefaultTiming = Timing{
	LeaseDuration: 15 * time.Second,
	RenewDeadline: 10 * time.Second,
	RetryPeriod:   2 * time.Second,
	SlotGrace:     60 * time.Second,
}

// maxTimingField is the largest value of any Timing field. It keeps the products in
// Validate far inside int64.
const maxTimingField = 24 * time.Hour

// Validate reports whether t is usable: every field is positive and at most 24 hours,
// RenewDeadline exceeds 1.2 times RetryPeriod (client-go's rule), LeaseDuration is at least
// 21/19 times RenewDeadline (a clock-rate tolerance of at least 5%), and SlotGrace is at
// least LeaseDuration. The error wraps ErrInvalidTiming.
func (t Timing) Validate() error {
	fields := [...]struct {
		name string
		d    time.Duration
	}{
		{"LeaseDuration", t.LeaseDuration},
		{"RenewDeadline", t.RenewDeadline},
		{"RetryPeriod", t.RetryPeriod},
		{"SlotGrace", t.SlotGrace},
	}
	for _, f := range fields {
		if f.d <= 0 || f.d > maxTimingField {
			return fmt.Errorf("%w: %s %v must be in (0s, 24h0m0s]", ErrInvalidTiming, f.name, f.d)
		}
	}
	// Integer forms of the spec's inequalities, on nanoseconds. Every field is at most
	// 24h, so the products cannot overflow.
	if 5*int64(t.RenewDeadline) <= 6*int64(t.RetryPeriod) {
		return fmt.Errorf("%w: RenewDeadline %v must be greater than 1.2 times RetryPeriod %v",
			ErrInvalidTiming, t.RenewDeadline, t.RetryPeriod)
	}
	if 19*int64(t.LeaseDuration) < 21*int64(t.RenewDeadline) {
		return fmt.Errorf("%w: LeaseDuration %v must be at least 21/19 times RenewDeadline %v",
			ErrInvalidTiming, t.LeaseDuration, t.RenewDeadline)
	}
	if t.SlotGrace < t.LeaseDuration {
		return fmt.Errorf("%w: SlotGrace %v must be at least LeaseDuration %v",
			ErrInvalidTiming, t.SlotGrace, t.LeaseDuration)
	}
	return nil
}

// DriftBound returns the clock-rate error t tolerates, (LeaseDuration - RenewDeadline) /
// (LeaseDuration + RenewDeadline), or 0 when that is negative or either duration is not
// positive. The owner stops before another holder may take over when every member's
// monotonic clock runs at a rate within 1 ± ρ for some ρ strictly below DriftBound, and no
// process pauses.
func (t Timing) DriftBound() float64 {
	if t.LeaseDuration <= 0 || t.RenewDeadline <= 0 {
		return 0
	}
	b := float64(t.LeaseDuration-t.RenewDeadline) / float64(t.LeaseDuration+t.RenewDeadline)
	if b <= 0 {
		return 0
	}
	return b
}
