// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package memstore

import "time"

// Clock is the time source of a Store. Monotonic drives session expiry; Wall gives the time new
// terms are computed from. A Store calls them while it holds its lock, so they must not call
// back into the Store.
type Clock interface {
	// Monotonic returns the time elapsed since some fixed point. A reading smaller than an
	// earlier one is taken as the earlier one, so the Store's time never moves back.
	Monotonic() time.Duration

	// Wall returns the current wall-clock time. It may step in either direction; it never
	// expires a session.
	Wall() time.Time
}

// systemClock is the Clock of New(nil): the process's monotonic clock since t0, and time.Now.
type systemClock struct {
	t0 time.Time
}

// Monotonic returns the time since t0, read from the monotonic clock.
func (c systemClock) Monotonic() time.Duration { return time.Since(c.t0) }

// Wall returns time.Now().
func (systemClock) Wall() time.Time { return time.Now() }
