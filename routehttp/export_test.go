// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package routehttp

// DefaultJitter is the random source of the retry waits.
var DefaultJitter = jitter

// SetJitter makes Transports draw their retry waits from f until the returned function restores
// the random source. Tests that call it must not run in parallel.
func SetJitter(f func(n int64) int64) (restore func()) {
	old := jitter
	jitter = f
	return func() { jitter = old }
}
