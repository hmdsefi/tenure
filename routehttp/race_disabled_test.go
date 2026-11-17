// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !race

package routehttp_test

// raceEnabled reports that the test binary runs with the race detector, which changes
// allocation counts.
const raceEnabled = false
