// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !race

package tenure

// raceEnabled reports that the test binary runs with the race detector, under which timing
// tests mean nothing.
const raceEnabled = false
