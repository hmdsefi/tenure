// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build race && !go1.27

package debughttp_test

import (
	"os"
	"runtime"
	"testing"
)

// TestMain runs the race tests on one P before Go 1.27. Go 1.26 runs every
// timer of a synctest bubble under one race context, and when two threads
// fire bubbled timers at once they corrupt it: the test binary dies with a
// SIGSEGV or "ThreadSanitizer: CHECK failed" (golang.org/issue/76691 and
// golang.org/issue/78156, fixed in Go 1.27 and not backported). With one P,
// only the thread that holds it runs timers. The race detector still reports
// every unsynchronized access, since it tracks happens-before, not timing.
func TestMain(m *testing.M) {
	runtime.GOMAXPROCS(1)
	os.Exit(m.Run())
}
