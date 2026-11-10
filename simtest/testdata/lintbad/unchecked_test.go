// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build ignore

package lintbad

import (
	"sync"
	"time"
)

// unchecked breaks the rules in a test file, which is not held to them: nothing here is
// marked, and nothing is found.
func unchecked() time.Time {
	var wg sync.WaitGroup
	wg.Add(1)
	go wg.Done()
	wg.Wait()
	return time.Now()
}
