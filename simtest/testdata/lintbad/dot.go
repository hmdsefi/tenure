// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build ignore

package lintbad

import (
	. "math/rand/v2" // want: dot import of math/rand/v2
	_ "sync"         // want: import of sync
	. "time"         // want: dot import of time
	wall "time"
)

// renamed uses time under another name, and names that only a dot import explains.
func renamed() Duration {
	started := wall.Now() // want: use of time.Now
	var timeout wall.Duration = wall.Second
	return started.Sub(wall.Time{}) + timeout + Duration(IntN(10))
}
