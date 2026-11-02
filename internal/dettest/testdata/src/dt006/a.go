// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

import (
	"fmt"
	"time"
)

type node struct{ id int }

type errT struct{}

func (*errT) Error() string { return "e" }

type holder struct{ n *int }

type stamp struct{ T time.Time }

var _ map[stamp]int // want DT006

func f(p, q *node, s []int, e *errT, h holder, m map[string]string, t1, t2 time.Time) {
	_ = fmt.Sprint(p)        // want DT006
	_ = fmt.Sprintf("%p", s) // want DT006
	_ = fmt.Sprint(e)
	_ = fmt.Sprint(h) // want DT006
	_ = fmt.Sprint(m)
	_ = t1 == t2 // want DT006
	switch t1 {  // want DT006
	}
	var u uintptr // want DT006
	_ = u
	_ = p == q
}
