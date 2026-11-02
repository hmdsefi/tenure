// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

import "time"

func f(t, u time.Time, d time.Duration) {
	_ = time.Now()       // want DT003
	_ = time.Since(t)    // want DT003
	time.Sleep(d)        // want DT003
	_ = time.NewTimer(d) // want DT003
	_ = time.Local       // want DT003
	_ = t.Year()         // want DT003
	_ = t.Add(d)
	_ = t.Before(u)
	_ = t.UnixMilli()
	_ = d.String()
	_ = time.Second
	var x time.Duration
	_ = x
	_ = time.Unix(0, 0) // want DT003
	_ = t.Unix()
	var y yearer = t // want DT003
	_ = y
	var s stringer = d
	_ = s
	_ = any(t) // want DT003
	_ = gm(t)  // want DT003
}

type yearer interface{ Year() int }

type stringer interface{ String() string }

func gm[X interface{ Year() int }](x X) int { return x.Year() }
