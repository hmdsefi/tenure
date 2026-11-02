// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

type state struct{ x int }

func (s *state) Inc() { s.x++ }

func (s state) Get() int { return s.x }

var n int

var buf []int

var st state

var limit = 3

func f() int {
	n = 1      // want DT009
	n++        // want DT009
	buf[0] = 1 // want DT009
	st.x = 2   // want DT009
	p := &n    // want DT009
	st.Inc()   // want DT009
	_ = st.Get()
	_ = n
	m := 1
	m++
	return *p + m + limit
}
