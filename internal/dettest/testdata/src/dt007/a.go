// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

import "math"

var c complex128 // want DT007

func f(a, b, c float64, k int) {
	x := a * b // want DT007
	y := float64(a * b)
	z := float64(a*b) + c
	x *= 2 // want DT007
	w := a / b
	_ = math.Exp(a) // want DT007
	_ = math.Sqrt(a)
	_ = math.Ceil(a)
	n := 3 * k
	_, _, _, _, _ = x, y, z, w, n
}
