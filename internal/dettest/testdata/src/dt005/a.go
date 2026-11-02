// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

func f(m map[string]int, s []int) {
	for k := range m { // want DT005
		_ = k
	}
	for range m { // want DT005
	}
	for i := range s {
		_ = i
	}
}
