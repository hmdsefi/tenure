// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

import "slices"

func cmpFn(a, b int) int { return a - b }

func f(m map[string]int, s []int) {
	//tenure:maporder keys are sorted below
	for range m {
	}
	for range m { //tenure:maporder order unused
	}
	/* want DT000 */ //tenure:maporder
	for range m {    // want DT005
	}
	/* want DT000 */          //tenure:sortorder by id
	slices.SortFunc(s, cmpFn) // want DT008
	slices.SortFunc(s, cmpFn) //tenure:totalorder score desc, id asc
	/* want DT000 */ //tenure:maporder nothing here
	x := 1
	_ = x
	/* want DT000 */ //tenure:totalorder by id
	for range m {    // want DT005
	}
}
