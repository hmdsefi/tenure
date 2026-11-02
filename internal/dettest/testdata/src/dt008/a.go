// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

import (
	"slices"
	"sort"
)

type byID []int

func (s byID) Len() int           { return len(s) }
func (s byID) Less(i, j int) bool { return s[i] < s[j] }
func (s byID) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func less(int, int) bool { return false }

func cmpFn(a, b int) int { return a - b }

func f(s, ints []int, names []string) {
	sort.Slice(s, less)       // want DT008
	sort.Sort(byID(s))        // want DT008
	slices.SortFunc(s, cmpFn) // want DT008
	slices.SortStableFunc(s, cmpFn)
	slices.Sort(ints)
	sort.Strings(names)
}
