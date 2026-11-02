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

func f(s, ints []int, names []string, fs []float64) {
	sort.Slice(s, less)       // want DT008
	sort.Sort(byID(s))        // want DT008
	slices.SortFunc(s, cmpFn) // want DT008
	slices.SortStableFunc(s, cmpFn)
	slices.Sort(ints)
	sort.Strings(names)
	sort.Float64s(fs)                    // want DT008
	sort.Float64Slice(fs).Sort()         // want DT008
	slices.Sort(fs)                      // want DT008
	_ = slices.Sorted(slices.Values(fs)) // want DT008
	_, _ = slices.BinarySearch(fs, 1)    // want DT008
	slices.Sort(fs)                      //tenure:totalorder no NaN or -0: values are checked above
}
