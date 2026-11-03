// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !race

package core

import "testing"

// Results go to package-level sinks so the compiler cannot drop the calls.
var (
	treeSinkBool bool
	treeSinkInt  int
	treeSinkStr  string
	treeSinkInts []int
	treeSinkErr  error
)

func TestATPTR11AllocsTopology(t *testing.T) {
	tq := Tree{Name: "tq", Read: 8, Write: 8, Degree: 3}
	tests := []struct {
		name string
		f    func()
		max  float64
	}{
		{"IsRoot", func() { treeSinkBool = tq.IsRoot(3) }, 0},
		{"Parent", func() { treeSinkInt = tq.Parent(7) }, 0},
		{"Key(0)", func() { treeSinkStr = tq.Key(0) }, 0},
		{"Key(5)", func() { treeSinkStr = tq.Key(5) }, 1},
		{"Key(4095)", func() { treeSinkStr = tq.Key(4095) }, 1},
		{"Children(3)", func() { treeSinkInts = tq.Children(3) }, 0},
		{"Validate", func() { treeSinkErr = tq.Validate() }, 0},
	}
	for _, tc := range tests {
		got := testing.AllocsPerRun(100, tc.f)
		if got > tc.max {
			t.Errorf("%s: %v allocations per run, want at most %v", tc.name, got, tc.max)
		}
	}
	// Children(0) allocates exactly once: its result is not empty.
	if got := testing.AllocsPerRun(100, func() { treeSinkInts = tq.Children(0) }); got != 1 {
		t.Errorf("Children(0): %v allocations per run, want exactly 1", got)
	}
}
