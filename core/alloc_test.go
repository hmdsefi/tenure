// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !race

package core

import (
	"strconv"
	"testing"
)

func TestATCOR10Allocs(t *testing.T) {
	var score uint64
	var shard int
	if got := testing.AllocsPerRun(100, func() { score += Score("account", "alice", "n1") }); got != 0 {
		t.Errorf(`Score("account", "alice", "n1") allocates %v times, want 0`, got)
	}
	if got := testing.AllocsPerRun(100, func() { shard += KeyShard("market-42", 64) }); got != 0 {
		t.Errorf(`KeyShard("market-42", 64) allocates %v times, want 0`, got)
	}
	_, _ = score, shard
}

func TestATCOR10AllocsRank(t *testing.T) {
	ms := make([]Member, 1000)
	for i := range ms {
		ms[i] = Member{ID: "n" + strconv.Itoa(i)}
	}
	p := NewRendezvous(ms)
	var sink int
	for _, n := range []int{3, 1} {
		if got := testing.AllocsPerRun(100, func() { sink += len(Rank("account", "k1", ms, n)) }); got > 2 {
			t.Errorf(`Rank("account", "k1", n0 … n999, %d) allocates %v times, want at most 2`, n, got)
		}
	}
	if got := testing.AllocsPerRun(100, func() { sink += len(p.Rank("account", "k1", 1)) }); got > 2 {
		t.Errorf(`NewRendezvous(n0 … n999).Rank("account", "k1", 1) allocates %v times, want at most 2`, got)
	}
	_ = sink
}

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
