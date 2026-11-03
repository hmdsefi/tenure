// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !race

package core

import (
	"runtime/debug"
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
	small := members("n1", "n2", "n3")
	p := NewRendezvous(ms)
	pSmall := NewRendezvous(small)
	var sink int
	tests := []struct {
		name string
		f    func()
	}{
		{`Rank("account", "k1", n0 … n999, 3)`, func() { sink += len(Rank("account", "k1", ms, 3)) }},
		{`Rank("account", "k1", n0 … n999, 1)`, func() { sink += len(Rank("account", "k1", ms, 1)) }},
		{`Rank("account", "k1", n1 … n3, 1)`, func() { sink += len(Rank("account", "k1", small, 1)) }},
		{`Rank("account", "k1", n1 … n3, 3)`, func() { sink += len(Rank("account", "k1", small, 3)) }},
		{`Rank("account", "k1", n1 … n3, 10)`, func() { sink += len(Rank("account", "k1", small, 10)) }},
		{`NewRendezvous(n0 … n999).Rank("account", "k1", 1)`, func() { sink += len(p.Rank("account", "k1", 1)) }},
		{`NewRendezvous(n0 … n999).Rank("account", "k1", 3)`, func() { sink += len(p.Rank("account", "k1", 3)) }},
		{`NewRendezvous(n1 … n3).Rank("account", "k1", 1)`, func() { sink += len(pSmall.Rank("account", "k1", 1)) }},
		{`NewRendezvous(n1 … n3).Rank("account", "k1", 10)`, func() { sink += len(pSmall.Rank("account", "k1", 10)) }},
	}
	for _, tt := range tests {
		if got := testing.AllocsPerRun(100, tt.f); got > 2 {
			t.Errorf("%s allocates %v times, want at most 2", tt.name, got)
		}
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

func TestATPTR11AllocsPicks(t *testing.T) {
	tq := Tree{Name: "tq", Read: 8, Write: 8, Degree: 3}
	tests := []struct {
		name string
		f    func()
	}{
		{"Pick", func() { treeSinkInt = tq.Pick(5) }},
		{"PickRead", func() { treeSinkInt = tq.PickRead(5) }},
		{"ValidateChange", func() { treeSinkErr = tq.ValidateChange(tq) }},
	}
	for _, tc := range tests {
		if got := testing.AllocsPerRun(100, tc.f); got != 0 {
			t.Errorf("%s: %v allocations per run, want 0", tc.name, got)
		}
	}
}

func TestATASG07Allocs(t *testing.T) {
	// A collection during a run can add an allocation; keep the collector off so the counts
	// compare exactly.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	var sink int
	calls := []struct {
		name  string
		space string
		n     int
		keys  []string
	}{
		{"Balanced(s, 64, m0 … m2)", "s", 64, seqKeys("m", 3)},
		{"Balanced(s, 4096, m0 … m99)", "s", 4096, seqKeys("m", 100)},
		{"Balanced(t, 1000, m0 … m9)", "t", 1000, seqKeys("m", 10)},
	}
	var first float64
	for i, c := range calls {
		got := testing.AllocsPerRun(10, func() { sink += len(Balanced(c.space, c.n, c.keys)) })
		if got > 4 {
			t.Errorf("%s allocates %v times, want at most 4", c.name, got)
		}
		if i == 0 {
			first = got
			t.Logf("%s allocates %v times", c.name, got)
		} else if got != first {
			t.Errorf("%s allocates %v times, want %v like %s", c.name, got, first, calls[0].name)
		}
	}
	_ = sink
}
