// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// TestRecoveredMessages checks the messages of tenure/recovered (SIM-680): an end recovered
// from 0 or 2 times, and a recovery whose session never renewed, reported for the first member
// in node order.
func TestRecoveredMessages(t *testing.T) {
	w := &World{members: []*Member{{Name: "m1"}, {Name: "m2"}}}
	at := kernel.Time(41207 * time.Millisecond)
	cases := []struct {
		name string
		ends [][]endOwed
		want string
	}{
		{"none", [][]endOwed{nil, nil}, ""},
		{"once", [][]endOwed{{{holder: "m1/a", at: at, recovers: 1, newHolder: "m1/b", renewed: true}}, nil}, ""},
		{
			"zero", [][]endOwed{nil, {{holder: "m2/a", at: at}}},
			"recovered: m2: session m2/a ended at 41.207000000s, recovered 0 times",
		},
		{
			"twice", [][]endOwed{{{holder: "m1/a", at: at, recovers: 2, newHolder: "m1/b", renewed: true}}, {{holder: "m2/a", at: at}}},
			"recovered: m1: session m1/a ended at 41.207000000s, recovered 2 times",
		},
		{
			"unrenewed", [][]endOwed{{{holder: "m1/a", at: at, recovers: 1, newHolder: "m1/b"}}, nil},
			"recovered: m1: session m1/a ended at 41.207000000s, recovered as m1/b, which never renewed",
		},
	}
	for _, c := range cases {
		sw := &sessionWatch{w: w, ends: c.ends}
		err := sw.err()
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != c.want {
			t.Errorf("%s: err = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestSessionEndTime checks the recovery time of a scenario with EndAfter: the latest end of a
// session opened within two delays of the start, on a store clock that may run slow by ρ.
func TestSessionEndTime(t *testing.T) {
	sc := Scenario{EndAfter: 40 * time.Second, Delta: 5 * time.Millisecond}
	if got, want := sessionEndTime(sc), kernel.Time(40010*time.Millisecond); got != want {
		t.Errorf("sessionEndTime = %v, want %v", got, want)
	}
	sc.Rho = 100_000 // 40.01 s / 0.9, rounded up
	if got, want := sessionEndTime(sc), kernel.Time(44_455_555_556); got != want {
		t.Errorf("sessionEndTime with Rho = %d, want %d", int64(got), int64(want))
	}
	st, err := newSetup(Scenario{EndAfter: 40 * time.Second}, kernel.Time(75*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if st.recovery != kernel.Time(40010*time.Millisecond) {
		t.Errorf("R = %v, want 40.010000000s", st.recovery)
	}
	if err := SetupError(Scenario{EndAfter: 60 * time.Second}, kernel.Time(75*time.Second)); err == nil {
		t.Error("EndAfter 60s in a 75 s run: no setup error, want the bound after the end")
	}
}

// TestLanesAbandon checks that abandoned lanes drop the queued requests and those in flight,
// and admit nothing from then on (O7).
func TestLanesAbandon(t *testing.T) {
	var sent []*call
	ls := newLanes(func(c *call) { sent = append(sent, c) })
	a := &call{key: shardLane("book", 1), act: core.Acquire{Space: "book", Shard: 1}}
	b := &call{key: shardLane("book", 1), act: core.Release{Space: "book", Shard: 1, Term: 3}}
	ls.submit(a)
	ls.submit(b)
	if len(sent) != 1 || ls.pending() != 1 {
		t.Fatalf("sent %d, pending %d, want 1 and 1", len(sent), ls.pending())
	}
	ls.abandon()
	if ls.pending() != 0 || ls.inFlight != 0 || ls.shardOut != 0 {
		t.Errorf("after abandon: pending %d, in flight %d, shard out %d, want 0", ls.pending(), ls.inFlight, ls.shardOut)
	}
	ls.finish(a) // a late answer frees nothing
	if ls.submit(&call{key: laneKey{kind: laneSession}, act: core.KeepAlive{}}) {
		t.Error("abandoned lanes queued a KeepAlive")
	}
	if len(sent) != 1 {
		t.Errorf("sent %d requests, want 1", len(sent))
	}
}
