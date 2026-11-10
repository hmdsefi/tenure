// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"slices"
	"testing"

	"github.com/hmdsefi/tenure/core"
)

// laneRig is a set of lanes whose sends a test records.
type laneRig struct {
	ls   *lanes
	sent []*call
}

// newLaneRig returns lanes that record what they send.
func newLaneRig() *laneRig {
	r := &laneRig{}
	r.ls = newLanes(func(c *call) { r.sent = append(r.sent, c) })
	return r
}

// submit submits a call for act on the lane the driver would use, and reports whether the lanes
// queued it.
func (r *laneRig) submit(act core.Action) bool {
	c := (&driver{}).callFor(act)
	if c == nil {
		panic(fmt.Sprintf("no request for %T", act))
	}
	return r.ls.submit(c)
}

// take returns the actions sent since the last take, in order.
func (r *laneRig) take() []core.Action {
	out := make([]core.Action, len(r.sent))
	for i, c := range r.sent {
		out[i] = c.act
	}
	r.sent = nil
	return out
}

// inFlight returns the call in flight for act, failing the test when there is none.
func (r *laneRig) inFlight(t *testing.T, act core.Action) *call {
	t.Helper()
	for _, l := range r.ls.all {
		if l.cur != nil && l.cur.act == act {
			return l.cur
		}
	}
	t.Fatalf("%T%+v is not in flight", act, act)
	return nil
}

// finish finishes the call in flight for act.
func (r *laneRig) finish(t *testing.T, act core.Action) {
	t.Helper()
	r.ls.finish(r.inFlight(t, act))
}

// wantSent checks the actions sent since the last take.
func (r *laneRig) wantSent(t *testing.T, clause string, want ...core.Action) {
	t.Helper()
	if got := r.take(); !slices.Equal(got, want) {
		t.Errorf("%s: sent %v, want %v", clause, got, want)
	}
}

// TestLanesFIFO checks that the requests of one lane go out one at a time, in the order they
// were queued, while the lanes are independent of each other (SIM-310).
func TestLanesFIFO(t *testing.T) {
	r := newLaneRig()
	acq0 := core.Acquire{Space: "book", Shard: 0}
	rel0 := core.Release{Space: "book", Shard: 0, Term: 5}
	conf0 := core.Confirm{Space: "book", Shard: 0, Term: 6}
	acq1 := core.Acquire{Space: "book", Shard: 1}
	for _, a := range []core.Action{acq0, rel0, conf0, acq1, core.KeepAlive{}, core.ClaimSlot{}} {
		if !r.submit(a) {
			t.Fatalf("%T%+v was dropped", a, a)
		}
	}
	r.wantSent(t, "first of each lane", acq0, acq1, core.KeepAlive{}, core.ClaimSlot{})
	r.finish(t, acq1)
	r.wantSent(t, "shard 1 answered")
	r.finish(t, acq0)
	r.wantSent(t, "shard 0 answered", rel0)
	r.finish(t, rel0)
	r.wantSent(t, "release answered", conf0)
	r.finish(t, conf0)
	r.wantSent(t, "confirm answered")
	if r.ls.inFlight != 2 || r.ls.shardOut != 0 || r.ls.pending() != 0 {
		t.Errorf("in flight %d, shard lanes %d, pending %d; want 2, 0, 0", r.ls.inFlight, r.ls.shardOut, r.ls.pending())
	}
	// A call that is not in flight finishes nothing.
	r.ls.finish(&call{key: shardLane("book", 0), act: acq0})
	if r.ls.inFlight != 2 {
		t.Errorf("finishing a call not in flight: in flight %d, want 2", r.ls.inFlight)
	}
}

// TestLanesPermits checks the 16 permits of the shard lanes: the 17th shard lane waits, the
// session and slot lanes do not, and the waiting lanes get the permits in the order they began
// to wait.
func TestLanesPermits(t *testing.T) {
	r := newLaneRig()
	acq := func(shard int) core.Action { return core.Acquire{Space: "book", Shard: shard} }
	var first []core.Action
	for shard := range 20 {
		r.submit(acq(shard))
		if shard < shardPermits {
			first = append(first, acq(shard))
		}
	}
	r.wantSent(t, "20 shard lanes", first...)
	r.submit(core.KeepAlive{})
	r.submit(core.ReleaseSlot{Slot: 1})
	r.wantSent(t, "session and slot lanes", core.KeepAlive{}, core.ReleaseSlot{Slot: 1})
	// Shard 3 queues a second request: its lane frees a permit, which the lane that waited
	// longest takes before shard 3's next request.
	rel3 := core.Release{Space: "book", Shard: 3, Term: 9}
	r.submit(rel3)
	r.finish(t, acq(3))
	r.wantSent(t, "a permit freed", acq(16))
	r.finish(t, acq(0))
	r.wantSent(t, "a second permit freed", acq(17))
	r.finish(t, acq(1))
	r.finish(t, acq(2))
	r.wantSent(t, "two more", acq(18), acq(19))
	r.finish(t, acq(4))
	r.wantSent(t, "the waiting release", rel3)
	if r.ls.shardOut != shardPermits {
		t.Errorf("shard lanes in flight %d, want %d", r.ls.shardOut, shardPermits)
	}
	if len(r.ls.waiting) != 0 {
		t.Errorf("%d lanes still wait", len(r.ls.waiting))
	}
}

// TestLanesO6Drops checks O6: a Release, ReleaseSlot, ClaimSlot or CloseSession equal to one
// queued or in flight is dropped; a KeepAlive is dropped while one is queued; Acquire and
// Confirm are never dropped; requests that differ are not dropped.
func TestLanesO6Drops(t *testing.T) {
	rel := core.Release{Space: "book", Shard: 2, Term: 7}
	tests := []struct {
		name    string
		first   []core.Action // submitted first; the first of each lane goes out
		then    core.Action
		dropped bool
	}{
		{"release equal to one in flight", []core.Action{rel}, rel, true},
		{"release equal to one queued", []core.Action{core.Acquire{Space: "book", Shard: 2}, rel}, rel, true},
		{"release of another term", []core.Action{rel}, core.Release{Space: "book", Shard: 2, Term: 8}, false},
		{"release of another shard", []core.Action{rel}, core.Release{Space: "book", Shard: 3, Term: 7}, false},
		{"release slot in flight", []core.Action{core.ReleaseSlot{Slot: 1}}, core.ReleaseSlot{Slot: 1}, true},
		{"release slot queued", []core.Action{core.ClaimSlot{}, core.ReleaseSlot{Slot: 1}}, core.ReleaseSlot{Slot: 1}, true},
		{"release of another slot", []core.Action{core.ReleaseSlot{Slot: 1}}, core.ReleaseSlot{Slot: 2}, false},
		{"claim in flight", []core.Action{core.ClaimSlot{}}, core.ClaimSlot{}, true},
		{"claim queued", []core.Action{core.ReleaseSlot{Slot: 0}, core.ClaimSlot{}}, core.ClaimSlot{}, true},
		{"close in flight", []core.Action{core.CloseSession{}}, core.CloseSession{}, true},
		{"keepalive in flight", []core.Action{core.KeepAlive{}}, core.KeepAlive{}, false},
		{"keepalive queued", []core.Action{core.KeepAlive{}, core.KeepAlive{}}, core.KeepAlive{}, true},
		{"acquire in flight", []core.Action{core.Acquire{Space: "book", Shard: 2}}, core.Acquire{Space: "book", Shard: 2}, false},
		{
			"acquire queued",
			[]core.Action{rel, core.Acquire{Space: "book", Shard: 2}},
			core.Acquire{Space: "book", Shard: 2}, false,
		},
		{
			"confirm in flight",
			[]core.Action{core.Confirm{Space: "book", Shard: 2, Term: 7}},
			core.Confirm{Space: "book", Shard: 2, Term: 7}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newLaneRig()
			for _, a := range tt.first {
				if !r.submit(a) {
					t.Fatalf("%T%+v was dropped", a, a)
				}
			}
			before := r.ls.pending()
			if got := !r.submit(tt.then); got != tt.dropped {
				t.Fatalf("dropped %v, want %v", got, tt.dropped)
			}
			if tt.dropped && r.ls.pending() != before {
				t.Errorf("a dropped request was queued")
			}
		})
	}
}

// TestLanesO7Close checks O7: a CloseSession drops every queued request, drops every later
// request but a CloseSession, and goes out once the requests in flight have been answered or
// have timed out; a CloseSession that timed out can go out again.
func TestLanesO7Close(t *testing.T) {
	r := newLaneRig()
	acq0 := core.Acquire{Space: "book", Shard: 0}
	rel1 := core.Release{Space: "book", Shard: 1, Term: 3}
	for _, a := range []core.Action{acq0, core.Confirm{Space: "book", Shard: 0, Term: 2}, rel1, core.KeepAlive{}, core.KeepAlive{}, core.ClaimSlot{}} {
		r.submit(a)
	}
	r.wantSent(t, "before closing", acq0, rel1, core.KeepAlive{}, core.ClaimSlot{})
	if !r.submit(core.CloseSession{}) {
		t.Fatal("the first CloseSession was dropped")
	}
	r.wantSent(t, "closing with four in flight")
	if r.ls.pending() != 1 {
		t.Errorf("pending %d after closing, want only the CloseSession", r.ls.pending())
	}
	for _, a := range []core.Action{core.KeepAlive{}, core.Acquire{Space: "book", Shard: 5}, core.ReleaseSlot{Slot: 0}, core.CloseSession{}} {
		if r.submit(a) {
			t.Errorf("%T%+v was queued while closing", a, a)
		}
	}
	r.finish(t, acq0)
	r.finish(t, rel1)
	r.finish(t, core.ClaimSlot{})
	r.wantSent(t, "one still in flight")
	r.finish(t, core.KeepAlive{})
	r.wantSent(t, "nothing in flight", core.CloseSession{})
	if r.submit(core.CloseSession{}) {
		t.Error("a CloseSession equal to the one in flight was queued")
	}
	r.finish(t, core.CloseSession{}) // its timeout
	if !r.submit(core.CloseSession{}) {
		t.Error("the repeat of a CloseSession that timed out was dropped")
	}
	r.wantSent(t, "the repeat", core.CloseSession{})

	// With nothing in flight, CloseSession goes out at once.
	r = newLaneRig()
	r.submit(core.CloseSession{})
	r.wantSent(t, "closing idle lanes", core.CloseSession{})
}
