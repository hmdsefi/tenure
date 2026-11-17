// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// routerSpaces is the one space of the router table tests.
var routerSpaces = []core.SpaceSpec{{Name: "book", Shards: 8}}

// testView returns a view of version with the members ids and no slots.
func testView(version uint64, ids ...string) core.View {
	v := core.View{Version: version}
	for _, id := range ids {
		v.Members = append(v.Members, core.Member{ID: id, Slot: -1})
	}
	return v
}

// newTestTable returns a router table of routerSpaces under DefaultTiming with v applied at 0.
func newTestTable(t *testing.T, v core.View) *routerTable {
	t.Helper()
	tb := newRouterTable(routerSpaces, core.DefaultTiming)
	if !tb.apply(v, 0) {
		t.Fatalf("apply(%v) = false", v)
	}
	return tb
}

// TestRouterTableLookup checks that a router's lookup routes each shard to the member its
// Assigner places the shard on, and gives RTR-230's details when there is none.
func TestRouterTableLookup(t *testing.T) {
	tb := newRouterTable(routerSpaces, core.DefaultTiming)
	if got, why := tb.lookup("book", 0, 0); got != "" || why != "no view yet" {
		t.Errorf("lookup before a view = %q, %q; want no view yet", got, why)
	}
	tb = newTestTable(t, testView(1, "m1", "m2", "m3"))
	want := core.Balanced("book", 8, []string{"m1", "m2", "m3"})
	for shard := range 8 {
		if got, _ := tb.lookup("book", shard, 0); got != []string{"m1", "m2", "m3"}[want[shard]] {
			t.Errorf("lookup(book/%d) = %q, want m%d", shard, got, want[shard]+1)
		}
	}
	tb.apply(testView(2), time.Second)
	if got, why := tb.lookup("book", 0, time.Second); got != "" || why != "no placement keys" {
		t.Errorf("lookup with no members = %q, %q; want no placement keys", got, why)
	}
	if tb.apply(testView(1, "m1"), 2*time.Second) {
		t.Error("apply took a view older than the applied one")
	}
	if tb.version != 2 {
		t.Errorf("version = %d after two applied views, want 2", tb.version)
	}
}

// TestRouterRouteOf checks how a slot key maps to a member: the member of the slot's holder, the
// slot the router holds to itself, and the details of a vacant slot and of a holder whose member
// is not in the view.
func TestRouterRouteOf(t *testing.T) {
	v := core.View{Version: 3, Slots: 3, SlotHolders: []string{"m1/aa", "", "m4/bb"},
		Members: []core.Member{{ID: "m1", Slot: 0}, {ID: "m2", Slot: -1}}}
	assign := []string{"slot-0", "slot-1", "slot-2", "", "m2"}
	cases := []struct {
		shard, held int
		want, why   string
	}{
		{0, -1, "m1", ""},
		{1, -1, "", "slot-1 is vacant"},
		{1, 1, "m2", ""},
		{2, -1, "", "m4 is not in the view"},
		{3, -1, "", "no placement keys"},
		{4, -1, "m2", ""},
		{9, -1, "", "no placement keys"},
	}
	for _, c := range cases {
		if got, why := routeOf(v, assign, c.shard, c.held, "m2"); got != c.want || why != c.why {
			t.Errorf("routeOf(shard %d, held %d) = %q, %q; want %q, %q", c.shard, c.held, got, why, c.want, c.why)
		}
	}
}

// TestRouterTableCorrect checks AT-RTR-09's rules as the model keeps them: a correction wins
// until the next snapshot or until it is LeaseDuration old, and a member that is not in the view
// is refused.
func TestRouterTableCorrect(t *testing.T) {
	tb := newTestTable(t, testView(1, "m1", "m2"))
	table, _ := tb.lookup("book", 3, 0)
	other := "m1"
	if table == "m1" {
		other = "m2"
	}
	if tb.correct("book", 3, "m9", 0) {
		t.Error("correct kept m9, which is not in the view")
	}
	if !tb.correct("book", 3, other, time.Second) {
		t.Fatalf("correct refused %s", other)
	}
	ld := core.DefaultTiming.LeaseDuration
	if got, _ := tb.lookup("book", 3, time.Second+ld-1); got != other {
		t.Errorf("lookup just before the TTL = %q, want the correction %q", got, other)
	}
	if got, _ := tb.lookup("book", 3, time.Second+ld); got != table {
		t.Errorf("lookup at the TTL = %q, want the table's %q", got, table)
	}
	tb.correct("book", 3, other, 2*ld)
	if got, _ := tb.lookup("book", 4, 2*ld); got == "" {
		t.Error("lookup of another shard found nothing")
	}
	tb.apply(testView(2, "m1", "m2"), 2*ld)
	if got, _ := tb.lookup("book", 3, 2*ld); got != table {
		t.Errorf("lookup after a new view = %q, want the table's %q", got, table)
	}
}

// TestRouterTableTick checks that a slot vacancy expires at SlotGrace after the view that
// showed it vacant, which is a new snapshot, and that a tick that changes nothing is none.
func TestRouterTableTick(t *testing.T) {
	v := core.View{Version: 1, Slots: 2, SlotHolders: []string{"m1/aa", ""},
		Members: []core.Member{{ID: "m1", Slot: 0}, {ID: "m2", Slot: -1}}}
	tb := newTestTable(t, v)
	grace := core.DefaultTiming.SlotGrace
	if at, ok := tb.assigner.NextWake(); !ok || at != grace {
		t.Fatalf("NextWake = %v, %v; want %v", at, ok, grace)
	}
	if tb.tick(grace - 1) {
		t.Error("tick before SlotGrace changed the table")
	}
	if !tb.tick(grace) || tb.version != 2 {
		t.Errorf("tick at SlotGrace: version %d, want a new snapshot 2", tb.version)
	}
	for shard := range 8 {
		if got, _ := tb.lookup("book", shard, grace); got != "m1" {
			t.Errorf("lookup(book/%d) after the vacancy expired = %q, want m1", shard, got)
		}
	}
}

// fakeMembers answers requests from a table of answers by member, and counts the fallback's
// picks from a list.
type fakeMembers struct {
	answers  map[string]answer
	fallback []string
}

func (f *fakeMembers) ask(id string) answer {
	if a, ok := f.answers[id]; ok {
		return a
	}
	return answer{kind: answerNone}
}

func (f *fakeMembers) next() (string, bool) {
	if len(f.fallback) == 0 {
		return "", false
	}
	id := f.fallback[0]
	f.fallback = f.fallback[1:]
	return id, true
}

// TestRouterSendRequest checks the sends of a request (RTR-810, RTR-430): a 421 corrects the
// shard and resends to it, a correction refused follows the 421 for that request only, no
// answer and a 503 go to the fallback, and a request stops after three sends.
func TestRouterSendRequest(t *testing.T) {
	served := answer{kind: answerServed, holder: "x/1", term: 1}
	to := func(id string) answer { return answer{kind: answerMisdirected, route: id} }
	cases := []struct {
		name     string
		members  []string // the router's view
		answers  map[string]answer
		fallback []string
		want     string
		kept     int // corrections kept
	}{
		{name: "owner", members: []string{"m1"}, answers: map[string]answer{"m1": served}, want: "m1 served"},
		{
			name: "one correction", members: []string{"m1", "m2"},
			answers: map[string]answer{"m1": to("m2"), "m2": served}, want: "m1 421 m2, m2 served", kept: 1,
		},
		{
			name: "refused correction", members: []string{"m1"},
			answers: map[string]answer{"m1": to("m3"), "m3": served}, want: "m1 421 m3, m3 served",
		},
		{
			name: "no answer", members: []string{"m1", "m2"},
			answers: map[string]answer{"m2": served, "m3": to("m2")}, fallback: []string{"m3"},
			want: "m1 none, fallback m3 421 m2, m2 served", kept: 1,
		},
		{
			name: "503", members: []string{"m1", "m2"},
			answers: map[string]answer{"m1": {kind: answerUnavailable}, "m2": served}, fallback: []string{"m2"},
			want: "m1 503, fallback m2 served",
		},
		{
			name: "three sends", members: []string{"m1", "m2"},
			answers: map[string]answer{"m1": to("m2"), "m2": to("m1")}, want: "m1 421 m2, m2 421 m1, m1 421 m2", kept: 3,
		},
		{name: "no fallback", members: []string{"m1"}, want: "m1 none"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tb := newTestTable(t, testView(1, c.members...))
			// Route every shard to m1 first, as a table that lags the members' does.
			tb.fixes[spaceShard{"book", 0}] = correction{member: "m1", version: tb.version}
			f := &fakeMembers{answers: c.answers, fallback: c.fallback}
			kept := 0
			hops := sendRequest(tb, "book", 0, 0, f.ask, f.next, func(ok bool) {
				if ok {
					kept++
				}
			})
			if got := path(hops); got != c.want {
				t.Errorf("path = %q, want %q", got, c.want)
			}
			if kept != c.kept {
				t.Errorf("%d corrections kept, want %d", kept, c.kept)
			}
		})
	}
}

// TestRouterNamedHolders checks when a request falls under the bound of three sends: every
// member that answered named a member that serves the shard.
func TestRouterNamedHolders(t *testing.T) {
	holders := []string{"m2"}
	cases := []struct {
		hops []hop
		want bool
	}{
		{[]hop{{to: "m1", ans: answer{kind: answerMisdirected, route: "m2"}}}, true},
		{[]hop{{to: "m1", ans: answer{kind: answerNone}}, {to: "m3", ans: answer{kind: answerMisdirected, route: "m2"}}}, true},
		{[]hop{{to: "m1", ans: answer{kind: answerMisdirected, route: "m3"}}}, false},
		{[]hop{{to: "m1", ans: answer{kind: answerUnavailable}}}, false},
		{[]hop{{to: "m2", ans: answer{kind: answerServed}}}, true},
	}
	for i, c := range cases {
		if got := namedHolders(c.hops, holders); got != c.want {
			t.Errorf("case %d (%s): namedHolders = %v, want %v", i, path(c.hops), got, c.want)
		}
	}
	if !slices.Equal(holders, []string{"m2"}) {
		t.Error("namedHolders changed holders")
	}
}
