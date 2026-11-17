// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// assignerWall is the wall-clock time of the Assigner tests' views at reading 0.
var assignerWall = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// assignerSpaces are the spaces of the Assigner tests.
var assignerSpaces = []AssignerSpace{{Name: "orders", Shards: 8}, {Name: "markets", Shards: 64}}

// newTestAssigner returns an Assigner of assignerSpaces with the default timing.
func newTestAssigner(t *testing.T) *Assigner {
	t.Helper()
	a, err := NewAssigner(assignerSpaces, Timing{})
	if err != nil {
		t.Fatalf("NewAssigner: %v", err)
	}
	return a
}

// slotView returns a view with one member per held slot: holders[s] is the ID of slot s's
// holder, "" for a vacant slot, and the member's holder string is "<ID>/h".
func slotView(version uint64, holders ...string) View {
	v := View{Version: version, Slots: len(holders), SlotHolders: make([]string, len(holders))}
	for s, id := range holders {
		if id == "" {
			continue
		}
		v.SlotHolders[s] = id + "/h"
		v.Members = append(v.Members, Member{ID: id, Addr: id + ":80", Slot: s})
	}
	slices.SortFunc(v.Members, func(x, y Member) int { return strings.Compare(x.ID, y.ID) })
	return v
}

// checkAssigner fails t unless a's keys are keys and every space's assignment is Balanced over
// them.
func checkAssigner(t *testing.T, a *Assigner, keys ...string) {
	t.Helper()
	if len(keys) == 0 {
		keys = nil
	}
	if got := a.Keys(); !slices.Equal(got, keys) {
		t.Fatalf("Keys() = %q, want %q", got, keys)
	}
	for _, s := range assignerSpaces {
		want := make([]string, s.Shards)
		for shard, k := range Balanced(s.Name, s.Shards, keys) {
			if k >= 0 {
				want[shard] = keys[k]
			}
		}
		if got := a.Assignment(s.Name); !slices.Equal(got, want) {
			t.Fatalf("Assignment(%q) = %q, want %q", s.Name, got, want)
		}
	}
}

// checkWake fails t unless a.NextWake() is (at, armed).
func checkWake(t *testing.T, a *Assigner, at time.Duration, armed bool) {
	t.Helper()
	if gotAt, gotArmed := a.NextWake(); gotAt != at || gotArmed != armed {
		t.Fatalf("NextWake() = %v, %v, want %v, %v", gotAt, gotArmed, at, armed)
	}
}

func TestNewAssigner(t *testing.T) {
	bad := Timing{LeaseDuration: time.Second}
	cases := []struct {
		name   string
		spaces []AssignerSpace
		timing Timing
		want   string
		is     error
	}{
		{"empty name", []AssignerSpace{{Name: "", Shards: 1}}, Timing{}, "core: invalid assigner: spaces[0]: empty Name", ErrInvalidAssigner},
		{"no shards", []AssignerSpace{{Name: "a", Shards: 0}}, Timing{}, `core: invalid assigner: space "a": Shards 0 is below 1`, ErrInvalidAssigner},
		{"duplicate", []AssignerSpace{{Name: "a", Shards: 1}, {Name: "a", Shards: 2}}, Timing{}, `core: invalid assigner: duplicate space "a"`, ErrInvalidAssigner},
		{"timing", []AssignerSpace{{Name: "a", Shards: 1}}, bad, bad.Validate().Error(), ErrInvalidTiming},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a, err := NewAssigner(c.spaces, c.timing)
			if a != nil || err == nil || err.Error() != c.want || !errors.Is(err, c.is) {
				t.Fatalf("NewAssigner = %v, %v, want nil and %q wrapping %v", a, err, c.want, c.is)
			}
		})
	}

	spaces := []AssignerSpace{{Name: "z", Shards: 2}, {Name: "a", Shards: 3}}
	a, err := NewAssigner(spaces, Timing{})
	if err != nil {
		t.Fatalf("NewAssigner: %v", err)
	}
	if spaces[0].Name != "z" {
		t.Fatalf("NewAssigner modified spaces: %v", spaces)
	}
	if a.timing != DefaultTiming {
		t.Fatalf("timing = %v, want DefaultTiming", a.timing)
	}
	if got := a.Assignment("a"); !slices.Equal(got, []string{"", "", ""}) {
		t.Fatalf(`Assignment("a") before a view = %q`, got)
	}
	if got := a.Assignment("missing"); got != nil {
		t.Fatalf(`Assignment("missing") = %q, want nil`, got)
	}
	if a.Keys() != nil {
		t.Fatalf("Keys() before a view = %q", a.Keys())
	}
	checkWake(t, a, 0, false)
}

func TestAssignerNoSlots(t *testing.T) {
	a := newTestAssigner(t)
	v := View{Version: 1, Members: []Member{{ID: "n1", Slot: -1}, {ID: "n2", Slot: -1}, {ID: "n3", Slot: -1}}}
	if !a.ApplyView(v, 0, assignerWall) {
		t.Fatal("first view: ApplyView = false")
	}
	checkAssigner(t, a, "n1", "n2", "n3")
	checkWake(t, a, 0, false)
	before := a.Assignment("orders")

	// The same members with another address: nothing changes, and the slice stays the same.
	v.Version = 2
	v.Members = slices.Clone(v.Members)
	v.Members[0].Addr = "elsewhere:80"
	if a.ApplyView(v, time.Second, assignerWall.Add(time.Second)) {
		t.Fatal("same keys: ApplyView = true")
	}
	if got := a.Assignment("orders"); &got[0] != &before[0] {
		t.Fatal("same keys: Assignment returned a new slice")
	}

	// A draining member drops out, and the earlier result does not change.
	saved := slices.Clone(before)
	v.Version = 3
	v.Members = slices.Clone(v.Members)
	v.Members[1].Draining = true
	if !a.ApplyView(v, 2*time.Second, assignerWall.Add(2*time.Second)) {
		t.Fatal("drain: ApplyView = false")
	}
	checkAssigner(t, a, "n1", "n3")
	if !slices.Equal(before, saved) {
		t.Fatal("drain: an earlier result changed")
	}

	// No eligible member: every key is "".
	v.Version = 4
	v.Members = nil
	if !a.ApplyView(v, 3*time.Second, assignerWall.Add(3*time.Second)) {
		t.Fatal("empty: ApplyView = false")
	}
	checkAssigner(t, a)
}

func TestAssignerIgnoredViews(t *testing.T) {
	a := newTestAssigner(t)
	v := View{Version: 5, Members: []Member{{ID: "n1", Slot: -1}, {ID: "n2", Slot: -1}}}
	a.ApplyView(v, 0, assignerWall)

	older := View{Version: 4, Members: []Member{{ID: "n1", Slot: -1}}}
	if a.ApplyView(older, time.Second, assignerWall) {
		t.Fatal("older view: ApplyView = true")
	}
	invalid := View{Version: 6, Members: []Member{{ID: "n2", Slot: -1}, {ID: "n1", Slot: -1}}}
	if a.ApplyView(invalid, time.Second, assignerWall) {
		t.Fatal("invalid view: ApplyView = true")
	}
	checkAssigner(t, a, "n1", "n2")

	same := View{Version: 5, Members: []Member{{ID: "n1", Slot: -1}}}
	if !a.ApplyView(same, 2*time.Second, assignerWall) {
		t.Fatal("same version: ApplyView = false")
	}
	checkAssigner(t, a, "n1")
}

func TestAssignerSlots(t *testing.T) {
	a := newTestAssigner(t)
	if !a.ApplyView(slotView(1, "n1", "n2", "n3"), 0, assignerWall) {
		t.Fatal("ApplyView = false")
	}
	checkAssigner(t, a, "slot-0", "slot-1", "slot-2")
	checkWake(t, a, 0, false)

	// Another member takes slot 1: the keys stay, so nothing changes.
	if a.ApplyView(slotView(2, "n1", "n4", "n3"), time.Second, assignerWall) {
		t.Fatal("new holder: ApplyView = true")
	}

	// A draining holder keeps its slot's key.
	v := slotView(3, "n1", "n4", "n3")
	v.Members[0].Draining = true
	if a.ApplyView(v, 2*time.Second, assignerWall) {
		t.Fatal("draining holder: ApplyView = true")
	}
	checkAssigner(t, a, "slot-0", "slot-1", "slot-2")

	// A fourth slot adds a key.
	if !a.ApplyView(slotView(4, "n1", "n4", "n3", "n5"), 3*time.Second, assignerWall) {
		t.Fatal("more slots: ApplyView = false")
	}
	checkAssigner(t, a, "slot-0", "slot-1", "slot-2", "slot-3")
}

func TestAssignerVacancy(t *testing.T) {
	grace := DefaultTiming.SlotGrace
	a := newTestAssigner(t)
	const r0 = 10 * time.Second
	a.ApplyView(slotView(1, "n1", "n2", "n3"), 0, assignerWall)

	// Slot 1 shows vacant from r0: its streak starts there, and later views keep the start.
	if a.ApplyView(slotView(2, "n1", "", "n3"), r0, assignerWall) {
		t.Fatal("vacant: ApplyView = true")
	}
	checkWake(t, a, r0+grace, true)
	if a.ApplyView(slotView(3, "n1", "", "n3"), r0+time.Second, assignerWall) {
		t.Fatal("still vacant: ApplyView = true")
	}
	checkWake(t, a, r0+grace, true)

	if a.Tick(r0 + grace - time.Nanosecond) {
		t.Fatal("Tick(SlotGrace-1ns) = true")
	}
	checkAssigner(t, a, "slot-0", "slot-1", "slot-2")
	if !a.Tick(r0 + grace) {
		t.Fatal("Tick(SlotGrace) = false")
	}
	checkAssigner(t, a, "slot-0", "slot-2")
	checkWake(t, a, 0, false)
	if a.Tick(r0 + 2*grace) {
		t.Fatal("Tick after expiry = true")
	}

	// A view that shows the slot held again brings its key back.
	r1 := r0 + 2*grace
	if !a.ApplyView(slotView(4, "n1", "n4", "n3"), r1, assignerWall) {
		t.Fatal("held again: ApplyView = false")
	}
	checkAssigner(t, a, "slot-0", "slot-1", "slot-2")
	checkWake(t, a, 0, false)

	// A new vacancy starts a new streak, and two streaks wake at the earlier expiry.
	r2 := r1 + time.Second
	a.ApplyView(slotView(5, "n1", "", "n3"), r2, assignerWall)
	a.ApplyView(slotView(6, "n1", "", ""), r2+5*time.Second, assignerWall)
	checkWake(t, a, r2+grace, true)
	if !a.Tick(r2 + grace) {
		t.Fatal("first expiry: Tick = false")
	}
	checkAssigner(t, a, "slot-0", "slot-2")
	checkWake(t, a, r2+5*time.Second+grace, true)
	if !a.Tick(r2 + 5*time.Second + grace) {
		t.Fatal("second expiry: Tick = false")
	}
	checkAssigner(t, a, "slot-0")

	// Dropping an expired slot from the view ends its streak, and slot 1's expired streak goes on:
	// the keys stay. A view that shows slot 1 held brings it back.
	r3 := r2 + time.Minute + grace
	if a.ApplyView(slotView(7, "n1", ""), r3, assignerWall) {
		t.Fatal("fewer slots: ApplyView = true")
	}
	checkAssigner(t, a, "slot-0")
	checkWake(t, a, 0, false)
	if !a.ApplyView(slotView(8, "n1", "n2"), r3+time.Second, assignerWall) {
		t.Fatal("held: ApplyView = false")
	}
	checkAssigner(t, a, "slot-0", "slot-1")
}

func TestAssignerVacancyExpiresOnView(t *testing.T) {
	// A view applied after the expiry reading expires the streak in the same call.
	grace := DefaultTiming.SlotGrace
	a := newTestAssigner(t)
	a.ApplyView(slotView(1, "n1", ""), 0, assignerWall)
	checkAssigner(t, a, "slot-0", "slot-1")
	if !a.ApplyView(slotView(2, "n1", ""), grace, assignerWall) {
		t.Fatal("ApplyView at the expiry = false")
	}
	checkAssigner(t, a, "slot-0")
}

func TestAssignerBounds(t *testing.T) {
	a := newTestAssigner(t)
	v := View{Version: 1, Members: []Member{
		{ID: "n1", Slot: -1, LeaveAt: assignerWall.Add(10 * time.Second)},
		{ID: "n2", Slot: -1},
		{ID: "n3", Slot: -1, JoinAt: assignerWall.Add(5 * time.Second)},
	}}
	a.ApplyView(v, 0, assignerWall)
	checkAssigner(t, a, "n1", "n2")
	checkWake(t, a, 0, false) // a bound is no wake: the machine recomputes only on its triggers

	// The JoinAt bound passes between two ticks: the assignment keeps the view's until the next
	// view, as a member's does.
	if a.Tick(4 * time.Second) {
		t.Fatal("Tick before JoinAt = true")
	}
	if a.Tick(6 * time.Second) {
		t.Fatal("Tick after JoinAt = true")
	}
	checkAssigner(t, a, "n1", "n2")

	// The next view is judged at its own wall-clock time: n3 has joined.
	v.Version = 2
	if !a.ApplyView(v, 6*time.Second, assignerWall.Add(6*time.Second)) {
		t.Fatal("view after JoinAt: ApplyView = false")
	}
	checkAssigner(t, a, "n1", "n2", "n3")

	// The LeaveAt bound passes between two ticks, and the next view drops n1.
	if a.Tick(9*time.Second) || a.Tick(11*time.Second) {
		t.Fatal("Tick across LeaveAt = true")
	}
	checkAssigner(t, a, "n1", "n2", "n3")
	v.Version = 3
	if !a.ApplyView(v, 11*time.Second, assignerWall.Add(11*time.Second)) {
		t.Fatal("view after LeaveAt: ApplyView = false")
	}
	checkAssigner(t, a, "n2", "n3")
}

func TestAssignerBackwardsPanics(t *testing.T) {
	a := newTestAssigner(t)
	a.Tick(time.Second)
	for name, f := range map[string]func(){
		"Tick":      func() { a.Tick(time.Second - 1) },
		"ApplyView": func() { a.ApplyView(View{}, time.Second-1, assignerWall) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s with an earlier reading did not panic", name)
				}
			}()
			f()
		}()
	}
}
