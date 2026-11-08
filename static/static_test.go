// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package static_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/static"
)

var (
	_ tenure.Membership           = (*static.Membership)(nil)
	_ tenure.PropagationEstimator = (*static.Membership)(nil)
)

// member returns a valid member with id and no slot.
func member(id string) core.Member {
	return core.Member{ID: id, Addr: id + ":8080", Slot: -1}
}

// newGroup returns a handle on a group that lists members, failing t on error.
func newGroup(t *testing.T, members ...core.Member) *static.Membership {
	t.Helper()
	m, err := static.New(members)
	if err != nil {
		t.Fatalf("New(%v) = %v, want nil", members, err)
	}
	return m
}

// watch calls m.Watch with a context canceled at the end of the test.
func watch(t *testing.T, m *static.Membership) <-chan core.View {
	t.Helper()
	ch, err := m.Watch(t.Context())
	if err != nil {
		t.Fatalf("Watch() = %v, want nil", err)
	}
	return ch
}

// next returns the view buffered in ch, failing t when there is none or ch is closed. static
// delivers inside the call that makes a change, so the view is there without waiting.
func next(t *testing.T, ch <-chan core.View) core.View {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("receive = closed channel, want a view")
		}
		return v
	default:
		t.Fatal("receive = no view, want a view")
		return core.View{}
	}
}

// none fails t when ch holds a view or is closed.
func none(t *testing.T, ch <-chan core.View) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("receive = closed channel, want no view")
		}
		t.Fatalf("receive = view %d %v, want no view", v.Version, ids(v))
	default:
	}
}

// isClosed fails t when ch is open or holds a view.
func isClosed(t *testing.T, ch <-chan core.View) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if ok {
			t.Fatalf("receive = view %d %v, want closed channel", v.Version, ids(v))
		}
	default:
		t.Fatal("receive = no view, want closed channel")
	}
}

// waitClosed waits up to 5 s for ch to be closed, failing t when a view arrives instead.
func waitClosed(t *testing.T, ch <-chan core.View) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if ok {
			t.Fatalf("receive = view %d %v, want closed channel", v.Version, ids(v))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel still open after 5s, want closed")
	}
}

// ids returns the member IDs of v in order.
func ids(v core.View) []string {
	out := make([]string, len(v.Members))
	for i, m := range v.Members {
		out[i] = m.ID
	}
	return out
}

// find returns the member of v with id.
func find(t *testing.T, v core.View, id string) core.Member {
	t.Helper()
	for _, m := range v.Members {
		if m.ID == id {
			return m
		}
	}
	t.Fatalf("view %d %v: no member %q", v.Version, ids(v), id)
	return core.Member{}
}

// canceled returns a context that is already canceled.
func canceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// TestATMEM01Canceled checks that each method returns ctx.Err() itself for a done context and
// changes nothing: no view, no join, no close.
func TestATMEM01Canceled(t *testing.T) {
	for _, ctx := range []context.Context{
		canceled(),
		func() context.Context {
			ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
			t.Cleanup(cancel)
			return ctx
		}(),
	} {
		m := newGroup(t, member("a"))
		ch := watch(t, m)
		next(t, ch)

		if got := m.Join(ctx, member("b")); got != ctx.Err() {
			t.Errorf("Join(done, b) = %v, want %v", got, ctx.Err())
		}
		if _, got := m.Watch(ctx); got != ctx.Err() {
			t.Errorf("Watch(done) = %v, want %v", got, ctx.Err())
		}
		if got := m.SetDraining(ctx, true); got != ctx.Err() {
			t.Errorf("SetDraining(done, true) = %v, want %v", got, ctx.Err())
		}
		if got := m.Leave(ctx); got != ctx.Err() {
			t.Errorf("Leave(done) = %v, want %v", got, ctx.Err())
		}
		none(t, ch)

		// The handle is neither joined nor closed: it can still join, and SetDraining works.
		if err := m.Join(t.Context(), member("b")); err != nil {
			t.Fatalf("Join(b) = %v, want nil", err)
		}
		if v := next(t, ch); !reflect.DeepEqual(ids(v), []string{"a", "b"}) {
			t.Errorf("view %d: %v, want [a b]", v.Version, ids(v))
		}
		if err := m.Leave(t.Context()); err != nil {
			t.Fatalf("Leave() = %v, want nil", err)
		}
		// A closed handle still answers a done context with ctx.Err().
		if got := m.Join(ctx, member("c")); got != ctx.Err() {
			t.Errorf("Join(done, c) after Leave = %v, want %v", got, ctx.Err())
		}
	}
}

// TestATMEM03InvalidJoin checks that Join refuses an invalid member with an error that wraps
// core.ErrInvalidMember, makes no view and leaves the handle free to join.
func TestATMEM03InvalidJoin(t *testing.T) {
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name string
		self core.Member
		want string
	}{
		{"EmptyID", core.Member{Slot: -1}, "static: core: invalid member: empty ID"},
		{"Slash", core.Member{ID: "a/b", Slot: -1}, `static: core: invalid member: ID "a/b" contains '/'`},
		{"ZeroByte", core.Member{ID: "a\x00", Slot: -1}, `static: core: invalid member: ID "a\x00" contains a zero byte`},
		{"Slot", core.Member{ID: "a", Slot: -2}, `static: core: invalid member "a": Slot -2 is below -1`},
		{"Window", core.Member{ID: "a", Slot: -1, JoinAt: t0, LeaveAt: t0}, `static: core: invalid member "a": LeaveAt is not after JoinAt`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newGroup(t, member("x"))
			ch := watch(t, m)
			next(t, ch)
			err := m.Join(t.Context(), tt.self)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("Join(%q) = %v, want %s", tt.self.ID, err, tt.want)
			}
			if !errors.Is(err, core.ErrInvalidMember) {
				t.Errorf("errors.Is(Join(%q), core.ErrInvalidMember) = false, want true", tt.self.ID)
			}
			none(t, ch)
			if err := m.Join(t.Context(), member("a")); err != nil {
				t.Fatalf("Join(a) after the refusal = %v, want nil", err)
			}
			if v := next(t, ch); v.Version != 2 || !reflect.DeepEqual(ids(v), []string{"a", "x"}) {
				t.Errorf("view %d: %v, want version 2 with [a x]", v.Version, ids(v))
			}
		})
	}
}

// TestATMEM06DrainingListed checks that a draining member stays in the views with Draining set,
// and that core.Eligible leaves it out.
func TestATMEM06DrainingListed(t *testing.T) {
	m := newGroup(t, member("a"), member("c"))
	ch := watch(t, m)
	next(t, ch)
	if err := m.Join(t.Context(), member("b")); err != nil {
		t.Fatalf("Join(b) = %v, want nil", err)
	}
	next(t, ch)
	if err := m.SetDraining(t.Context(), true); err != nil {
		t.Fatalf("SetDraining(true) = %v, want nil", err)
	}
	v := next(t, ch)
	if !reflect.DeepEqual(ids(v), []string{"a", "b", "c"}) {
		t.Fatalf("view %d: %v, want [a b c]", v.Version, ids(v))
	}
	if !find(t, v, "b").Draining {
		t.Errorf("view %d: b.Draining = false, want true", v.Version)
	}
	if got := ids(core.View{Members: core.Eligible(v, time.Now())}); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Errorf("Eligible(view %d) = %v, want [a c]", v.Version, got)
	}
	if err := m.SetDraining(t.Context(), false); err != nil {
		t.Fatalf("SetDraining(false) = %v, want nil", err)
	}
	if v := next(t, ch); find(t, v, "b").Draining {
		t.Errorf("view %d: b.Draining = true, want false", v.Version)
	}
}

// TestATMEM07Versions checks that the first view has Version 1, and that the version grows by
// one with each change of member data and only then: Slot alone and Meta of the same content
// make no view.
func TestATMEM07Versions(t *testing.T) {
	listed := member("a")
	listed.Slot = 3
	listed.Meta = map[string]string{"zone": "z1"}
	m := newGroup(t, listed, member("c"))
	ch := watch(t, m)
	v := next(t, ch)
	if v.Version != 1 || v.Slots != 0 || v.SlotHolders != nil {
		t.Fatalf("first view = version %d, Slots %d, SlotHolders %v, want 1, 0, nil", v.Version, v.Slots, v.SlotHolders)
	}

	// Join replaces the listed a with the same data but another Slot and an equal Meta map.
	same := member("a")
	same.Meta = map[string]string{"zone": "z1"}
	if err := m.Join(t.Context(), same); err != nil {
		t.Fatalf("Join(a) = %v, want nil", err)
	}
	none(t, ch)

	steps := []struct {
		name string
		do   func(*static.Membership) error
	}{
		{"SetDraining(true)", func(m *static.Membership) error { return m.SetDraining(t.Context(), true) }},
		{"SetDraining(true) again", func(m *static.Membership) error { return m.SetDraining(t.Context(), true) }},
		{"SetDraining(false)", func(m *static.Membership) error { return m.SetDraining(t.Context(), false) }},
		{"Leave()", func(m *static.Membership) error { return m.Leave(t.Context()) }},
	}
	wantViews := []bool{true, false, true, true}
	version := uint64(1)
	for i, s := range steps {
		if err := s.do(m); err != nil {
			t.Fatalf("%s = %v, want nil", s.name, err)
		}
		if !wantViews[i] {
			none(t, ch)
			continue
		}
		version++
		if v := next(t, ch); v.Version != version {
			t.Errorf("after %s: view %d, want version %d", s.name, v.Version, version)
		}
	}
}

// TestATMEM08Coalesce checks that a channel holds one view and that a newer view replaces one
// not yet received.
func TestATMEM08Coalesce(t *testing.T) {
	m := newGroup(t, member("a"))
	ch := watch(t, m)
	for _, id := range []string{"b", "c", "d"} {
		if err := m.Share().Join(t.Context(), member(id)); err != nil {
			t.Fatalf("Join(%s) = %v, want nil", id, err)
		}
	}
	if got := len(ch); got != 1 {
		t.Fatalf("len(channel) = %d, want 1", got)
	}
	v := next(t, ch)
	if v.Version != 4 || !reflect.DeepEqual(ids(v), []string{"a", "b", "c", "d"}) {
		t.Errorf("view %d: %v, want version 4 with [a b c d]", v.Version, ids(v))
	}
	none(t, ch)
}

// TestATMEM09ClosedChannel checks that a watch channel is closed when its context ends, that
// other channels stay open, and that Watch still works on the handle.
func TestATMEM09ClosedChannel(t *testing.T) {
	m := newGroup(t, member("a"))
	ctx, cancel := context.WithCancel(t.Context())
	ch := watch(t, m)
	gone, err := m.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch() = %v, want nil", err)
	}
	next(t, ch)
	next(t, gone)
	cancel()
	waitClosed(t, gone)

	if err := m.Join(t.Context(), member("b")); err != nil {
		t.Fatalf("Join(b) = %v, want nil", err)
	}
	if v := next(t, ch); v.Version != 2 {
		t.Errorf("view %d, want version 2", v.Version)
	}
	isClosed(t, gone)
	again := watch(t, m)
	if v := next(t, again); v.Version != 2 {
		t.Errorf("view %d on a new channel, want version 2", v.Version)
	}
}

// TestATMEM10Multiple checks a group with several handles and several channels per handle:
// every channel gets the same views, an unjoined handle observes the group, and an ID can join
// on one handle at a time.
func TestATMEM10Multiple(t *testing.T) {
	a := newGroup(t, member("l"))
	b, x := a.Share(), a.Share()
	chans := []<-chan core.View{watch(t, a), watch(t, a), watch(t, b), watch(t, x)}
	for _, ch := range chans {
		next(t, ch)
	}
	if err := a.Join(t.Context(), member("a")); err != nil {
		t.Fatalf("a.Join(a) = %v, want nil", err)
	}
	if err := b.Join(t.Context(), member("b")); err != nil {
		t.Fatalf("b.Join(b) = %v, want nil", err)
	}
	if err, want := x.Join(t.Context(), member("a")), `static: member "a" already joined`; err == nil || err.Error() != want {
		t.Errorf("x.Join(a) = %v, want %s", err, want)
	}
	var first core.View
	for i, ch := range chans {
		v := next(t, ch)
		if v.Version != 3 || !reflect.DeepEqual(ids(v), []string{"a", "b", "l"}) {
			t.Errorf("channel %d: view %d %v, want version 3 with [a b l]", i, v.Version, ids(v))
		}
		if i == 0 {
			first = v
		} else if !reflect.DeepEqual(v, first) {
			t.Errorf("channel %d: view %+v, want %+v", i, v, first)
		}
	}
	if err := a.Leave(t.Context()); err != nil {
		t.Fatalf("a.Leave() = %v, want nil", err)
	}
	if err := x.Join(t.Context(), member("a")); err != nil {
		t.Errorf("x.Join(a) after a left = %v, want nil", err)
	}
	for i, ch := range chans[:2] {
		if v := next(t, ch); v.Version != 4 || !reflect.DeepEqual(ids(v), []string{"b", "l"}) {
			t.Errorf("channel %d: view %d %v, want version 4 with [b l]", i, v.Version, ids(v))
		}
		isClosed(t, ch)
	}
	// View 4, without a, was replaced on the open channels by view 5, with a joined on x.
	for i, ch := range chans[2:] {
		if v := next(t, ch); v.Version != 5 || !reflect.DeepEqual(ids(v), []string{"a", "b", "l"}) {
			t.Errorf("channel %d: view %d %v, want version 5 with [a b l]", i+2, v.Version, ids(v))
		}
	}
}

// TestATMEM12Propagation checks that static reports a propagation of 0.
func TestATMEM12Propagation(t *testing.T) {
	var m tenure.Membership = newGroup(t)
	p, ok := m.(tenure.PropagationEstimator)
	if !ok {
		t.Fatal("*static.Membership does not implement tenure.PropagationEstimator")
	}
	if got := p.Propagation(); got != 0 {
		t.Errorf("Propagation() = %v, want 0", got)
	}
}

// TestATMEM13New checks New's validation order and messages, its copy of the members and the
// first view.
func TestATMEM13New(t *testing.T) {
	tests := []struct {
		name    string
		members []core.Member
		want    string
	}{
		{"Slot", []core.Member{{ID: "a", Slot: -2}}, `static: member 0: core: invalid member "a": Slot -2 is below -1`},
		{"EmptyID", []core.Member{member("a"), {Slot: -1}}, "static: member 1: core: invalid member: empty ID"},
		{"Duplicate", []core.Member{member("a"), member("b"), member("a")}, `static: member 2: duplicate ID "a"`},
		// Member 1 is a duplicate and member 2 is invalid: the first failure in order wins.
		{"FirstInOrder", []core.Member{member("a"), member("a"), {ID: "c", Slot: -5}}, `static: member 1: duplicate ID "a"`},
		{"InvalidBeforeDuplicate", []core.Member{member("a"), {ID: "a", Slot: -3}}, `static: member 1: core: invalid member "a": Slot -3 is below -1`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := static.New(tt.members)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("New(%v) = %v, want %s", tt.members, err, tt.want)
			}
			if m != nil {
				t.Errorf("New(%v) handle = %p, want nil", tt.members, m)
			}
		})
	}
	if _, err := static.New([]core.Member{{ID: "a", Slot: -2}}); !errors.Is(err, core.ErrInvalidMember) {
		t.Errorf("errors.Is(New(Slot -2), core.ErrInvalidMember) = false, want true")
	}

	t.Run("Copy", func(t *testing.T) {
		in := []core.Member{member("c"), member("a"), member("b")}
		in[1].Meta = map[string]string{"zone": "z1"}
		m := newGroup(t, in...)
		in[0].ID = "z"
		in[1].Meta["zone"] = "changed"
		v := next(t, watch(t, m))
		if v.Version != 1 || !reflect.DeepEqual(ids(v), []string{"a", "b", "c"}) {
			t.Fatalf("view %d: %v, want version 1 with [a b c]", v.Version, ids(v))
		}
		if got := find(t, v, "a").Meta["zone"]; got != "z1" {
			t.Errorf("view 1: a.Meta[zone] = %q, want z1", got)
		}
		if v.Slots != 0 || v.SlotHolders != nil {
			t.Errorf("view 1: Slots %d, SlotHolders %v, want 0, nil", v.Slots, v.SlotHolders)
		}
		if err := v.Validate(); err != nil {
			t.Errorf("view 1: Validate() = %v, want nil", err)
		}
	})

	t.Run("Empty", func(t *testing.T) {
		v := next(t, watch(t, newGroup(t)))
		if v.Version != 1 || len(v.Members) != 0 {
			t.Errorf("view %d: %v, want version 1 with no members", v.Version, ids(v))
		}
	})
}

// TestATMEM14Share checks that a shared handle has its own joined state and channels and the
// same members and views.
func TestATMEM14Share(t *testing.T) {
	a := newGroup(t, member("l"))
	b := a.Share()
	cha, chb := watch(t, a), watch(t, b)
	if va, vb := next(t, cha), next(t, chb); !reflect.DeepEqual(va, vb) {
		t.Fatalf("first views differ: %+v and %+v", va, vb)
	}
	if err := a.Join(t.Context(), member("a")); err != nil {
		t.Fatalf("a.Join(a) = %v, want nil", err)
	}
	// b has not joined: its own state is separate from a's.
	if err, want := b.SetDraining(t.Context(), true), "static: not joined"; err == nil || err.Error() != want {
		t.Errorf("b.SetDraining(true) = %v, want %s", err, want)
	}
	if err := b.Join(t.Context(), member("b")); err != nil {
		t.Fatalf("b.Join(b) = %v, want nil", err)
	}
	if va, vb := next(t, cha), next(t, chb); !reflect.DeepEqual(va, vb) || va.Version != 3 {
		t.Fatalf("views %+v and %+v, want the same version 3", va, vb)
	}
	// Leave on b closes b's channels only.
	if err := b.Leave(t.Context()); err != nil {
		t.Fatalf("b.Leave() = %v, want nil", err)
	}
	if v := next(t, chb); !reflect.DeepEqual(ids(v), []string{"a", "l"}) {
		t.Errorf("b's last view %d: %v, want [a l]", v.Version, ids(v))
	}
	isClosed(t, chb)
	if v := next(t, cha); v.Version != 4 || !reflect.DeepEqual(ids(v), []string{"a", "l"}) {
		t.Errorf("a's view %d: %v, want version 4 with [a l]", v.Version, ids(v))
	}
	if err := a.SetDraining(t.Context(), true); err != nil {
		t.Errorf("a.SetDraining(true) = %v, want nil", err)
	}
	next(t, cha)
	// A handle shared from a closed handle is open.
	c := b.Share()
	if err := c.Join(t.Context(), member("b")); err != nil {
		t.Errorf("b.Share().Join(b) = %v, want nil", err)
	}
	if v := next(t, cha); !reflect.DeepEqual(ids(v), []string{"a", "b", "l"}) {
		t.Errorf("view %d: %v, want [a b l]", v.Version, ids(v))
	}
}

// TestATMEM15Join checks Join: it adds a member or replaces the listed one, copies Meta, and
// refuses in order a closed handle, an invalid member, a second Join and an ID joined on another
// handle.
func TestATMEM15Join(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		m := newGroup(t, member("a"), member("c"))
		ch := watch(t, m)
		next(t, ch)
		if err := m.Join(t.Context(), member("b")); err != nil {
			t.Fatalf("Join(b) = %v, want nil", err)
		}
		v := next(t, ch)
		if v.Version != 2 || !reflect.DeepEqual(v.Members, []core.Member{member("a"), member("b"), member("c")}) {
			t.Errorf("view %d: %+v, want version 2 with a, b, c", v.Version, v.Members)
		}
	})

	t.Run("Replace", func(t *testing.T) {
		m := newGroup(t, member("a"), member("b"))
		ch := watch(t, m)
		next(t, ch)
		self := core.Member{ID: "a", Addr: "10.0.0.1:9000", Slot: -1}
		if err := m.Join(t.Context(), self); err != nil {
			t.Fatalf("Join(a) = %v, want nil", err)
		}
		v := next(t, ch)
		if v.Version != 2 || !reflect.DeepEqual(v.Members, []core.Member{self, member("b")}) {
			t.Errorf("view %d: %+v, want version 2 with the joined a and b", v.Version, v.Members)
		}
	})

	t.Run("MetaCopy", func(t *testing.T) {
		m := newGroup(t)
		ch := watch(t, m)
		next(t, ch)
		self := member("a")
		self.Meta = map[string]string{"zone": "z1"}
		if err := m.Join(t.Context(), self); err != nil {
			t.Fatalf("Join(a) = %v, want nil", err)
		}
		self.Meta["zone"] = "changed"
		v2 := next(t, ch)
		if got := find(t, v2, "a").Meta["zone"]; got != "z1" {
			t.Errorf("view 2: a.Meta[zone] = %q after the caller changed its map, want z1", got)
		}
		// A view's Meta is not the group's either: a later view keeps the joined value.
		find(t, v2, "a").Meta["zone"] = "scribbled"
		if err := m.SetDraining(t.Context(), true); err != nil {
			t.Fatalf("SetDraining(true) = %v, want nil", err)
		}
		if got := find(t, next(t, ch), "a").Meta["zone"]; got != "z1" {
			t.Errorf("view 3: a.Meta[zone] = %q, want z1", got)
		}
		// Each channel gets its own copy: a change to one receiver's view reaches no other.
		other := m.Share()
		first, second := watch(t, m), watch(t, other)
		find(t, next(t, first), "a").Meta["zone"] = "scribbled"
		if got := find(t, next(t, second), "a").Meta["zone"]; got != "z1" {
			t.Errorf("view 3 on another channel: a.Meta[zone] = %q after a receiver changed its map, want z1", got)
		}
		if err := m.SetDraining(t.Context(), false); err != nil {
			t.Fatalf("SetDraining(false) = %v, want nil", err)
		}
		v4a, v4b := next(t, first), next(t, second)
		find(t, v4a, "a").Meta["zone"] = "scribbled"
		if got := find(t, v4b, "a").Meta["zone"]; got != "z1" {
			t.Errorf("view 4 on another channel: a.Meta[zone] = %q after a receiver changed its map, want z1", got)
		}
	})

	t.Run("Twice", func(t *testing.T) {
		m := newGroup(t)
		ch := watch(t, m)
		next(t, ch)
		if err := m.Join(t.Context(), member("a")); err != nil {
			t.Fatalf("Join(a) = %v, want nil", err)
		}
		next(t, ch)
		for _, id := range []string{"a", "b"} {
			if err, want := m.Join(t.Context(), member(id)), "static: already joined"; err == nil || err.Error() != want {
				t.Errorf("second Join(%s) = %v, want %s", id, err, want)
			}
		}
		// An invalid member is reported before the second Join.
		if err, want := m.Join(t.Context(), core.Member{ID: "b", Slot: -2}), `static: core: invalid member "b": Slot -2 is below -1`; err == nil || err.Error() != want {
			t.Errorf("second Join(invalid b) = %v, want %s", err, want)
		}
		none(t, ch)
	})

	t.Run("OtherHandle", func(t *testing.T) {
		a := newGroup(t)
		b := a.Share()
		if err := a.Join(t.Context(), member("a")); err != nil {
			t.Fatalf("a.Join(a) = %v, want nil", err)
		}
		ch := watch(t, a)
		next(t, ch)
		if err, want := b.Join(t.Context(), member("a")), `static: member "a" already joined`; err == nil || err.Error() != want {
			t.Errorf("b.Join(a) = %v, want %s", err, want)
		}
		none(t, ch)
		// The refusal leaves b unjoined.
		if err := b.Join(t.Context(), member("b")); err != nil {
			t.Errorf("b.Join(b) = %v, want nil", err)
		}
	})

	t.Run("Closed", func(t *testing.T) {
		m := newGroup(t)
		if err := m.Leave(t.Context()); err != nil {
			t.Fatalf("Leave() = %v, want nil", err)
		}
		// Closed comes before validation.
		for _, self := range []core.Member{member("a"), {ID: "a", Slot: -2}} {
			if err := m.Join(t.Context(), self); !errors.Is(err, tenure.ErrClosed) {
				t.Errorf("Join(%+v) after Leave = %v, want %v", self, err, tenure.ErrClosed)
			}
		}
	})
}

// TestATMEM16SetDraining checks SetDraining's refusals and that it changes only Draining.
func TestATMEM16SetDraining(t *testing.T) {
	m := newGroup(t, member("a"))
	ch := watch(t, m)
	next(t, ch)
	if err, want := m.SetDraining(t.Context(), true), "static: not joined"; err == nil || err.Error() != want {
		t.Errorf("SetDraining(true) before Join = %v, want %s", err, want)
	}
	none(t, ch)
	self := core.Member{ID: "b", Addr: "b:1", Meta: map[string]string{"k": "v"}, Slot: -1}
	if err := m.Join(t.Context(), self); err != nil {
		t.Fatalf("Join(b) = %v, want nil", err)
	}
	next(t, ch)
	if err := m.SetDraining(t.Context(), true); err != nil {
		t.Fatalf("SetDraining(true) = %v, want nil", err)
	}
	want := self
	want.Draining = true
	if v := next(t, ch); !reflect.DeepEqual(v.Members, []core.Member{member("a"), want}) {
		t.Errorf("view %d: %+v, want a and the draining b", v.Version, v.Members)
	}
	if err := m.SetDraining(t.Context(), true); err != nil {
		t.Fatalf("SetDraining(true) again = %v, want nil", err)
	}
	none(t, ch)
	if err := m.Leave(t.Context()); err != nil {
		t.Fatalf("Leave() = %v, want nil", err)
	}
	if err := m.SetDraining(t.Context(), false); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("SetDraining(false) after Leave = %v, want %v", err, tenure.ErrClosed)
	}
	if err := newGroup(t).SetDraining(t.Context(), false); err == nil || err.Error() != "static: not joined" {
		t.Errorf("SetDraining(false) before Join = %v, want static: not joined", err)
	}
}

// TestATMEM17Leave checks that Leave delivers the view without the member, then closes every
// channel of the handle, and that the handle then answers with tenure.ErrClosed.
func TestATMEM17Leave(t *testing.T) {
	m := newGroup(t, member("a"), member("b"))
	other := m.Share()
	mine := []<-chan core.View{watch(t, m), watch(t, m)}
	theirs := watch(t, other)
	for _, ch := range append(mine, theirs) {
		next(t, ch)
	}
	if err := m.Join(t.Context(), member("a")); err != nil {
		t.Fatalf("Join(a) = %v, want nil", err)
	}
	if err := m.Leave(t.Context()); err != nil {
		t.Fatalf("Leave() = %v, want nil", err)
	}
	for i, ch := range mine {
		if v := next(t, ch); v.Version != 2 || !reflect.DeepEqual(ids(v), []string{"b"}) {
			t.Errorf("channel %d: view %d %v, want version 2 with [b]", i, v.Version, ids(v))
		}
		isClosed(t, ch)
	}
	if v := next(t, theirs); v.Version != 2 || !reflect.DeepEqual(ids(v), []string{"b"}) {
		t.Errorf("other handle: view %d %v, want version 2 with [b]", v.Version, ids(v))
	}
	none(t, theirs)

	if err := m.Join(t.Context(), member("c")); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Join(c) after Leave = %v, want %v", err, tenure.ErrClosed)
	}
	if err := m.SetDraining(t.Context(), true); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("SetDraining(true) after Leave = %v, want %v", err, tenure.ErrClosed)
	}
	if ch, err := m.Watch(t.Context()); !errors.Is(err, tenure.ErrClosed) || ch != nil {
		t.Errorf("Watch() after Leave = %v, %v, want nil, %v", ch, err, tenure.ErrClosed)
	}
	if err := m.Leave(t.Context()); err != nil {
		t.Errorf("second Leave() = %v, want nil", err)
	}
	none(t, theirs)

	// Leave before Join makes no view and closes the handle's channels.
	ch := watch(t, other)
	next(t, ch)
	if err := other.Leave(t.Context()); err != nil {
		t.Fatalf("Leave() before Join = %v, want nil", err)
	}
	isClosed(t, ch)
	isClosed(t, theirs)
	if _, err := other.Watch(t.Context()); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Watch() after Leave before Join = %v, want %v", err, tenure.ErrClosed)
	}
}

// TestATMEM18Deterministic checks that the same calls give the same views, whatever the order of
// the members passed to New.
func TestATMEM18Deterministic(t *testing.T) {
	run := func(listed []core.Member) []core.View {
		m := newGroup(t, listed...)
		ch := watch(t, m)
		views := []core.View{next(t, ch)}
		b, c := m.Share(), m.Share()
		steps := []func() error{
			func() error {
				return b.Join(t.Context(), core.Member{ID: "b", Meta: map[string]string{"k": "v"}, Slot: -1})
			},
			func() error { return c.Join(t.Context(), core.Member{ID: "a", Addr: "10.0.0.1:1", Slot: -1}) },
			func() error { return b.SetDraining(t.Context(), true) },
			func() error { return c.Leave(t.Context()) },
			func() error { return b.Leave(t.Context()) },
		}
		for i, step := range steps {
			if err := step(); err != nil {
				t.Fatalf("step %d = %v, want nil", i, err)
			}
			views = append(views, next(t, ch))
		}
		return views
	}
	want := run([]core.Member{member("a"), member("d"), member("c")})
	for _, listed := range [][]core.Member{
		{member("c"), member("a"), member("d")},
		{member("d"), member("c"), member("a")},
	} {
		if got := run(listed); !reflect.DeepEqual(got, want) {
			t.Errorf("views with members %v:\n  %+v\nwant:\n  %+v", ids(core.View{Members: listed}), got, want)
		}
	}
	for i, v := range want {
		if v.Version != uint64(i+1) {
			t.Errorf("view %d has version %d, want %d", i, v.Version, i+1)
		}
		if err := v.Validate(); err != nil {
			t.Errorf("view %d: Validate() = %v, want nil", v.Version, err)
		}
	}
}

// TestATMEM25Synctest runs static inside a synctest bubble: it starts no goroutine that
// outlives a call and arms no timer, so no time passes, and a canceled watch closes once the
// bubble is idle.
func TestATMEM25Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		m := newGroup(t, member("a"))
		ctx, cancel := context.WithCancel(t.Context())
		ch, err := m.Watch(ctx)
		if err != nil {
			t.Fatalf("Watch() = %v, want nil", err)
		}
		next(t, ch)
		b := m.Share()
		bch := watch(t, b)
		next(t, bch)
		if err := b.Join(t.Context(), member("b")); err != nil {
			t.Fatalf("Join(b) = %v, want nil", err)
		}
		if err := b.SetDraining(t.Context(), true); err != nil {
			t.Fatalf("SetDraining(true) = %v, want nil", err)
		}
		synctest.Wait()
		if v := next(t, ch); v.Version != 3 {
			t.Errorf("view %d, want version 3", v.Version)
		}

		cancel()
		synctest.Wait()
		isClosed(t, ch)

		if err := b.Leave(t.Context()); err != nil {
			t.Fatalf("Leave() = %v, want nil", err)
		}
		synctest.Wait()
		next(t, bch)
		isClosed(t, bch)
		if got := time.Since(start); got != 0 {
			t.Errorf("time passed in the bubble = %v, want 0", got)
		}
	})
}

// TestConcurrent runs Join, SetDraining, Watch, context ends and Leave on shared handles at the
// same time, for the race detector: every channel is closed once, by its context or by Leave,
// and no view is sent on a closed channel.
func TestConcurrent(t *testing.T) {
	m := newGroup(t, member("l"))
	var wg sync.WaitGroup
	for i := range 16 {
		h := m.Share()
		wg.Go(func() {
			ctx, cancel := context.WithCancel(t.Context())
			chans := make([]<-chan core.View, 0, 3)
			for range 3 {
				ch, err := h.Watch(ctx)
				if err != nil {
					t.Errorf("Watch() = %v, want nil", err)
					cancel()
					return
				}
				chans = append(chans, ch)
			}
			id := "h" + strconv.Itoa(i)
			if err := h.Join(t.Context(), member(id)); err != nil {
				t.Errorf("Join(%s) = %v, want nil", id, err)
			}
			if err := h.SetDraining(t.Context(), true); err != nil {
				t.Errorf("SetDraining(true) on %s = %v, want nil", id, err)
			}
			// Half the handles end their watches and leave at once, racing the
			// context.AfterFunc against Leave; the rest leave with their watches open.
			if i%2 == 0 {
				cancel()
			}
			if err := h.Leave(t.Context()); err != nil {
				t.Errorf("Leave() on %s = %v, want nil", id, err)
			}
			cancel()
			for _, ch := range chans {
				for range ch {
				}
			}
		})
	}
	wg.Wait()
	if v := next(t, watch(t, m)); v.Version < 2 || !reflect.DeepEqual(ids(v), []string{"l"}) {
		t.Errorf("view %d: %v, want a version above 1 with [l]", v.Version, ids(v))
	}
}
