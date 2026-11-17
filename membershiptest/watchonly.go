// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package membershiptest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// WatchOnlyFixture is one group under test for RunWatchOnly: writer memberships that the suite
// joins and changes, and one watch-only membership that follows the same group without joining
// it.
type WatchOnlyFixture struct {
	// Writers holds one Membership per ID passed to the fixture function, in the same order, none
	// of which has joined. The suite joins Writers[i] as the member with the i-th ID, with the
	// member data Run uses. Every entry is required.
	Writers []tenure.Membership

	// WatchOnly follows the group of Writers and never joins. It is required.
	WatchOnly tenure.Membership

	// Slots changes the slot data of the group. nil skips the case about slots.
	Slots *SlotControl

	// Settle is how long, in real time, a change made through a writer may take to reach the
	// views of every membership, as in Fixture. It must not be negative.
	Settle time.Duration
}

// SlotControl changes the slot data of a group under test, as the members' slot store does. A
// holder belongs to the member whose ID is the holder's part before the first "/", or the whole
// holder when it has none.
type SlotControl struct {
	// SetCount sets the group's slot count.
	SetCount func(ctx context.Context, count int) error

	// Claim has the member with ID id claim a slot as a holder of its own, opening any session
	// the claim needs, and returns the slot.
	Claim func(ctx context.Context, id string) (int, error)

	// Release has the member with ID id release slot, which it claimed with Claim.
	Release func(ctx context.Context, id string, slot int) error
}

// RunWatchOnly runs every watch-only conformance case against groups from newFixture, each as a
// parallel subtest of t named WatchOnly/<Case>. newFixture is called once per case, with the
// case's *testing.T and the IDs "m0" to "m7", and must return a fresh group each time.
//
// The cases check that the watch-only membership's first view lists the group as its writers
// see it; that its views follow joins, drains and leaves made through the writers, and slot
// changes made through Slots; that Join, SetDraining and Leave on it return an error matching
// tenure.ErrWatchOnly and change nothing; that ending the context of one of its Watch calls
// closes that channel only; and, when it has a Stale() bool method, that Stale reports false
// while views arrive. A membership without that method skips that case, and a fixture without
// Slots skips the case about slots; each skip says what is missing.
//
// Every view a case receives on a channel is checked as Run checks it, except that a change of
// slot data alone (Slots, SlotHolders or a member's Slot) counts as a change. Writers a, b, c and
// d of the failure messages are Writers[0] to Writers[3]; w is the watch-only membership.
func RunWatchOnly(t *testing.T, newFixture func(t *testing.T, ids []string) WatchOnlyFixture) {
	t.Helper()
	for _, c := range watchOnlyCases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, slices.Clone(groupIDs))
			e := newWatchOnlyEnv(t, &f)
			e.checkWatchOnlyNeeds(c.needs)
			c.run(e)
		})
	}
}

// Features a watch-only case needs, beyond those of Run's cases.
const (
	needSlots needs = 1 << (iota + 4)
	needStale
)

// watchOnlyCases is RTR-780's case table, in the order RunWatchOnly starts them.
var watchOnlyCases = []testCase{
	{"WatchOnly/Initial", 0, watchOnlyInitial},
	{"WatchOnly/Follow", 0, watchOnlyFollow},
	{"WatchOnly/Slots", needSlots, watchOnlySlots},
	{"WatchOnly/Refuse", 0, watchOnlyRefuse},
	{"WatchOnly/Cancel", 0, watchOnlyCancel},
	{"WatchOnly/Fresh", needStale, watchOnlyFresh},
}

// stale is the optional method through which a membership reports that its views may be old.
type stale interface {
	Stale() bool
}

// newWatchOnlyEnv checks f and returns the env of a watch-only case: instances a to d are
// Writers[0] to Writers[3], and w is the watch-only membership.
func newWatchOnlyEnv(t *testing.T, f *WatchOnlyFixture) *env {
	t.Helper()
	switch {
	case f.WatchOnly == nil:
		t.Fatal("membershiptest: WatchOnlyFixture.WatchOnly is nil")
	case len(f.Writers) != len(groupIDs):
		t.Fatalf("membershiptest: WatchOnlyFixture has %d Writers for %d ids", len(f.Writers), len(groupIDs))
	case f.Settle < 0:
		t.Fatalf("membershiptest: WatchOnlyFixture.Settle %v is negative", f.Settle)
	case f.Slots != nil && (f.Slots.SetCount == nil || f.Slots.Claim == nil || f.Slots.Release == nil):
		t.Fatal("membershiptest: WatchOnlyFixture.Slots has a nil function")
	}
	if i := slices.Index(f.Writers, nil); i >= 0 {
		t.Fatalf("membershiptest: WatchOnlyFixture.Writers[%d] is nil", i)
	}
	e := newEnv(t, Fixture{Settle: f.Settle})
	e.wo = f
	for name, i := range joiners {
		e.insts[name] = f.Writers[i]
	}
	e.insts["w"] = f.WatchOnly
	return e
}

// checkWatchOnlyNeeds skips the case when the fixture or the membership lacks a feature it
// needs.
func (e *env) checkWatchOnlyNeeds(n needs) {
	e.t.Helper()
	_, hasStale := e.wo.WatchOnly.(stale)
	switch {
	case n&needSlots != 0 && e.wo.Slots == nil:
		e.t.Skip("membershiptest: fixture has no Slots")
	case n&needStale != 0 && !hasStale:
		e.t.Skip("membershiptest: membership does not implement Stale() bool")
	}
}

// watchOnlyInitial: the first view of the watch-only membership lists exactly the member data
// that a writer's view lists once the changes have settled.
func watchOnlyInitial(e *env) {
	e.join("a")
	e.join("b")
	e.join("c")
	e.setDraining("b", true)
	ref := e.watch("a")
	ref.eventually(lists(self(0), draining(self(1)), self(2)))
	time.Sleep(e.f.Settle)
	w := e.watch("w")
	if !slices.EqualFunc(w.last.Members, ref.last.Members, sameData) {
		e.t.Fatalf("view %d on w: %s, want exactly the members of view %d on a: %s", w.last.Version,
			fmtMembers(w.last), ref.last.Version, fmtMembers(ref.last))
	}
}

// watchOnlyFollow: the watch-only membership's views follow joins, drains and leaves.
func watchOnlyFollow(e *env) {
	w := e.watch("w")
	e.join("a")
	w.eventually(lists(self(0)))
	e.join("b")
	w.eventually(lists(self(0), self(1)))
	e.setDraining("b", true)
	w.eventually(lists(self(0), draining(self(1))))
	e.setDraining("b", false)
	w.eventually(lists(self(0), self(1)))
	e.leave("b")
	w.eventually(both(lists(self(0)), omits("m1")))
}

// watchOnlySlots: the watch-only membership's views follow the slot count, a claim and a
// release.
func watchOnlySlots(e *env) {
	sc := e.wo.Slots
	w := e.watch("w")
	e.join("a")
	e.join("b")
	w.eventually(lists(self(0), self(1)))
	if err := sc.SetCount(e.ctx, 2); err != nil {
		e.t.Fatalf("SetCount(ctx, 2) = %s, want nil", errText(err))
	}
	w.eventually(slotCount(2))
	slot, err := sc.Claim(e.ctx, "m0")
	if err != nil || slot < 0 || slot >= 2 {
		e.t.Fatalf("Claim(ctx, %q) = %d, %s, want a slot in [0, 2), nil", "m0", slot, errText(err))
	}
	w.eventually(both(slotCount(2), heldBy(slot, "m0")))
	if err := sc.Release(e.ctx, "m0", slot); err != nil {
		e.t.Fatalf("Release(ctx, %q, %d) = %s, want nil", "m0", slot, errText(err))
	}
	w.eventually(both(slotCount(2), heldBy(slot, "")))
	if err := sc.SetCount(e.ctx, 3); err != nil {
		e.t.Fatalf("SetCount(ctx, 3) = %s, want nil", errText(err))
	}
	w.eventually(slotCount(3))
}

// watchOnlyRefuse: Join, SetDraining and Leave on the watch-only membership return an error
// matching tenure.ErrWatchOnly and change nothing: no writer sees a new view, and the watch-only
// membership keeps its channels open and goes on following the group.
func watchOnlyRefuse(e *env) {
	e.join("a")
	ref := e.watch("a")
	ref.eventually(lists(self(0)))
	w := e.watch("w")
	w.eventually(lists(self(0)))
	m := e.inst("w")
	if err := m.Join(e.ctx, self(4)); !errors.Is(err, tenure.ErrWatchOnly) {
		e.t.Fatalf("Join(ctx, %s) = %s, want an error matching %s", fmtMember(self(4)), errText(err),
			tenure.ErrWatchOnly)
	}
	if err := m.SetDraining(e.ctx, true); !errors.Is(err, tenure.ErrWatchOnly) {
		e.t.Fatalf("SetDraining(ctx, true) = %s, want an error matching %s", errText(err), tenure.ErrWatchOnly)
	}
	if err := m.Leave(e.ctx); !errors.Is(err, tenure.ErrWatchOnly) {
		e.t.Fatalf("Leave(ctx) = %s, want an error matching %s", errText(err), tenure.ErrWatchOnly)
	}
	ref.quiet()
	w.quiet()
	again := e.watch("w")
	e.join("b")
	w.eventually(lists(self(0), self(1)))
	again.eventually(lists(self(0), self(1)))
}

// watchOnlyCancel: ending the context of one Watch on the watch-only membership closes that
// channel, and another channel stays open and follows the group.
func watchOnlyCancel(e *env) {
	e.join("a")
	ctx, cancel := context.WithCancel(e.ctx)
	w1 := e.watchCtx(ctx, "w")
	w2 := e.watch("w")
	w2.eventually(lists(self(0)))
	cancel()
	w1.closes("its context ended")
	e.join("b")
	w2.eventually(lists(self(0), self(1)))
}

// watchOnlyFresh: Stale reports false while views arrive.
func watchOnlyFresh(e *env) {
	s, _ := e.inst("w").(stale) // checkWatchOnlyNeeds has skipped the case without it
	w := e.watch("w")
	e.join("a")
	w.eventually(lists(self(0)))
	if s.Stale() {
		e.t.Fatalf("Stale() = true after view %d on w, want false while views arrive", w.last.Version)
	}
	e.setDraining("a", true)
	w.eventually(lists(draining(self(0))))
	if s.Stale() {
		e.t.Fatalf("Stale() = true after view %d on w, want false while views arrive", w.last.Version)
	}
}

// both is a view that meets a and b.
func both(a, b cond) cond {
	return cond{
		want: a.want + " and " + b.want,
		ok:   func(v core.View) bool { return a.ok(v) && b.ok(v) },
	}
}

// slotCount is a view whose slot count is n, with n SlotHolders.
func slotCount(n int) cond {
	return cond{
		want: fmt.Sprintf("Slots %d", n),
		ok:   func(v core.View) bool { return v.Slots == n && len(v.SlotHolders) == n },
	}
}

// heldBy is a view in which slot has a holder of member id, or is vacant when id is "".
func heldBy(slot int, id string) cond {
	want := fmt.Sprintf("slot %d held by a holder of %q", slot, id)
	if id == "" {
		want = fmt.Sprintf("slot %d vacant", slot)
	}
	return cond{
		want: want,
		ok: func(v core.View) bool {
			if slot >= len(v.SlotHolders) {
				return false
			}
			h := v.SlotHolders[slot]
			if id == "" {
				return h == ""
			}
			owner, _, _ := strings.Cut(h, "/")
			return h != "" && owner == id
		},
	}
}

// sameView reports whether a and b hold the same member and slot data: the members' data as
// sameData compares it and their Slots, the slot count and the slot holders.
func sameView(a, b core.View) bool {
	return slices.EqualFunc(a.Members, b.Members, func(x, y core.Member) bool {
		return sameData(x, y) && x.Slot == y.Slot
	}) && a.Slots == b.Slots && slices.Equal(a.SlotHolders, b.SlotHolders)
}
