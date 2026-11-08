// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package membershiptest is the conformance suite for tenure.Membership implementations. Every
// membership adapter runs it unchanged; a membership that passes it keeps the contract the tenure
// runtime relies on.
//
// An adapter calls Run from one of its tests, with a function that sets up a fresh group for each
// case:
//
//	func TestConformance(t *testing.T) {
//		membershiptest.Run(t, func(t *testing.T, ids []string) membershiptest.Fixture {
//			group := mymembership.NewGroup(t, ids) // a fresh group; clean it up with t.Cleanup
//			return membershiptest.Fixture{
//				New:    func() tenure.Membership { return group.Client() },
//				Settle: 2 * time.Second,
//			}
//		})
//	}
//
// Run starts every case as a parallel subtest named <Group>/<Case>, such as Join/Visible. Each
// case calls newMembership once, with its own *testing.T and the IDs "m0" to "m7", checks the
// fixture and then runs against that group alone, so newMembership must return a group that
// shares no members with any other. Because the cases are parallel subtests, they run only after
// the test function that calls Run has returned, so release anything the groups share with
// t.Cleanup, not with defer.
//
// A case calls Fixture.New for each instance when it first needs it: instances a, b, c and d,
// which join as members m0, m1, m2 and m3, and instance x, which never joins. Run makes a while
// it checks the fixture, before the case starts. Member m<i> has Addr "10.0.0.<i+1>:7000", Meta
// {"zone": "z<i%2>"} and Slot -1. The suite calls Leave only in the cases about Leave, so a
// fixture that must stop its instances does so itself, for example with t.Cleanup. Every context
// the suite passes to a method ends when its case ends, at the latest.
//
// A change may take up to Fixture.Settle to reach every instance. A case waits for a view that
// shows a change for at most Settle plus 5s of real time. Before it checks that no new view came,
// it waits Settle, and before it checks that the newest of two quick changes replaced the view
// not yet received, it waits Settle, too. A membership whose changes reach every view inside the
// call that makes them sets Settle to 0. A membership that implements tenure.PropagationEstimator
// must estimate at most Settle.
//
// Every view a case receives is checked against the view received before it on the same
// channel: its Version is at least 1 and above the earlier one, its member data differs (Slot is
// ignored and Meta is compared by content), and its IDs are strictly ascending.
//
// With Fixture.Listed and Fixture.Observer, Run checks that the first view of instance a, before
// it joins, lists exactly the IDs. A fixture that is not an Observer skips the case that watches
// an instance that never joins, and a membership that does not implement
// tenure.PropagationEstimator skips the case that checks the estimate. Each skip says what is
// missing.
//
// A failure reads "<Method>(<args>) = <got>, want <want>" or "view <Version> on <instance>: <got>,
// want <condition>", with strings quoted, errors as their text and nil as nil.
package membershiptest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Fixture is one group under test, with what the suite needs to drive it.
type Fixture struct {
	// New returns a new instance of the group: a client that has not joined yet. It is
	// required, and it must not return nil.
	New func() tenure.Membership

	// Listed reports that the group lists every ID it was set up with in its views from the
	// start, before any instance joins.
	Listed bool

	// Observer reports that an instance that never joins still sees the group: its views list
	// the members that joined.
	Observer bool

	// Settle is how long, in real time, a change of one member may take to reach the views of
	// every instance. It must not be negative.
	Settle time.Duration
}

// Run runs every conformance case against groups from newMembership, each as a parallel subtest
// of t named <Group>/<Case>. newMembership is called once per case, with the case's *testing.T
// and the IDs "m0" to "m7", and must return a fresh group each time.
func Run(t *testing.T, newMembership func(t *testing.T, ids []string) Fixture) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, newMembership(t, slices.Clone(groupIDs)))
			e.validate()
			e.checkNeeds(c.needs)
			c.run(e)
		})
	}
}

// needs is the set of optional features a case needs.
type needs uint8

const (
	needObserver needs = 1 << iota
	needEstimator
)

// testCase is one entry of the case table.
type testCase struct {
	name  string
	needs needs
	run   func(e *env)
}

// cases is MEM-530's Phase 1 case table, in the order Run starts them.
var cases = []testCase{
	{"Watch/Initial", 0, watchInitial},
	{"Watch/Sorted", 0, watchSorted},
	{"Watch/Versions", 0, watchVersions},
	{"Watch/Coalesce", 0, watchCoalesce},
	{"Watch/Cancel", 0, watchCancel},
	{"Watch/Multiple", 0, watchMultiple},
	{"Watch/WithoutJoin", needObserver, watchWithoutJoin},
	{"Join/Visible", 0, joinVisible},
	{"Join/MetaCopy", 0, joinMetaCopy},
	{"Join/Twice", 0, joinTwice},
	{"Join/Invalid", 0, joinInvalid},
	{"Join/Canceled", 0, joinCanceled},
	{"Draining/Visible", 0, drainingVisible},
	{"Draining/Noop", 0, drainingNoop},
	{"Draining/BeforeJoin", 0, drainingBeforeJoin},
	{"Leave/Visible", 0, leaveVisible},
	{"Leave/Closed", 0, leaveClosed},
	{"Leave/BeforeJoin", 0, leaveBeforeJoin},
	{"Propagation/Estimate", needEstimator, propagationEstimate},
}

// groupIDs are the IDs every group is set up with.
var groupIDs = []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7"}

// grace is how much real time beyond Settle a case waits for a view or a closed channel.
const grace = 5 * time.Second

// joiners maps each instance that joins to the index of the member it joins as.
var joiners = map[string]int{"a": 0, "b": 1, "c": 2, "d": 3}

// self returns member i of the group, as an instance joins with it. Each call returns its own
// Meta map.
func self(i int) core.Member {
	return core.Member{
		ID:   groupIDs[i],
		Addr: fmt.Sprintf("10.0.0.%d:7000", i+1),
		Meta: map[string]string{"zone": fmt.Sprintf("z%d", i%2)},
		Slot: -1,
	}
}

// draining returns m with Draining set.
func draining(m core.Member) core.Member {
	m.Draining = true
	return m
}

// env is one running case: its fixture and the instances it has made.
type env struct {
	t     *testing.T
	ctx   context.Context
	f     Fixture
	insts map[string]tenure.Membership
}

func newEnv(t *testing.T, f Fixture) *env {
	return &env{t: t, ctx: t.Context(), f: f, insts: make(map[string]tenure.Membership)}
}

// validate fails the case when the fixture is unusable. The first failing check wins, with
// MEM-510's messages. It makes instance a.
func (e *env) validate() {
	e.t.Helper()
	switch {
	case e.f.New == nil:
		e.t.Fatal("membershiptest: Fixture.New is nil")
	case e.f.Settle < 0:
		e.t.Fatalf("membershiptest: Fixture.Settle %v is negative", e.f.Settle)
	}
	e.inst("a")
	if !e.f.Listed || !e.f.Observer {
		return
	}
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	v := e.watchCtx(ctx, "a").last
	listed := make(map[string]bool, len(v.Members))
	for _, m := range v.Members {
		if !slices.Contains(groupIDs, m.ID) {
			e.t.Fatalf("membershiptest: group lists %q, which is not in ids", m.ID)
		}
		listed[m.ID] = true
	}
	for _, id := range groupIDs {
		if !listed[id] {
			e.t.Fatalf("membershiptest: group does not list %q", id)
		}
	}
}

// checkNeeds skips the case when the fixture or the membership lacks a feature it needs,
// checking the features in a fixed order, with MEM-520's messages.
func (e *env) checkNeeds(n needs) {
	e.t.Helper()
	_, estimator := e.inst("a").(tenure.PropagationEstimator)
	switch {
	case n&needObserver != 0 && !e.f.Observer:
		e.t.Skip("membershiptest: fixture is not an Observer")
	case n&needEstimator != 0 && !estimator:
		e.t.Skip("membershiptest: membership does not implement tenure.PropagationEstimator")
	}
}

// inst returns the instance called name, making it with Fixture.New on first use.
func (e *env) inst(name string) tenure.Membership {
	e.t.Helper()
	if m, ok := e.insts[name]; ok {
		return m
	}
	m := e.f.New()
	if m == nil {
		e.t.Fatal("membershiptest: Fixture.New returned nil")
	}
	e.insts[name] = m
	return m
}

// join checks that the instance called name joins as its member.
func (e *env) join(name string) {
	e.t.Helper()
	e.joinAs(name, self(joiners[name]))
}

// joinAs checks that the instance called name joins as m.
func (e *env) joinAs(name string, m core.Member) {
	e.t.Helper()
	if err := e.inst(name).Join(e.ctx, m); err != nil {
		e.t.Fatalf("Join(ctx, %s) = %s, want nil", fmtMember(m), errText(err))
	}
}

// setDraining checks that the instance called name sets Draining to d.
func (e *env) setDraining(name string, d bool) {
	e.t.Helper()
	if err := e.inst(name).SetDraining(e.ctx, d); err != nil {
		e.t.Fatalf("SetDraining(ctx, %t) = %s, want nil", d, errText(err))
	}
}

// leave checks that the instance called name leaves.
func (e *env) leave(name string) {
	e.t.Helper()
	if err := e.inst(name).Leave(e.ctx); err != nil {
		e.t.Fatalf("Leave(ctx) = %s, want nil", errText(err))
	}
}

// closed checks that Join, SetDraining and Watch on the instance called name return
// tenure.ErrClosed.
func (e *env) closed(name string) {
	e.t.Helper()
	m := e.inst(name)
	if err := m.Join(e.ctx, self(joiners[name])); !errors.Is(err, tenure.ErrClosed) {
		e.t.Fatalf("Join(ctx, %s) = %s, want %s", fmtMember(self(joiners[name])), errText(err),
			tenure.ErrClosed)
	}
	if err := m.SetDraining(e.ctx, true); !errors.Is(err, tenure.ErrClosed) {
		e.t.Fatalf("SetDraining(ctx, true) = %s, want %s", errText(err), tenure.ErrClosed)
	}
	if ch, err := m.Watch(e.ctx); !errors.Is(err, tenure.ErrClosed) {
		e.t.Fatalf("Watch(ctx) = %s, %s, want nil, %s", fmtChan(ch), errText(err), tenure.ErrClosed)
	}
}

// watch checks that Watch on the instance called name returns a channel holding a view, with
// the case's context.
func (e *env) watch(name string) *watcher {
	e.t.Helper()
	return e.watchCtx(e.ctx, name)
}

// watchCtx checks that Watch(ctx) on the instance called name returns a channel holding a view,
// and receives that view.
func (e *env) watchCtx(ctx context.Context, name string) *watcher {
	e.t.Helper()
	ch, err := e.inst(name).Watch(ctx)
	if err != nil || ch == nil {
		e.t.Fatalf("Watch(ctx) = %s, %s, want a channel, nil", fmtChan(ch), errText(err))
	}
	w := &watcher{e: e, name: name, ch: ch}
	select {
	case v, ok := <-ch:
		if !ok {
			e.t.Fatal("Watch(ctx) = a closed channel, nil, want a channel holding the current view, nil")
		}
		w.take(v)
	default:
		e.t.Fatal("Watch(ctx) = a channel with no view, nil, want a channel holding the current view, nil")
	}
	return w
}

// watcher is one channel Watch returned, with the latest view received on it.
type watcher struct {
	e    *env
	name string
	ch   <-chan core.View
	last core.View
	n    int // views received
}

// take runs the per-view checks on v, the next view received, and makes it the latest.
func (w *watcher) take(v core.View) {
	w.e.t.Helper()
	t := w.e.t
	switch {
	case v.Version < 1:
		t.Fatalf("view %d on %s: Version %d, want Version at least 1", v.Version, w.name, v.Version)
	case w.n > 0 && v.Version <= w.last.Version:
		t.Fatalf("view %d on %s: Version %d after view %d, want Version above %d", v.Version, w.name,
			v.Version, w.last.Version, w.last.Version)
	case w.n > 0 && slices.EqualFunc(v.Members, w.last.Members, sameData):
		t.Fatalf("view %d on %s: the member data of view %d, want a change of member data", v.Version,
			w.name, w.last.Version)
	}
	for i := 1; i < len(v.Members); i++ {
		if v.Members[i-1].ID >= v.Members[i].ID {
			t.Fatalf("view %d on %s: IDs %q, want IDs strictly ascending", v.Version, w.name, viewIDs(v))
		}
	}
	w.last = v
	w.n++
}

// cond is what a case waits for a view to show.
type cond struct {
	want string
	ok   func(v core.View) bool
}

// lists is a view that has each of ms, with the same member data (Slot ignored).
func lists(ms ...core.Member) cond {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = fmtMember(m)
	}
	return cond{
		want: "members including " + strings.Join(parts, ", "),
		ok: func(v core.View) bool {
			for _, m := range ms {
				got, found := find(v, m.ID)
				if !found || !sameData(got, m) {
					return false
				}
			}
			return true
		},
	}
}

// omits is a view without a member called id.
func omits(id string) cond {
	return cond{
		want: fmt.Sprintf("no member %q", id),
		ok: func(v core.View) bool {
			_, found := find(v, id)
			return !found
		},
	}
}

// eventually receives views until the latest one meets c, for at most Settle plus 5s of real
// time, and returns that view. The case fails when the time runs out or the channel closes.
func (w *watcher) eventually(c cond) core.View {
	w.e.t.Helper()
	if c.ok(w.last) {
		return w.last
	}
	budget := w.e.f.Settle + grace
	timer := time.NewTimer(budget)
	defer timer.Stop()
	for {
		select {
		case v, ok := <-w.ch:
			if !ok {
				w.e.t.Fatalf("view %d on %s: %s and then a closed channel, want %s", w.last.Version, w.name,
					fmtMembers(w.last), c.want)
			}
			w.take(v)
			if c.ok(v) {
				return v
			}
		case <-timer.C:
			w.e.t.Fatalf("view %d on %s: %s for %v, want %s", w.last.Version, w.name, fmtMembers(w.last),
				budget, c.want)
		}
	}
}

// quiet waits Settle and then checks that no new view is waiting on the channel.
func (w *watcher) quiet() {
	w.e.t.Helper()
	time.Sleep(w.e.f.Settle)
	select {
	case v, ok := <-w.ch:
		if !ok {
			w.e.t.Fatalf("view %d on %s: a closed channel, want an open channel and no new view without a change",
				w.last.Version, w.name)
		}
		prev := w.last.Version
		w.take(v)
		w.e.t.Fatalf("view %d on %s: a new view after view %d, want no new view without a change",
			v.Version, w.name, prev)
	default:
	}
}

// closes receives views until the channel closes, for at most Settle plus 5s of real time after
// what, which has just happened.
func (w *watcher) closes(what string) {
	w.e.t.Helper()
	budget := w.e.f.Settle + grace
	timer := time.NewTimer(budget)
	defer timer.Stop()
	for {
		select {
		case v, ok := <-w.ch:
			if !ok {
				return
			}
			w.take(v)
		case <-timer.C:
			w.e.t.Fatalf("Watch(ctx) = a channel still open %v after %s, want a channel closed by then",
				budget, what)
		}
	}
}

// find returns the member of v called id.
func find(v core.View, id string) (core.Member, bool) {
	for _, m := range v.Members {
		if m.ID == id {
			return m, true
		}
	}
	return core.Member{}, false
}

// sameData reports whether a and b hold the same member data: Slot is ignored and Meta is
// compared by content.
func sameData(a, b core.Member) bool {
	return a.ID == b.ID && a.Addr == b.Addr && a.Draining == b.Draining &&
		maps.Equal(a.Meta, b.Meta) && a.JoinAt.Equal(b.JoinAt) && a.LeaveAt.Equal(b.LeaveAt)
}

// viewIDs returns the IDs of v's members, in order.
func viewIDs(v core.View) []string {
	out := make([]string, len(v.Members))
	for i, m := range v.Members {
		out[i] = m.ID
	}
	return out
}

// errText returns err's text, or "nil".
func errText(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

// fmtChan describes a channel Watch returned.
func fmtChan(ch <-chan core.View) string {
	if ch == nil {
		return "nil"
	}
	return "a channel"
}

// fmtMember formats a member's data; Slot only when it is not -1, and the times only when set.
func fmtMember(m core.Member) string {
	var b strings.Builder
	fmt.Fprintf(&b, "{ID: %q, Addr: %q, Meta: %v, Draining: %t", m.ID, m.Addr, m.Meta, m.Draining)
	if m.Slot != -1 {
		fmt.Fprintf(&b, ", Slot: %d", m.Slot)
	}
	if !m.JoinAt.IsZero() {
		fmt.Fprintf(&b, ", JoinAt: %v", m.JoinAt)
	}
	if !m.LeaveAt.IsZero() {
		fmt.Fprintf(&b, ", LeaveAt: %v", m.LeaveAt)
	}
	b.WriteString("}")
	return b.String()
}

// fmtMembers formats the members of v.
func fmtMembers(v core.View) string {
	if len(v.Members) == 0 {
		return "no members"
	}
	parts := make([]string, len(v.Members))
	for i, m := range v.Members {
		parts[i] = fmtMember(m)
	}
	return "members " + strings.Join(parts, ", ")
}
