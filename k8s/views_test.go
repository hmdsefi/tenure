// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// memberLease returns the member Lease of id in group g as a test writes it: with the member
// record record unless it is "", naming holder unless it is "", with leaseDurationSeconds
// seconds and renewTime now.
func memberLease(g, id, record, holder string, seconds int32) *coordinationv1.Lease {
	rt := metav1.NewMicroTime(time.Now())
	l := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: memberName(g, id), Labels: leaseLabels(g, kindMember, "")},
		Spec:       coordinationv1.LeaseSpec{LeaseDurationSeconds: &seconds, RenewTime: &rt},
	}
	if record != "" {
		l.Annotations = map[string]string{annotationMember: record}
	}
	if holder != "" {
		l.Spec.HolderIdentity = &holder
	}
	return l
}

// viewer follows one channel Watch returned: it checks every view it receives (a Version above
// the one before, a valid view) and keeps the latest.
type viewer struct {
	t    testing.TB
	name string
	ch   <-chan core.View
	last core.View
	n    int
}

// watchView calls a.Watch(ctx) and checks that the channel holds the current view at once.
func watchView(t testing.TB, ctx context.Context, a *Adapter, name string) *viewer {
	t.Helper()
	ch, err := a.Watch(ctx)
	if err != nil || ch == nil {
		t.Fatalf("Watch on %s = %v, %v; want a channel, nil", name, ch, err)
	}
	w := &viewer{t: t, name: name, ch: ch}
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatalf("Watch on %s returned a closed channel", name)
		}
		w.take(v)
	default:
		t.Fatalf("Watch on %s returned a channel with no view", name)
	}
	return w
}

// take checks v, the next view received, and makes it the latest.
func (w *viewer) take(v core.View) {
	w.t.Helper()
	switch {
	case v.Version < 1:
		w.t.Fatalf("view on %s: Version %d, want at least 1", w.name, v.Version)
	case w.n > 0 && v.Version <= w.last.Version:
		w.t.Fatalf("view on %s: Version %d after %d, want it above", w.name, v.Version, w.last.Version)
	case w.n > 0 && sameView(v, w.last):
		w.t.Fatalf("view %d on %s: the same as view %d, want a change", v.Version, w.name, w.last.Version)
	}
	if err := v.Validate(); err != nil {
		w.t.Fatalf("view %d on %s: %v", v.Version, w.name, err)
	}
	w.last = v
	w.n++
}

// next receives the next view within d; ok is false when none came. A closed channel fails the
// test.
func (w *viewer) next(d time.Duration) (v core.View, ok bool) {
	w.t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case v, open := <-w.ch:
		if !open {
			w.t.Fatalf("view %d on %s and then a closed channel, want an open channel", w.last.Version, w.name)
		}
		w.take(v)
		return v, true
	case <-timer.C:
		return core.View{}, false
	}
}

// waitFor receives views until the latest one meets ok, for at most d, and returns it.
func (w *viewer) waitFor(d time.Duration, what string, ok func(core.View) bool) core.View {
	w.t.Helper()
	deadline := time.Now().Add(d)
	for !ok(w.last) {
		left := time.Until(deadline)
		if _, got := w.next(max(left, 0)); !got {
			w.t.Fatalf("view %d on %s: %s for %v, want %s", w.last.Version, w.name, fmtView(w.last), d, what)
		}
	}
	return w.last
}

// quiet checks that no view comes for d.
func (w *viewer) quiet(d time.Duration) {
	w.t.Helper()
	prev := w.last.Version
	if v, ok := w.next(d); ok {
		w.t.Fatalf("view %d on %s after view %d: %s, want no new view", v.Version, w.name, prev, fmtView(v))
	}
}

// closes receives views until the channel closes, for at most d.
func (w *viewer) closes(d time.Duration) {
	w.t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case v, ok := <-w.ch:
			if !ok {
				return
			}
			w.take(v)
		case <-timer.C:
			w.t.Fatalf("channel of %s still open after %v", w.name, d)
		}
	}
}

// ids returns the IDs of v's members.
func ids(v core.View) []string {
	out := []string{}
	for _, m := range v.Members {
		out = append(out, m.ID)
	}
	return out
}

// listing is a view whose member IDs are exactly want.
func listing(want ...string) func(core.View) bool {
	return func(v core.View) bool { return slices.Equal(ids(v), want) }
}

// has is a view that lists the member id.
func has(id string) func(core.View) bool {
	return func(v core.View) bool { return slices.Contains(ids(v), id) }
}

// lacks is a view that does not list the member id.
func lacks(id string) func(core.View) bool {
	return func(v core.View) bool { return !slices.Contains(ids(v), id) }
}

// fmtView formats v's members and slot data.
func fmtView(v core.View) string {
	parts := make([]string, len(v.Members))
	for i, m := range v.Members {
		parts[i] = fmt.Sprintf("%s{%q %v draining %t slot %d}", m.ID, m.Addr, m.Meta, m.Draining, m.Slot)
	}
	return fmt.Sprintf("members [%s], slots %d %q", strings.Join(parts, " "), v.Slots, v.SlotHolders)
}

// logged returns the records of level with message msg, each as its attributes' values by key.
func (h *logRecorder) logged(level slog.Level, msg string) []map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]string
	for _, r := range h.records {
		if r.Level != level || r.Message != msg {
			continue
		}
		attrs := map[string]string{}
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		out = append(out, attrs)
	}
	return out
}

// waitDelivered waits up to 2 s for the informer to deliver every own member Lease write of a
// that freshness waits for.
func waitDelivered(t testing.TB, a *Adapter) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c := a.runningCache()
		c.mu.Lock()
		n := len(c.pending)
		c.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d own writes not delivered in 2s", n)
		}
		time.Sleep(time.Millisecond)
	}
}

// buildView builds a's view as the builder does and returns it with its next instant.
func buildView(a *Adapter) (core.View, time.Time) {
	a.views.mu.Lock()
	defer a.views.mu.Unlock()
	return a.buildViewLocked("")
}

// TestK8S610Views checks what a view holds, on a fake client: the member Leases of the group
// with a valid member record, while their resourceVersion was first seen less than their wait
// ago, with the record's fields, Slot the lowest live slot of the member, and the slot data;
// no session-only Lease, foreign Lease or invalid record, which is logged at Warn once per
// resourceVersion. Members drop out at rvSeenAt + wait, slot holders at tokenSeenAt + wait.
func TestK8S610Views(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	logs := &logRecorder{}
	a := fakeAdapter(t, client, withClock(clock.now), WithLogger(slog.New(logs)))
	leases := client.CoordinationV1().Leases("default")
	create := func(l *coordinationv1.Lease) *coordinationv1.Lease {
		t.Helper()
		l, err := leases.Create(ctx, l, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		return l
	}
	t0 := clock.now()
	m0 := core.Member{ID: "pod-0", Addr: "10.0.0.1:80", Meta: map[string]string{"zone": "a"}, Slot: -1,
		JoinAt: time.Date(2026, 11, 2, 11, 0, 0, 0, time.UTC)}
	m1 := core.Member{ID: "pod-1", Addr: "10.0.0.2:80", Draining: true, Slot: -1}
	p0 := create(memberLease("g", "pod-0", encodeMember(m0), "", 3))
	p1 := create(memberLease("g", "pod-1", encodeMember(m1), "pod-1/1", 5))
	create(memberLease("g", "pod-2", "", "pod-2/1", 3))
	p3 := create(memberLease("g", "pod-3", "{", "", 3))
	create(memberLease("g", "pod-4", `{"v":2,"id":"pod-4"}`, "", 3))
	create(memberLease("g", "pod-5", `{"v":1,"id":"pod-6"}`, "", 3))
	foreign := memberLease("g", "pod-7", encodeMember(member("pod-7")), "", 3)
	foreign.Labels = map[string]string{"app": "other"}
	create(foreign)
	create(slotsLease("g", "4"))
	create(slotLease("g", 1, "pod-1/1", p1.UID))
	create(slotLease("g", 2, "pod-0/1", p0.UID)) // pod-0's session ended: its Lease names no holder
	create(slotLease("g", 3, "pod-1/1", p1.UID))

	w := watchView(t, ctx, a, "a")
	want0, want1 := m0, m1
	want1.Slot = 1
	v := w.last
	if !slices.Equal(ids(v), []string{"pod-0", "pod-1"}) || !sameMember(v.Members[0], want0) || !sameMember(v.Members[1], want1) ||
		v.Slots != 4 || !slices.Equal(v.SlotHolders, []string{"", "pod-1/1", "", "pod-1/1"}) {
		t.Fatalf("first view %s, want pod-0 and pod-1 (draining, slot 1), slots 4 [\"\" pod-1/1 \"\" pod-1/1]", fmtView(v))
	}
	if v.Version != 1 {
		t.Errorf("first view Version %d, want 1", v.Version)
	}
	warned := func() []string {
		var out []string
		for _, r := range logs.logged(slog.LevelWarn, "k8s: invalid member record") {
			out = append(out, r["lease"]+": "+r["err"])
		}
		slices.Sort(out)
		return out
	}
	wantWarned := []string{
		`g-member-pod-3: k8s: member record: unexpected end of JSON input`,
		`g-member-pod-4: k8s: member record: version 2, want 1`,
		`g-member-pod-5: k8s: member record: id "pod-6", want "pod-5"`,
	}
	if got := warned(); !slices.Equal(got, wantWarned) {
		t.Errorf("warnings %q, want %q", got, wantWarned)
	}
	if _, next := buildView(a); !next.Equal(t0.Add(3 * time.Second)) {
		t.Errorf("next instant %v, want %v: pod-0's rvSeenAt + 3s", next, t0.Add(3*time.Second))
	}
	if got := actions(client, "get"); len(got) != 0 {
		t.Errorf("reads %v, want none", got)
	}

	// pod-0's Lease has not changed for its wait: it leaves the views.
	clock.advance(3 * time.Second)
	a.viewsChanged()
	w.waitFor(time.Second, "only pod-1", listing("pod-1"))
	if _, next := buildView(a); !next.Equal(t0.Add(5 * time.Second)) {
		t.Errorf("next instant %v, want %v: pod-1's rvSeenAt and token + 5s", next, t0.Add(5*time.Second))
	}
	// pod-1's too, and its session: its slots turn vacant.
	clock.advance(2 * time.Second)
	a.viewsChanged()
	v = w.waitFor(time.Second, "no members", listing())
	if !slices.Equal(v.SlotHolders, []string{"", "", "", ""}) {
		t.Errorf("slot holders %q, want all vacant", v.SlotHolders)
	}
	if _, next := buildView(a); !next.IsZero() {
		t.Errorf("next instant %v, want none", next)
	}
	// Nothing changed: no view.
	a.viewsChanged()
	w.quiet(100 * time.Millisecond)

	// A new state of an invalid record is logged again, once; a valid record joins the views.
	l := fakeGet(t, client, p3.Name)
	l.Annotations[annotationMember] = `{"v":1}`
	l, err := leases.Update(ctx, l, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	waitObserved(t, a, l.Name, l.ResourceVersion)
	a.viewsChanged()
	deadline := time.Now().Add(time.Second)
	for len(warned()) != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	wantWarned = append(wantWarned, `g-member-pod-3: k8s: member record: id "", want "pod-3"`)
	slices.Sort(wantWarned)
	if got := warned(); !slices.Equal(got, wantWarned) {
		t.Errorf("warnings %q, want %q", got, wantWarned)
	}
	l.Annotations[annotationMember] = encodeMember(member("pod-3"))
	if _, err := leases.Update(ctx, l, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	w.waitFor(time.Second, "pod-3", listing("pod-3"))
	if got := len(warned()); got != 4 {
		t.Errorf("%d warnings, want 4", got)
	}
}

// TestK8S620Rebuild checks when views are built again, on a fake client: on informer events,
// on the Adapter's own writes at once, and on freshness changes; while the cache is not fresh
// the views are frozen, so time alone removes no member, and once it is fresh again, members
// that did not change for their wait leave at once.
func TestK8S620Rebuild(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	// The heartbeat beats every 6 s: none during the test.
	slow := core.Timing{LeaseDuration: 20 * time.Second, RenewDeadline: 8 * time.Second, RetryPeriod: 6 * time.Second, SlotGrace: 20 * time.Second}
	a := fakeAdapter(t, client, withClock(clock.now), WithTiming(slow))
	w := watchView(t, ctx, a, "a")
	if len(w.last.Members) != 0 || w.last.Slots != 0 || w.last.SlotHolders != nil {
		t.Fatalf("first view %s, want no members and no slots", fmtView(w.last))
	}

	// Join shows at once, from the Adapter's own write.
	mustJoin(t, a, member("pod-0"))
	w.waitFor(time.Second, "pod-0", listing("pod-0"))
	waitFresh(t, a, true)

	// An informer event: another member joins.
	leases := client.CoordinationV1().Leases("default")
	if _, err := leases.Create(ctx, memberLease("g", "pod-9", encodeMember(member("pod-9")), "", 3), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	w.waitFor(time.Second, "pod-0 and pod-9", listing("pod-0", "pod-9"))

	// No own write delivered for RenewDeadline: stale, so pod-9 stays past its wait.
	clock.advance(slow.RenewDeadline)
	waitFresh(t, a, false)
	a.viewsChanged()
	w.quiet(100 * time.Millisecond)
	if v, _ := buildView(a); !listing("pod-0", "pod-9")(v) {
		t.Fatalf("view of a stale cache %s, want pod-0 and pod-9", fmtView(v))
	}

	// SetDraining's write shows at once; delivered, it makes the cache fresh, and pod-9 leaves.
	if err := a.SetDraining(ctx, true); err != nil {
		t.Fatalf("SetDraining: %v", err)
	}
	draining := member("pod-0")
	draining.Draining = true
	v := w.waitFor(time.Second, "pod-0 draining alone", listing("pod-0"))
	if !sameMember(v.Members[0], draining) {
		t.Errorf("member %+v, want %+v", v.Members[0], draining)
	}
	w.quiet(100 * time.Millisecond)

	// Slot data: a count and a session's slot.
	if err := a.KeepAlive(ctx, "pod-0/1", slow); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	closeAtEnd(t, a, "pod-0/1")
	if err := a.SetSlots(ctx, 2); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	mustClaim(t, a, "pod-0/1", 0)
	v = w.waitFor(time.Second, "pod-0 in slot 0", func(v core.View) bool {
		return listing("pod-0")(v) && v.Members[0].Slot == 0 && slices.Equal(v.SlotHolders, []string{"pod-0/1", ""})
	})
	if v.Slots != 2 {
		t.Errorf("Slots %d, want 2", v.Slots)
	}
}

// TestK8S630Watch checks Watch on a fake client: before Join and repeatedly, each channel
// holding the current view on return, one Version sequence, one view held with the newest
// winning, the channel closed when its ctx ends, a last view without the member and every
// channel closed at Leave, tenure.ErrClosed after Leave, and the informer stopped once no
// channel is open.
func TestK8S630Watch(t *testing.T) {
	ctx := t.Context()
	client := newFake()
	a := fakeAdapter(t, client)

	done, cancel := context.WithCancel(ctx)
	cancel()
	if ch, err := a.Watch(done); ch != nil || !errors.Is(err, context.Canceled) {
		t.Errorf("Watch with a done ctx = %v, %v; want nil, context.Canceled", ch, err)
	}
	if got := client.Actions(); len(got) != 0 || a.runningCache() != nil {
		t.Errorf("Watch with a done ctx: requests %v, informer %v; want none", got, a.runningCache() != nil)
	}

	// Before Join: the informer runs while a channel is open.
	w1 := watchView(t, ctx, a, "w1")
	if len(w1.last.Members) != 0 || w1.last.Version != 1 {
		t.Errorf("first view %d %s, want Version 1 with no members", w1.last.Version, fmtView(w1.last))
	}
	if a.runningCache() == nil {
		t.Fatal("no informer runs while a channel is open")
	}
	ctx2, cancel2 := context.WithCancel(ctx)
	w2 := watchView(t, ctx2, a, "w2")
	if w2.last.Version != w1.last.Version {
		t.Errorf("second channel's view Version %d, want the current %d", w2.last.Version, w1.last.Version)
	}

	mustJoin(t, a, member("pod-0"))
	w1.waitFor(time.Second, "pod-0", listing("pod-0"))
	w2.waitFor(time.Second, "pod-0", listing("pod-0"))
	if w1.last.Version != w2.last.Version {
		t.Errorf("Versions %d and %d of one view, want one sequence", w1.last.Version, w2.last.Version)
	}

	// Two changes not received: the channel holds the newest only.
	prev := w1.last.Version
	for i, d := range []bool{true, false} {
		if err := a.SetDraining(ctx, d); err != nil {
			t.Fatalf("SetDraining(%t): %v", d, err)
		}
		want := prev + uint64(i) + 1
		deadline := time.Now().Add(time.Second)
		for {
			a.views.mu.Lock()
			version := a.views.version
			a.views.mu.Unlock()
			if version >= want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("view Version %d after SetDraining(%t), want %d", version, d, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	// The newest view has the data of the one before the two changes: received directly, as the
	// viewer wants each view to differ from the one received before.
	var v core.View
	select {
	case v = <-w1.ch:
	default:
	}
	if v.Version != prev+2 || !listing("pod-0")(v) || v.Members[0].Draining {
		t.Errorf("view after two changes: %d %s, want Version %d with pod-0 not draining", v.Version, fmtView(v), prev+2)
	}
	w1.last = v
	w1.quiet(50 * time.Millisecond)

	// Ending ctx2 closes w2 alone; the view it holds is taken out first.
	select {
	case <-w2.ch:
	default:
	}
	cancel2()
	w2.closes(time.Second)
	if a.runningCache() == nil {
		t.Fatal("the informer stopped with a channel open and the member joined")
	}
	w3 := watchView(t, ctx, a, "w3")

	// Leave: a last view without the member, then closed channels.
	if err := a.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	for _, w := range []*viewer{w1, w3} {
		w.closes(time.Second)
		if !lacks("pod-0")(w.last) {
			t.Errorf("last view on %s %s, want no pod-0", w.name, fmtView(w.last))
		}
	}
	client.ClearActions()
	if ch, err := a.Watch(ctx); ch != nil || !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Watch after Leave = %v, %v; want nil, tenure.ErrClosed", ch, err)
	}
	if got := client.Actions(); len(got) != 0 {
		t.Errorf("Watch after Leave sent %v, want nothing", got)
	}
	waitStopped(t, a)

	t.Run("observer", func(t *testing.T) {
		client := newFake()
		b := fakeAdapter(t, client)
		w := watchView(t, ctx, b, "b")
		if err := b.Leave(ctx); err != nil {
			t.Fatalf("Leave before Join: %v", err)
		}
		w.closes(time.Second)
		if ch, err := b.Watch(ctx); ch != nil || !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("Watch after Leave = %v, %v; want nil, tenure.ErrClosed", ch, err)
		}
		if got := writes(client); len(got) != 0 {
			t.Errorf("writes %v, want none", got)
		}
		waitStopped(t, b)
	})
}

// TestK8S260MemberGC checks member GC on a fake client: a member Lease is deleted once it has
// not changed for max(d, 2·wait), with its UID and resourceVersion as preconditions and one Info
// log; never the Adapter's own member Leases; and not while the cache is not fresh.
func TestK8S260MemberGC(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	logs := &logRecorder{}
	// The heartbeat and the GC tick every 6 s: the test calls collect itself.
	slow := core.Timing{LeaseDuration: 20 * time.Second, RenewDeadline: 8 * time.Second, RetryPeriod: 6 * time.Second, SlotGrace: 20 * time.Second}
	long := core.Timing{LeaseDuration: 100 * time.Second, RenewDeadline: 60 * time.Second, RetryPeriod: time.Second, SlotGrace: 100 * time.Second}
	a := fakeAdapter(t, client, withClock(clock.now), WithTiming(slow), WithMemberGC(10*time.Second), WithLogger(slog.New(logs)))
	leases := client.CoordinationV1().Leases("default")
	t0 := clock.now()

	mustJoin(t, a, member("pod-0"))
	if err := a.KeepAlive(ctx, "pod-1/1", long); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	closeAtEnd(t, a, "pod-1/1")
	short, err := leases.Create(ctx, memberLease("g", "pod-8", "", "pod-8/1", 3), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	slow30, err := leases.Create(ctx, memberLease("g", "pod-9", encodeMember(member("pod-9")), "", 30), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	waitObserved(t, a, short.Name, short.ResourceVersion)
	waitObserved(t, a, slow30.Name, slow30.ResourceVersion)
	waitFresh(t, a, true)
	deleted := func() []string {
		var out []string
		for _, x := range actions(client, "delete") {
			d, _ := x.(k8stesting.DeleteActionImpl)
			out = append(out, d.Name)
		}
		return out
	}
	collectAt := func(at time.Duration, want ...string) {
		t.Helper()
		clock.advance(t0.Add(at).Sub(clock.now()))
		a.collect(ctx)
		if got := deleted(); !slices.Equal(got, want) {
			t.Fatalf("deletions at t0+%v: %q, want %q", at, got, want)
		}
	}

	// pod-8 waits max(10s, 2·3s) = 10s.
	collectAt(9 * time.Second)
	collectAt(10*time.Second, short.Name)
	x, _ := actions(client, "delete")[0].(k8stesting.DeleteActionImpl)
	if pre := x.DeleteOptions.Preconditions; pre == nil || pre.UID == nil || *pre.UID != short.UID ||
		pre.ResourceVersion == nil || *pre.ResourceVersion != short.ResourceVersion {
		t.Errorf("preconditions %+v, want UID %s and resourceVersion %s", pre, short.UID, short.ResourceVersion)
	}
	infos := logs.logged(slog.LevelInfo, "k8s: deleted member lease")
	if len(infos) != 1 || infos[0]["lease"] != short.Name {
		t.Errorf("Info logs %v, want one for %s", infos, short.Name)
	}

	// At t0+50 the own joined Lease has not changed for 50s > 2·20s, and stays; the session's
	// renewal keeps the cache fresh.
	clock.advance(t0.Add(50 * time.Second).Sub(clock.now()))
	if err := a.KeepAlive(ctx, "pod-1/1", long); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	waitDelivered(t, a)
	if o, _ := a.cached(memberName("g", "pod-0")); !o.rvSeenAt.Equal(t0) {
		t.Fatalf("own Lease seen at %v, want unchanged since %v", o.rvSeenAt, t0)
	}
	collectAt(50*time.Second, short.Name)

	// pod-9 waits 2·30s = 60s, but at t0+111 the cache is stale: nothing.
	collectAt(111*time.Second, short.Name)
	if fresh, _ := a.freshness(); fresh {
		t.Fatal("fresh at t0+111, want stale")
	}
	if err := a.KeepAlive(ctx, "pod-1/1", long); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	waitDelivered(t, a)
	waitFresh(t, a, true)
	collectAt(111*time.Second, short.Name, slow30.Name)
	if got := len(logs.logged(slog.LevelInfo, "k8s: deleted member lease")); got != 2 {
		t.Errorf("%d Info logs, want 2", got)
	}
}

// TestATK8S05InformerLifecycle checks on a cluster that the informer runs only while something
// needs it: none before the first Watch, one shared by Watch and Join, kept by an open channel or
// the joined member, a Watch channel closed when its ctx ends and within 2 s of Leave, with a
// last view without the member; and no goroutine of the Adapter left afterwards. The acceptance
// test's steps were written from the issue's text: the original text is unavailable.
func TestATK8S05InformerLifecycle(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	before := goleak.IgnoreCurrent()
	a, rec := c.adapter(t, g)
	starts := func() int { return rec.count(on(verbList, "")) + rec.count(on(verbWatch, "")) }

	if a.runningCache() != nil || len(rec.requests()) != 0 {
		t.Fatalf("before any use: informer %v, requests %v; want none", a.runningCache() != nil, requestStrings(rec.requests()))
	}
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	w1 := watchView(t, ctx1, a, "a")
	if a.runningCache() == nil || starts() == 0 {
		t.Fatal("Watch returned with no informer running")
	}
	if len(w1.last.Members) != 0 {
		t.Errorf("first view %s, want no members", fmtView(w1.last))
	}
	n := starts()
	if err := a.Join(ctx, member("pod-0")); err != nil {
		t.Fatalf("Join: %v", err)
	}
	w1.waitFor(2*time.Second, "pod-0", listing("pod-0"))
	ctx2, cancel2 := context.WithCancel(ctx)
	w2 := watchView(t, ctx2, a, "a2")
	if !listing("pod-0")(w2.last) || w2.last.Version != w1.last.Version {
		t.Errorf("second Watch: view %d %s, want the current view %d", w2.last.Version, fmtView(w2.last), w1.last.Version)
	}
	if starts() != n {
		t.Errorf("Join and a second Watch started another informer: %v", requestStrings(rec.filter(func(r request) bool {
			return r.verb == verbList || r.verb == verbWatch
		})))
	}
	cancel2()
	w2.closes(time.Second)
	if a.runningCache() == nil {
		t.Fatal("the informer stopped while joined and with a channel open")
	}

	// An observer that never joins: its informer stops within 1 s of its last channel.
	b, _ := c.adapter(t, g)
	ctx3, cancel3 := context.WithCancel(ctx)
	wb := watchView(t, ctx3, b, "b")
	wb.waitFor(2*time.Second, "pod-0", listing("pod-0"))
	cancel3()
	wb.closes(time.Second)
	waitStopped(t, b)

	// Leave: the last view lacks pod-0 and the channel closes within 2 s.
	if err := a.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	w1.closes(2 * time.Second)
	if !lacks("pod-0")(w1.last) {
		t.Errorf("last view %s, want no pod-0", fmtView(w1.last))
	}
	waitStopped(t, a)
	rec.reset()
	if ch, err := a.Watch(ctx); ch != nil || !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Watch after Leave = %v, %v; want nil, tenure.ErrClosed", ch, err)
	}
	if got := rec.requests(); len(got) != 0 {
		t.Errorf("Watch after Leave sent %v, want nothing", requestStrings(got))
	}
	// client-go's HTTP connections stay open for reuse: only they may remain.
	goleak.VerifyNone(t, before,
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreAnyFunction("net/http.(*http2ClientConn).readLoop"),
		goleak.IgnoreAnyFunction("golang.org/x/net/http2.(*ClientConn).readLoop"),
	)
}

// TestATK8S09Freeze checks on a cluster that views freeze while the cache is not fresh: with its
// watch stalled, an Adapter keeps listing a member whose Lease stopped changing well past its
// wait, and runs no member GC; once its watch delivers again, the member leaves its views and
// its Lease is collected. The acceptance test's steps were written from the issue's text: the
// original text is unavailable.
func TestATK8S09Freeze(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	b, rec := c.adapter(t, g, WithMemberGC(time.Second))
	mustJoin(t, b, member("pod-1"))
	w := watchView(t, ctx, b, "b")
	name := memberName(g, "pod-9")
	rawCreate(t, c, memberLease(g, "pod-9", encodeMember(member("pod-9")), "", 3))
	w.waitFor(3*time.Second, "pod-1 and pod-9", listing("pod-1", "pod-9"))

	// pod-9 changes every RetryPeriod until b's cache has gone stale.
	renewing, stopRenewing := context.WithCancel(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		tick := time.NewTicker(T.RetryPeriod)
		defer tick.Stop()
		for {
			select {
			case <-renewing.Done():
				return
			case <-tick.C:
			}
			if l, err := c.leases().Get(renewing, name, metav1.GetOptions{}); err == nil {
				l.Spec.RenewTime = new(metav1.NewMicroTime(time.Now()))
				_, _ = c.leases().Update(renewing, l, metav1.UpdateOptions{})
			}
		}
	}()
	release := rec.stall(on(verbWatch, ""))
	deadline := time.Now().Add(T.RenewDeadline + 3*time.Second)
	for fresh, _ := b.freshness(); fresh; fresh, _ = b.freshness() {
		if time.Now().After(deadline) {
			t.Fatalf("still fresh %v after the watch stalled", T.RenewDeadline+3*time.Second)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopRenewing()
	<-renewed
	stopped := time.Now()

	// Frozen: pod-9 stays past 2·wait, and nothing is collected.
	for time.Since(stopped) < 2*T.LeaseDuration+T.RetryPeriod {
		if v, ok := w.next(100 * time.Millisecond); ok && !listing("pod-1", "pod-9")(v) {
			t.Fatalf("view %d %s while stale, want pod-1 and pod-9", v.Version, fmtView(v))
		}
	}
	if fresh, _ := b.freshness(); fresh {
		t.Fatal("fresh with the watch stalled")
	}
	if got := rec.filter(on(verbDelete, "")); len(got) != 0 {
		t.Fatalf("deletions %v while stale, want none", requestStrings(got))
	}

	// The watch delivers again: the cache turns fresh, and pod-9, whose last changes b sees only
	// now, leaves its wait later; its Lease is collected 2·wait after that.
	release()
	w.waitFor(T.LeaseDuration+3*time.Second, "pod-1 alone", listing("pod-1"))
	x := rec.waitFor(t, T.LeaseDuration+T.RetryPeriod+3*time.Second, func(r request) bool { return on(verbDelete, name)(r) && !r.done.IsZero() })
	if x.status != 200 {
		t.Errorf("GC: %v, want DELETE 200", x)
	}
	if rawGet(t, c, name) != nil {
		t.Error("pod-9's Lease still exists")
	}
	w.quiet(T.RetryPeriod)
}

// TestATK8S16Heartbeat checks on a cluster that a member that joined stays in the views of the
// group while its heartbeat moves its Lease on, also with no session and beyond its
// LeaseDuration, and while a session renews the Lease instead; that it leaves another
// Adapter's views about LeaseDuration after its heartbeat stops getting through, while its own
// views, stale, keep it; and that it is back once the heartbeat gets through again. The
// acceptance test's steps were written from the issue's text: the original text is unavailable.
func TestATK8S16Heartbeat(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, recA := c.adapter(t, g)
	o, _ := c.adapter(t, g)
	name := memberName(g, "pod-0")
	mustJoin(t, a, member("pod-0"))
	wa := watchView(t, ctx, a, "a")
	wo := watchView(t, ctx, o, "o")
	wo.waitFor(3*time.Second, "pod-0", listing("pod-0"))

	// Two LeaseDurations of heartbeats: pod-0 stays.
	recA.reset()
	wo.quiet(2 * T.LeaseDuration)
	if n := recA.count(func(r request) bool { return on(verbUpdate, name)(r) && r.status == 200 }); n < 8 {
		t.Errorf("%d heartbeats in %v, want about 12", n, 2*T.LeaseDuration)
	}

	// A session renews the Lease instead: pod-0 stays.
	r := startRenewer(t, a, "pod-0/1")
	wo.quiet(2 * T.LeaseDuration)
	if renewals, errs := r.stop(); renewals == 0 || len(errs) != 0 {
		t.Fatalf("renewer: %d renewals, errors %v", renewals, errs)
	}
	if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	wo.quiet(T.LeaseDuration)

	// The heartbeat fails: pod-0 leaves o's views about LeaseDuration after its last change, and
	// stays in a's, whose cache turns stale.
	off := recA.fail(on(verbUpdate, name))
	failed := time.Now()
	wo.waitFor(T.LeaseDuration+3*time.Second, "no pod-0", listing())
	if d := time.Since(failed); d < T.LeaseDuration-T.RetryPeriod-200*time.Millisecond {
		t.Errorf("pod-0 left o's views %v after its heartbeat failed, want about %v", d, T.LeaseDuration)
	}
	if !listing("pod-0")(wa.last) {
		t.Errorf("a's view %s, want pod-0", fmtView(wa.last))
	}
	for v, ok := wa.next(10 * time.Millisecond); ok; v, ok = wa.next(10 * time.Millisecond) {
		if !has("pod-0")(v) {
			t.Errorf("a's view %d %s with a stale cache, want pod-0", v.Version, fmtView(v))
		}
	}
	if fresh, _ := a.freshness(); fresh {
		t.Error("a's cache fresh while its writes fail")
	}

	// Through again: pod-0 is back.
	off()
	wo.waitFor(T.RetryPeriod+3*time.Second, "pod-0", listing("pod-0"))
}

// TestATK8S17MemberGC checks member GC on a cluster: an Adapter with WithMemberGC(1s) deletes a
// member Lease, with or without a member record or a holder, once it has not changed for
// max(1s, 2·wait) as the Adapter observes it, each with its UID and resourceVersion as
// preconditions and one Info log; it keeps a Lease that changes, and its own; an Adapter with
// WithMemberGC(0) deletes nothing. The acceptance test's steps were written from the issue's
// text: the original text is unavailable.
func TestATK8S17MemberGC(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	logs := &logRecorder{}
	b, rec := c.adapter(t, g, WithMemberGC(time.Second), WithLogger(slog.New(logs)))
	off, recOff := c.adapter(t, g, WithMemberGC(0))
	mustJoin(t, b, member("pod-1"))
	mustJoin(t, off, member("pod-2"))
	start := time.Now()
	dead := []*coordinationv1.Lease{
		rawCreate(t, c, memberLease(g, "pod-7", encodeMember(member("pod-7")), "", 3)),
		rawCreate(t, c, memberLease(g, "pod-8", "", "pod-8/1", 3)),
	}
	live := memberName(g, "pod-6")
	rawCreate(t, c, memberLease(g, "pod-6", encodeMember(member("pod-6")), "", 3))
	changing, stop := context.WithCancel(ctx)
	changed := make(chan struct{})
	go func() {
		defer close(changed)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-changing.Done():
				return
			case <-tick.C:
			}
			if l, err := c.leases().Get(changing, live, metav1.GetOptions{}); err == nil {
				l.Spec.RenewTime = new(metav1.NewMicroTime(time.Now()))
				_, _ = c.leases().Update(changing, l, metav1.UpdateOptions{})
			}
		}
	}()

	for _, l := range dead {
		x := rec.waitFor(t, 2*T.LeaseDuration+T.RetryPeriod+3*time.Second, func(r request) bool { return on(verbDelete, l.Name)(r) && !r.done.IsZero() })
		if x.status != 200 {
			t.Errorf("GC of %s: %v, want DELETE 200", l.Name, x)
		}
		if d := x.sent.Sub(start); d < 2*T.LeaseDuration {
			t.Errorf("GC of %s %v after its creation, want at least %v", l.Name, d, 2*T.LeaseDuration)
		}
		pre := x.deleteOptions().Preconditions
		if pre == nil || pre.UID == nil || *pre.UID != l.UID || pre.ResourceVersion == nil || *pre.ResourceVersion != l.ResourceVersion {
			t.Errorf("GC of %s: preconditions %+v, want UID %s and resourceVersion %s", l.Name, pre, l.UID, l.ResourceVersion)
		}
		if rawGet(t, c, l.Name) != nil {
			t.Errorf("%s still exists", l.Name)
		}
	}
	time.Sleep(2 * T.RetryPeriod)
	stop()
	<-changed

	var gone []string
	for _, r := range logs.logged(slog.LevelInfo, "k8s: deleted member lease") {
		gone = append(gone, r["lease"])
	}
	slices.Sort(gone)
	if want := []string{dead[0].Name, dead[1].Name}; !slices.Equal(gone, want) {
		t.Errorf("Info logs of deletions %q, want %q", gone, want)
	}
	for _, x := range rec.filter(on(verbDelete, "")) {
		if x.name != dead[0].Name && x.name != dead[1].Name {
			t.Errorf("deletion %v, want only the dead Leases'", x)
		}
	}
	if got := recOff.filter(on(verbDelete, "")); len(got) != 0 {
		t.Errorf("deletions with WithMemberGC(0): %v, want none", requestStrings(got))
	}
	for _, name := range []string{live, memberName(g, "pod-1"), memberName(g, "pod-2")} {
		if rawGet(t, c, name) == nil {
			t.Errorf("%s was deleted, want it kept", name)
		}
	}
}

// TestATK8S28JoinAndViews checks the views on a cluster: members that join reach every
// Adapter, also one that never joins, sorted, with the record's fields; a member's own views
// show it as soon as Join returns; slot data comes from slot Leases; a member Lease that stops
// changing leaves the views its wait after it was first seen, with no event; and one Adapter's
// channels share one Version sequence. The acceptance test's steps were written from the issue's
// text: the original text is unavailable.
func TestATK8S28JoinAndViews(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, _ := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	o, recO := c.adapter(t, g)
	wo := watchView(t, ctx, o, "o")
	if len(wo.last.Members) != 0 {
		t.Fatalf("first view %s, want no members", fmtView(wo.last))
	}

	m0 := core.Member{ID: "pod-0", Addr: "10.0.0.1:8080", Meta: map[string]string{"zone": "a", "rack": "1"}, Slot: -1,
		JoinAt: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
	m1 := core.Member{ID: "pod-1", Addr: "10.0.0.2:8080", Slot: -1}
	mustJoin(t, b, m1)
	mustJoin(t, a, m0)
	wa := watchView(t, ctx, a, "a")
	if !has("pod-0")(wa.last) {
		t.Errorf("a's view right after Join %s, want pod-0", fmtView(wa.last))
	}
	for _, w := range []*viewer{wo, wa} {
		v := w.waitFor(3*time.Second, "pod-0 and pod-1", listing("pod-0", "pod-1"))
		if !sameMember(v.Members[0], m0) || !sameMember(v.Members[1], m1) {
			t.Errorf("view on %s %s, want the joined members' data", w.name, fmtView(v))
		}
	}

	// Slot data: pod-0's session holds slot 0 of 2.
	startRenewer(t, a, "pod-0/1")
	closeAtEnd(t, a, "pod-0/1")
	if err := b.SetSlots(ctx, 2); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := a.ClaimSlot(ctx, "pod-0/1"); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("ClaimSlot: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	inSlot := func(v core.View) bool {
		return listing("pod-0", "pod-1")(v) && v.Slots == 2 && slices.Equal(v.SlotHolders, []string{"pod-0/1", ""}) &&
			v.Members[0].Slot == 0 && v.Members[1].Slot == -1
	}
	wo.waitFor(3*time.Second, "pod-0 in slot 0 of 2", inSlot)
	wa.waitFor(3*time.Second, "pod-0 in slot 0 of 2", inSlot)

	// One Version sequence: a second channel starts at the current view.
	wo.quiet(T.RetryPeriod)
	w2 := watchView(t, ctx, o, "o2")
	if w2.last.Version != wo.last.Version || !sameView(w2.last, wo.last) {
		t.Errorf("second channel's view %d %s, want the current view %d", w2.last.Version, fmtView(w2.last), wo.last.Version)
	}

	// A member Lease that never changes: listed, then gone its wait after it was first seen.
	recO.reset()
	before := time.Now()
	rawCreate(t, c, memberLease(g, "pod-5", encodeMember(member("pod-5")), "", 3))
	wo.waitFor(2*time.Second, "pod-5", has("pod-5"))
	wo.waitFor(T.LeaseDuration+2*time.Second, "no pod-5", lacks("pod-5"))
	if d := time.Since(before); d < T.LeaseDuration {
		t.Errorf("pod-5 left %v after its creation, want at least %v", d, T.LeaseDuration)
	}
	// o2 holds the newest view, delivered with o's: the view that listed pod-5 may have been
	// replaced before o2 received it, so it is taken directly.
	select {
	case v := <-w2.ch:
		w2.last = v
	default:
	}
	if w2.last.Version != wo.last.Version || !sameView(w2.last, wo.last) {
		t.Errorf("views %d %s and %d %s on one Adapter, want the same view, of one sequence",
			w2.last.Version, fmtView(w2.last), wo.last.Version, fmtView(wo.last))
	}
	if got := recO.filter(func(r request) bool { return r.verb != verbWatch && r.verb != verbList }); len(got) != 0 {
		t.Errorf("o sent %v, want no request besides its informer's", requestStrings(got))
	}
}

// TestATK8S29SetDrainingAndLeave checks on a cluster that SetDraining reaches every view with
// one write, that setting the value a member has makes no request and no view, and that Leave
// removes the member from every view, gives its own channels a last view without it before it
// closes them, and makes Join, SetDraining and Watch return tenure.ErrClosed with no request.
// The acceptance test's steps were written from the issue's text: the original text is
// unavailable.
func TestATK8S29SetDrainingAndLeave(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, recA := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	name := memberName(g, "pod-0")
	mustJoin(t, a, member("pod-0"))
	mustJoin(t, b, member("pod-1"))
	wa := watchView(t, ctx, a, "a")
	wb := watchView(t, ctx, b, "b")
	for _, w := range []*viewer{wa, wb} {
		w.waitFor(3*time.Second, "pod-0 and pod-1", listing("pod-0", "pod-1"))
	}

	// writes returns the requests on pod-0's Lease that a sent since from, but for heartbeats:
	// updates whose renewTime is from or later.
	writes := func(from time.Time) []request {
		from = from.Truncate(time.Microsecond)
		return recA.filter(func(r request) bool {
			if r.name != name || r.start.Before(from) {
				return false
			}
			l := r.lease()
			return r.verb != verbUpdate || l == nil || l.Spec.RenewTime == nil || l.Spec.RenewTime.Before(&metav1.MicroTime{Time: from})
		})
	}
	for _, d := range []bool{true, false} {
		from := time.Now()
		if err := a.SetDraining(ctx, d); err != nil {
			t.Fatalf("SetDraining(%t): %v", d, err)
		}
		want := member("pod-0")
		want.Draining = d
		got := writes(from)
		if len(got) == 0 || got[len(got)-1].verb != verbUpdate || got[len(got)-1].status != 200 ||
			got[len(got)-1].lease().Annotations[annotationMember] != encodeMember(want) {
			t.Errorf("SetDraining(%t): requests %v, want an UPDATE of the record", d, requestStrings(got))
		}
		for _, w := range []*viewer{wa, wb} {
			v := w.waitFor(3*time.Second, fmt.Sprintf("pod-0 draining %t", d), func(v core.View) bool {
				return listing("pod-0", "pod-1")(v) && v.Members[0].Draining == d
			})
			if !sameMember(v.Members[0], want) {
				t.Errorf("view on %s: %+v, want %+v", w.name, v.Members[0], want)
			}
		}
		// The same value again: no request, no view.
		from = time.Now()
		if err := a.SetDraining(ctx, d); err != nil {
			t.Fatalf("SetDraining(%t) again: %v", d, err)
		}
		if got := writes(from); len(got) != 0 {
			t.Errorf("SetDraining(%t) again sent %v, want nothing", d, requestStrings(got))
		}
		wa.quiet(T.RetryPeriod)
		wb.quiet(time.Second)
	}

	recA.reset()
	if err := a.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	wa.closes(2 * time.Second)
	if !listing("pod-1")(wa.last) {
		t.Errorf("a's last view %s, want pod-1 alone", fmtView(wa.last))
	}
	wb.waitFor(2*time.Second, "pod-1 alone", listing("pod-1"))
	recA.reset()
	if err := a.Join(ctx, member("pod-0")); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Join after Leave = %v, want tenure.ErrClosed", err)
	}
	if err := a.SetDraining(ctx, true); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("SetDraining after Leave = %v, want tenure.ErrClosed", err)
	}
	if ch, err := a.Watch(ctx); ch != nil || !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Watch after Leave = %v, %v; want nil, tenure.ErrClosed", ch, err)
	}
	if got := recA.requests(); len(got) != 0 {
		t.Errorf("requests after Leave %v, want none", requestStrings(got))
	}
	wb.quiet(time.Second)
}

// TestK8S060InvalidMemberRecord checks on a cluster that a member Lease whose tenure.member
// record is not valid JSON, has another version or another member's id, an unreadable time or
// a LeaveAt not after its JoinAt stays out of the views, and is logged at Warn once per
// resourceVersion however often the views are built; a valid record written on it lists it.
func TestK8S060InvalidMemberRecord(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	logs := &logRecorder{}
	o, _ := c.adapter(t, g, WithLogger(slog.New(logs)))
	wo := watchView(t, ctx, o, "o")
	records := []struct{ id, record, err string }{
		{"pod-1", "not json", "k8s: member record: invalid character 'o' in literal null (expecting 'u')"},
		{"pod-2", `{"v":2,"id":"pod-2"}`, "k8s: member record: version 2, want 1"},
		{"pod-3", `{"v":1,"id":"pod-9"}`, `k8s: member record: id "pod-9", want "pod-3"`},
		{"pod-4", `{"v":1,"id":"pod-4","joinAt":"yesterday"}`, `k8s: member record: joinAt "yesterday" is not an RFC 3339 time`},
		{"pod-5", `{"v":1,"id":"pod-5","joinAt":"2026-11-02T12:00:00Z","leaveAt":"2026-11-02T11:00:00Z"}`,
			`k8s: member record: core: invalid member "pod-5": LeaveAt is not after JoinAt`},
	}
	rawCreate(t, c, memberLease(g, "pod-0", encodeMember(member("pod-0")), "", 60))
	var want []string
	for _, r := range records {
		rawCreate(t, c, memberLease(g, r.id, r.record, "", 60))
		want = append(want, memberName(g, r.id)+": "+r.err)
	}
	wo.waitFor(3*time.Second, "pod-0", listing("pod-0"))
	warned := func() []string {
		var out []string
		for _, r := range logs.logged(slog.LevelWarn, "k8s: invalid member record") {
			out = append(out, r["lease"]+": "+r["err"])
		}
		slices.Sort(out)
		return out
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(warned()) < len(records) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := warned(); !slices.Equal(got, want) {
		t.Fatalf("warnings %q, want %q", got, want)
	}

	// Views built again (pod-0 changes) log nothing new; a new resourceVersion of pod-1, still
	// invalid, is logged once more.
	rawChange(t, c, memberName(g, "pod-0"), func(l *coordinationv1.Lease) {
		m := member("pod-0")
		m.Draining = true
		l.Annotations[annotationMember] = encodeMember(m)
	})
	wo.waitFor(3*time.Second, "pod-0 draining", func(v core.View) bool { return listing("pod-0")(v) && v.Members[0].Draining })
	rawChange(t, c, memberName(g, "pod-1"), func(l *coordinationv1.Lease) { l.Spec.RenewTime = new(metav1.NewMicroTime(time.Now())) })
	want = slices.Insert(want, 0, want[0])
	deadline = time.Now().Add(3 * time.Second)
	for len(warned()) < len(want) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(T.RetryPeriod)
	if got := warned(); !slices.Equal(got, want) {
		t.Errorf("warnings %q, want %q", got, want)
	}

	// A valid record lists the member.
	rawChange(t, c, memberName(g, "pod-1"), func(l *coordinationv1.Lease) { l.Annotations[annotationMember] = encodeMember(member("pod-1")) })
	wo.waitFor(3*time.Second, "pod-0 and pod-1", listing("pod-0", "pod-1"))
}
