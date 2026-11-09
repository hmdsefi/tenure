// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// slotLease returns slot record n of group g naming holder (none when empty) with session as
// its tenure.session (none when empty), as a test writes it.
func slotLease(g string, n int, holder string, session types.UID) *coordinationv1.Lease {
	l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: slotName(g, n), Labels: leaseLabels(g, kindSlot, "")}}
	if holder != "" {
		l.Spec.HolderIdentity = &holder
	}
	if session != "" {
		l.Annotations = map[string]string{annotationSession: string(session)}
	}
	return l
}

// slotsLease returns the count Lease of group g with tenure.slots value, as a test writes it.
func slotsLease(g, value string) *coordinationv1.Lease {
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Name: slotsName(g), Labels: leaseLabels(g, kindSlots, ""), Annotations: map[string]string{annotationSlots: value},
	}}
}

// mustClaim calls ClaimSlot and fails the test unless it returns want.
func mustClaim(t testing.TB, a *Adapter, holder string, want int) {
	t.Helper()
	if got, err := a.ClaimSlot(t.Context(), holder); got != want || err != nil {
		t.Fatalf("ClaimSlot(%s) = %d, %v; want %d, nil", holder, got, err, want)
	}
}

// wantNoSlot checks that ClaimSlot returned -1 and an error matching tenure.ErrNoSlot with text
// want.
func wantNoSlot(t testing.TB, slot int, err error, want string) {
	t.Helper()
	if slot != -1 || !errors.Is(err, tenure.ErrNoSlot) || err.Error() != want {
		t.Fatalf("ClaimSlot = %d, %v; want -1, %s", slot, err, want)
	}
}

// wantSlots checks that Slots returns count and holders.
func wantSlots(t testing.TB, a *Adapter, holders ...string) {
	t.Helper()
	count, got, err := a.Slots(t.Context())
	if err != nil || count != len(holders) || !slices.Equal(got, holders) {
		t.Fatalf("Slots = %d, %q, %v; want %d, %q, nil", count, got, err, len(holders), holders)
	}
}

// waitSlots waits up to 3 s for Slots to return the holders, and fails the test when it does not.
func waitSlots(t testing.TB, a *Adapter, holders ...string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		count, got, err := a.Slots(t.Context())
		if err == nil && count == len(holders) && slices.Equal(got, holders) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Slots = %d, %q, %v after 3s; want %d, %q", count, got, err, len(holders), holders)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitFresh waits up to 2 s for a's freshness to be want.
func waitFresh(t testing.TB, a *Adapter, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if fresh, _ := a.freshness(); fresh == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("freshness %v after 2s, want %v", !want, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// signalled checks that ch is closed, waiting up to 2 s.
func signalled(t testing.TB, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Errorf("%s: freshness change not signalled", what)
	}
}

// slotRequests matches the requests on slot record Leases of group g, creations included.
func slotRequests(g string) match {
	return func(r request) bool {
		if r.resource != resourceLeases {
			return false
		}
		name := r.name
		if l := r.lease(); name == "" && l != nil {
			name = l.Name
		}
		kind, _, _, _, ok := parseName(g, name)
		return ok && kind == kindSlot
	}
}

// slotSteps returns "<VERB> <name> <status>" of the requests m matches, with the name of a
// created Lease.
func slotSteps(rec *recorder, m match) []string {
	var out []string
	for _, r := range rec.filter(m) {
		if l := r.lease(); r.name == "" && l != nil {
			r.name = l.Name
		}
		out = append(out, r.String())
	}
	return out
}

// TestK8S150Freshness checks the cache's freshness with a fake client and clock: fresh with
// neither a session nor Join; after a session opens, fresh once the informer delivers an own
// member Lease write, for RenewDeadline of the session's Timing; while not fresh, Slots keeps a
// current session's slot held past its wait and ClaimSlot does not take it, without a request;
// a delivered own write makes the cache fresh again and frees the slot; a write by someone else
// to the own member Lease does not count; and opening and ending sessions, Join, Leave, a
// session's RenewDeadline changing and a stale cache turning fresh are signalled. An own write
// counts from when it was sent, not from its delivery: a renewal whose request took
// RenewDeadline leaves the cache stale, and a write the watch delivered before its response
// came back counts at once.
func TestK8S150Freshness(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	a := fakeAdapter(t, client, withClock(clock.now))
	const own, other = "pod-0/1", "pod-1/1"
	closeAtEnd(t, a, own)
	t0 := clock.now()
	// A long Timing: the session lives on past the freshness checks.
	long := core.Timing{LeaseDuration: 20 * time.Second, RenewDeadline: 8 * time.Second, RetryPeriod: time.Second, SlotGrace: 20 * time.Second}

	if fresh, next := a.freshness(); !fresh || !next.IsZero() {
		t.Errorf("freshness with neither a session nor Join = %v, %v; want true, none", fresh, next)
	}

	// Another member's session holds slot 0 of 1.
	leases := client.CoordinationV1().Leases("default")
	holder, seconds, rt := other, int32(3), metav1.NewMicroTime(t0)
	m, err := leases.Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: memberName("g", "pod-1"), Labels: leaseLabels("g", kindMember, "")},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &seconds, RenewTime: &rt},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	slot, err := leases.Create(ctx, slotLease("g", 0, other, m.UID), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	count, err := leases.Create(ctx, slotsLease("g", "1"), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// With no informer, Slots lists once; every holder is first seen then, so live.
	client.ClearActions()
	wantSlots(t, a, other)
	if got := client.Actions(); len(got) != 1 || got[0].GetVerb() != "list" {
		t.Errorf("Slots with no informer sent %v, want one LIST", got)
	}

	opened := a.freshChanged()
	if err := a.KeepAlive(ctx, own, long); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	signalled(t, opened, "a session opened")
	waitFresh(t, a, true)
	if _, next := a.freshness(); !next.Equal(t0.Add(long.RenewDeadline)) {
		t.Errorf("fresh until %v, want %v", next, t0.Add(long.RenewDeadline))
	}
	for _, l := range []*coordinationv1.Lease{m, slot, count} {
		waitObserved(t, a, l.Name, l.ResourceVersion)
	}
	wantSlots(t, a, other)

	// No own write delivered for RenewDeadline: stale until the session would expire.
	clock.advance(long.RenewDeadline)
	if fresh, next := a.freshness(); fresh || !next.Equal(t0.Add(long.LeaseDuration)) {
		t.Errorf("freshness after RenewDeadline = %v, %v; want false, %v", fresh, next, t0.Add(long.LeaseDuration))
	}
	// The other session is expired by its wait, yet stays held.
	obs, tombs, _ := a.snapshot()
	if v := a.slotVerdict(obs[slot.Name], obs, tombs, clock.now(), true); v.live || v.why != caseExpired {
		t.Fatalf("verdict on slot 0 when fresh %+v, want gone as expired", v)
	}
	client.ClearActions()
	wantSlots(t, a, other)
	got, err := a.ClaimSlot(ctx, own)
	wantNoSlot(t, got, err, "k8s: no free slot among 1: tenure: no free slot")
	if got := client.Actions(); len(got) != 0 {
		t.Errorf("Slots and ClaimSlot on a stale cache sent %v, want nothing", got)
	}
	clock.advance(5 * time.Second)
	wantSlots(t, a, other)

	// A write by someone else to the own member Lease is no own write.
	l := fakeGet(t, client, memberName("g", "pod-0"))
	l.Annotations = map[string]string{"example.com/note": "x"}
	if l, err = leases.Update(ctx, l, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	waitObserved(t, a, l.Name, l.ResourceVersion)
	if fresh, _ := a.freshness(); fresh {
		t.Error("fresh after another client wrote the own member Lease")
	}

	// The renewal, delivered, makes the cache fresh again; the expired session's slot is free.
	renewed := a.freshChanged()
	if err := a.KeepAlive(ctx, own, long); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	signalled(t, renewed, "a stale cache turned fresh")
	waitFresh(t, a, true)
	wantSlots(t, a, "")
	mustClaim(t, a, own, 0)
	if l := fakeGet(t, client, slot.Name); holderOf(l) != own {
		t.Errorf("slot 0 names %q, want %s", holderOf(l), own)
	}

	ended := a.freshChanged()
	if err := a.CloseSession(ctx, own); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	signalled(t, ended, "a session ended")
	if fresh, next := a.freshness(); !fresh || !next.IsZero() {
		t.Errorf("freshness after CloseSession = %v, %v; want true, none", fresh, next)
	}

	t.Run("joined", func(t *testing.T) {
		clock := newFakeClock()
		// The heartbeat beats every 6 s: none during the test.
		slow := core.Timing{LeaseDuration: 20 * time.Second, RenewDeadline: 8 * time.Second, RetryPeriod: 6 * time.Second, SlotGrace: 20 * time.Second}
		b := fakeAdapter(t, newFake(), withClock(clock.now), WithTiming(slow))
		joined := b.freshChanged()
		mustJoin(t, b, member("pod-2"))
		signalled(t, joined, "Join")
		waitFresh(t, b, true)
		clock.advance(slow.RenewDeadline)
		if fresh, next := b.freshness(); fresh || !next.IsZero() {
			t.Errorf("freshness of a joined member after RenewDeadline = %v, %v; want false, none", fresh, next)
		}
		left := b.freshChanged()
		if err := b.Leave(t.Context()); err != nil {
			t.Fatalf("Leave: %v", err)
		}
		signalled(t, left, "Leave")
		if fresh, _ := b.freshness(); !fresh {
			t.Error("not fresh after Leave")
		}
	})

	t.Run("slow renewal", func(t *testing.T) {
		clock := newFakeClock()
		client := newFake()
		b := fakeAdapter(t, client, withClock(clock.now))
		closeAtEnd(t, b, own)
		if err := b.KeepAlive(ctx, own, long); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		waitFresh(t, b, true)
		// A renewal with a shorter RenewDeadline changes the rule's bound; on a fresh cache, its
		// delivery signals nothing.
		shorter := long
		shorter.RenewDeadline -= time.Second
		changed := b.freshChanged()
		if err := b.KeepAlive(ctx, own, shorter); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		signalled(t, changed, "a session's RenewDeadline changed")
		name := memberName("g", "pod-0")
		waitDelivered(t, b, name, fakeGet(t, client, name).ResourceVersion)
		if _, next := b.freshness(); !next.Equal(clock.now().Add(shorter.RenewDeadline)) {
			t.Errorf("fresh until %v, want %v", next, clock.now().Add(shorter.RenewDeadline))
		}

		// The renewal's request takes RenewDeadline: its delivery shows no change committed
		// since it was sent, so it leaves the cache stale.
		var slow sync.Once
		client.PrependReactor("update", resourceLeases, func(k8stesting.Action) (bool, runtime.Object, error) {
			slow.Do(func() { clock.advance(shorter.RenewDeadline) })
			return false, nil, nil
		})
		if err := b.KeepAlive(ctx, own, shorter); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		waitDelivered(t, b, name, fakeGet(t, client, name).ResourceVersion)
		waitFresh(t, b, false)
		if err := b.KeepAlive(ctx, own, shorter); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		waitFresh(t, b, true)
	})

	t.Run("delivered before the response", func(t *testing.T) {
		clock := newFakeClock()
		c := &leaseCache{now: clock.now, pending: map[string][]pendingWrite{}, delivered: map[string]uint64{}}
		lease := func(rv string) *coordinationv1.Lease {
			return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "g-member-pod-0", ResourceVersion: rv, Labels: leaseLabels("g", kindMember, "")}}
		}
		t0 := clock.now()
		if _, ok := c.ownWrite(lease("10"), t0); ok {
			t.Error("an own write not delivered yet counted")
		}
		clock.advance(time.Second)
		if prev, own := c.deliver(lease("10")); !own || !prev.IsZero() {
			t.Errorf("deliver = %v, %v; want none, true", prev, own)
		}
		if !c.ownSentAt.Equal(t0) {
			t.Errorf("own write counted at %v, want its send time %v", c.ownSentAt, t0)
		}
		if _, own := c.deliver(lease("12")); own {
			t.Error("a delivery no own write waits for counted")
		}
		t1 := clock.advance(time.Second)
		if prev, ok := c.ownWrite(lease("11"), t1); !ok || !prev.Equal(t0) {
			t.Errorf("ownWrite of a write delivered already = %v, %v; want %v, true", prev, ok, t0)
		}
		if !c.ownSentAt.Equal(t1) {
			t.Errorf("own write counted at %v, want its send time %v", c.ownSentAt, t1)
		}
	})
}

// waitDelivered waits up to 2 s for a's informer to deliver Lease name at resourceVersion rv or
// later. Unlike waitObserved, it does not count the Adapter's own notes of its writes.
func waitDelivered(t testing.TB, a *Adapter, name, rv string) {
	t.Helper()
	want, _ := parseUint(rv)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if c := a.runningCache(); c != nil {
			c.mu.Lock()
			got := c.delivered[name]
			c.mu.Unlock()
			if got >= want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s at %s not delivered after 2s", name, rv)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestK8S400SlotChecks checks what ClaimSlot, ReleaseSlot, SetSlots and Slots return with a fake
// client: ctx.Err() first and the argument errors, with no request; ClaimSlot's error matching
// tenure.ErrSessionLost for a holder with no active session, with no request; an error for a
// malformed count; and a Lease at a slot name that lacks the group's labels, which Slots skips
// and the writers refuse to write. ClaimSlot returns -1 with every error.
func TestK8S400SlotChecks(t *testing.T) {
	ctx := t.Context()
	client := newFake()
	a := fakeAdapter(t, client)
	closeAtEnd(t, a, "pod-0/1")
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	none := func(t *testing.T) {
		t.Helper()
		if got := client.Actions(); len(got) != 0 {
			t.Errorf("requests %v, want none", got)
		}
	}
	claim := func(ctx context.Context, holder string) error {
		slot, err := a.ClaimSlot(ctx, holder)
		if slot != -1 {
			t.Errorf("ClaimSlot(%q) = %d, %v; want -1", holder, slot, err)
		}
		return err
	}
	t.Run("canceled", func(t *testing.T) {
		client.ClearActions()
		for name, err := range map[string]error{
			"ClaimSlot":   claim(canceled, ""),
			"ReleaseSlot": a.ReleaseSlot(canceled, "", -1),
			"SetSlots":    a.SetSlots(canceled, -1),
		} {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s with a canceled ctx = %v, want %v", name, err, context.Canceled)
			}
		}
		if _, _, err := a.Slots(canceled); !errors.Is(err, context.Canceled) {
			t.Errorf("Slots with a canceled ctx = %v, want %v", err, context.Canceled)
		}
		none(t)
	})
	t.Run("no session", func(t *testing.T) {
		client.ClearActions()
		err := claim(ctx, "pod-1/1")
		if !errors.Is(err, tenure.ErrSessionLost) || err.Error() != "k8s: session pod-1/1 is not open: tenure: session lost" {
			t.Errorf("ClaimSlot without a session = %v, want k8s: session pod-1/1 is not open: tenure: session lost", err)
		}
		none(t)
	})

	mustKeepAlive(t, a, "pod-0/1")
	leases := client.CoordinationV1().Leases("default")
	t.Run("malformed count", func(t *testing.T) {
		l, err := leases.Create(ctx, slotsLease("g", "x"), metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		waitObserved(t, a, l.Name, l.ResourceVersion)
		const want = `k8s: lease g-slots: tenure.slots "x" is not a slot count in [0, 10000]`
		if count, holders, err := a.Slots(ctx); count != 0 || holders != nil || err == nil || err.Error() != want {
			t.Errorf("Slots = %d, %q, %v; want 0, nil, %s", count, holders, err, want)
		}
		if err := claim(ctx, "pod-0/1"); err == nil || err.Error() != want {
			t.Errorf("ClaimSlot = %v, want %s", err, want)
		}
		// SetSlots repairs it.
		if err := a.SetSlots(ctx, 2); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		wantSlots(t, a, "", "")
		if err := leases.Delete(ctx, l.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		waitForgotten(t, a, l.Name)
	})
	t.Run("foreign", func(t *testing.T) {
		foreign := func(name string) *coordinationv1.Lease {
			holder := "pod-0/1"
			l, err := leases.Create(ctx, &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{annotationSlots: "3"}},
				Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
			}, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			return l
		}
		count := foreign(slotsName("g"))
		client.ClearActions()
		wantSlots(t, a)
		none(t)
		want := "k8s: lease g-slots is not a slots lease of group g"
		if err := claim(ctx, "pod-0/1"); err == nil || err.Error() != want {
			t.Errorf("ClaimSlot = %v, want %s", err, want)
		}
		if err := a.SetSlots(ctx, 1); err == nil || err.Error() != want {
			t.Errorf("SetSlots = %v, want %s", err, want)
		}
		if got := writes(client); len(got) != 0 {
			t.Errorf("writes %v, want none", got)
		}
		if err := leases.Delete(ctx, count.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}

		if err := a.SetSlots(ctx, 1); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		slot := foreign(slotName("g", 0))
		client.ClearActions()
		wantSlots(t, a, "")
		none(t)
		want = "k8s: lease g-slot-0 is not a slot lease of group g"
		// The informer does not see the Lease: ClaimSlot's create finds it there.
		if err := claim(ctx, "pod-0/1"); err == nil || err.Error() != want {
			t.Errorf("ClaimSlot = %v, want %s", err, want)
		}
		if err := a.ReleaseSlot(ctx, "pod-0/1", 0); err == nil || err.Error() != want {
			t.Errorf("ReleaseSlot = %v, want %s", err, want)
		}
		if got := actions(client, "update"); len(got) != 0 {
			t.Errorf("updates %v, want none", got)
		}
		if l := fakeGet(t, client, slot.Name); l.ResourceVersion != slot.ResourceVersion {
			t.Errorf("%s written: resourceVersion %s, want %s", slot.Name, l.ResourceVersion, slot.ResourceVersion)
		}
	})
}

// TestATK8S24Slots checks the slot count and Slots on a cluster: SetSlots creates <group>-slots
// with its labels and tenure.slots, then changes only tenure.slots with an update from the
// state it read, and writes nothing when the count is the same; counts 0 to 10000 and their
// errors; Slots lists once with no informer, reads the cache with one, returns 0, nil, nil for 0
// and one entry per slot below the count, with the holder while its session is live, "" when
// its session is observed unchanged for its LeaseDuration, and leaves out retired slots. The
// acceptance test's steps were written from the issue's text: the original text is unavailable.
func TestATK8S24Slots(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	name := slotsName(g)
	other := func(r request) bool { return r.resource == resourceLeases && r.verb != verbWatch }

	t.Run("count", func(t *testing.T) {
		rec.reset()
		wantSlots(t, a)
		lists := rec.filter(other)
		if len(lists) != 1 || lists[0].verb != verbList || lists[0].query.Get("labelSelector") != labelGroup+"="+g {
			t.Errorf("Slots with no informer sent %v, want one LIST of the group's Leases", requestStrings(lists))
		}

		rec.reset()
		if err := a.SetSlots(ctx, 3); err != nil {
			t.Fatalf("SetSlots(3): %v", err)
		}
		if got, want := slotSteps(rec, other), []string{"GET " + name + " 404", "CREATE " + name + " 201"}; !slices.Equal(got, want) {
			t.Errorf("SetSlots(3) sent %v, want %v", got, want)
		}
		l := rawGet(t, c, name)
		if !maps.Equal(l.Labels, leaseLabels(g, kindSlots, "")) || !maps.Equal(l.Annotations, map[string]string{annotationSlots: "3"}) || holderOf(l) != "" {
			t.Errorf("%s labels %v, annotations %v, holder %q; want the slots labels, tenure.slots 3, no holder", name, l.Labels, l.Annotations, holderOf(l))
		}
		wantSlots(t, a, "", "", "")

		// Labels and annotations of others survive; only tenure.slots changes.
		before := rawUpdate(t, c, name, func(l *coordinationv1.Lease) {
			l.Labels["example.com/team"] = "a"
			l.Annotations["example.com/note"] = "x"
		})
		rec.reset()
		if err := a.SetSlots(ctx, 5); err != nil {
			t.Fatalf("SetSlots(5): %v", err)
		}
		ups := rec.filter(on(verbUpdate, name))
		if len(ups) != 1 || ups[0].lease().ResourceVersion != before.ResourceVersion {
			t.Errorf("SetSlots(5) sent %v, want one UPDATE from resourceVersion %s", requestStrings(rec.filter(other)), before.ResourceVersion)
		}
		after := rawGet(t, c, name)
		wantAnn := maps.Clone(before.Annotations)
		wantAnn[annotationSlots] = "5"
		if !maps.Equal(after.Labels, before.Labels) || !maps.Equal(after.Annotations, wantAnn) || !equalSpec(after.Spec, before.Spec) {
			t.Errorf("after SetSlots(5): labels %v, annotations %v; want %v, %v and the spec unchanged", after.Labels, after.Annotations, before.Labels, wantAnn)
		}

		rec.reset()
		if err := a.SetSlots(ctx, 5); err != nil {
			t.Fatalf("SetSlots(5) again: %v", err)
		}
		if got, want := slotSteps(rec, other), []string{"GET " + name + " 200"}; !slices.Equal(got, want) {
			t.Errorf("SetSlots of the same count sent %v, want %v", got, want)
		}

		if err := a.SetSlots(ctx, 0); err != nil {
			t.Fatalf("SetSlots(0): %v", err)
		}
		wantSlots(t, a)
		if err := a.SetSlots(ctx, maxSlots); err != nil {
			t.Fatalf("SetSlots(10000): %v", err)
		}
		if count, holders, err := a.Slots(ctx); err != nil || count != maxSlots || len(holders) != maxSlots || slices.ContainsFunc(holders, func(h string) bool { return h != "" }) {
			t.Errorf("Slots = %d, %d holders, %v; want 10000 free slots", count, len(holders), err)
		}
		rec.reset()
		for _, n := range []int{maxSlots + 1, -1} {
			want := fmt.Sprintf("k8s: SetSlots: count %d: must be in [0, 10000]", n)
			if err := a.SetSlots(ctx, n); err == nil || err.Error() != want {
				t.Errorf("SetSlots(%d) = %v, want %s", n, err, want)
			}
		}
		if got := rec.requests(); len(got) != 0 {
			t.Errorf("invalid counts sent %v, want nothing", requestStrings(got))
		}
	})

	t.Run("holders", func(t *testing.T) {
		const h0, h1 = "pod-0/1", "pod-1/1"
		closeAtEnd(t, a, h0)
		closeAtEnd(t, b, h1)
		mustKeepAlive(t, a, h0)
		startRenewer(t, a, h0)
		mustKeepAlive(t, b, h1)
		if err := a.SetSlots(ctx, 3); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		mustClaim(t, a, h0, 0)
		waitSlots(t, b, h0, "", "")
		mustClaim(t, b, h1, 1)
		lastSent := time.Now()
		if err := b.KeepAlive(ctx, h1, T); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		waitSlots(t, a, h0, h1, "")

		// The informer runs: Slots sends nothing.
		rec.reset()
		wantSlots(t, a, h0, h1, "")
		if got := rec.filter(func(r request) bool { return other(r) && r.name != memberName(g, "pod-0") }); len(got) != 0 {
			t.Errorf("Slots with an informer sent %v, want nothing", requestStrings(got))
		}

		// h1's session is no longer renewed: its slot reads free once a has observed its member
		// Lease unchanged for 3 s.
		var gone time.Time
		for deadline := lastSent.Add(T.LeaseDuration + 3*time.Second); gone.IsZero(); time.Sleep(50 * time.Millisecond) {
			_, holders, err := a.Slots(ctx)
			if err != nil {
				t.Fatalf("Slots: %v", err)
			}
			switch {
			case holders[1] == "":
				gone = time.Now()
			case time.Now().After(deadline):
				t.Fatalf("slot 1 still held by %s %v after the last renewal", holders[1], time.Since(lastSent))
			}
		}
		if gone.Before(lastSent.Add(T.LeaseDuration)) {
			t.Errorf("slot 1 free %v after the last renewal was sent, want at least %v", gone.Sub(lastSent), T.LeaseDuration)
		}
		wantSlots(t, a, h0, "", "")

		// Lowering the count retires slots: Slots leaves them out.
		if err := a.SetSlots(ctx, 1); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		wantSlots(t, a, h0)
	})
}

// TestATK8S25ClaimSlot checks ClaimSlot on a cluster: it claims the lowest free slot with the
// record's fields; returns the holder's own slot with no request; passes slots of live holders
// without a request on them; decides again on the same slot after a conflict; gives racing
// claims distinct slots; returns -1 and tenure.ErrNoSlot when every slot is held or the holder's
// slot is retired; takes the slot of a holder observed unchanged for its LeaseDuration only
// after that, reading its member Lease first; and returns -1 with tenure.ErrSessionLost without
// a session, and with a failed request. The acceptance test's steps were written from the
// issue's text: the original text is unavailable.
func TestATK8S25ClaimSlot(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	const h0, h1, h2, h3 = "pod-0/1", "pod-1/1", "pod-2/1", "pod-3/1"
	closeAtEnd(t, a, h0, h3)
	closeAtEnd(t, b, h1, h2)
	mustKeepAlive(t, a, h0)
	startRenewer(t, a, h0)
	mustKeepAlive(t, b, h1)
	r1 := startRenewer(t, b, h1)
	mustKeepAlive(t, b, h2)
	startRenewer(t, b, h2)
	mustKeepAlive(t, a, h3)
	startRenewer(t, a, h3)
	if err := a.SetSlots(ctx, 3); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	notWatch := func(r request) bool { return r.resource == resourceLeases && r.verb != verbWatch }
	// notRenewal matches the requests but WATCHes and those on member Leases, which the
	// renewers send.
	notRenewal := func(r request) bool { return notWatch(r) && !strings.HasPrefix(r.name, g+memberInfix) }

	t.Run("lowest free", func(t *testing.T) {
		rec.reset()
		from := time.Now()
		mustClaim(t, a, h0, 0)
		to := time.Now()
		if got, want := slotSteps(rec, slotRequests(g)), []string{"CREATE " + slotName(g, 0) + " 201"}; !slices.Equal(got, want) {
			t.Errorf("requests on slot Leases %v, want %v", got, want)
		}
		l := rawGet(t, c, slotName(g, 0))
		m := rawGet(t, c, memberName(g, "pod-0"))
		s := l.Spec
		var at time.Time
		if s.AcquireTime != nil {
			at = s.AcquireTime.Time
		}
		switch {
		case !maps.Equal(l.Labels, leaseLabels(g, kindSlot, "")):
			t.Errorf("labels %v, want the slot labels", l.Labels)
		case holderOf(l) != h0 || l.Annotations[annotationSession] != string(m.UID):
			t.Errorf("holder %q, tenure.session %q; want %s, %s", holderOf(l), l.Annotations[annotationSession], h0, m.UID)
		case s.AcquireTime == nil || s.RenewTime == nil || !s.AcquireTime.Equal(s.RenewTime) ||
			at.Before(from.Truncate(time.Microsecond)) || at.After(to):
			t.Errorf("acquireTime %v, renewTime %v; want equal, within [%v, %v]", s.AcquireTime, s.RenewTime, from, to)
		case s.LeaseDurationSeconds == nil || *s.LeaseDurationSeconds != 3 || s.LeaseTransitions == nil || *s.LeaseTransitions != 0:
			t.Errorf("leaseDurationSeconds %v, leaseTransitions %v; want 3, 0", s.LeaseDurationSeconds, s.LeaseTransitions)
		}
	})

	t.Run("own slot", func(t *testing.T) {
		rec.reset()
		mustClaim(t, a, h0, 0)
		if got := rec.filter(notRenewal); len(got) != 0 {
			t.Errorf("ClaimSlot of the own slot sent %v, want nothing", requestStrings(got))
		}
	})

	t.Run("live holders passed", func(t *testing.T) {
		waitSlots(t, b, h0, "", "")
		recB.reset()
		mustClaim(t, b, h1, 1)
		if got, want := slotSteps(recB, slotRequests(g)), []string{"CREATE " + slotName(g, 1) + " 201"}; !slices.Equal(got, want) {
			t.Errorf("requests on slot Leases %v, want %v", got, want)
		}
		if n := recB.count(on(verbGet, memberName(g, "pod-0"))); n != 0 {
			t.Errorf("%d GETs of the live holder's member Lease, want none", n)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		free := rawCreate(t, c, slotLease(g, 2, "", ""))
		waitObserved(t, b, free.Name, free.ResourceVersion)
		recB.reset()
		recB.bump(free.Name, time.Time{})
		mustClaim(t, b, h2, 2)
		want := []string{"UPDATE " + free.Name + " 409", "GET " + free.Name + " 200", "UPDATE " + free.Name + " 200"}
		if got := slotSteps(recB, slotRequests(g)); !slices.Equal(got, want) {
			t.Errorf("requests on slot Leases %v, want %v", got, want)
		}
		if l := rawGet(t, c, free.Name); holderOf(l) != h2 || l.Annotations[bumpAnnotation] != "1" {
			t.Errorf("%s names %q, annotations %v; want %s with the bump kept", free.Name, holderOf(l), l.Annotations, h2)
		}
	})

	t.Run("full", func(t *testing.T) {
		waitSlots(t, a, h0, h1, h2)
		rec.reset()
		got, err := a.ClaimSlot(ctx, h3)
		wantNoSlot(t, got, err, "k8s: no free slot among 3: tenure: no free slot")
		if got := rec.filter(notRenewal); len(got) != 0 {
			t.Errorf("ClaimSlot with every slot held sent %v, want nothing", requestStrings(got))
		}
	})

	t.Run("race", func(t *testing.T) {
		if err := a.SetSlots(ctx, 7); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		waitSlots(t, a, h0, h1, h2, "", "", "", "")
		waitSlots(t, b, h0, h1, h2, "", "", "", "")
		holders := []struct {
			a *Adapter
			h string
		}{{a, "pod-4/1"}, {b, "pod-5/1"}, {a, "pod-6/1"}, {b, "pod-7/1"}}
		for _, x := range holders {
			closeAtEnd(t, x.a, x.h)
			mustKeepAlive(t, x.a, x.h)
		}
		got := make([]int, len(holders))
		var wg sync.WaitGroup
		for i, x := range holders {
			wg.Go(func() {
				slot, err := x.a.ClaimSlot(ctx, x.h)
				if err != nil {
					t.Errorf("ClaimSlot(%s): %v", x.h, err)
				}
				got[i] = slot
			})
		}
		wg.Wait()
		seen := map[int]bool{}
		for i, slot := range got {
			if slot < 3 || slot > 6 || seen[slot] {
				t.Errorf("ClaimSlot results %v, want distinct slots 3 to 6", got)
				break
			}
			seen[slot] = true
			if l := rawGet(t, c, slotName(g, slot)); holderOf(l) != holders[i].h {
				t.Errorf("slot %d names %q, want %s", slot, holderOf(l), holders[i].h)
			}
		}
		for _, x := range holders {
			if err := x.a.CloseSession(ctx, x.h); err != nil {
				t.Fatalf("CloseSession(%s): %v", x.h, err)
			}
		}
	})

	t.Run("retired", func(t *testing.T) {
		if err := a.SetSlots(ctx, 2); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		waitSlots(t, b, h0, h1)
		got, err := b.ClaimSlot(ctx, h2)
		wantNoSlot(t, got, err, "k8s: "+h2+" holds slot 2, not below the count 2: tenure: no free slot")
	})

	t.Run("gone holder", func(t *testing.T) {
		waitSlots(t, a, h0, h1)
		r1.stop()
		lastSent := time.Now()
		if err := b.KeepAlive(ctx, h1, T); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		var claimedAt time.Time
		for deadline := lastSent.Add(T.LeaseDuration + 3*time.Second); claimedAt.IsZero(); time.Sleep(100 * time.Millisecond) {
			rec.reset()
			slot, err := a.ClaimSlot(ctx, h3)
			switch {
			case err == nil && slot == 1:
				claimedAt = time.Now()
			case !errors.Is(err, tenure.ErrNoSlot):
				t.Fatalf("ClaimSlot = %d, %v; want 1 or tenure.ErrNoSlot", slot, err)
			case time.Now().After(deadline):
				t.Fatalf("slot 1 not claimed %v after the last renewal", time.Since(lastSent))
			}
		}
		if claimedAt.Before(lastSent.Add(T.LeaseDuration)) {
			t.Errorf("slot 1 claimed %v after the last renewal was sent, want at least %v", claimedAt.Sub(lastSent), T.LeaseDuration)
		}
		m1, s1 := memberName(g, "pod-1"), slotName(g, 1)
		want := []string{"GET " + m1 + " 200", "UPDATE " + s1 + " 200"}
		if got := slotSteps(rec, func(r request) bool { return notWatch(r) && (r.name == m1 || r.name == s1) }); !slices.Equal(got, want) {
			t.Errorf("requests %v, want %v", got, want)
		}
		if l := rawGet(t, c, s1); holderOf(l) != h3 || l.Spec.LeaseTransitions == nil || *l.Spec.LeaseTransitions != 1 {
			t.Errorf("slot 1 names %q with leaseTransitions %v, want %s with 1", holderOf(l), l.Spec.LeaseTransitions, h3)
		}
	})

	t.Run("errors", func(t *testing.T) {
		rec.reset()
		got, err := a.ClaimSlot(ctx, "pod-9/1")
		if got != -1 || !errors.Is(err, tenure.ErrSessionLost) {
			t.Errorf("ClaimSlot without a session = %d, %v; want -1 and tenure.ErrSessionLost", got, err)
		}
		if got := rec.requests(); len(got) != 0 {
			t.Errorf("ClaimSlot without a session sent %v", requestStrings(got))
		}

		// Slots 0 to 2 are held; slot 3 has a record with no holder since the race.
		if err := a.SetSlots(ctx, 4); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		waitSlots(t, a, h0, h3, h2, "")
		const h = "pod-8/1"
		closeAtEnd(t, a, h)
		mustKeepAlive(t, a, h)
		// Two claims whose write fails return -1 and the error; the next one claims the slot.
		rec.failNext(on(verbUpdate, slotName(g, 3)), 2)
		for range 2 {
			got, err = a.ClaimSlot(ctx, h)
			if prefix := "k8s: claim " + slotName(g, 3) + ": "; got != -1 || err == nil || !strings.HasPrefix(err.Error(), prefix) {
				t.Errorf("ClaimSlot with a failed update = %d, %v; want -1, %s<a server error>", got, err, prefix)
			}
		}
		if l := rawGet(t, c, slotName(g, 3)); holderOf(l) != "" {
			t.Errorf("slot 3 names %q after failed claims, want no holder", holderOf(l))
		}
		mustClaim(t, a, h, 3)
	})
}

// TestATK8S26ReleaseSlot checks ReleaseSlot on a cluster: a record that names the holder loses
// its holderIdentity and tenure.session and keeps everything else, also after a conflict; a
// record of another holder or incarnation, or none, is checked against the API server and left
// with no write; it needs no session; the slot is free for a claim at once. The acceptance
// test's steps were written from the issue's text: the original text is unavailable.
func TestATK8S26ReleaseSlot(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	const h0, h1 = "pod-0/1", "pod-1/1"
	closeAtEnd(t, a, h0)
	closeAtEnd(t, b, h1)
	mustKeepAlive(t, a, h0)
	startRenewer(t, a, h0)
	mustKeepAlive(t, b, h1)
	startRenewer(t, b, h1)
	if err := a.SetSlots(ctx, 2); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	mustClaim(t, a, h0, 0)
	s0, s1 := slotName(g, 0), slotName(g, 1)

	t.Run("other holder", func(t *testing.T) {
		waitSlots(t, b, h0, "")
		before := rawGet(t, c, s0)
		for _, x := range []struct {
			holder string
			slot   int
			want   []string
		}{
			{h1, 0, []string{"GET " + s0 + " 200"}},
			{"pod-0/2", 0, []string{"GET " + s0 + " 200"}},
			{h1, 1, []string{"GET " + s1 + " 404"}},
		} {
			recB.reset()
			if err := b.ReleaseSlot(ctx, x.holder, x.slot); err != nil {
				t.Fatalf("ReleaseSlot(%s, %d): %v", x.holder, x.slot, err)
			}
			if got := slotSteps(recB, slotRequests(g)); !slices.Equal(got, x.want) {
				t.Errorf("ReleaseSlot(%s, %d) sent %v, want %v", x.holder, x.slot, got, x.want)
			}
		}
		if l := rawGet(t, c, s0); l.ResourceVersion != before.ResourceVersion {
			t.Errorf("%s written: resourceVersion %s, want %s", s0, l.ResourceVersion, before.ResourceVersion)
		}
	})

	t.Run("holder", func(t *testing.T) {
		before := rawUpdate(t, c, s0, func(l *coordinationv1.Lease) {
			l.Labels["example.com/team"] = "a"
			l.Annotations["example.com/note"] = "x"
		})
		waitObserved(t, a, s0, before.ResourceVersion)
		rec.reset()
		if err := a.ReleaseSlot(ctx, h0, 0); err != nil {
			t.Fatalf("ReleaseSlot: %v", err)
		}
		if got, want := slotSteps(rec, slotRequests(g)), []string{"UPDATE " + s0 + " 200"}; !slices.Equal(got, want) {
			t.Errorf("ReleaseSlot sent %v, want %v", got, want)
		}
		after := rawGet(t, c, s0)
		wantAnn := maps.Clone(before.Annotations)
		delete(wantAnn, annotationSession)
		wantSpec := before.Spec
		wantSpec.HolderIdentity = nil
		if !maps.Equal(after.Labels, before.Labels) || !maps.Equal(after.Annotations, wantAnn) || !equalSpec(after.Spec, wantSpec) || holderOf(after) != "" {
			t.Errorf("after ReleaseSlot: labels %v, annotations %v, spec %+v; want %v, %v, %+v", after.Labels, after.Annotations, after.Spec,
				before.Labels, wantAnn, wantSpec)
		}
		waitSlots(t, b, "", "")
		mustClaim(t, b, h1, 0)
	})

	t.Run("conflict", func(t *testing.T) {
		waitSlots(t, a, h1, "")
		recB.reset()
		recB.bump(s0, time.Time{})
		if err := b.ReleaseSlot(ctx, h1, 0); err != nil {
			t.Fatalf("ReleaseSlot: %v", err)
		}
		want := []string{"UPDATE " + s0 + " 409", "GET " + s0 + " 200", "UPDATE " + s0 + " 200"}
		if got := slotSteps(recB, slotRequests(g)); !slices.Equal(got, want) {
			t.Errorf("ReleaseSlot sent %v, want %v", got, want)
		}
		if l := rawGet(t, c, s0); holderOf(l) != "" {
			t.Errorf("%s names %q, want no holder", s0, holderOf(l))
		}
	})

	t.Run("no session", func(t *testing.T) {
		mustClaim(t, b, h1, 0)
		d, recD := c.adapter(t, g)
		if err := d.ReleaseSlot(ctx, h1, 0); err != nil {
			t.Fatalf("ReleaseSlot: %v", err)
		}
		want := []string{"GET " + s0 + " 200", "UPDATE " + s0 + " 200"}
		if got := slotSteps(recD, slotRequests(g)); !slices.Equal(got, want) {
			t.Errorf("ReleaseSlot with no session sent %v, want %v", got, want)
		}
		if l := rawGet(t, c, s0); holderOf(l) != "" {
			t.Errorf("%s names %q, want no holder", s0, holderOf(l))
		}
	})
}

// TestATK8S13DeletedMemberLease checks a slot whose holder's member Lease is deleted: the slot
// stays held while the tombstone of the deleted Lease lasts, LeaseDuration from when the observer
// last saw the session renew, and is free for Slots and ClaimSlot from then on. The acceptance
// test's steps were written from the issue's text: the original text is unavailable.
func TestATK8S13DeletedMemberLease(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, _ := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	const h0, h1, h2, h3 = "pod-0/1", "pod-1/1", "pod-2/1", "pod-3/1"
	closeAtEnd(t, a, h0, h2, h3)
	closeAtEnd(t, b, h1)
	for _, h := range []string{h0, h2, h3} {
		mustKeepAlive(t, a, h)
		startRenewer(t, a, h)
	}
	// The deleted session's Lease lasts 6 s: room for the claim while its tombstone lasts, on a
	// loaded cluster too.
	six := core.Timing{LeaseDuration: 6 * time.Second, RenewDeadline: 4 * time.Second, RetryPeriod: time.Second, SlotGrace: 6 * time.Second}
	if err := b.KeepAlive(ctx, h1, six); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	if err := b.SetSlots(ctx, 2); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	mustClaim(t, b, h1, 0)
	waitSlots(t, a, h1, "")

	name := memberName(g, "pod-1")
	lastSent := time.Now()
	if err := b.KeepAlive(ctx, h1, six); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	waitObserved(t, a, name, rawGet(t, c, name).ResourceVersion)
	if err := c.leases().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete %s: %v", name, err)
	}
	waitForgotten(t, a, name)

	// While the tombstone lasts, slot 0 is held: another holder gets slot 1.
	if slot, err := a.ClaimSlot(ctx, h2); err != nil || slot != 1 {
		t.Fatalf("ClaimSlot(%s) = %d, %v %v after the last renewal; want 1", h2, slot, err, time.Since(lastSent))
	}
	wantSlots(t, a, h1, h2)
	var gone time.Time
	for deadline := lastSent.Add(six.LeaseDuration + 3*time.Second); gone.IsZero(); time.Sleep(50 * time.Millisecond) {
		_, holders, err := a.Slots(ctx)
		if err != nil {
			t.Fatalf("Slots: %v", err)
		}
		switch {
		case holders[0] == "":
			gone = time.Now()
		case time.Now().After(deadline):
			t.Fatalf("slot 0 still held by %s %v after the last renewal", holders[0], time.Since(lastSent))
		}
	}
	if gone.Before(lastSent.Add(six.LeaseDuration)) {
		t.Errorf("slot 0 free %v after the last renewal was sent, want at least %v", gone.Sub(lastSent), six.LeaseDuration)
	}
	wantSlots(t, a, "", h2)
	mustClaim(t, a, h3, 0)
	if err := b.KeepAlive(ctx, h1, six); !errors.Is(err, tenure.ErrSessionLost) {
		t.Errorf("KeepAlive after the member Lease was deleted = %v, want tenure.ErrSessionLost", err)
	}
}

// TestATK8S15CloseSession checks that CloseSession releases the holder's records after one LIST:
// shard records by name, then slot records by slot number, a retired slot too, each keeping its
// other fields; leaves other holders' records and foreign Leases; deletes the member Lease;
// goes on past a failed release and returns its error; and that a later CloseSession releases
// what is left. The acceptance test's steps were written from the issue's text: the original
// text is unavailable.
func TestATK8S15CloseSession(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	const h1 = "pod-1/1"
	closeAtEnd(t, b, h1)
	mustKeepAlive(t, b, h1)
	startRenewer(t, b, h1)
	if err := b.SetSlots(ctx, 12); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	mustClaim(t, b, h1, 0)
	steps := func() []string {
		return slotSteps(rec, func(r request) bool {
			return r.resource == resourceLeases && r.verb != verbWatch
		})
	}
	// slotsOf creates slot records naming holder, whose member Lease is m, at slots ns.
	slotsOf := func(t *testing.T, holder string, m *coordinationv1.Lease, ns ...int) map[int]*coordinationv1.Lease {
		out := map[int]*coordinationv1.Lease{}
		for _, n := range ns {
			l := slotLease(g, n, holder, m.UID)
			l.Labels["example.com/team"] = "a"
			out[n] = rawCreate(t, c, l)
		}
		return out
	}
	// released checks that slot record l, as created, names no holder and has no
	// tenure.session, its other fields kept.
	released := func(t *testing.T, l *coordinationv1.Lease) {
		t.Helper()
		got := rawGet(t, c, l.Name)
		if holderOf(got) != "" || len(got.Annotations) != 0 || !maps.Equal(got.Labels, l.Labels) {
			t.Errorf("%s names %q, labels %v, annotations %v; want no holder, %v and no annotations", l.Name, holderOf(got), got.Labels, got.Annotations, l.Labels)
		}
	}

	t.Run("release", func(t *testing.T) {
		const h = "pod-0/1"
		mustKeepAlive(t, a, h)
		m1, m3 := shardName(g, "markets", 1), shardName(g, "markets", 3)
		mustAcquire(t, a, h, "markets", 3)
		mustAcquire(t, a, h, "markets", 1)
		slots := slotsOf(t, h, rawGet(t, c, memberName(g, "pod-0")), 10, 2, 15)
		holder := h
		foreign := rawCreate(t, c, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: slotName(g, 5)},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
		})
		other := rawGet(t, c, slotName(g, 0))

		rec.reset()
		if err := a.CloseSession(ctx, h); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		want := []string{
			"LIST 200",
			"UPDATE " + m1 + " 200",
			"UPDATE " + m3 + " 200",
			"UPDATE " + slotName(g, 2) + " 200",
			"UPDATE " + slotName(g, 10) + " 200",
			"UPDATE " + slotName(g, 15) + " 200",
			"DELETE " + memberName(g, "pod-0") + " 200",
		}
		if got := steps(); !slices.Equal(got, want) {
			t.Errorf("requests %v, want %v", got, want)
		}
		for _, l := range slots {
			released(t, l)
		}
		for _, name := range []string{m1, m3} {
			if l := rawGet(t, c, name); holderOf(l) != "" {
				t.Errorf("%s names %q, want no holder", name, holderOf(l))
			}
		}
		for _, l := range []*coordinationv1.Lease{foreign, other} {
			if got := rawGet(t, c, l.Name); got.ResourceVersion != l.ResourceVersion {
				t.Errorf("%s written: resourceVersion %s, want %s", l.Name, got.ResourceVersion, l.ResourceVersion)
			}
		}
	})

	t.Run("failed release", func(t *testing.T) {
		const h = "pod-2/1"
		mustKeepAlive(t, a, h)
		mustAcquire(t, a, h, "markets", 7)
		slots := slotsOf(t, h, rawGet(t, c, memberName(g, "pod-2")), 4, 6)
		rec.failNext(on(verbUpdate, slotName(g, 4)), 1)
		err := a.CloseSession(ctx, h)
		prefix := "k8s: release " + slotName(g, 4) + ": "
		if err == nil || !strings.HasPrefix(err.Error(), prefix) || errors.Is(err, tenure.ErrSessionLost) {
			t.Errorf("CloseSession = %v, want %s<a server error>", err, prefix)
		}
		released(t, slots[6])
		if l := rawGet(t, c, shardName(g, "markets", 7)); holderOf(l) != "" {
			t.Errorf("shard 7 names %q, want no holder", holderOf(l))
		}
		if l := rawGet(t, c, slots[4].Name); holderOf(l) != h {
			t.Errorf("slot 4 names %q, want %s still", holderOf(l), h)
		}
		if rawGet(t, c, memberName(g, "pod-2")) != nil {
			t.Error("the member Lease still exists")
		}
		if err := a.KeepAlive(ctx, h, T); !errors.Is(err, tenure.ErrSessionLost) {
			t.Errorf("KeepAlive after CloseSession = %v, want tenure.ErrSessionLost", err)
		}
		if err := b.CloseSession(ctx, h); err != nil {
			t.Fatalf("CloseSession by another Adapter: %v", err)
		}
		released(t, slots[4])
	})
}
