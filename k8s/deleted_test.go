// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// bigTerm is a term far above the wall clock's in Unix milliseconds: core.NextTerm of it is
// bigTerm + 1.
const bigTerm = core.Term(5_000_000_000_000_000)

// setTerm sets the tenure.term of shard record name to term through the fake client.
func setTerm(t testing.TB, client *fake.Clientset, name string, term core.Term) *coordinationv1.Lease {
	t.Helper()
	l := fakeGet(t, client, name)
	l.Annotations[annotationTerm] = encodeTerm(term)
	l, err := client.CoordinationV1().Leases("default").Update(t.Context(), l, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("update %s: %v", name, err)
	}
	return l
}

// TestK8S800DeletedLeases deletes Leases behind the Adapter's back on a fake client: a deleted
// shard record leaves its term, so Confirm finds no record and Acquire grants the next term; a
// deleted slot record is vacant; without the count's Lease the count is 0; a renewal that finds
// its member Lease gone loses the session.
func TestK8S800DeletedLeases(t *testing.T) {
	const h1, h2, h3 = "pod-0/1", "pod-1/1", "pod-2/1"

	t.Run("shard", func(t *testing.T) {
		ctx := t.Context()
		client := newFake()
		a := fakeAdapter(t, client)
		closeAtEnd(t, a, h1)
		mustKeepAlive(t, a, h1)
		ch := mustWatch(t, a, ctx)
		name := shardName("g", "markets", 0)

		t0 := mustAcquire(t, a, h1, "markets", 0)
		nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: t0})
		l := setTerm(t, client, name, bigTerm)
		nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: bigTerm})
		waitObserved(t, a, name, l.ResourceVersion)
		if err := client.CoordinationV1().Leases("default").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0})
		want := tenure.Holding{Space: "markets", Shard: 0}
		if got, err := a.Confirm(ctx, h1, "markets", 0, bigTerm); err != nil || got != want {
			t.Errorf("Confirm of a deleted record = %+v, %v; want %+v", got, err, want)
		}
		if got := mustAcquire(t, a, h1, "markets", 0); got != bigTerm+1 {
			t.Errorf("Acquire of a deleted record = %d, want %d", got, bigTerm+1)
		}
		nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: bigTerm + 1})
	})

	t.Run("slots", func(t *testing.T) {
		ctx := t.Context()
		client := newFake()
		a := fakeAdapter(t, client)
		closeAtEnd(t, a, h1, h2, h3)
		for _, h := range []string{h1, h2, h3} {
			mustKeepAlive(t, a, h)
		}
		if err := a.SetSlots(ctx, 2); err != nil {
			t.Fatalf("SetSlots: %v", err)
		}
		mustClaim(t, a, h1, 0)
		mustClaim(t, a, h2, 1)
		leases := client.CoordinationV1().Leases("default")
		if err := leases.Delete(ctx, slotName("g", 0), metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		waitForgotten(t, a, slotName("g", 0))
		wantSlots(t, a, "", h2)
		mustClaim(t, a, h3, 0)
		slot, err := a.ClaimSlot(ctx, h1)
		wantNoSlot(t, slot, err, "k8s: no free slot among 2: tenure: no free slot")

		if err := leases.Delete(ctx, slotsName("g"), metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		waitForgotten(t, a, slotsName("g"))
		wantSlots(t, a)
		slot, err = a.ClaimSlot(ctx, h1)
		wantNoSlot(t, slot, err, "k8s: no free slot among 0: tenure: no free slot")
		// With no informer running, Slots lists the Leases: no count either.
		b := fakeAdapter(t, client)
		wantSlots(t, b)
	})

	t.Run("renewal", func(t *testing.T) {
		ctx := t.Context()
		client := newFake()
		a := fakeAdapter(t, client)
		closeAtEnd(t, a, h1)
		mustKeepAlive(t, a, h1)
		if err := client.CoordinationV1().Leases("default").Delete(ctx, memberName("g", "pod-0"), metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		const want = "k8s: session pod-0/1 lost: lease deleted: tenure: session lost"
		if err := a.KeepAlive(ctx, h1, T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive after its member Lease was deleted = %v, want %s", err, want)
		}
		if err := a.KeepAlive(ctx, h1, T); !errors.Is(err, tenure.ErrSessionLost) {
			t.Errorf("KeepAlive after the session was lost = %v, want an error matching tenure.ErrSessionLost", err)
		}
	})
}

// TestATK8S32DeletedShardLease deletes a shard Lease on a cluster: the deletion is sent to
// WatchHolders, Confirm finds no record, and the next grant has term T1 + 1, T1 being the
// deleted Lease's term. The acceptance test's steps were written from the issue's text: the
// original text is unavailable.
func TestATK8S32DeletedShardLease(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, _ := c.adapter(t, g)
	const h1 = "pod-0/1"
	closeAtEnd(t, a, h1)
	startRenewer(t, a, h1)
	mustKeepAlive(t, a, h1)
	ch := mustWatch(t, a, ctx)
	name := shardName(g, "markets", 0)

	t0 := mustAcquire(t, a, h1, "markets", 0)
	nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: t0})
	// T1 lies far above the wall clock, so the next grant is T1 + 1 only if T1 is kept.
	const t1 = bigTerm
	rawUpdate(t, c, name, func(l *coordinationv1.Lease) { l.Annotations[annotationTerm] = encodeTerm(t1) })
	nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: t1})
	if h, err := a.Confirm(ctx, h1, "markets", 0, t1); err != nil || h.Holder != h1 || h.Term != t1 {
		t.Fatalf("Confirm = %+v, %v; want the record of %s at %d", h, err, h1, t1)
	}

	if err := c.leases().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete %s: %v", name, err)
	}
	nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0})
	want := tenure.Holding{Space: "markets", Shard: 0}
	if got, err := a.Confirm(ctx, h1, "markets", 0, t1); err != nil || got != want {
		t.Errorf("Confirm of the deleted record = %+v, %v; want %+v", got, err, want)
	}
	if got := mustAcquire(t, a, h1, "markets", 0); got != t1+1 {
		t.Errorf("Acquire of the deleted record = %d, want T1 + 1 = %d", got, t1+1)
	}
	nextHolding(t, ch, tenure.Holding{Space: "markets", Shard: 0, Holder: h1, Term: t1 + 1})
	if l := rawGet(t, c, name); l == nil || l.Annotations[annotationTerm] != encodeTerm(t1+1) {
		t.Errorf("record after the grant = %+v, want term %d", l, t1+1)
	}
}

// TestATK8S33SlotLeasesAndNamespace deletes slot Leases, the count's Lease and then the whole
// namespace on a cluster: a deleted slot is vacant; without the count's Lease the count is 0;
// once the namespace is gone every session is lost, its renewal finding the member Lease
// deleted; in the namespace created anew a grant goes on from the term of the deleted record,
// and slots start vacant at count 0. The acceptance test's steps were written from the issue's
// text: the original text is unavailable.
func TestATK8S33SlotLeasesAndNamespace(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, _ := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	const h1, h2, h3, h4 = "pod-0/1", "pod-1/1", "pod-2/1", "pod-3/1"
	closeAtEnd(t, a, h1, h4)
	closeAtEnd(t, b, h2, h3)
	renewers := []*renewer{startRenewer(t, a, h1), startRenewer(t, b, h2), startRenewer(t, b, h3)}
	mustKeepAlive(t, a, h1)
	mustKeepAlive(t, b, h2)
	mustKeepAlive(t, b, h3)

	// A deleted slot Lease is vacant.
	if err := a.SetSlots(ctx, 2); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	mustClaim(t, a, h1, 0)
	mustClaim(t, b, h2, 1)
	if err := c.leases().Delete(ctx, slotName(g, 0), metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitSlots(t, a, "", h2)
	waitSlots(t, b, "", h2)
	mustClaim(t, b, h3, 0)
	slot, err := a.ClaimSlot(ctx, h1)
	wantNoSlot(t, slot, err, "k8s: no free slot among 2: tenure: no free slot")

	// Without the count's Lease the count is 0.
	if err := c.leases().Delete(ctx, slotsName(g), metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitSlots(t, a)
	waitSlots(t, b)
	slot, err = a.ClaimSlot(ctx, h1)
	wantNoSlot(t, slot, err, "k8s: no free slot among 0: tenure: no free slot")

	// The namespace goes, with every Lease in it. The watch holds the informer until the
	// shard record's deletion is seen.
	t0 := mustAcquire(t, a, h1, "markets", 0)
	const t1 = bigTerm
	rawUpdate(t, c, shardName(g, "markets", 0), func(l *coordinationv1.Lease) { l.Annotations[annotationTerm] = encodeTerm(t1) })
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := mustWatch(t, a, wctx)
	waitObserved(t, a, shardName(g, "markets", 0), rawGet(t, c, shardName(g, "markets", 0)).ResourceVersion)
	if err := c.raw.CoreV1().Namespaces().Delete(ctx, c.ns, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete namespace %s: %v", c.ns, err)
	}
	for {
		h, ok := recvHolding(t, ch, time.Minute)
		if !ok {
			t.Fatalf("WatchHolders closed before the deletion of the record at term %d (granted %d) was sent", t1, t0)
		}
		if h == (tenure.Holding{Space: "markets", Shard: 0, Version: h.Version}) {
			break
		}
	}
	cancel()
	const lost = "lost: lease deleted: tenure: session lost"
	for i, r := range renewers {
		select {
		case <-r.done:
		case <-time.After(time.Minute):
			t.Fatalf("renewer %d still renews a minute after the namespace was deleted", i)
		}
		_, errs := r.stop()
		if n := len(errs); n == 0 || !errors.Is(errs[n-1], tenure.ErrSessionLost) || !strings.HasSuffix(errs[n-1].Error(), lost) {
			t.Errorf("renewer %d errors %v, want the last ending in %q", i, errs, lost)
		}
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		_, err := c.raw.CoreV1().Namespaces().Get(ctx, c.ns, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %s still there 2 minutes after its deletion: %v", c.ns, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, err := c.raw.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: c.ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace %s again: %v", c.ns, err)
	}

	// In the namespace created anew: a new session; no count; grants go on from the deleted
	// record's term.
	mustKeepAlive(t, a, h4)
	startRenewer(t, a, h4)
	wantSlots(t, a)
	slot, err = a.ClaimSlot(ctx, h4)
	wantNoSlot(t, slot, err, "k8s: no free slot among 0: tenure: no free slot")
	if err := a.SetSlots(ctx, 2); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	mustClaim(t, a, h4, 0)
	wantSlots(t, a, h4, "")
	if got := mustAcquire(t, a, h4, "markets", 0); got != t1+1 {
		t.Errorf("Acquire in the namespace created anew = %d, want %d", got, t1+1)
	}
	if err := a.KeepAlive(ctx, h1, T); !errors.Is(err, tenure.ErrSessionLost) {
		t.Errorf("KeepAlive of a lost session = %v, want an error matching tenure.ErrSessionLost", err)
	}
}
