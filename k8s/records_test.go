// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// mustAcquire calls Acquire and fails the test unless it grants a term.
func mustAcquire(t testing.TB, a *Adapter, holder, space string, shard int) core.Term {
	t.Helper()
	term, err := a.Acquire(t.Context(), holder, space, shard)
	if err != nil || term == 0 {
		t.Fatalf("Acquire(%s, %s, %d) = %d, %v; want a term", holder, space, shard, term, err)
	}
	return term
}

// wantHeld checks that Acquire returned 0 and a *tenure.HeldError naming holder, with RetryAfter
// retry.
func wantHeld(t testing.TB, term core.Term, err error, holder string, retry time.Duration) {
	t.Helper()
	var held *tenure.HeldError
	if term != 0 || !errors.As(err, &held) || held.Holder != holder || held.RetryAfter != retry {
		t.Fatalf("Acquire = %d, %v; want 0 and a *tenure.HeldError naming %s with RetryAfter %v", term, err, holder, retry)
	}
}

// shardRequests matches the requests on shard Leases of group g, creations included.
func shardRequests(g string) match {
	return func(r request) bool {
		if r.resource != resourceLeases {
			return false
		}
		name := r.name
		if l := r.lease(); name == "" && l != nil {
			name = l.Name
		}
		kind, _, _, _, ok := parseName(g, name)
		return ok && kind == kindShard
	}
}

// grant is the outcome of acquireUntil.
type grant struct {
	term core.Term
	// at is when the granting call returned.
	at time.Time
	// held counts the calls that returned a *tenure.HeldError before.
	held int
}

// acquireUntil calls Acquire every interval until it grants a term, and fails the test when
// that has not happened by deadline or a call returns any error but a *tenure.HeldError naming
// by with RetryAfter 500ms.
func acquireUntil(t testing.TB, a *Adapter, holder, space string, shard int, by string, deadline time.Time, interval time.Duration) grant {
	t.Helper()
	var g grant
	for {
		term, err := a.Acquire(t.Context(), holder, space, shard)
		if err == nil {
			g.term, g.at = term, time.Now()
			return g
		}
		wantHeld(t, term, err, by, T.RetryPeriod)
		g.held++
		if time.Now().After(deadline) {
			t.Fatalf("Acquire(%s, %s, %d) still held by %s after %d tries", holder, space, shard, by, g.held)
		}
		time.Sleep(interval)
	}
}

// rawCreate creates Lease l through the raw client and returns it as stored.
func rawCreate(t testing.TB, c *cluster, l *coordinationv1.Lease) *coordinationv1.Lease {
	t.Helper()
	l, err := c.leases().Create(t.Context(), l, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create %s: %v", l.Name, err)
	}
	return l
}

// shardLease returns a shard Lease of group g at shard n of space with holder (none when
// empty) and annotations, as a test writes it.
func shardLease(g, space string, n int, holder string, annotations map[string]string) *coordinationv1.Lease {
	l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Name: shardName(g, space, n), Labels: leaseLabels(g, kindShard, space), Annotations: annotations,
	}}
	if holder != "" {
		l.Spec.HolderIdentity = &holder
	}
	return l
}

// checkGrant checks the fields of shard record l that Acquire granted (K8S-320): labels,
// holderIdentity, tenure.term, tenure.session, no tenure.confirm, acquireTime == renewTime within
// [from, to], leaseDurationSeconds 3 and leaseTransitions.
func checkGrant(t testing.TB, l *coordinationv1.Lease, g, space, holder string, term core.Term, session types.UID,
	transitions int32, from, to time.Time,
) {
	t.Helper()
	s := l.Spec
	if l.Labels[labelGroup] != g || l.Labels[labelKind] != kindShard || l.Labels[labelSpace] != space {
		t.Errorf("labels %v, want %s=%s, %s=shard and %s=%s", l.Labels, labelGroup, g, labelKind, labelSpace, space)
	}
	if holderOf(l) != holder {
		t.Errorf("holderIdentity %q, want %q", holderOf(l), holder)
	}
	if got := l.Annotations[annotationTerm]; got != encodeTerm(term) {
		t.Errorf("%s %q, want %d", annotationTerm, got, term)
	}
	if got := l.Annotations[annotationSession]; got != string(session) {
		t.Errorf("%s %q, want %s", annotationSession, got, session)
	}
	if got, ok := l.Annotations[annotationConfirm]; ok {
		t.Errorf("%s %q, want none", annotationConfirm, got)
	}
	if s.AcquireTime == nil || s.RenewTime == nil || !s.AcquireTime.Equal(s.RenewTime) {
		t.Errorf("acquireTime %v, renewTime %v; want equal", s.AcquireTime, s.RenewTime)
	} else if at := s.AcquireTime.Time; at.Before(from.Truncate(time.Microsecond)) || at.After(to) {
		t.Errorf("acquireTime %v, want within [%v, %v]", at, from, to)
	}
	if s.LeaseDurationSeconds == nil || *s.LeaseDurationSeconds != 3 {
		t.Errorf("leaseDurationSeconds %v, want 3", s.LeaseDurationSeconds)
	}
	if s.LeaseTransitions == nil || *s.LeaseTransitions != transitions {
		t.Errorf("leaseTransitions %v, want %d", s.LeaseTransitions, transitions)
	}
}

// wallTerms returns the terms core.NextTerm(0, wall) gives for wall in [from, to].
func wallTerms(from, to time.Time) (lo, hi core.Term) {
	return core.Term(from.UnixMilli()), core.Term(to.UnixMilli())
}

// TestK8S300AcquireChecks checks what Acquire returns without a request: ctx.Err() first, then
// the holder's, the space's and the shard's errors in that order, then an error matching
// tenure.ErrSessionLost for a holder with no active session in this Adapter: never opened,
// expired, or ended.
func TestK8S300AcquireChecks(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	a := fakeAdapter(t, client, withClock(clock.now))
	closeAtEnd(t, a, "pod-0/1")
	if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	if err := a.CloseSession(ctx, "pod-2/1"); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	tests := []struct {
		name   string
		ctx    context.Context
		holder string
		space  string
		shard  int
		want   string
		lost   bool
	}{
		{name: "canceled first", ctx: canceled, holder: "", space: "Bad", shard: -1, want: context.Canceled.Error()},
		{name: "empty holder", holder: "", space: "Bad", shard: -1, want: "k8s: Acquire: empty holder"},
		{name: "holder without /", holder: "pod-0", space: "markets", want: `k8s: Acquire: holder "pod-0": has no "/"`},
		{name: "holder with an invalid member", holder: "Pod/1", space: "markets", want: `k8s: Acquire: holder "Pod/1": member ID must be a DNS-1123 subdomain`},
		{name: "space Bad", holder: "pod-0/1", space: "Bad", shard: -1, want: `k8s: Acquire: space "Bad": must be a DNS-1123 label`},
		{name: "space empty", holder: "pod-0/1", space: "", want: `k8s: Acquire: space "": must be a DNS-1123 label`},
		{name: "space reserved", holder: "pod-0/1", space: "slots", want: `k8s: Acquire: space "slots": is reserved`},
		{name: "space member-x", holder: "pod-0/1", space: "member-x", want: `k8s: Acquire: space "member-x": must not start with "member-"`},
		{name: "negative shard", holder: "pod-0/1", space: "markets", shard: -1, want: "k8s: Acquire: shard -1: must not be negative"},
		{name: "never opened", holder: "pod-1/1", space: "markets", want: "k8s: session pod-1/1 is not open: tenure: session lost", lost: true},
		{name: "ended by CloseSession", holder: "pod-2/1", space: "markets", want: "k8s: session pod-2/1 ended: tenure: session lost", lost: true},
	}
	check := func(t *testing.T, ctx context.Context, holder, space string, shard int, want string, lost bool) {
		t.Helper()
		client.ClearActions()
		term, err := a.Acquire(ctx, holder, space, shard)
		if term != 0 || err == nil || err.Error() != want {
			t.Errorf("Acquire(%q, %q, %d) = %d, %v; want 0, %s", holder, space, shard, term, err, want)
		}
		if errors.Is(err, tenure.ErrSessionLost) != lost {
			t.Errorf("errors.Is(%v, tenure.ErrSessionLost) = %v, want %v", err, !lost, lost)
		}
		// The informer's own LIST and WATCH may come at any time: only Acquire's requests count.
		if got := append(actions(client, "get"), writes(client)...); len(got) != 0 {
			t.Errorf("requests %v, want none", got)
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				ctx = t.Context()
			}
			check(t, ctx, tc.holder, tc.space, tc.shard, tc.want, tc.lost)
		})
	}

	t.Run("expired", func(t *testing.T) {
		clock.advance(T.LeaseDuration)
		check(t, t.Context(), "pod-0/1", "markets", 0, "k8s: session pod-0/1 expired: tenure: session lost", true)
		check(t, t.Context(), "pod-0/1", "markets", 0, "k8s: session pod-0/1 ended: tenure: session lost", true)
	})
}

// TestK8S300TombstoneTerm checks that the term of a deleted shard Lease that the informer saw
// outlives the informer's run: a record created anew gets the next term after it.
func TestK8S300TombstoneTerm(t *testing.T) {
	ctx := t.Context()
	client := newFake()
	a := fakeAdapter(t, client)
	closeAtEnd(t, a, "pod-0/1", "pod-0/2")
	mustKeepAlive(t, a, "pod-0/1")

	const big = core.Term(5_000_000_000_000_000)
	leases := client.CoordinationV1().Leases("default")
	name := shardName("g", "markets", 0)
	l, err := leases.Create(ctx, shardLease("g", "markets", 0, "", map[string]string{annotationTerm: encodeTerm(big)}), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	waitObserved(t, a, name, l.ResourceVersion)
	if err := leases.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForgotten(t, a, name)
	// A later Lease of the shard deleted with a smaller term does not lower it.
	l, err = leases.Create(ctx, shardLease("g", "markets", 0, "", map[string]string{annotationTerm: "7"}), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	waitObserved(t, a, name, l.ResourceVersion)
	if err := leases.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForgotten(t, a, name)
	// A Lease deleted with a malformed term leaves no term.
	l, err = leases.Create(ctx, shardLease("g", "orders", 1, "", map[string]string{annotationTerm: "x"}), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	waitObserved(t, a, l.Name, l.ResourceVersion)
	if err := leases.Delete(ctx, l.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForgotten(t, a, l.Name)

	// The session's end stops the informer; the next session starts a new run.
	if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	waitStopped(t, a)
	mustKeepAlive(t, a, "pod-0/2")
	if got := mustAcquire(t, a, "pod-0/2", "markets", 0); got != big+1 {
		t.Errorf("Acquire of a Lease created anew = %d, want %d", got, big+1)
	}
	before := time.Now()
	got := mustAcquire(t, a, "pod-0/2", "orders", 1)
	if lo, hi := wallTerms(before, time.Now()); got < lo || got > hi {
		t.Errorf("Acquire after a malformed term was deleted = %d, want within [%d, %d]", got, lo, hi)
	}
}

// TestK8S110NotedReads checks the cache with states read from the API server noted in it: an
// earlier state the informer delivers late is ignored, also when it is of the Lease that the
// noted one replaced; a shard Lease replaced without a deletion event leaves its term.
func TestK8S110NotedReads(t *testing.T) {
	clock := newFakeClock()
	terms := &shardTerms{}
	c := &leaseCache{now: clock.now, defaultWait: T.LeaseDuration, terms: terms, obs: map[string]*observation{}, tombs: map[types.UID]tombstone{}}
	const u1, u2 = types.UID("6f1c1a43-6c55-4f4e-9d0e-2b8f2b0c1d11"), types.UID("0b7d2b4e-1f3a-4c5d-8e9f-a0b1c2d3e4f5")
	holder := "pod-0/1"
	lease := func(name, kind string, uid types.UID, rv string) *coordinationv1.Lease {
		return &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid, ResourceVersion: rv, Labels: leaseLabels("g", kind, "markets"),
				Annotations: map[string]string{annotationTerm: rv}},
			Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder},
		}
	}
	t0 := clock.now()
	c.observe(lease("g-member-pod-0", kindMember, u1, "10"))
	clock.advance(time.Second)
	c.observe(lease("g-member-pod-0", kindMember, u2, "12")) // noted from a GET
	clock.advance(time.Second)
	c.observe(lease("g-member-pod-0", kindMember, u1, "11")) // the informer, late
	o := c.obs["g-member-pod-0"]
	if o.lease.UID != u2 || o.lease.ResourceVersion != "12" || !o.rvSeenAt.Equal(t0.Add(time.Second)) {
		t.Errorf("observation %s at %s seen at %v, want %s at 12 seen at %v", o.lease.UID, o.lease.ResourceVersion, o.rvSeenAt, u2, t0.Add(time.Second))
	}
	// Replaced without a deletion event: renewals in between may have been missed, so the
	// tombstone is stamped when the replacement shows.
	if tb, ok := c.tombs[u1]; !ok || !tb.tokenSeenAt.Equal(t0.Add(time.Second)) {
		t.Errorf("tombstone of the replaced member Lease %+v, %v; want one from %v", tb, ok, t0.Add(time.Second))
	}

	c.observe(lease("g-markets-0", kindShard, u1, "20"))
	c.observe(lease("g-markets-0", kindShard, u2, "30"))
	if got := terms.of("g-markets-0"); got != 20 {
		t.Errorf("term of the replaced shard Lease %d, want 20", got)
	}
}

// TestK8S710RecordSemaphore checks the record semaphore: n places, then callers wait in arrival
// order; a caller whose ctx ends while it waits leaves the line holding nothing.
func TestK8S710RecordSemaphore(t *testing.T) {
	ctx := t.Context()
	s := newSemaphore(2)
	for range 2 {
		if err := s.acquire(ctx); err != nil {
			t.Fatalf("acquire: %v", err)
		}
	}
	waitLoad := func(holding, waiting int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			h, w := s.load()
			if h == holding && w == waiting {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("load %d holding, %d waiting; want %d, %d", h, w, holding, waiting)
			}
			time.Sleep(time.Millisecond)
		}
	}
	granted := make(chan int, 8)
	var wg sync.WaitGroup
	start := func(ctx context.Context, i int) {
		wg.Go(func() {
			if err := s.acquire(ctx); err != nil {
				granted <- -i
				return
			}
			granted <- i
		})
	}
	start(ctx, 1)
	waitLoad(2, 1)
	start(ctx, 2)
	waitLoad(2, 2)
	gone, leave := context.WithCancel(ctx)
	start(gone, 3)
	waitLoad(2, 3)
	start(ctx, 4)
	waitLoad(2, 4)
	start(ctx, 5)
	waitLoad(2, 5)
	leave()
	if got := <-granted; got != -3 {
		t.Fatalf("the canceled caller got %d, want -3: ctx.Err() and no place", got)
	}
	waitLoad(2, 4)
	for _, want := range []int{1, 2, 4, 5} {
		s.release()
		if got := <-granted; got != want {
			t.Fatalf("release granted caller %d, want %d", got, want)
		}
	}
	waitLoad(2, 0)
	s.release()
	s.release()
	waitLoad(0, 0)
	wg.Wait()
	for range 2 {
		if err := s.acquire(ctx); err != nil {
			t.Fatalf("acquire: %v", err)
		}
	}
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := s.acquire(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a third acquire = %v, want context.DeadlineExceeded", err)
	}
	waitLoad(2, 0)
}

// TestATK8S06LocalObservation checks that another holder takes a shard only once it has itself
// observed the holder's member Lease unchanged for the holder's LeaseDuration, on its own clock:
// the takeover comes within [t0+3s, t0+5s] of the last change at t0, however far in the future
// the member Lease's renewTime and however far in the past the record's times are written. The
// takeover reads the member Lease from the API server before it writes. The acceptance test's
// steps were written from the issue's text: the original text is unavailable.
func TestATK8S06LocalObservation(t *testing.T) {
	c := newCluster(t)
	g := group()
	a, _ := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1")
	closeAtEnd(t, b, "pod-1/1")
	mustKeepAlive(t, a, "pod-0/1")
	renewA := startRenewer(t, a, "pod-0/1")
	mustKeepAlive(t, b, "pod-1/1")
	startRenewer(t, b, "pod-1/1")
	first := mustAcquire(t, a, "pod-0/1", "markets", 0)
	name, mname := shardName(g, "markets", 0), memberName(g, "pod-0")
	waitObserved(t, b, name, rawGet(t, c, name).ResourceVersion)

	renewA.stop()
	t0 := time.Now()
	future := metav1.NewMicroTime(time.Now().Add(time.Hour))
	past := metav1.NewMicroTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	m := rawUpdate(t, c, mname, func(l *coordinationv1.Lease) { l.Spec.RenewTime = &future })
	r := rawUpdate(t, c, name, func(l *coordinationv1.Lease) { l.Spec.AcquireTime, l.Spec.RenewTime = &past, &past })
	waitObserved(t, b, mname, m.ResourceVersion)
	waitObserved(t, b, name, r.ResourceVersion)

	recB.reset()
	got := acquireUntil(t, b, "pod-1/1", "markets", 0, "pod-0/1", t0.Add(8*time.Second), 50*time.Millisecond)
	t.Logf("taken over %v after t0, after %d held calls", got.at.Sub(t0), got.held)
	if got.held == 0 {
		t.Error("the first Acquire was granted, want it held while the member Lease was live")
	}
	if lo, hi := t0.Add(3*time.Second), t0.Add(5*time.Second); got.at.Before(lo) || got.at.After(hi) {
		t.Errorf("takeover %v after t0, want within [3s, 5s]", got.at.Sub(t0))
	}
	if got.term <= first {
		t.Errorf("takeover term %d, want above %d", got.term, first)
	}
	checkGrant(t, rawGet(t, c, name), g, "markets", "pod-1/1", got.term, rawGet(t, c, memberName(g, "pod-1")).UID, 1, t0, got.at)
	var seq []string
	for _, x := range recB.filter(func(r request) bool { return r.name == name || r.name == mname }) {
		seq = append(seq, x.String())
	}
	if want := []string{"GET " + mname + " 200", "UPDATE " + name + " 200"}; strings.Join(seq, ", ") != strings.Join(want, ", ") {
		t.Errorf("requests on the record and the member Lease %v, want %v: held calls decide from the cache", seq, want)
	}
}

// TestATK8S07ClosedSessionFrees checks that a closed session frees its shard at once: when the
// member Lease stays, as a joined member's does, with its holder cleared, another holder takes
// the shard on its next Acquire without reading the member Lease. The acceptance test's steps
// were written from the issue's text: the original text is unavailable.
func TestATK8S07ClosedSessionFrees(t *testing.T) {
	c := newCluster(t)
	g := group()
	a, _ := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1")
	closeAtEnd(t, b, "pod-1/1")
	mustKeepAlive(t, b, "pod-1/1")
	startRenewer(t, b, "pod-1/1")
	mustKeepAlive(t, a, "pod-0/1")
	name, mname := shardName(g, "markets", 0), memberName(g, "pod-0")
	// The member record that Join writes keeps the member Lease when the session closes.
	rawUpdate(t, c, mname, func(l *coordinationv1.Lease) {
		if l.Annotations == nil {
			l.Annotations = map[string]string{}
		}
		l.Annotations[annotationMember] = encodeMember(core.Member{ID: "pod-0", Slot: -1})
	})
	mustKeepAlive(t, a, "pod-0/1")
	first := mustAcquire(t, a, "pod-0/1", "markets", 0)
	waitObserved(t, b, name, rawGet(t, c, name).ResourceVersion)
	held, err := b.Acquire(t.Context(), "pod-1/1", "markets", 0)
	wantHeld(t, held, err, "pod-0/1", T.RetryPeriod)

	if err := a.CloseSession(t.Context(), "pod-0/1"); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	m := rawGet(t, c, mname)
	if m == nil || holderOf(m) != "" {
		t.Fatalf("member Lease after CloseSession %+v, want one with no holder", m)
	}
	waitObserved(t, b, mname, m.ResourceVersion)
	recB.reset()
	if term := mustAcquire(t, b, "pod-1/1", "markets", 0); term <= first {
		t.Errorf("term %d, want above %d", term, first)
	}
	if n := recB.count(on(verbGet, mname)); n != 0 {
		t.Errorf("%d GETs of the member Lease, want none: an ended session needs no confirmation", n)
	}
	if l := rawGet(t, c, name); holderOf(l) != "pod-1/1" {
		t.Errorf("holder %q, want pod-1/1", holderOf(l))
	}
}

// TestATK8S08Confirmation checks that a takeover is confirmed against the API server: with its
// informer stalled, an Adapter whose cached member Lease looks expired reads it before writing,
// finds the holder renewed and stays held, and the read resets its observation; once the holder
// stops renewing, the reads show the same session for the LeaseDuration and the takeover
// follows the read. The acceptance test's steps were written from the issue's text: the
// original text is unavailable.
func TestATK8S08Confirmation(t *testing.T) {
	c := newCluster(t)
	g := group()
	a, _ := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1")
	closeAtEnd(t, b, "pod-1/1")
	mustKeepAlive(t, a, "pod-0/1")
	renewA := startRenewer(t, a, "pod-0/1")
	mustKeepAlive(t, b, "pod-1/1")
	startRenewer(t, b, "pod-1/1")
	first := mustAcquire(t, a, "pod-0/1", "markets", 0)
	name, mname := shardName(g, "markets", 0), memberName(g, "pod-0")
	waitObserved(t, b, name, rawGet(t, c, name).ResourceVersion)

	release := recB.stall(on(verbWatch, ""))
	t.Cleanup(release) // before the sessions close
	time.Sleep(T.LeaseDuration + time.Second)

	recB.reset()
	term, err := b.Acquire(t.Context(), "pod-1/1", "markets", 0)
	wantHeld(t, term, err, "pod-0/1", T.RetryPeriod)
	if n := recB.count(on(verbGet, mname)); n != 1 {
		t.Errorf("%d GETs of the member Lease, want 1", n)
	}
	if n := recB.count(shardRequests(g)); n != 0 {
		t.Errorf("%d requests on the record, want none", n)
	}
	term, err = b.Acquire(t.Context(), "pod-1/1", "markets", 0)
	wantHeld(t, term, err, "pod-0/1", T.RetryPeriod)
	if n := recB.count(on(verbGet, mname)); n != 1 {
		t.Errorf("%d GETs of the member Lease after a second Acquire, want still 1: the GET reset the observation", n)
	}

	renewA.stop()
	stopped := time.Now()
	recB.reset()
	got := acquireUntil(t, b, "pod-1/1", "markets", 0, "pod-0/1", stopped.Add(10*time.Second), 100*time.Millisecond)
	if got.term <= first {
		t.Errorf("takeover term %d, want above %d", got.term, first)
	}
	// The last renewal came at most a RetryPeriod, and a request, before the renewals stopped.
	if least := T.LeaseDuration - time.Second; got.at.Before(stopped.Add(least)) {
		t.Errorf("takeover %v after the renewals stopped, want at least %v", got.at.Sub(stopped), least)
	}
	var seq []string
	for _, x := range recB.filter(func(r request) bool { return r.name == name || r.name == mname }) {
		seq = append(seq, x.String())
	}
	if n := len(seq); n < 2 || seq[n-2] != "GET "+mname+" 200" || seq[n-1] != "UPDATE "+name+" 200" {
		t.Errorf("requests on the record and the member Lease %v, want them to end with the GET and the UPDATE", seq)
	}
	release()
}

// TestATK8S11NewIncarnation checks that a new incarnation of a member ends the old one's session
// and frees its records at once, for another member and for the new incarnation itself; the
// old incarnation's renewal reports its session lost, and its Acquire is refused without a
// request. The acceptance test's steps were written from the issue's text: the original text
// is unavailable.
func TestATK8S11NewIncarnation(t *testing.T) {
	c := newCluster(t)
	g := group()
	a, recA := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	d, recD := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1")
	closeAtEnd(t, b, "pod-0/2")
	closeAtEnd(t, d, "pod-1/1")
	mustKeepAlive(t, a, "pod-0/1")
	first0 := mustAcquire(t, a, "pod-0/1", "markets", 0)
	first1 := mustAcquire(t, a, "pod-0/1", "markets", 1)
	mustKeepAlive(t, d, "pod-1/1")
	name0, name1, mname := shardName(g, "markets", 0), shardName(g, "markets", 1), memberName(g, "pod-0")
	uid := rawGet(t, c, mname).UID

	mustKeepAlive(t, b, "pod-0/2")
	m := rawGet(t, c, mname)
	if m.UID != uid || holderOf(m) != "pod-0/2" {
		t.Fatalf("member Lease %s naming %q, want %s naming pod-0/2", m.UID, holderOf(m), uid)
	}
	waitObserved(t, d, mname, m.ResourceVersion)
	recD.reset()
	if term := mustAcquire(t, d, "pod-1/1", "markets", 0); term <= first0 {
		t.Errorf("term %d, want above %d", term, first0)
	}
	want := []string{"GET " + mname + " 200", "UPDATE " + name0 + " 200"}
	if got := requestStrings(recD.filter(func(r request) bool { return r.name == name0 || r.name == mname })); strings.Join(got, ", ") != strings.Join(want, ", ") {
		t.Errorf("another member's takeover: requests %v, want %v", got, want)
	}

	waitObserved(t, b, mname, m.ResourceVersion)
	waitObserved(t, b, name1, rawGet(t, c, name1).ResourceVersion)
	recB.reset()
	if term := mustAcquire(t, b, "pod-0/2", "markets", 1); term <= first1 {
		t.Errorf("term %d, want above %d", term, first1)
	}
	want = []string{"GET " + mname + " 200", "UPDATE " + name1 + " 200"}
	if got := requestStrings(recB.filter(func(r request) bool { return r.name == name1 || r.name == mname })); strings.Join(got, ", ") != strings.Join(want, ", ") {
		t.Errorf("the new incarnation's takeover: requests %v, want %v", got, want)
	}

	if err := a.KeepAlive(t.Context(), "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) {
		t.Errorf("KeepAlive of the old incarnation = %v, want an error matching tenure.ErrSessionLost", err)
	}
	recA.reset()
	if term, err := a.Acquire(t.Context(), "pod-0/1", "markets", 2); term != 0 || !errors.Is(err, tenure.ErrSessionLost) {
		t.Errorf("Acquire of the old incarnation = %d, %v; want 0 and an error matching tenure.ErrSessionLost", term, err)
	}
	if n := len(recA.requests()); n != 0 {
		t.Errorf("%d requests, want none: %v", n, recA.requests())
	}
}

// TestATK8S18Acquire checks Acquire's grants on a cluster: the term of a new record is the wall
// time in milliseconds; the record's fields; a re-grant to the holder is a new term, raw term + 1
// above the wall clock, and removes tenure.confirm while foreign labels and annotations stay; a
// takeover counts a transition; a malformed term counts as 0 once its holder is gone and does
// not free a live holder's record; a foreign Lease is never written. The acceptance test's
// steps were written from the issue's text: the original text is unavailable.
func TestATK8S18Acquire(t *testing.T) {
	c := newCluster(t)
	g := group()
	a, rec := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1")
	closeAtEnd(t, b, "pod-1/1")
	mustKeepAlive(t, a, "pod-0/1")
	startRenewer(t, a, "pod-0/1")
	const h = "pod-0/1"
	uid := rawGet(t, c, memberName(g, "pod-0")).UID
	// pod-9's session has ended, and a saw it end: its member Lease named pod-9/1, then no
	// holder.
	ended := rawCreate(t, c, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: memberName(g, "pod-9"), Labels: leaseLabels(g, kindMember, "")},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: new("pod-9/1")},
	})
	waitObserved(t, a, ended.Name, ended.ResourceVersion)
	ended = rawUpdate(t, c, ended.Name, func(l *coordinationv1.Lease) { l.Spec.HolderIdentity = nil })
	waitObserved(t, a, ended.Name, ended.ResourceVersion)
	endedRecord := func(n int, term string) *coordinationv1.Lease {
		l := shardLease(g, "markets", n, "pod-9/1", map[string]string{annotationTerm: term, annotationSession: string(ended.UID)})
		l.Spec.LeaseTransitions = new(int32(2))
		return rawCreate(t, c, l)
	}
	name0 := shardName(g, "markets", 0)

	t.Run("new record", func(t *testing.T) {
		rec.reset()
		before := time.Now()
		term := mustAcquire(t, a, h, "markets", 0)
		after := time.Now()
		if lo, hi := wallTerms(before, after); term < lo || term > hi {
			t.Errorf("term %d, want within [%d, %d]", term, lo, hi)
		}
		checkGrant(t, rawGet(t, c, name0), g, "markets", h, term, uid, 0, before, after)
		if n := rec.count(on(verbCreate, "")); n != 1 {
			t.Errorf("%d creations, want 1", n)
		}

		// Each space has records of its own, labeled with the space.
		before = time.Now()
		term = mustAcquire(t, a, h, "orders", 0)
		checkGrant(t, rawGet(t, c, shardName(g, "orders", 0)), g, "orders", h, term, uid, 0, before, time.Now())
	})

	t.Run("re-grant", func(t *testing.T) {
		const raw = core.Term(4_000_000_000_000_000)
		l := rawUpdate(t, c, name0, func(l *coordinationv1.Lease) {
			l.Annotations[annotationTerm] = encodeTerm(raw)
			l.Annotations[annotationConfirm] = "7"
			l.Annotations["example.com/note"] = "kept"
			l.Labels["example.com/team"] = "a"
		})
		waitObserved(t, a, name0, l.ResourceVersion)
		before := time.Now()
		if term := mustAcquire(t, a, h, "markets", 0); term != raw+1 {
			t.Errorf("term %d, want raw term + 1 = %d", term, raw+1)
		}
		l = rawGet(t, c, name0)
		checkGrant(t, l, g, "markets", h, raw+1, uid, 0, before, time.Now())
		if l.Annotations["example.com/note"] != "kept" || l.Labels["example.com/team"] != "a" {
			t.Errorf("annotations %v, labels %v; want the foreign ones kept", l.Annotations, l.Labels)
		}
	})

	t.Run("takeover", func(t *testing.T) {
		const raw = core.Term(5_000_000_000_000_000)
		l := endedRecord(1, encodeTerm(raw))
		waitObserved(t, a, l.Name, l.ResourceVersion)
		rec.reset()
		before := time.Now()
		if term := mustAcquire(t, a, h, "markets", 1); term != raw+1 {
			t.Errorf("term %d, want %d", term, raw+1)
		}
		checkGrant(t, rawGet(t, c, l.Name), g, "markets", h, raw+1, uid, 3, before, time.Now())
		if n := rec.count(on(verbGet, ended.Name)); n != 0 {
			t.Errorf("%d GETs of the ended member Lease, want none", n)
		}

		l = endedRecord(2, "5")
		waitObserved(t, a, l.Name, l.ResourceVersion)
		before = time.Now()
		term := mustAcquire(t, a, h, "markets", 2)
		after := time.Now()
		if lo, hi := wallTerms(before, after); term < lo || term > hi {
			t.Errorf("term %d after a small term, want the wall time within [%d, %d]", term, lo, hi)
		}
		checkGrant(t, rawGet(t, c, l.Name), g, "markets", h, term, uid, 3, before, after)
	})

	t.Run("malformed term", func(t *testing.T) {
		l := endedRecord(3, "x1")
		waitObserved(t, a, l.Name, l.ResourceVersion)
		before := time.Now()
		term := mustAcquire(t, a, h, "markets", 3)
		after := time.Now()
		if lo, hi := wallTerms(before, after); term < lo || term > hi {
			t.Errorf("term %d after a malformed term, want within [%d, %d]", term, lo, hi)
		}
		checkGrant(t, rawGet(t, c, l.Name), g, "markets", h, term, uid, 3, before, after)

		// A malformed term on a live holder's record frees nothing.
		l = rawUpdate(t, c, l.Name, func(l *coordinationv1.Lease) { l.Annotations[annotationTerm] = "-1" })
		mustKeepAlive(t, b, "pod-1/1")
		waitObserved(t, b, l.Name, l.ResourceVersion)
		got, err := b.Acquire(t.Context(), "pod-1/1", "markets", 3)
		wantHeld(t, got, err, h, T.RetryPeriod)
		if after := rawGet(t, c, l.Name); after.ResourceVersion != l.ResourceVersion {
			t.Errorf("the held record was written: resourceVersion %s, want %s", after.ResourceVersion, l.ResourceVersion)
		}
	})

	t.Run("foreign lease", func(t *testing.T) {
		unlabeled := rawCreate(t, c, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: shardName(g, "markets", 7)}})
		member := rawCreate(t, c, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
			Name: shardName(g, "markets", 8), Labels: leaseLabels(g, kindMember, ""),
		}})
		for i, l := range []*coordinationv1.Lease{unlabeled, member} {
			rec.reset()
			_, err := a.Acquire(t.Context(), h, "markets", 7+i)
			want := "k8s: lease " + l.Name + " is not a shard lease of group " + g
			if err == nil || err.Error() != want {
				t.Errorf("Acquire of %s = %v, want %s", l.Name, err, want)
			}
			if n := rec.count(func(r request) bool { return r.verb != verbGet && shardRequests(g)(r) }); n != 0 {
				t.Errorf("%d writes, want none: %v", n, rec.filter(shardRequests(g)))
			}
			if after := rawGet(t, c, l.Name); after.ResourceVersion != l.ResourceVersion {
				t.Errorf("%s written: resourceVersion %s, want %s", l.Name, after.ResourceVersion, l.ResourceVersion)
			}
		}
	})
}

// TestATK8S19HeldAndSessionless checks that Acquire returns a *tenure.HeldError, without a
// request, while the record's holder is live, with the RetryPeriod of the acquirer's own
// Timing; that it needs an active session of the acquirer, never opened, closed or expired
// being refused without a request; and that a record whose tenure.session is missing or
// malformed is held for its own Lease's duration as this Adapter observes it, then taken after
// a read of the member Lease. The acceptance test's steps were written from the issue's text:
// the original text is unavailable.
func TestATK8S19HeldAndSessionless(t *testing.T) {
	c := newCluster(t)
	g := group()
	a, _ := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	d, recD := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1", "pod-3/1", "pod-4/1")
	closeAtEnd(t, b, "pod-1/1", "pod-2/1")
	mustKeepAlive(t, a, "pod-0/1")
	startRenewer(t, a, "pod-0/1")
	mustKeepAlive(t, b, "pod-1/1")
	startRenewer(t, b, "pod-1/1")
	name := shardName(g, "markets", 0)

	t.Run("held", func(t *testing.T) {
		mustAcquire(t, a, "pod-0/1", "markets", 0)
		waitObserved(t, b, name, rawGet(t, c, name).ResourceVersion)
		recB.reset()
		term, err := b.Acquire(t.Context(), "pod-1/1", "markets", 0)
		wantHeld(t, term, err, "pod-0/1", T.RetryPeriod)
		if want := "tenure: held by pod-0/1, retry after 500ms"; err.Error() != want {
			t.Errorf("error %q, want %q", err, want)
		}
		if err := b.KeepAlive(t.Context(), "pod-2/1", core.DefaultTiming); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		term, err = b.Acquire(t.Context(), "pod-2/1", "markets", 0)
		wantHeld(t, term, err, "pod-0/1", core.DefaultTiming.RetryPeriod)
		if n := recB.count(func(r request) bool { return shardRequests(g)(r) || r.name == memberName(g, "pod-0") }); n != 0 {
			t.Errorf("%d requests on the record or the holder's member Lease, want none: %v", n, recB.requests())
		}
	})

	t.Run("acquirer without a session", func(t *testing.T) {
		mustKeepAlive(t, a, "pod-3/1")
		if err := a.CloseSession(t.Context(), "pod-3/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		mustKeepAlive(t, a, "pod-4/1")
		expires := time.Now().Add(T.LeaseDuration)
		checks := []struct {
			a      *Adapter
			rec    *recorder
			holder string
			want   string
		}{
			{d, recD, "pod-5/1", "k8s: session pod-5/1 is not open: tenure: session lost"},
			{a, nil, "pod-3/1", "k8s: session pod-3/1 ended: tenure: session lost"},
		}
		for _, x := range checks {
			if x.rec != nil {
				x.rec.reset()
			}
			term, err := x.a.Acquire(t.Context(), x.holder, "markets", 1)
			if term != 0 || err == nil || err.Error() != x.want || !errors.Is(err, tenure.ErrSessionLost) {
				t.Errorf("Acquire(%s) = %d, %v; want 0, %s", x.holder, term, err, x.want)
			}
			if x.rec != nil && len(x.rec.requests()) != 0 {
				t.Errorf("Acquire(%s): requests %v, want none", x.holder, x.rec.requests())
			}
		}
		time.Sleep(time.Until(expires) + 100*time.Millisecond)
		term, err := a.Acquire(t.Context(), "pod-4/1", "markets", 1)
		if want := "k8s: session pod-4/1 expired: tenure: session lost"; term != 0 || err == nil || err.Error() != want {
			t.Errorf("Acquire after the session expired = %d, %v; want 0, %s", term, err, want)
		}
	})

	t.Run("holder without a session", func(t *testing.T) {
		for i, session := range []string{"", "not-a-uuid"} {
			n := 2 + i
			ann := map[string]string{annotationTerm: "1"}
			if session != "" {
				ann[annotationSession] = session
			}
			l := shardLease(g, "markets", n, "pod-9/1", ann)
			l.Spec.LeaseDurationSeconds = new(int32(3))
			created := time.Now()
			l = rawCreate(t, c, l)
			waitObserved(t, b, l.Name, l.ResourceVersion)
			recB.reset()
			got := acquireUntil(t, b, "pod-1/1", "markets", n, "pod-9/1", created.Add(8*time.Second), 100*time.Millisecond)
			if got.held == 0 || got.at.Before(created.Add(T.LeaseDuration)) {
				t.Errorf("tenure.session %q: taken %v after the record was created, after %d held calls; want 3s or more",
					session, got.at.Sub(created), got.held)
			}
			mname := memberName(g, "pod-9")
			want := []string{"GET " + mname + " 404", "UPDATE " + l.Name + " 200"}
			if seq := requestStrings(recB.filter(func(r request) bool { return r.name == l.Name || r.name == mname })); strings.Join(seq, ", ") != strings.Join(want, ", ") {
				t.Errorf("tenure.session %q: requests %v, want %v", session, seq, want)
			}
		}
	})
}

// TestATK8S20Conflicts checks Acquire's compare-and-swap: three conflicting writes end it with
// "k8s: lease <name>: 3 conflicts: …" and the record as the conflicts left it; and of holders
// racing for a shard, created anew or vacant, exactly one wins each round and the others are
// held by it. The acceptance test's steps were written from the issue's text: the original
// text is unavailable.
func TestATK8S20Conflicts(t *testing.T) {
	c := newCluster(t)
	g := group()

	t.Run("three conflicts", func(t *testing.T) {
		a, rec := c.adapter(t, g)
		closeAtEnd(t, a, "pod-0/1")
		mustKeepAlive(t, a, "pod-0/1")
		name := shardName(g, "markets", 0)
		term := mustAcquire(t, a, "pod-0/1", "markets", 0)
		waitObserved(t, a, name, rawGet(t, c, name).ResourceVersion)
		for range 3 {
			rec.bump(name, time.Time{})
		}
		rec.reset()
		got, err := a.Acquire(t.Context(), "pod-0/1", "markets", 0)
		prefix := "k8s: lease " + name + ": 3 conflicts: "
		if got != 0 || err == nil || !strings.HasPrefix(err.Error(), prefix) || !apierrors.IsConflict(errors.Unwrap(err)) {
			t.Errorf("Acquire = %d, %v; want 0 and %s<a Conflict>", got, err, prefix)
		}
		want := []string{
			"UPDATE " + name + " 409", "GET " + name + " 200", "UPDATE " + name + " 409", "GET " + name + " 200", "UPDATE " + name + " 409",
		}
		if seq := requestStrings(rec.filter(shardRequests(g))); strings.Join(seq, ", ") != strings.Join(want, ", ") {
			t.Errorf("requests %v, want %v", seq, want)
		}
		l := rawGet(t, c, name)
		if l.Annotations[annotationTerm] != encodeTerm(term) || l.Annotations[bumpAnnotation] != "3" {
			t.Errorf("record %v, want term %d and three bumps", l.Annotations, term)
		}
	})

	t.Run("one winner per round", func(t *testing.T) {
		holders := []string{"pod-1/1", "pod-2/1", "pod-3/1"}
		adapters := make([]*Adapter, len(holders))
		for i, h := range holders {
			adapters[i], _ = c.adapter(t, g)
			closeAtEnd(t, adapters[i], h)
			mustKeepAlive(t, adapters[i], h)
			startRenewer(t, adapters[i], h)
		}
		for round := range 6 {
			shard := 10 + round
			if round%2 == 1 {
				rawCreate(t, c, shardLease(g, "markets", shard, "", map[string]string{annotationTerm: "1"}))
			}
			start := make(chan struct{})
			terms := make([]core.Term, len(holders))
			errs := make([]error, len(holders))
			var wg sync.WaitGroup
			for i, h := range holders {
				wg.Go(func() {
					<-start
					terms[i], errs[i] = adapters[i].Acquire(t.Context(), h, "markets", shard)
				})
			}
			close(start)
			wg.Wait()
			var winner string
			var won core.Term
			for i, h := range holders {
				if errs[i] == nil && terms[i] != 0 {
					if winner != "" {
						t.Errorf("round %d: %s and %s both won", round, winner, h)
					}
					winner, won = h, terms[i]
				}
			}
			if winner == "" {
				t.Fatalf("round %d: no winner: %v", round, errs)
			}
			for i, h := range holders {
				if h != winner {
					wantHeld(t, terms[i], errs[i], winner, T.RetryPeriod)
				}
			}
			if l := rawGet(t, c, shardName(g, "markets", shard)); holderOf(l) != winner || l.Annotations[annotationTerm] != encodeTerm(won) {
				t.Errorf("round %d: record names %q at %s, want %s at %d", round, holderOf(l), l.Annotations[annotationTerm], winner, won)
			}
		}
	})
}

// TestATK8S31RecordConcurrency checks WithRecordConcurrency(2): of eight Acquires whose record
// requests are delayed, at most two have shard Lease requests in flight at once, they are let
// through in arrival order, and a KeepAlive called while they wait does not wait for them. The
// acceptance test's steps were written from the issue's text: the original text is unavailable.
func TestATK8S31RecordConcurrency(t *testing.T) {
	c := newCluster(t)
	g := group()
	a, rec := c.adapter(t, g, WithRecordConcurrency(2))
	closeAtEnd(t, a, "pod-0/1")
	mustKeepAlive(t, a, "pod-0/1")
	const n = 8
	const delay = time.Second
	for i := range n {
		rec.delay(shardName(g, "markets", i), delay)
	}
	rec.reset()

	type result struct {
		term core.Term
		err  error
		done time.Time
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			term, err := a.Acquire(t.Context(), "pod-0/1", "markets", i)
			results[i] = result{term, err, time.Now()}
		})
		// The next Acquire arrives once this one holds a place or waits for one.
		deadline := time.Now().Add(2 * time.Second)
		for {
			holding, waiting := a.records.load()
			if holding+waiting == i+1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Acquire %d did not arrive: %d holding, %d waiting", i, holding, waiting)
			}
			time.Sleep(time.Millisecond)
		}
	}
	if err := a.KeepAlive(t.Context(), "pod-0/1", T); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	renewed := time.Now()
	// The Acquires take longer than the LeaseDuration: keep the session they need alive.
	startRenewer(t, a, "pod-0/1")
	wg.Wait()

	for i, r := range results {
		if r.err != nil || r.term == 0 {
			t.Errorf("Acquire of shard %d = %d, %v; want a term", i, r.term, r.err)
		}
		if !renewed.Before(r.done) {
			t.Errorf("Acquire of shard %d finished %v before the KeepAlive, want the KeepAlive first", i, renewed.Sub(r.done))
		}
	}
	if got := rec.maxInFlight(shardRequests(g)); got != 2 {
		t.Errorf("at most %d shard Lease requests in flight, want 2", got)
	}
	// In arrival order: every request of shards 2k and 2k+1 starts before those of later pairs.
	starts := map[int]time.Time{}
	for _, r := range rec.filter(on(verbGet, "")) {
		if kind, _, _, shard, ok := parseName(g, r.name); ok && kind == kindShard {
			starts[shard] = r.start
		}
	}
	for i := range n {
		for j := range n {
			if i/2 < j/2 && !starts[i].Before(starts[j]) {
				t.Errorf("the record read of shard %d started at %v, not before shard %d's at %v", i, starts[i], j, starts[j])
			}
		}
	}
}

// staleCache sets the observation of Lease name in a's cache to o, or drops it when o is nil,
// and drops the tombstone of uid when it is not empty, as an informer that has not delivered the
// latest changes would have it.
func staleCache(t testing.TB, a *Adapter, name string, o *observation, uid types.UID) {
	t.Helper()
	c := a.runningCache()
	if c == nil {
		t.Fatal("the informer does not run")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if o == nil {
		delete(c.obs, name)
	} else {
		c.obs[name] = o
	}
	if uid != "" {
		delete(c.tombs, uid)
	}
}

// TestK8S130StaleCache checks that a takeover never rests on a cached member Lease older than
// the record it replaces. A member Lease cached without a holder, from before the record's
// holder opened its session on it, is confirmed by a GET that finds the session live; once the
// Adapter has seen the holder's session cleared, the record is taken without that GET. A member
// Lease deleted while the informer lags reads as a tombstone seen at the GET, so the holder,
// whose renewals the Adapter missed, stays live for its LeaseDuration from then.
func TestK8S130StaleCache(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	a := fakeAdapter(t, client, withClock(clock.now))
	closeAtEnd(t, a, "pod-1/1")
	mustKeepAlive(t, a, "pod-1/1")
	leases := client.CoordinationV1().Leases("default")
	gets := func(name string) int {
		n := 0
		for _, x := range actions(client, "get") {
			if get, ok := x.(k8stesting.GetAction); ok && get.GetName() == name {
				n++
			}
		}
		return n
	}
	memberLease := func(id, holder string) *coordinationv1.Lease {
		l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: memberName("g", id), Labels: leaseLabels("g", kindMember, "")}}
		if holder != "" {
			l.Spec.HolderIdentity = &holder
			l.Spec.RenewTime = new(metav1.NewMicroTime(clock.now()))
		}
		l.Spec.LeaseDurationSeconds = new(int32(3))
		return l
	}
	record := func(n int, holder string, session types.UID) *coordinationv1.Lease {
		l := shardLease("g", "markets", n, holder, map[string]string{annotationTerm: "1", annotationSession: string(session)})
		l.Spec.LeaseDurationSeconds = new(int32(3))
		l, err := leases.Create(ctx, l, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create record: %v", err)
		}
		waitObserved(t, a, l.Name, l.ResourceVersion)
		return l
	}
	update := func(l *coordinationv1.Lease) *coordinationv1.Lease {
		l, err := leases.Update(ctx, l, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("update %s: %v", l.Name, err)
		}
		waitObserved(t, a, l.Name, l.ResourceVersion)
		return l
	}

	t.Run("ended", func(t *testing.T) {
		mname := memberName("g", "pod-0")
		m, err := leases.Create(ctx, memberLease("pod-0", ""), metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create member Lease: %v", err)
		}
		stale := waitObserved(t, a, mname, m.ResourceVersion)
		m.Spec.HolderIdentity = new("pod-0/1")
		m.Spec.RenewTime = new(metav1.NewMicroTime(clock.now()))
		m = update(m)
		r := record(0, "pod-0/1", m.UID)

		// The informer has delivered neither the session's opening nor the record.
		staleCache(t, a, mname, &stale, "")
		staleCache(t, a, r.Name, nil, "")
		client.ClearActions()
		term, err := a.Acquire(ctx, "pod-1/1", "markets", 0)
		wantHeld(t, term, err, "pod-0/1", T.RetryPeriod)
		if n := gets(mname); n != 1 {
			t.Errorf("%d GETs of the member Lease, want 1", n)
		}
		if w := writes(client); len(w) != 0 {
			t.Errorf("writes %v, want none", w)
		}

		// The Adapter sees the session cleared: the record is free without a GET.
		m.Spec.HolderIdentity = nil
		update(m)
		client.ClearActions()
		mustAcquire(t, a, "pod-1/1", "markets", 0)
		if n := gets(mname); n != 0 {
			t.Errorf("%d GETs of the member Lease, want none", n)
		}
	})

	t.Run("deleted", func(t *testing.T) {
		mname := memberName("g", "pod-2")
		m, err := leases.Create(ctx, memberLease("pod-2", "pod-2/1"), metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create member Lease: %v", err)
		}
		stale := waitObserved(t, a, mname, m.ResourceVersion)
		record(1, "pod-2/1", m.UID)
		clock.advance(2 * time.Second)
		mustKeepAlive(t, a, "pod-1/1")
		m.Spec.RenewTime = new(metav1.NewMicroTime(clock.now()))
		update(m)
		if err := leases.Delete(ctx, mname, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		waitForgotten(t, a, mname)

		// The informer has delivered neither the renewal nor the deletion: the cached state is
		// LeaseDuration old once the clock moves on.
		staleCache(t, a, mname, &stale, m.UID)
		clock.advance(time.Second)
		client.ClearActions()
		term, err := a.Acquire(ctx, "pod-1/1", "markets", 1)
		wantHeld(t, term, err, "pod-2/1", T.RetryPeriod)
		if n := gets(mname); n != 1 {
			t.Errorf("%d GETs of the member Lease, want 1", n)
		}
		if w := writes(client); len(w) != 0 {
			t.Errorf("writes %v, want none", w)
		}
	})
}

// TestK8S300VanishedRecord checks that a record deleted under Acquire, before the informer told,
// still bounds the term: the update of the cached state finds no Lease, and the record created
// anew gets the next term after the deleted one.
func TestK8S300VanishedRecord(t *testing.T) {
	client := newFake()
	a := fakeAdapter(t, client)
	closeAtEnd(t, a, "pod-0/1")
	mustKeepAlive(t, a, "pod-0/1")
	const big = core.Term(5_000_000_000_000_000)
	l := shardLease("g", "markets", 0, "", map[string]string{annotationTerm: encodeTerm(big)})
	l.UID, l.ResourceVersion = "6f1c1a43-6c55-4f4e-9d0e-2b8f2b0c1d11", "1"
	staleCache(t, a, l.Name, &observation{lease: l, rvSeenAt: a.now(), tokenSeenAt: a.now()}, "")
	client.ClearActions()
	if got := mustAcquire(t, a, "pod-0/1", "markets", 0); got != big+1 {
		t.Errorf("Acquire = %d, want %d", got, big+1)
	}
	var verbs []string
	for _, x := range writes(client) {
		verbs = append(verbs, x.GetVerb())
	}
	if got := strings.Join(verbs, " "); got != "create update" {
		t.Errorf("writes %q, want an update, then a create", got)
	}
}

// TestK8S710SessionAfterWait checks that an Acquire that waited for a place of the record
// semaphore checks its session again: a session that expired meanwhile gets an error matching
// tenure.ErrSessionLost, and no record request.
func TestK8S710SessionAfterWait(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	a := fakeAdapter(t, client, withClock(clock.now), WithRecordConcurrency(1))
	closeAtEnd(t, a, "pod-0/1")
	mustKeepAlive(t, a, "pod-0/1")
	if err := a.records.acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	client.ClearActions()
	done := make(chan error, 1)
	go func() {
		_, err := a.Acquire(ctx, "pod-0/1", "markets", 0)
		done <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, waiting := a.records.load(); waiting == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Acquire does not wait for a place")
		}
		time.Sleep(time.Millisecond)
	}
	clock.advance(T.LeaseDuration)
	a.records.release()
	err := <-done
	if want := "k8s: session pod-0/1 expired: tenure: session lost"; err == nil || err.Error() != want || !errors.Is(err, tenure.ErrSessionLost) {
		t.Errorf("Acquire = %v, want %s", err, want)
	}
	if got := append(actions(client, "get"), writes(client)...); len(got) != 0 {
		t.Errorf("requests %v, want none", got)
	}
}

// TestK8S710RecordOpsWait checks that Release, Confirm and CloseSession's releases wait for a
// place of the record semaphore before any request on a record, and that Confirm checks the
// session again once it has its place.
func TestK8S710RecordOpsWait(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	a := fakeAdapter(t, client, withClock(clock.now), WithRecordConcurrency(1))
	closeAtEnd(t, a, "pod-0/1", "pod-1/1", "pod-2/1")
	mustKeepAlive(t, a, "pod-0/1")
	mustKeepAlive(t, a, "pod-1/1")
	t0 := mustAcquire(t, a, "pod-0/1", "markets", 0)
	t1 := mustAcquire(t, a, "pod-1/1", "markets", 1)
	waitWaiting := func(n int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			if _, waiting := a.records.load(); waiting == n {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d calls wait for a place, want %d", func() int { _, w := a.records.load(); return w }(), n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	// hold takes the only place of the record semaphore and returns the func that gives it back,
	// also called at the end of the test.
	hold := func() (free func()) {
		t.Helper()
		if err := a.records.acquire(ctx); err != nil {
			t.Fatalf("acquire: %v", err)
		}
		var once sync.Once
		free = func() { once.Do(a.records.release) }
		t.Cleanup(free)
		return free
	}

	free := hold()
	client.ClearActions()
	released := make(chan error, 1)
	confirmed := make(chan error, 1)
	go func() { released <- a.Release(ctx, "pod-0/1", "markets", 0, t0) }()
	go func() {
		_, err := a.Confirm(ctx, "pod-1/1", "markets", 1, t1)
		confirmed <- err
	}()
	waitWaiting(2)
	if got := client.Actions(); len(got) != 0 {
		t.Errorf("requests %v while Release and Confirm wait, want none", got)
	}
	// pod-1/1's session expires while Confirm waits.
	clock.advance(T.LeaseDuration)
	free()
	if err := <-released; err != nil {
		t.Errorf("Release = %v, want nil", err)
	}
	if err := <-confirmed; err == nil || err.Error() != "k8s: session pod-1/1 expired: tenure: session lost" || !errors.Is(err, tenure.ErrSessionLost) {
		t.Errorf("Confirm after the wait = %v, want k8s: session pod-1/1 expired: tenure: session lost", err)
	}
	// Release decides on the cache's state: one update, and Confirm sends nothing.
	got := writes(client)
	if update, ok := firstUpdate(got); len(got) != 1 || len(actions(client, "get")) != 0 || !ok || update.Name != shardName("g", "markets", 0) || holderOf(update) != "" {
		t.Errorf("requests %v, want Release's update of %s alone", client.Actions(), shardName("g", "markets", 0))
	}

	// CloseSession lists the group's Leases, then waits for a place before it releases a record.
	mustKeepAlive(t, a, "pod-2/1")
	t2 := mustAcquire(t, a, "pod-2/1", "markets", 2)
	free = hold()
	client.ClearActions()
	closed := make(chan error, 1)
	go func() { closed <- a.CloseSession(ctx, "pod-2/1") }()
	waitWaiting(1)
	if got := append(actions(client, "get"), writes(client)...); len(got) != 0 {
		t.Errorf("requests %v besides the LIST while CloseSession waits, want none", got)
	}
	free()
	if err := <-closed; err != nil {
		t.Errorf("CloseSession = %v, want nil", err)
	}
	l, err := client.CoordinationV1().Leases("default").Get(ctx, shardName("g", "markets", 2), metav1.GetOptions{})
	if err != nil || holderOf(l) != "" || l.Annotations[annotationTerm] != encodeTerm(t2) {
		t.Errorf("record after CloseSession: %v, %v; want no holder at %d", l, err, t2)
	}
}

// firstUpdate returns the Lease of the first update action in xs.
func firstUpdate(xs []k8stesting.Action) (*coordinationv1.Lease, bool) {
	for _, x := range xs {
		if u, ok := x.(k8stesting.UpdateAction); ok {
			l, ok := u.GetObject().(*coordinationv1.Lease)
			return l, ok
		}
	}
	return nil, false
}

// wantCloseLists checks that the fake client's only requests are n LISTs of the Leases of group
// g, as CloseSession sends to find the holder's records.
func wantCloseLists(t testing.TB, client *fake.Clientset, n int) {
	t.Helper()
	got := client.Actions()
	ok := len(got) == n
	for _, x := range got {
		l, isList := x.(k8stesting.ListActionImpl)
		ok = ok && isList && x.GetResource() == leasesResource && l.GetListRestrictions().Labels.String() == "tenure.group=g"
	}
	if !ok {
		t.Errorf("requests %v, want %d LISTs of the group's Leases", got, n)
	}
}

// version returns the resourceVersion of Lease l as a Holding's Version.
func version(t testing.TB, l *coordinationv1.Lease) uint64 {
	t.Helper()
	v, err := parseVersion(l.Name, l.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestK8S370RecordChecks checks what Release, Confirm and Holdings return without a request:
// ctx.Err() first, then the holder's, the space's, the shard's and the term's errors in that
// order; and, for Confirm only, an error matching tenure.ErrSessionLost for a holder with no
// active session in this Adapter: never opened, ended, or expired.
func TestK8S370RecordChecks(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock()
	client := newFake()
	a := fakeAdapter(t, client, withClock(clock.now))
	closeAtEnd(t, a, "pod-0/1")
	mustKeepAlive(t, a, "pod-0/1")
	if err := a.CloseSession(ctx, "pod-2/1"); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	release := func(holder, space string, shard int, term core.Term) func(context.Context) error {
		return func(ctx context.Context) error { return a.Release(ctx, holder, space, shard, term) }
	}
	confirm := func(holder, space string, shard int, term core.Term) func(context.Context) error {
		return func(ctx context.Context) error {
			h, err := a.Confirm(ctx, holder, space, shard, term)
			if h != (tenure.Holding{}) {
				t.Errorf("Confirm(%q, %q, %d, %d) = %+v with its error, want the zero Holding", holder, space, shard, term, h)
			}
			return err
		}
	}
	holdings := func(space string) func(context.Context) error {
		return func(ctx context.Context) error {
			hs, err := a.Holdings(ctx, space)
			if hs != nil {
				t.Errorf("Holdings(%q) = %v with its error, want nil", space, hs)
			}
			return err
		}
	}
	tests := []struct {
		name string
		ctx  context.Context
		call func(context.Context) error
		want string
		lost bool
	}{
		{name: "Release canceled", ctx: canceled, call: release("", "Bad", -1, 0), want: context.Canceled.Error()},
		{name: "Confirm canceled", ctx: canceled, call: confirm("", "Bad", -1, 0), want: context.Canceled.Error()},
		{name: "Holdings canceled", ctx: canceled, call: holdings("Bad"), want: context.Canceled.Error()},
		{name: "Release holder first", call: release("pod-0", "Bad", -1, 0), want: `k8s: Release: holder "pod-0": has no "/"`},
		{name: "Release space next", call: release("pod-0/1", "Bad", -1, 0), want: `k8s: Release: space "Bad": must be a DNS-1123 label`},
		{name: "Release shard next", call: release("pod-0/1", "markets", -1, 0), want: "k8s: Release: shard -1: must not be negative"},
		{name: "Release term last", call: release("pod-0/1", "markets", 0, 0), want: "k8s: Release: term 0: must not be 0"},
		{name: "Confirm empty holder", call: confirm("", "Bad", -1, 0), want: "k8s: Confirm: empty holder"},
		{name: "Confirm holder zero byte", call: confirm("pod-0/\x00", "markets", 0, 1), want: `k8s: Confirm: holder "pod-0/\x00": contains a zero byte`},
		{name: "Confirm holder Pod/1", call: confirm("Pod/1", "markets", 0, 1), want: `k8s: Confirm: holder "Pod/1": member ID must be a DNS-1123 subdomain`},
		{name: "Confirm space Bad", call: confirm("pod-0/1", "Bad", -1, 0), want: `k8s: Confirm: space "Bad": must be a DNS-1123 label`},
		{name: "Confirm space a/0", call: confirm("pod-0/1", "a/0", 0, 1), want: `k8s: Confirm: space "a/0": must be a DNS-1123 label`},
		{name: "Confirm space slot", call: confirm("pod-0/1", "slot", 0, 1), want: `k8s: Confirm: space "slot": is reserved`},
		{name: "Confirm shard -1", call: confirm("pod-0/1", "markets", -1, 0), want: "k8s: Confirm: shard -1: must not be negative"},
		{name: "Confirm term 0", call: confirm("pod-0/1", "markets", 0, 0), want: "k8s: Confirm: term 0: must not be 0"},
		{name: "Confirm term before the session", call: confirm("pod-1/1", "markets", 0, 0), want: "k8s: Confirm: term 0: must not be 0"},
		{name: "Confirm never opened", call: confirm("pod-1/1", "markets", 0, 1), want: "k8s: session pod-1/1 is not open: tenure: session lost", lost: true},
		{name: "Confirm ended", call: confirm("pod-2/1", "markets", 0, 1), want: "k8s: session pod-2/1 ended: tenure: session lost", lost: true},
		{name: "Holdings space empty", call: holdings(""), want: `k8s: Holdings: space "": must be a DNS-1123 label`},
		{name: "Holdings space zero byte", call: holdings("a\x00"), want: `k8s: Holdings: space "a\x00": must be a DNS-1123 label`},
		{name: "Holdings space slots", call: holdings("slots"), want: `k8s: Holdings: space "slots": is reserved`},
		{name: "Holdings space request-1", call: holdings("request-1"), want: `k8s: Holdings: space "request-1": must not start with "request-"`},
	}
	check := func(t *testing.T, ctx context.Context, call func(context.Context) error, want string, lost bool) {
		t.Helper()
		client.ClearActions()
		err := call(ctx)
		if err == nil || err.Error() != want {
			t.Errorf("err = %v, want %s", err, want)
		}
		if errors.Is(err, tenure.ErrSessionLost) != lost {
			t.Errorf("errors.Is(%v, tenure.ErrSessionLost) = %v, want %v", err, !lost, lost)
		}
		if n := len(client.Actions()); n != 0 {
			t.Errorf("%d requests, want none: %v", n, client.Actions())
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				ctx = t.Context()
			}
			check(t, ctx, tc.call, tc.want, tc.lost)
		})
	}

	t.Run("Confirm expired", func(t *testing.T) {
		clock.advance(T.LeaseDuration)
		check(t, t.Context(), confirm("pod-0/1", "markets", 0, 1), "k8s: session pod-0/1 expired: tenure: session lost", true)
		check(t, t.Context(), confirm("pod-0/1", "markets", 0, 1), "k8s: session pod-0/1 ended: tenure: session lost", true)
	})
}

// TestK8S070VersionFallback checks that a resourceVersion that does not parse, in a write's
// response or a listed record, makes Confirm and Holdings read the record again and report the
// version read; a version that still does not parse is an error, and a record gone by then is
// left out of Holdings.
func TestK8S070VersionFallback(t *testing.T) {
	ctx := t.Context()
	name := shardName("g", "markets", 1)

	t.Run("Holdings", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		tracker := client.Tracker()
		for _, l := range []*coordinationv1.Lease{
			shardLease("g", "markets", 1, "pod-0/1", map[string]string{annotationTerm: "5"}),
			shardLease("g", "markets", 2, "", map[string]string{annotationTerm: "3"}),
		} {
			l.Namespace = "default"
			l.ResourceVersion = "21"
			if l.Name == name {
				l.ResourceVersion = "abc"
			}
			if err := tracker.Add(l); err != nil {
				t.Fatalf("add %s: %v", l.Name, err)
			}
		}
		// A GET of name answers with the version in reread, or NotFound when reread is "".
		reread := "17"
		client.PrependReactor("get", resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
			get, _ := action.(k8stesting.GetAction)
			if get.GetName() != name {
				return false, nil, nil
			}
			if reread == "" {
				return true, nil, apierrors.NewNotFound(leasesResource.GroupResource(), name)
			}
			obj, err := tracker.Get(leasesResource, "default", name)
			if err != nil {
				return true, nil, err
			}
			l, _ := obj.(*coordinationv1.Lease)
			l = l.DeepCopy()
			l.ResourceVersion = reread
			return true, l, nil
		})

		got, err := a.Holdings(ctx, "markets")
		want := []tenure.Holding{
			{Space: "markets", Shard: 1, Holder: "pod-0/1", Term: 5, Version: 17},
			{Space: "markets", Shard: 2, Term: 3, Version: 21},
		}
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("Holdings = %+v, %v; want %+v", got, err, want)
		}
		if n, m := len(actions(client, "list")), len(actions(client, "get")); n != 1 || m != 1 {
			t.Errorf("%d LISTs and %d GETs, want 1 and 1", n, m)
		}

		reread = "abc"
		const wantErr = `k8s: lease g-markets-1: resourceVersion "abc" is not an integer`
		if got, err := a.Holdings(ctx, "markets"); got != nil || err == nil || err.Error() != wantErr {
			t.Errorf("Holdings with a reread version abc = %+v, %v; want nil, %s", got, err, wantErr)
		}

		reread = ""
		if got, err := a.Holdings(ctx, "markets"); err != nil || !slices.Equal(got, want[1:]) {
			t.Errorf("Holdings with the record gone on reread = %+v, %v; want %+v", got, err, want[1:])
		}
	})

	t.Run("Confirm", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		closeAtEnd(t, a, "pod-0/1")
		mustKeepAlive(t, a, "pod-0/1")
		term := mustAcquire(t, a, "pod-0/1", "markets", 1)
		// The update is stored at version 40, and its response says "abc".
		tracker := client.Tracker()
		client.PrependReactor("update", resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
			update, _ := action.(k8stesting.UpdateAction)
			l, _ := update.GetObject().(*coordinationv1.Lease)
			if l.Name != name {
				return false, nil, nil
			}
			l = l.DeepCopy()
			l.ResourceVersion = "40"
			if err := tracker.Update(leasesResource, l, "default"); err != nil {
				return true, nil, err
			}
			l = l.DeepCopy()
			l.ResourceVersion = "abc"
			return true, l, nil
		})
		client.ClearActions()
		got, err := a.Confirm(ctx, "pod-0/1", "markets", 1, term)
		if want := (tenure.Holding{Space: "markets", Shard: 1, Holder: "pod-0/1", Term: term, Version: 40}); err != nil || got != want {
			t.Errorf("Confirm = %+v, %v; want %+v", got, err, want)
		}
		var seq []string
		for _, x := range actions(client, "") {
			if x.GetVerb() != "watch" {
				seq = append(seq, x.GetVerb())
			}
		}
		if strings.Join(seq, " ") != "update get" {
			t.Errorf("requests %v, want [update get]", seq)
		}
	})
}

// TestATK8S21Release checks Release on a cluster: by the holder at its term it clears
// holderIdentity, tenure.session and tenure.confirm with one write from the cache's state,
// keeping the term, the times, leaseTransitions and foreign labels and annotations, and frees the
// shard at once; at another term or by another holder or incarnation, for no record, or for a
// record already released, it returns nil after a GET and writes nothing; it does not need a
// session; a cache that shows another term is checked by a GET; a conflict makes it read the
// record and write again; and a foreign Lease is never written. The acceptance test's steps were
// written from the issue's text: the original text is unavailable.
func TestATK8S21Release(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, recA := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1")
	closeAtEnd(t, b, "pod-1/1")
	mustKeepAlive(t, a, "pod-0/1")
	startRenewer(t, a, "pod-0/1")
	mustKeepAlive(t, b, "pod-1/1")
	startRenewer(t, b, "pod-1/1")
	const h = "pod-0/1"
	// release calls x.Release on shard of markets and checks that it returns nil, with the
	// requests want on the shard's Lease.
	release := func(t *testing.T, x *Adapter, rec *recorder, holder string, shard int, term core.Term, want ...string) {
		t.Helper()
		name := shardName(g, "markets", shard)
		rec.reset()
		if err := x.Release(ctx, holder, "markets", shard, term); err != nil {
			t.Errorf("Release(%s, markets, %d, %d) = %v, want nil", holder, shard, term, err)
		}
		if got := requestStrings(rec.filter(on("", name))); strings.Join(got, ", ") != strings.Join(want, ", ") {
			t.Errorf("Release(%s, markets, %d, %d): requests %v, want %v", holder, shard, term, got, want)
		}
	}

	t.Run("match", func(t *testing.T) {
		name := shardName(g, "markets", 0)
		t1 := mustAcquire(t, a, h, "markets", 0)
		before := rawUpdate(t, c, name, func(l *coordinationv1.Lease) {
			l.Annotations[annotationConfirm] = "4"
			l.Annotations["example.com/note"] = "kept"
			l.Labels["example.com/team"] = "a"
			l.Spec.LeaseTransitions = new(int32(5))
		})
		waitObserved(t, a, name, before.ResourceVersion)
		release(t, a, recA, h, 0, t1, "UPDATE "+name+" 200")
		after := rawGet(t, c, name)
		wantSpec := before.Spec.DeepCopy()
		wantSpec.HolderIdentity = nil
		if !apiequality.Semantic.DeepEqual(after.Spec, *wantSpec) {
			t.Errorf("spec after Release %+v, want %+v", after.Spec, *wantSpec)
		}
		wantAnnotations := maps.Clone(before.Annotations)
		delete(wantAnnotations, annotationSession)
		delete(wantAnnotations, annotationConfirm)
		if !maps.Equal(after.Annotations, wantAnnotations) || after.Annotations[annotationTerm] != encodeTerm(t1) {
			t.Errorf("annotations after Release %v, want %v", after.Annotations, wantAnnotations)
		}
		if !maps.Equal(after.Labels, before.Labels) {
			t.Errorf("labels after Release %v, want %v", after.Labels, before.Labels)
		}

		// The shard is free at once: another holder takes it without reading the member Lease.
		waitObserved(t, b, name, after.ResourceVersion)
		recB.reset()
		if t2 := mustAcquire(t, b, "pod-1/1", "markets", 0); t2 <= t1 {
			t.Errorf("term %d after the Release, want above %d", t2, t1)
		}
		if n := recB.count(on(verbGet, memberName(g, "pod-0"))); n != 0 {
			t.Errorf("%d GETs of the released holder's member Lease, want none", n)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		name := shardName(g, "markets", 1)
		t1 := mustAcquire(t, a, h, "markets", 1)
		l := rawGet(t, c, name)
		waitObserved(t, a, name, l.ResourceVersion)
		for _, x := range []struct {
			holder string
			term   core.Term
		}{{h, t1 + 1}, {h, t1 - 1}, {"pod-1/1", t1}, {"pod-0/2", t1}} {
			release(t, a, recA, x.holder, 1, x.term, "GET "+name+" 200")
		}
		if got := rawGet(t, c, name); got.ResourceVersion != l.ResourceVersion {
			t.Errorf("record written: resourceVersion %s, want %s", got.ResourceVersion, l.ResourceVersion)
		}

		absent := shardName(g, "markets", 99)
		release(t, a, recA, h, 99, t1, "GET "+absent+" 404")
		if rawGet(t, c, absent) != nil {
			t.Errorf("Release created %s", absent)
		}

		release(t, a, recA, h, 1, t1, "UPDATE "+name+" 200")
		l = rawGet(t, c, name)
		release(t, a, recA, h, 1, t1, "GET "+name+" 200")
		if got := rawGet(t, c, name); got.ResourceVersion != l.ResourceVersion {
			t.Errorf("the released record written again: resourceVersion %s, want %s", got.ResourceVersion, l.ResourceVersion)
		}
	})

	t.Run("without a session", func(t *testing.T) {
		r, recR := c.adapter(t, g)
		name := shardName(g, "markets", 2)
		t1 := mustAcquire(t, a, h, "markets", 2)
		release(t, r, recR, h, 2, t1, "GET "+name+" 200", "UPDATE "+name+" 200")
		if l := rawGet(t, c, name); holderOf(l) != "" || l.Annotations[annotationTerm] != encodeTerm(t1) {
			t.Errorf("record names %q at %s, want no holder at %d", holderOf(l), l.Annotations[annotationTerm], t1)
		}
	})

	t.Run("stale cache", func(t *testing.T) {
		name := shardName(g, "markets", 3)
		t1 := mustAcquire(t, a, h, "markets", 3)
		waitObserved(t, b, name, rawGet(t, c, name).ResourceVersion)
		resume := recB.stall(on(verbWatch, ""))
		t.Cleanup(resume) // before the sessions close
		t2 := mustAcquire(t, a, h, "markets", 3)
		if o, _ := b.cached(name); o.lease.Annotations[annotationTerm] != encodeTerm(t1) {
			t.Fatalf("the stalled cache shows term %s, want %d", o.lease.Annotations[annotationTerm], t1)
		}
		release(t, b, recB, h, 3, t2, "GET "+name+" 200", "UPDATE "+name+" 200")
		resume()
		if l := rawGet(t, c, name); holderOf(l) != "" || l.Annotations[annotationTerm] != encodeTerm(t2) {
			t.Errorf("record names %q at %s, want no holder at %d", holderOf(l), l.Annotations[annotationTerm], t2)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		name := shardName(g, "markets", 4)
		t1 := mustAcquire(t, a, h, "markets", 4)
		recA.bump(name, time.Time{})
		release(t, a, recA, h, 4, t1, "UPDATE "+name+" 409", "GET "+name+" 200", "UPDATE "+name+" 200")
		if l := rawGet(t, c, name); holderOf(l) != "" || l.Annotations[annotationTerm] != encodeTerm(t1) || l.Annotations[bumpAnnotation] != "1" {
			t.Errorf("record names %q, annotations %v; want no holder at %d, bumped once", holderOf(l), l.Annotations, t1)
		}
	})

	t.Run("foreign lease", func(t *testing.T) {
		holder := h
		l := rawCreate(t, c, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: shardName(g, "markets", 7), Annotations: map[string]string{annotationTerm: "5"}},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
		})
		recA.reset()
		want := "k8s: lease " + l.Name + " is not a shard lease of group " + g
		if err := a.Release(ctx, h, "markets", 7, 5); err == nil || err.Error() != want {
			t.Errorf("Release of %s = %v, want %s", l.Name, err, want)
		}
		if got := requestStrings(recA.filter(on("", l.Name))); len(got) != 1 || got[0] != "GET "+l.Name+" 200" {
			t.Errorf("requests %v, want [GET %s 200]", got, l.Name)
		}
		if got := rawGet(t, c, l.Name); got.ResourceVersion != l.ResourceVersion {
			t.Errorf("%s written: resourceVersion %s, want %s", l.Name, got.ResourceVersion, l.ResourceVersion)
		}
	})
}

// TestATK8S22Holdings checks Holdings on a cluster: the records of the space with their holders,
// terms and versions as stored, sorted by shard, also records with no holder, a holder whose
// session ended or never existed, or a malformed term (term 0); not the records of other spaces
// or groups, Leases without the group's shard labels, or names that are no record's of the space;
// nil for a space with no records; a slice of the caller's own; each from one LIST with the
// space's label selector and no other request, with or without a session; and versions that grow
// with every write. The acceptance test's steps were written from the issue's text: the
// original text is unavailable.
func TestATK8S22Holdings(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, recA := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	h, rec := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1")
	closeAtEnd(t, b, "pod-1/1")
	mustKeepAlive(t, a, "pod-0/1")
	startRenewer(t, a, "pod-0/1")
	mustKeepAlive(t, b, "pod-1/1")
	startRenewer(t, b, "pod-1/1")

	t7 := mustAcquire(t, a, "pod-0/1", "markets", 7)
	t2 := mustAcquire(t, b, "pod-1/1", "markets", 2)
	t11 := mustAcquire(t, a, "pod-0/1", "markets", 11)
	t0 := mustAcquire(t, b, "pod-1/1", "markets", 0)
	if err := b.Release(ctx, "pod-1/1", "markets", 2, t2); err != nil {
		t.Fatalf("Release: %v", err)
	}
	t3 := mustAcquire(t, a, "pod-0/1", "orders", 3)
	// pod-8/1's session has ended; pod-9/1 never had one, and its record's term is malformed.
	ended := rawCreate(t, c, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: memberName(g, "pod-8"), Labels: leaseLabels(g, kindMember, "")}})
	rawCreate(t, c, shardLease(g, "markets", 9, "pod-8/1", map[string]string{annotationTerm: "3", annotationSession: string(ended.UID)}))
	rawCreate(t, c, shardLease(g, "markets", 5, "pod-9/1", map[string]string{annotationTerm: "x"}))
	// No records of markets in g: another group's record, a Lease without the labels, a labeled
	// Lease whose name is no record's, and one named as a record of another space.
	rawCreate(t, c, shardLease(group(), "markets", 4, "pod-0/1", map[string]string{annotationTerm: "1"}))
	rawCreate(t, c, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: shardName(g, "markets", 6)}})
	rawCreate(t, c, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: g + "-markets-x", Labels: leaseLabels(g, kindShard, "markets")}})
	rawCreate(t, c, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: shardName(g, "orders", 8), Labels: leaseLabels(g, kindShard, "markets")}})

	// stored returns hs in space with the versions their Leases have now.
	stored := func(space string, hs ...tenure.Holding) []tenure.Holding {
		for i := range hs {
			hs[i].Space = space
			hs[i].Version = version(t, rawGet(t, c, shardName(g, space, hs[i].Shard)))
		}
		return hs
	}
	// holdings checks that x's Holdings of space returns want from one LIST and no other request
	// but those of x's own member Lease.
	holdings := func(t *testing.T, x *Adapter, rec *recorder, space string, want []tenure.Holding) []tenure.Holding {
		t.Helper()
		rec.reset()
		got, err := x.Holdings(ctx, space)
		if err != nil || !slices.Equal(got, want) || (want == nil) != (got == nil) {
			t.Errorf("Holdings(%s) = %+v, %v; want %+v", space, got, err, want)
		}
		reqs := rec.filter(func(r request) bool {
			return r.verb != verbWatch && r.name != memberName(g, "pod-0")
		})
		sel := labelGroup + "=" + g + "," + labelKind + "=shard," + labelSpace + "=" + space
		if len(reqs) != 1 || reqs[0].verb != verbList || reqs[0].status != 200 || reqs[0].query.Get("labelSelector") != sel {
			t.Errorf("Holdings(%s): requests %v (%v), want one LIST with labelSelector %s", space, requestStrings(reqs), reqs, sel)
		}
		return got
	}

	markets := stored("markets",
		tenure.Holding{Shard: 0, Holder: "pod-1/1", Term: t0},
		tenure.Holding{Shard: 2, Term: t2},
		tenure.Holding{Shard: 5, Holder: "pod-9/1"},
		tenure.Holding{Shard: 7, Holder: "pod-0/1", Term: t7},
		tenure.Holding{Shard: 9, Holder: "pod-8/1", Term: 3},
		tenure.Holding{Shard: 11, Holder: "pod-0/1", Term: t11},
	)

	t.Run("as stored", func(t *testing.T) {
		got := holdings(t, h, rec, "markets", markets)
		holdings(t, a, recA, "markets", markets)
		holdings(t, h, rec, "orders", stored("orders", tenure.Holding{Shard: 3, Holder: "pod-0/1", Term: t3}))
		holdings(t, h, rec, "trades", nil)
		if len(got) == len(markets) {
			got[0].Holder, got[1].Term = "x", 0
			holdings(t, h, rec, "markets", markets)
		}
	})

	t.Run("versions", func(t *testing.T) {
		var top uint64
		for _, x := range markets {
			top = max(top, x.Version)
		}
		if err := a.Release(ctx, "pod-0/1", "markets", 7, t7); err != nil {
			t.Fatalf("Release: %v", err)
		}
		t2b := mustAcquire(t, b, "pod-1/1", "markets", 2)
		now := slices.Clone(markets)
		now[1].Holder, now[1].Term = "pod-1/1", t2b
		now[3].Holder = ""
		now[1].Version = version(t, rawGet(t, c, shardName(g, "markets", 2)))
		now[3].Version = version(t, rawGet(t, c, shardName(g, "markets", 7)))
		holdings(t, h, rec, "markets", now)
		if now[3].Version <= top || now[1].Version <= now[3].Version {
			t.Errorf("versions %d (release) and %d (grant) after %d, want growing", now[3].Version, now[1].Version, top)
		}
	})
}

// TestATK8S37Confirm checks Confirm on a cluster. counts: the holder's Confirm at its term
// writes tenure.confirm 1, 2, 3, changing nothing else, from the cache's state with one update,
// and returns the record written with a growing version; another holder's, or one at another
// term, returns the record as stored after a GET with no write; for no record it returns
// Holding{Space, Shard}; a conflict makes it read the record and write again; a cache that shows
// another term is checked by a GET; Release and a re-grant remove the counter. race: a degraded owner, whose clock stands still so its sessions
// stay active by it while another Adapter sees them unchanged for their LeaseDuration, renews and
// confirms while the other takes the shard over, in 10 rounds; each round has exactly one
// outcome: the takeover commits first and Confirm shows it, or Confirm commits first and the
// takeover, decided on an earlier state, fails and is held. in flight: in 3 rounds a takeover
// decided before the owner's renewal is held back until the owner has renewed and confirmed;
// its write then fails, and the takeover, decided again, is held. The acceptance test's steps
// were written from the issue's text: the original text is unavailable.
func TestATK8S37Confirm(t *testing.T) {
	c := newCluster(t)
	ctx := t.Context()

	t.Run("counts", func(t *testing.T) {
		g := group()
		a, recA := c.adapter(t, g)
		b, recB := c.adapter(t, g)
		closeAtEnd(t, a, "pod-0/1")
		closeAtEnd(t, b, "pod-1/1")
		mustKeepAlive(t, a, "pod-0/1")
		startRenewer(t, a, "pod-0/1")
		mustKeepAlive(t, b, "pod-1/1")
		startRenewer(t, b, "pod-1/1")
		const h = "pod-0/1"
		name := shardName(g, "markets", 0)
		t1 := mustAcquire(t, a, h, "markets", 0)
		base := rawUpdate(t, c, name, func(l *coordinationv1.Lease) {
			l.Annotations["example.com/note"] = "kept"
			l.Labels["example.com/team"] = "a"
		})
		waitObserved(t, a, name, base.ResourceVersion)

		// confirm calls x.Confirm on shard of markets and checks that it returns no error, with the
		// requests want on the shard's Lease.
		confirm := func(t *testing.T, x *Adapter, rec *recorder, holder string, shard int, term core.Term, want ...string) tenure.Holding {
			t.Helper()
			rec.reset()
			got, err := x.Confirm(ctx, holder, "markets", shard, term)
			if err != nil {
				t.Fatalf("Confirm(%s, markets, %d, %d) = %v", holder, shard, term, err)
			}
			if seq := requestStrings(rec.filter(on("", shardName(g, "markets", shard)))); strings.Join(seq, ", ") != strings.Join(want, ", ") {
				t.Errorf("Confirm(%s, markets, %d, %d): requests %v, want %v", holder, shard, term, seq, want)
			}
			return got
		}
		// stored checks that got is the record of shard as stored now, and returns that.
		stored := func(t *testing.T, got tenure.Holding, shard int) *coordinationv1.Lease {
			t.Helper()
			l := rawGet(t, c, shardName(g, "markets", shard))
			want := tenure.Holding{Space: "markets", Shard: shard}
			if l != nil {
				term, _ := decodeTerm(l.Annotations)
				want.Holder, want.Term, want.Version = holderOf(l), term, version(t, l)
			}
			if got != want {
				t.Errorf("Confirm = %+v, want the record as stored, %+v", got, want)
			}
			return l
		}

		last := version(t, base)
		for n := 1; n <= 3; n++ {
			got := confirm(t, a, recA, h, 0, t1, "UPDATE "+name+" 200")
			l := stored(t, got, 0)
			if got.Holder != h || got.Term != t1 || got.Version <= last {
				t.Errorf("Confirm %d = %+v, want %s at %d with a version above %d", n, got, h, t1, last)
			}
			last = got.Version
			wantAnnotations := maps.Clone(base.Annotations)
			wantAnnotations[annotationConfirm] = strconv.Itoa(n)
			if !maps.Equal(l.Annotations, wantAnnotations) || !maps.Equal(l.Labels, base.Labels) || !apiequality.Semantic.DeepEqual(l.Spec, base.Spec) {
				t.Errorf("record after Confirm %d: %+v; want only %s=%d changed from %+v", n, l, annotationConfirm, n, base)
			}
		}

		l := rawGet(t, c, name)
		waitObserved(t, b, name, l.ResourceVersion)
		stored(t, confirm(t, b, recB, "pod-1/1", 0, t1, "GET "+name+" 200"), 0)
		stored(t, confirm(t, a, recA, h, 0, t1+1, "GET "+name+" 200"), 0)
		if got := rawGet(t, c, name); got.ResourceVersion != l.ResourceVersion {
			t.Errorf("record written: resourceVersion %s, want %s", got.ResourceVersion, l.ResourceVersion)
		}

		absent := shardName(g, "markets", 9)
		if got := confirm(t, a, recA, h, 9, t1, "GET "+absent+" 404"); got != (tenure.Holding{Space: "markets", Shard: 9}) {
			t.Errorf("Confirm of no record = %+v, want {Space: markets, Shard: 9}", got)
		}
		if rawGet(t, c, absent) != nil {
			t.Errorf("Confirm created %s", absent)
		}

		recA.bump(name, time.Time{})
		got := confirm(t, a, recA, h, 0, t1, "UPDATE "+name+" 409", "GET "+name+" 200", "UPDATE "+name+" 200")
		if l := stored(t, got, 0); l.Annotations[annotationConfirm] != "4" || l.Annotations[bumpAnnotation] != "1" {
			t.Errorf("annotations %v after a conflict, want %s=4 and the bump", l.Annotations, annotationConfirm)
		}

		// A cache that shows another term is checked by a GET, and the record written when it
		// names the holder at the term.
		name1 := shardName(g, "markets", 1)
		t1b := mustAcquire(t, a, h, "markets", 1)
		waitObserved(t, a, name1, rawGet(t, c, name1).ResourceVersion)
		resume := recA.stall(on(verbWatch, ""))
		t.Cleanup(resume) // before the sessions close
		rawUpdate(t, c, name1, func(l *coordinationv1.Lease) { l.Annotations[annotationTerm] = encodeTerm(t1b + 1) })
		got = confirm(t, a, recA, h, 1, t1b+1, "GET "+name1+" 200", "UPDATE "+name1+" 200")
		resume()
		if l := stored(t, got, 1); l.Annotations[annotationConfirm] != "1" || got.Term != t1b+1 {
			t.Errorf("Confirm over a stale cache = %+v, annotations %v; want term %d and %s=1", got, l.Annotations, t1b+1, annotationConfirm)
		}

		if err := a.Release(ctx, h, "markets", 0, t1); err != nil {
			t.Fatalf("Release: %v", err)
		}
		l = stored(t, confirm(t, a, recA, h, 0, t1, "GET "+name+" 200"), 0)
		if _, ok := l.Annotations[annotationConfirm]; ok || holderOf(l) != "" || l.Annotations[annotationTerm] != encodeTerm(t1) {
			t.Errorf("record after Release %+v, want no holder and no %s at %d", l, annotationConfirm, t1)
		}
		t2 := mustAcquire(t, a, h, "markets", 0)
		confirm(t, a, recA, h, 0, t2, "UPDATE "+name+" 200")
		mustAcquire(t, a, h, "markets", 0)
		if l := rawGet(t, c, name); l.Annotations[annotationConfirm] != "" {
			t.Errorf("%s %q after a re-grant, want none", annotationConfirm, l.Annotations[annotationConfirm])
		}
	})

	t.Run("race", func(t *testing.T) {
		g := group()
		clock := newFakeClock()
		a, _ := c.adapter(t, g, withClock(clock.now))
		b, _ := c.adapter(t, g)
		const rounds = 10
		const taker = "pod-10/1"
		closeAtEnd(t, b, taker)
		mustKeepAlive(t, b, taker)
		startRenewer(t, b, taker)
		owners := make([]string, rounds)
		terms := make([]core.Term, rounds)
		for i := range rounds {
			owners[i] = fmt.Sprintf("pod-%d/1", i)
			closeAtEnd(t, a, owners[i])
			mustKeepAlive(t, a, owners[i])
			terms[i] = mustAcquire(t, a, owners[i], "race", i)
		}
		for i := range rounds {
			for _, name := range []string{memberName(g, memberOf(owners[i])), shardName(g, "race", i)} {
				waitObserved(t, b, name, rawGet(t, c, name).ResourceVersion)
			}
		}
		// b has observed every owner's session unchanged for its LeaseDuration.
		time.Sleep(T.LeaseDuration + 200*time.Millisecond)

		var taken, confirmed int
		for i := range rounds {
			var takeTerm core.Term
			var takeErr, renewErr, confirmErr error
			var got tenure.Holding
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				takeTerm, takeErr = b.Acquire(ctx, taker, "race", i)
			})
			wg.Go(func() {
				<-start
				if renewErr = a.KeepAlive(ctx, owners[i], T); renewErr == nil {
					got, confirmErr = a.Confirm(ctx, owners[i], "race", i, terms[i])
				}
			})
			close(start)
			wg.Wait()
			if renewErr != nil || confirmErr != nil {
				t.Errorf("round %d: KeepAlive = %v, Confirm = %v; want nil, nil", i, renewErr, confirmErr)
				continue
			}
			l := rawGet(t, c, shardName(g, "race", i))
			var held *tenure.HeldError
			switch {
			case takeErr == nil:
				taken++
				want := tenure.Holding{Space: "race", Shard: i, Holder: taker, Term: takeTerm, Version: version(t, l)}
				if takeTerm <= terms[i] || got != want || holderOf(l) != taker {
					t.Errorf("round %d: taken over at %d after %d; Confirm = %+v, want %+v", i, takeTerm, terms[i], got, want)
				}
			case errors.As(takeErr, &held) && held.Holder == owners[i] && takeTerm == 0:
				confirmed++
				want := tenure.Holding{Space: "race", Shard: i, Holder: owners[i], Term: terms[i], Version: version(t, l)}
				if got != want || l.Annotations[annotationConfirm] != "1" {
					t.Errorf("round %d: held; Confirm = %+v, record %s=%q; want %+v and 1", i, got, annotationConfirm, l.Annotations[annotationConfirm], want)
				}
			default:
				t.Errorf("round %d: Acquire = %d, %v; want a term or a *tenure.HeldError naming %s", i, takeTerm, takeErr, owners[i])
			}
		}
		t.Logf("%d rounds with the takeover first, %d with Confirm first", taken, confirmed)
	})

	t.Run("in flight", func(t *testing.T) {
		g := group()
		clock := newFakeClock()
		a, _ := c.adapter(t, g, withClock(clock.now))
		b, recB := c.adapter(t, g)
		const rounds = 3
		const taker = "pod-10/1"
		closeAtEnd(t, b, taker)
		mustKeepAlive(t, b, taker)
		startRenewer(t, b, taker)
		owners := make([]string, rounds)
		terms := make([]core.Term, rounds)
		for i := range rounds {
			owners[i] = fmt.Sprintf("pod-%d/1", i)
			closeAtEnd(t, a, owners[i])
			mustKeepAlive(t, a, owners[i])
			terms[i] = mustAcquire(t, a, owners[i], "race", i)
		}
		for i := range rounds {
			for _, name := range []string{memberName(g, memberOf(owners[i])), shardName(g, "race", i)} {
				waitObserved(t, b, name, rawGet(t, c, name).ResourceVersion)
			}
		}
		time.Sleep(T.LeaseDuration + 200*time.Millisecond)

		// In each round the takeover is decided before the owner's renewal, its write held until
		// the owner has renewed and Confirm has returned: the write must fail, and the takeover,
		// decided again, must see the renewal.
		for i := range rounds {
			name := shardName(g, "race", i)
			recB.reset()
			resume := recB.stall(on(verbUpdate, name))
			var takeTerm core.Term
			var takeErr error
			done := make(chan struct{})
			go func() {
				defer close(done)
				takeTerm, takeErr = b.Acquire(ctx, taker, "race", i)
			}()
			recB.waitFor(t, 5*time.Second, on(verbUpdate, name))
			if err := a.KeepAlive(ctx, owners[i], T); err != nil {
				resume()
				<-done
				t.Fatalf("round %d: KeepAlive: %v", i, err)
			}
			got, err := a.Confirm(ctx, owners[i], "race", i, terms[i])
			l := rawGet(t, c, name)
			resume()
			<-done
			if err != nil {
				t.Fatalf("round %d: Confirm: %v", i, err)
			}
			want := tenure.Holding{Space: "race", Shard: i, Holder: owners[i], Term: terms[i], Version: version(t, l)}
			if got != want || l.Annotations[annotationConfirm] != "1" {
				t.Errorf("round %d: Confirm = %+v, record %s=%q; want %+v and 1", i, got, annotationConfirm, l.Annotations[annotationConfirm], want)
			}
			var held *tenure.HeldError
			if !errors.As(takeErr, &held) || held.Holder != owners[i] || takeTerm != 0 {
				t.Errorf("round %d: Acquire after Confirm = %d, %v; want a *tenure.HeldError naming %s", i, takeTerm, takeErr, owners[i])
			}
			if seq := requestStrings(recB.filter(on("", name))); strings.Join(seq, ", ") != "UPDATE "+name+" 409, GET "+name+" 200" {
				t.Errorf("round %d: the takeover's requests %v, want [UPDATE %s 409, GET %s 200]", i, seq, name, name)
			}
			if after := rawGet(t, c, name); after.ResourceVersion != l.ResourceVersion {
				t.Errorf("round %d: record written after Confirm: names %q at %s, resourceVersion %s; want %s", i,
					holderOf(after), after.Annotations[annotationTerm], after.ResourceVersion, l.ResourceVersion)
			}
		}
	})
}

// TestK8S240CloseSessionShards checks step 1 of CloseSession on a cluster: one LIST of the
// group's Leases, then each shard record naming the holder released at its own term, in name
// order, also one with a malformed term, and after a conflict; other holders' records and
// foreign Leases are left; a failed release or LIST does not stop the other records or steps 2
// and 3, and its error is returned; and a later CloseSession, also by another Adapter, releases
// what is left.
func TestK8S240CloseSessionShards(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g)
	b, recB := c.adapter(t, g)
	closeAtEnd(t, b, "pod-1/1")
	mustKeepAlive(t, b, "pod-1/1")
	startRenewer(t, b, "pod-1/1")
	// steps returns the requests of rec on the group's Leases but WATCHes and pod-1's renewals.
	steps := func(rec *recorder) []string {
		return requestStrings(rec.filter(func(r request) bool {
			return r.resource == resourceLeases && r.verb != verbWatch && r.name != memberName(g, "pod-1")
		}))
	}
	// released checks that record name names no holder, at term, without tenure.session and
	// tenure.confirm.
	released := func(t *testing.T, name, term string) {
		t.Helper()
		l := rawGet(t, c, name)
		_, session := l.Annotations[annotationSession]
		_, confirm := l.Annotations[annotationConfirm]
		if holderOf(l) != "" || l.Annotations[annotationTerm] != term || session || confirm {
			t.Errorf("%s names %q, annotations %v; want no holder at term %s, no session and no confirm", name, holderOf(l), l.Annotations, term)
		}
	}

	t.Run("release", func(t *testing.T) {
		const h = "pod-0/1"
		mustKeepAlive(t, a, h)
		terms := map[string]string{}
		for _, x := range []struct {
			space string
			shard int
		}{{"markets", 3}, {"markets", 1}, {"markets", 10}, {"orders", 2}} {
			terms[shardName(g, x.space, x.shard)] = encodeTerm(mustAcquire(t, a, h, x.space, x.shard))
		}
		t3, _ := decodeTerm(map[string]string{annotationTerm: terms[shardName(g, "markets", 3)]})
		if _, err := a.Confirm(ctx, h, "markets", 3, t3); err != nil {
			t.Fatalf("Confirm: %v", err)
		}
		malformed := rawCreate(t, c, shardLease(g, "orders", 0, h, map[string]string{annotationTerm: "x"}))
		terms[malformed.Name] = "x"
		mustAcquire(t, b, "pod-1/1", "markets", 2)
		other := rawGet(t, c, shardName(g, "markets", 2))
		holder := h
		foreign := rawCreate(t, c, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: shardName(g, "markets", 20)},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
		})
		rec.bump(shardName(g, "markets", 10), time.Time{})

		rec.reset()
		if err := a.CloseSession(ctx, h); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		m1, m3, m10 := shardName(g, "markets", 1), shardName(g, "markets", 3), shardName(g, "markets", 10)
		o0, o2 := shardName(g, "orders", 0), shardName(g, "orders", 2)
		want := []string{
			"LIST 200",
			"UPDATE " + m1 + " 200",
			"UPDATE " + m10 + " 409", "GET " + m10 + " 200", "UPDATE " + m10 + " 200",
			"UPDATE " + m3 + " 200",
			"UPDATE " + o0 + " 200",
			"UPDATE " + o2 + " 200",
			"DELETE " + memberName(g, "pod-0") + " 200",
		}
		if got := steps(rec); strings.Join(got, ", ") != strings.Join(want, ", ") {
			t.Errorf("requests %v, want %v", got, want)
		}
		if lists := rec.filter(on(verbList, "")); len(lists) == 0 || lists[0].query.Get("labelSelector") != labelGroup+"="+g {
			t.Errorf("LIST %v, want one with labelSelector %s=%s", lists, labelGroup, g)
		}
		for name, term := range terms {
			released(t, name, term)
		}
		if l := rawGet(t, c, m10); l.Annotations[bumpAnnotation] != "1" {
			t.Errorf("%s annotations %v, want the bump kept", m10, l.Annotations)
		}
		for _, l := range []*coordinationv1.Lease{other, foreign} {
			if got := rawGet(t, c, l.Name); got.ResourceVersion != l.ResourceVersion {
				t.Errorf("%s written: resourceVersion %s, want %s", l.Name, got.ResourceVersion, l.ResourceVersion)
			}
		}
	})

	t.Run("failed release", func(t *testing.T) {
		const h = "pod-2/1"
		mustKeepAlive(t, a, h)
		names := make([]string, 3)
		terms := map[string]string{}
		for i := range names {
			names[i] = shardName(g, "markets", 30+i)
			terms[names[i]] = encodeTerm(mustAcquire(t, a, h, "markets", 30+i))
		}
		rec.failNext(on(verbUpdate, names[1]), 1)
		err := a.CloseSession(ctx, h)
		prefix := "k8s: release " + names[1] + ": "
		if err == nil || !strings.HasPrefix(err.Error(), prefix) || errors.Is(err, tenure.ErrSessionLost) {
			t.Errorf("CloseSession = %v, want %s<a server error>", err, prefix)
		}
		released(t, names[0], terms[names[0]])
		released(t, names[2], terms[names[2]])
		if l := rawGet(t, c, names[1]); holderOf(l) != h {
			t.Errorf("%s names %q, want %s still", names[1], holderOf(l), h)
		}
		mname := memberName(g, "pod-2")
		if rawGet(t, c, mname) != nil {
			t.Errorf("member Lease %s still exists", mname)
		}
		if err := a.KeepAlive(ctx, h, T); !errors.Is(err, tenure.ErrSessionLost) {
			t.Errorf("KeepAlive after CloseSession = %v, want an error matching tenure.ErrSessionLost", err)
		}

		// Another Adapter, which does not know the holder, releases the record left.
		waitForgotten(t, b, mname)
		recB.reset()
		if err := b.CloseSession(ctx, h); err != nil {
			t.Fatalf("CloseSession by another Adapter: %v", err)
		}
		want := []string{"LIST 200", "UPDATE " + names[1] + " 200", "GET " + mname + " 404"}
		if got := steps(recB); strings.Join(got, ", ") != strings.Join(want, ", ") {
			t.Errorf("requests %v, want %v", got, want)
		}
		released(t, names[1], terms[names[1]])
	})

	t.Run("failed list", func(t *testing.T) {
		const h = "pod-3/1"
		mustKeepAlive(t, a, h)
		name := shardName(g, "markets", 40)
		term := encodeTerm(mustAcquire(t, a, h, "markets", 40))
		rec.failNext(on(verbList, ""), 1)
		err := a.CloseSession(ctx, h)
		prefix := "k8s: close " + h + ": list leases: "
		if err == nil || !strings.HasPrefix(err.Error(), prefix) {
			t.Errorf("CloseSession = %v, want %s<a server error>", err, prefix)
		}
		if l := rawGet(t, c, name); holderOf(l) != h {
			t.Errorf("%s names %q, want %s still", name, holderOf(l), h)
		}
		if rawGet(t, c, memberName(g, "pod-3")) != nil {
			t.Error("the member Lease still exists")
		}
		if err := a.CloseSession(ctx, h); err != nil {
			t.Fatalf("CloseSession again: %v", err)
		}
		released(t, name, term)
	})
}

// TestK8S330AcquireRaces checks how Acquire's conflicts count: writes that lose to grants of the
// shard to other holders, which leave the record free again with a larger term, do not use its
// three attempts up, so racing acquirers get a term or a *tenure.HeldError; writes that lose to
// changes that keep the term end it after three with "k8s: lease <name>: 3 conflicts: …".
func TestK8S330AcquireRaces(t *testing.T) {
	const big = core.Term(5_000_000_000_000_000)
	name := shardName("g", "markets", 0)
	// race makes the first n updates of the record conflict, each after change changed the
	// stored record, and returns the Adapter, with opts, and its client.
	race := func(t *testing.T, n int, change func(l *coordinationv1.Lease), opts ...Option) (*Adapter, *fake.Clientset) {
		client := newFake()
		a := fakeAdapter(t, client, opts...)
		closeAtEnd(t, a, "pod-0/1")
		mustKeepAlive(t, a, "pod-0/1")
		l := shardLease("g", "markets", 0, "", map[string]string{annotationTerm: encodeTerm(big)})
		if _, err := client.CoordinationV1().Leases("default").Create(t.Context(), l, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create: %v", err)
		}
		lost := 0
		client.PrependReactor("update", resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
			update, _ := action.(k8stesting.UpdateAction)
			if l, _ := update.GetObject().(*coordinationv1.Lease); l == nil || l.Name != name || lost == n {
				return false, nil, nil
			}
			lost++
			obj, err := client.Tracker().Get(leasesResource, "default", name)
			if err != nil {
				return true, nil, err
			}
			stored, _ := obj.(*coordinationv1.Lease)
			stored = stored.DeepCopy()
			change(stored)
			if err := client.Tracker().Update(leasesResource, stored, "default"); err != nil {
				return true, nil, err
			}
			return true, nil, apierrors.NewConflict(leasesResource.GroupResource(), name, errors.New("injected"))
		})
		client.ClearActions()
		return a, client
	}

	t.Run("grants to others", func(t *testing.T) {
		// Each lost write met a grant and a release by another holder: one term more.
		a, client := race(t, 5, func(l *coordinationv1.Lease) {
			term, _ := decodeTerm(l.Annotations)
			l.Annotations[annotationTerm] = encodeTerm(term + 1)
		})
		if got := mustAcquire(t, a, "pod-0/1", "markets", 0); got != big+6 {
			t.Errorf("Acquire = %d, want %d", got, big+6)
		}
		if got := actions(client, "update"); len(got) != 6 {
			t.Errorf("updates %v, want 6", got)
		}
	})

	t.Run("changes that keep the term", func(t *testing.T) {
		a, client := race(t, 3, func(l *coordinationv1.Lease) {
			l.Annotations["other"] += "x"
		})
		term, err := a.Acquire(t.Context(), "pod-0/1", "markets", 0)
		const prefix = "k8s: lease g-markets-0: 3 conflicts: "
		if term != 0 || err == nil || !strings.HasPrefix(err.Error(), prefix) || !apierrors.IsConflict(err) {
			t.Errorf("Acquire = %d, %v; want 0 and %s<a Conflict>", term, err, prefix)
		}
		if got := actions(client, "update"); len(got) != 3 {
			t.Errorf("updates %v, want 3", got)
		}
	})

	t.Run("grants, then changes that keep the term", func(t *testing.T) {
		// Three writes lose to grants, which start the attempts over from a new read of the
		// record; the next three lose to changes that keep the term, which use them up.
		lost := 0
		a, client := race(t, 6, func(l *coordinationv1.Lease) {
			if lost++; lost <= 3 {
				term, _ := decodeTerm(l.Annotations)
				l.Annotations[annotationTerm] = encodeTerm(term + 1)
				return
			}
			l.Annotations["other"] += "x"
		})
		term, err := a.Acquire(t.Context(), "pod-0/1", "markets", 0)
		const prefix = "k8s: lease g-markets-0: 3 conflicts: "
		if term != 0 || err == nil || !strings.HasPrefix(err.Error(), prefix) || !apierrors.IsConflict(err) {
			t.Errorf("Acquire = %d, %v; want 0 and %s<a Conflict>", term, err, prefix)
		}
		if got := actions(client, "update"); len(got) != 6 {
			t.Errorf("updates %v, want 6", got)
		}
	})

	t.Run("session expired over the attempts", func(t *testing.T) {
		// The session expires while the writes lose to grants: Acquire starts no new round.
		clk := newFakeClock()
		lost := 0
		a, client := race(t, 6, func(l *coordinationv1.Lease) {
			term, _ := decodeTerm(l.Annotations)
			l.Annotations[annotationTerm] = encodeTerm(term + 1)
			if lost++; lost == 3 {
				clk.advance(T.LeaseDuration)
			}
		}, withClock(clk.now))
		term, err := a.Acquire(t.Context(), "pod-0/1", "markets", 0)
		const want = "k8s: session pod-0/1 expired: tenure: session lost"
		if term != 0 || !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("Acquire = %d, %v; want 0 and %s", term, err, want)
		}
		if got := actions(client, "update"); len(got) != 3 {
			t.Errorf("updates %v, want 3", got)
		}
	})
}
