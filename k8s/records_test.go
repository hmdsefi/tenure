// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	if tb, ok := c.tombs[u1]; !ok || !tb.tokenSeenAt.Equal(t0) {
		t.Errorf("tombstone of the replaced member Lease %+v, %v; want one from %v", tb, ok, t0)
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
