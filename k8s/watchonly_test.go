// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/membershiptest"
)

// readonlyVerbs are the verbs rbac-readonly.yaml grants on Leases.
var readonlyVerbs = []string{"get", "list", "watch"}

// applyReadonlyRBAC creates the objects of rbac-readonly.yaml in c.ns, after checking them, and
// returns the user of its service account.
func applyReadonlyRBAC(t *testing.T, c *cluster) string {
	t.Helper()
	applyManifest(t, c, "rbac-readonly.yaml", "tenure-readonly", readonlyVerbs)
	return "system:serviceaccount:" + c.ns + ":tenure-readonly"
}

// refusedCall is one call that a watch-only Adapter refuses.
type refusedCall struct {
	method string
	call   func(ctx context.Context, a *Adapter) error
}

// refusedCalls are the calls of every method a watch-only Adapter refuses, with valid arguments.
var refusedCalls = []refusedCall{
	{"KeepAlive", func(ctx context.Context, a *Adapter) error { return a.KeepAlive(ctx, "m9/1", T) }},
	{"Acquire", func(ctx context.Context, a *Adapter) error {
		_, err := a.Acquire(ctx, "m9/1", "orders", 0)
		return err
	}},
	{"Release", func(ctx context.Context, a *Adapter) error { return a.Release(ctx, "m9/1", "orders", 0, 1) }},
	{"Confirm", func(ctx context.Context, a *Adapter) error {
		_, err := a.Confirm(ctx, "m9/1", "orders", 0, 1)
		return err
	}},
	{"ClaimSlot", func(ctx context.Context, a *Adapter) error {
		_, err := a.ClaimSlot(ctx, "m9/1")
		return err
	}},
	{"ReleaseSlot", func(ctx context.Context, a *Adapter) error { return a.ReleaseSlot(ctx, "m9/1", 0) }},
	{"SetSlots", func(ctx context.Context, a *Adapter) error { return a.SetSlots(ctx, 2) }},
	{"CloseSession", func(ctx context.Context, a *Adapter) error { return a.CloseSession(ctx, "m9/1") }},
	{"Join", func(ctx context.Context, a *Adapter) error { return a.Join(ctx, member("m9")) }},
	{"SetDraining", func(ctx context.Context, a *Adapter) error { return a.SetDraining(ctx, true) }},
	{"Leave", func(ctx context.Context, a *Adapter) error { return a.Leave(ctx) }},
	{"WatchHolders", func(ctx context.Context, a *Adapter) error {
		_, err := a.WatchHolders(ctx)
		return err
	}},
}

// checkRefused checks that every refused call on a returns "k8s: <method>: tenure: watch only",
// matching both ErrWatchOnly and tenure.ErrWatchOnly.
func checkRefused(t *testing.T, ctx context.Context, a *Adapter) {
	t.Helper()
	for _, rc := range refusedCalls {
		err := rc.call(ctx, a)
		if want := "k8s: " + rc.method + ": tenure: watch only"; err == nil || err.Error() != want {
			t.Errorf("%s = %v, want %q", rc.method, err, want)
		}
		if !errors.Is(err, ErrWatchOnly) || !errors.Is(err, tenure.ErrWatchOnly) {
			t.Errorf("%s = %v, want an error matching ErrWatchOnly", rc.method, err)
		}
	}
}

// TestRTR700Refuse checks a watch-only Adapter's refusals on a fake client: every writing method,
// and WatchHolders, returns an error wrapping ErrWatchOnly that names the method and sends no
// request, after ctx.Err() and the configuration error; ErrWatchOnly is tenure.ErrWatchOnly.
func TestRTR700Refuse(t *testing.T) {
	if ErrWatchOnly != tenure.ErrWatchOnly {
		t.Fatalf("ErrWatchOnly = %v, want tenure.ErrWatchOnly", ErrWatchOnly)
	}
	client := newFake()
	a := fakeAdapter(t, client, WithWatchOnly())
	checkRefused(t, t.Context(), a)
	if got := client.Actions(); len(got) != 0 {
		t.Errorf("requests %v, want none", got)
	}

	done, cancel := context.WithCancel(t.Context())
	cancel()
	for _, rc := range refusedCalls {
		if err := rc.call(done, a); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a done ctx = %v, want context.Canceled", rc.method, err)
		}
	}
	bad := New(nil, "default", "g", WithWatchOnly())
	for _, rc := range refusedCalls {
		if err := rc.call(t.Context(), bad); err == nil || err.Error() != "k8s: nil client" {
			t.Errorf("%s without a client = %v, want k8s: nil client", rc.method, err)
		}
		if err := rc.call(done, bad); !errors.Is(err, context.Canceled) {
			t.Errorf("%s without a client, with a done ctx = %v, want context.Canceled", rc.method, err)
		}
	}
	if got := client.Actions(); len(got) != 0 {
		t.Errorf("requests %v, want none", got)
	}
}

// TestRTR700Selector checks on a fake client that a watch-only Adapter's informer lists and
// watches only the group's member, slots and slot Leases, that its views list the members, and
// that it runs no member GC; a writer Adapter's informer selects every Lease of the group.
func TestRTR700Selector(t *testing.T) {
	ctx := t.Context()
	client := newFake()
	leases := client.CoordinationV1().Leases("default")
	if _, err := leases.Create(ctx, memberLease("g", "m0", encodeMember(member("m0")), "", 3), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	a := fakeAdapter(t, client, WithWatchOnly(), WithMemberGC(time.Nanosecond))
	w := watchView(t, ctx, a, "a")
	if !listing("m0")(w.last) {
		t.Errorf("first view %s, want m0", fmtView(w.last))
	}
	waitWatching(t, client)
	want := "tenure.group=g,tenure.kind in (member,slots,slot)"
	if got := a.leaseSelector(); got != want {
		t.Errorf("selector %q, want %q", got, want)
	}
	parsed, err := labels.Parse(want)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range append(actions(client, "list"), actions(client, "watch")...) {
		var got string
		switch r := x.(type) {
		case k8stesting.ListAction:
			got = r.GetListRestrictions().Labels.String()
		case k8stesting.WatchAction:
			got = r.GetWatchRestrictions().Labels.String()
		}
		if got != parsed.String() {
			t.Errorf("%s with selector %q, want %q", x.GetVerb(), got, parsed)
		}
	}

	// collectMembers returns at once, before its first tick.
	gcDone := make(chan struct{})
	go func() {
		a.collectMembers(ctx)
		close(gcDone)
	}()
	select {
	case <-gcDone:
	case <-time.After(time.Second):
		t.Fatal("member GC of a watch-only Adapter still runs after 1s")
	}

	if got, want := New(client, "default", "g").leaseSelector(), "tenure.group=g"; got != want {
		t.Errorf("writer selector %q, want %q", got, want)
	}
}

// TestRTR700SlotsList checks on a fake client that Slots on a watch-only Adapter without a
// running watch lists only the group's member, slots and slot Leases, in one request, and reads
// the slot data from them.
func TestRTR700SlotsList(t *testing.T) {
	ctx := t.Context()
	client := newFake()
	leases := client.CoordinationV1().Leases("default")
	for _, l := range []*coordinationv1.Lease{
		memberLease("g", "m0", encodeMember(member("m0")), "m0/1", 3),
		slotsLease("g", "2"),
		slotLease("g", 1, "m0/1", ""),
		shardLease("g", "orders", 0, "m0/1", nil),
	} {
		if _, err := leases.Create(ctx, l, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create %s: %v", l.Name, err)
		}
	}
	client.ClearActions()
	a := fakeAdapter(t, client, WithWatchOnly())
	count, holders, err := a.Slots(ctx)
	if err != nil || count != 2 || !slices.Equal(holders, []string{"", "m0/1"}) {
		t.Errorf("Slots = %d, %q, %v; want 2, [\"\" \"m0/1\"], nil", count, holders, err)
	}
	got := client.Actions()
	if len(got) != 1 {
		t.Fatalf("requests %v, want one LIST", got)
	}
	list, ok := got[0].(k8stesting.ListAction)
	if !ok {
		t.Fatalf("request %v, want a LIST", got[0])
	}
	want, err := labels.Parse("tenure.group=g,tenure.kind in (member,slots,slot)")
	if err != nil {
		t.Fatal(err)
	}
	if sel := list.GetListRestrictions().Labels.String(); sel != want.String() {
		t.Errorf("LIST with selector %q, want %q", sel, want)
	}
}

// renewing opens session holder on a with timing T and renews it every RetryPeriod until the
// test ends, then closes it.
func renewing(t *testing.T, a *Adapter, holder string) {
	t.Helper()
	if err := a.KeepAlive(t.Context(), holder, T); err != nil {
		t.Fatalf("KeepAlive(%s): %v", holder, err)
	}
	closeAtEnd(t, a, holder)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		tick := time.NewTicker(T.RetryPeriod)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if err := a.KeepAlive(ctx, holder, T); err != nil && ctx.Err() == nil {
				t.Errorf("KeepAlive(%s): %v", holder, err)
			}
		}
	})
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

// watchOnlyAdapter returns a watch-only Adapter of group g in c.ns with timing T on a new client
// that impersonates user. When the test ends, it checks that the Adapter's goroutines stop and
// that no request of its client got 403 Forbidden or wrote.
func watchOnlyAdapter(t *testing.T, c *cluster, g, user string) *Adapter {
	t.Helper()
	cl, rec := c.clientAs(t, user)
	a := New(cl, c.ns, g, WithTiming(T), WithWatchOnly())
	if a.err != nil {
		t.Fatalf("New: %v", a.err)
	}
	t.Cleanup(func() {
		for _, r := range rec.filter(func(r request) bool { return r.status == http.StatusForbidden }) {
			t.Errorf("request %s %v got 403 Forbidden", r, r.query)
		}
		for _, r := range rec.filter(func(r request) bool { return r.verb != verbGet && r.verb != verbList && r.verb != verbWatch }) {
			t.Errorf("request %s of the watch-only Adapter, want only GET, LIST and WATCH", r)
		}
		waitStopped(t, a)
	})
	return a
}

// TestATRTR14WatchOnly checks a watch-only Adapter on a cluster, on a client that impersonates
// the service account of rbac-readonly.yaml: its views follow two writer Adapters through joins,
// slot changes, a drain and a leave; its informer receives no shard Lease event while four shards
// change hands, which a writer's informer receives; every write returns ErrWatchOnly; Slots and
// Holdings work; and no request of its client gets 403 Forbidden. The acceptance test's steps
// were written from the issue's text: the original text is unavailable.
func TestATRTR14WatchOnly(t *testing.T) {
	c := newCluster(t)
	t.Parallel()
	user := applyReadonlyRBAC(t, c)
	g := group()
	ctx := t.Context()

	wo := watchOnlyAdapter(t, c, g, user)
	var mu sync.Mutex
	seen := map[string]int{}
	wo.onEvent = func(l *coordinationv1.Lease) {
		mu.Lock()
		defer mu.Unlock()
		seen[l.Labels[labelKind]]++
	}
	var writerShards atomic.Int64
	a, _ := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	a.onEvent = func(l *coordinationv1.Lease) {
		if l.Labels[labelKind] == kindShard {
			writerShards.Add(1)
		}
	}

	w := watchView(t, ctx, wo, "w")
	mustJoin(t, a, member("m0"))
	mustJoin(t, b, member("m1"))
	w.waitFor(5*time.Second, "m0 and m1", listing("m0", "m1"))

	// Four shards go from m0 to m1.
	renewing(t, a, "m0/1")
	renewing(t, b, "m1/1")
	const shards = 4
	for shard := range shards {
		term, err := a.Acquire(ctx, "m0/1", "orders", shard)
		if err != nil {
			t.Fatalf("Acquire(m0/1, %d): %v", shard, err)
		}
		if err := a.Release(ctx, "m0/1", "orders", shard, term); err != nil {
			t.Fatalf("Release(m0/1, %d): %v", shard, err)
		}
		// b may not have seen the release yet: it retries until it has.
		for deadline := time.Now().Add(5 * time.Second); ; {
			_, err := b.Acquire(ctx, "m1/1", "orders", shard)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Acquire(m1/1, %d): %v", shard, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Slots: a count, a claim by m1.
	if err := a.SetSlots(ctx, 2); err != nil {
		t.Fatalf("SetSlots: %v", err)
	}
	slot, err := b.ClaimSlot(ctx, "m1/1")
	if err != nil {
		t.Fatalf("ClaimSlot: %v", err)
	}
	w.waitFor(5*time.Second, "slot "+strconv.Itoa(slot)+" held by m1/1", func(v core.View) bool {
		return v.Slots == 2 && v.SlotHolders[slot] == "m1/1" && len(v.Members) == 2 && v.Members[1].Slot == slot
	})
	if err := b.SetDraining(ctx, true); err != nil {
		t.Fatalf("SetDraining: %v", err)
	}
	w.waitFor(5*time.Second, "m1 draining", func(v core.View) bool {
		return len(v.Members) == 2 && v.Members[1].Draining
	})

	checkRefused(t, ctx, wo)
	count, holders, err := wo.Slots(ctx)
	if err != nil || count != 2 || holders[slot] != "m1/1" {
		t.Errorf("Slots = %d, %q, %v; want 2 with slot %d held by m1/1", count, holders, err, slot)
	}
	hs, err := wo.Holdings(ctx, "orders")
	if err != nil || len(hs) != shards {
		t.Fatalf("Holdings = %v, %v; want %d holdings", hs, err, shards)
	}
	for _, h := range hs {
		if h.Holder != "m1/1" {
			t.Errorf("Holdings: %+v, want held by m1/1", h)
		}
	}

	if err := b.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	w.waitFor(5*time.Second, "m0 alone", listing("m0"))

	// Each shard changed hands three times; the writer's informer saw it.
	deadline := time.Now().Add(5 * time.Second)
	for writerShards.Load() < 3*shards && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := writerShards.Load(); n < 3*shards {
		t.Errorf("writer's informer saw %d shard Lease events, want at least %d", n, 3*shards)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen[kindShard] != 0 || seen[kindMember] == 0 || seen[kindSlots] == 0 || seen[kindSlot] == 0 {
		t.Errorf("watch-only informer events by kind %v, want none of kind shard and some of member, slots and slot", seen)
	}
}

// TestRTR780WatchOnlyConformance runs membershiptest.RunWatchOnly on a cluster: each case on a
// group of its own, the writers Adapters on admin clients of their own, the watch-only Adapter on
// a client that impersonates the service account of rbac-readonly.yaml, and the slot data changed
// through the writers, each claim by a session <id>/1 of the member's writer that is renewed until
// the case ends.
func TestRTR780WatchOnlyConformance(t *testing.T) {
	c := newCluster(t)
	t.Parallel()
	user := applyReadonlyRBAC(t, c)
	admin := func(t *testing.T) (kubernetes.Interface, *recorder) { return c.client(t) }
	membershiptest.RunWatchOnly(t, func(t *testing.T, ids []string) membershiptest.WatchOnlyFixture {
		g := group()
		writers := make([]*Adapter, len(ids))
		f := membershiptest.WatchOnlyFixture{Settle: settle}
		for i := range ids {
			writers[i] = conformanceAdapter(t, c, admin, g)
			f.Writers = append(f.Writers, writers[i])
		}
		f.WatchOnly = watchOnlyAdapter(t, c, g, user)
		writer := func(id string) *Adapter { return writers[slices.Index(ids, id)] }
		var mu sync.Mutex
		started := map[string]bool{}
		f.Slots = &membershiptest.SlotControl{
			SetCount: func(ctx context.Context, count int) error { return writers[0].SetSlots(ctx, count) },
			Claim: func(ctx context.Context, id string) (int, error) {
				mu.Lock()
				if !started[id] {
					started[id] = true
					renewing(t, writer(id), id+"/1")
				}
				mu.Unlock()
				return writer(id).ClaimSlot(ctx, id+"/1")
			},
			Release: func(ctx context.Context, id string, slot int) error {
				return writer(id).ReleaseSlot(ctx, id+"/1", slot)
			},
		}
		return f
	})
}
