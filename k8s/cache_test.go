// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure/core"
)

// withClock replaces the Adapter's clock with now: the test-only clock option.
func withClock(now func() time.Time) Option {
	return Option{apply: func(a *Adapter) error {
		a.now = now
		return nil
	}}
}

// fakeClock is a clock that moves only when the test advances it. Its readings have no
// monotonic part, so they compare by their wall times.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

// leasesResource is the resource of the fake client's Leases.
var leasesResource = coordinationv1.SchemeGroupVersion.WithResource(resourceLeases)

// newFake returns a fake clientset whose Lease writes behave as the API server's do: a create
// gives the Lease a UID, and every create and update a new resourceVersion; an update with a
// resourceVersion or UID other than the stored ones, and a delete whose preconditions do not
// hold, fail with Conflict. Watches see every write.
func newFake() *fake.Clientset {
	client := fake.NewClientset()
	tracker := client.Tracker()
	var mu sync.Mutex
	rv := 100
	next := func() string {
		rv++
		return strconv.Itoa(rv)
	}
	stored := func(ns, name string) (*coordinationv1.Lease, error) {
		obj, err := tracker.Get(leasesResource, ns, name)
		if err != nil {
			return nil, err
		}
		l, _ := obj.(*coordinationv1.Lease)
		return l, nil
	}
	conflict := func(name, why string) error {
		return apierrors.NewConflict(leasesResource.GroupResource(), name, errors.New(why))
	}
	client.PrependReactor("create", resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		create, _ := action.(k8stesting.CreateAction)
		obj, _ := create.GetObject().(*coordinationv1.Lease)
		l := obj.DeepCopy()
		l.UID, l.ResourceVersion = uuid.NewUUID(), next()
		if err := tracker.Create(leasesResource, l, action.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, l.DeepCopy(), nil
	})
	client.PrependReactor("update", resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		update, _ := action.(k8stesting.UpdateAction)
		obj, _ := update.GetObject().(*coordinationv1.Lease)
		l := obj.DeepCopy()
		cur, err := stored(action.GetNamespace(), l.Name)
		switch {
		case err != nil:
			return true, nil, err
		case l.ResourceVersion != "" && l.ResourceVersion != cur.ResourceVersion:
			return true, nil, conflict(l.Name, "the object has been modified")
		case l.UID != "" && l.UID != cur.UID:
			return true, nil, conflict(l.Name, "Precondition failed: UID")
		}
		l.UID, l.ResourceVersion = cur.UID, next()
		if err := tracker.Update(leasesResource, l, action.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, l.DeepCopy(), nil
	})
	client.PrependReactor("delete", resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		del, _ := action.(k8stesting.DeleteActionImpl)
		cur, err := stored(action.GetNamespace(), del.Name)
		if err != nil {
			return true, nil, err
		}
		if pre := del.DeleteOptions.Preconditions; pre != nil {
			if pre.UID != nil && *pre.UID != cur.UID || pre.ResourceVersion != nil && *pre.ResourceVersion != cur.ResourceVersion {
				return true, nil, conflict(del.Name, "Precondition failed")
			}
		}
		return true, nil, tracker.Delete(leasesResource, action.GetNamespace(), del.Name)
	})
	return client
}

// fakeAdapter returns an Adapter of group g in namespace default on client with timing T and
// then opts. When the test ends, it checks that the Adapter's informer has stopped within 1 s.
func fakeAdapter(t testing.TB, client *fake.Clientset, opts ...Option) *Adapter {
	t.Helper()
	a := New(client, "default", "g", append([]Option{WithTiming(T)}, opts...)...)
	if a.err != nil {
		t.Fatalf("New: %v", a.err)
	}
	t.Cleanup(func() { waitStopped(t, a) })
	return a
}

// waitStopped fails the test unless every informer goroutine of a ends within 1 s.
func waitStopped(t testing.TB, a *Adapter) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		a.runs.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("the informer still runs 1s after the Adapter's last use")
	}
	if c := a.runningCache(); c != nil {
		t.Errorf("the Adapter still has a running cache with %d holds", c.refs)
	}
}

// waitObserved waits up to 2 s for the Adapter to observe Lease name at resourceVersion rv and
// returns the observation.
func waitObserved(t testing.TB, a *Adapter, name, rv string) observation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if o, ok := a.cached(name); ok && o.lease.ResourceVersion == rv {
			return o
		}
		if time.Now().After(deadline) {
			o, ok := a.cached(name)
			t.Fatalf("lease %s at resourceVersion %s not observed in 2s; observed %v: %+v", name, rv, ok, o.lease)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForgotten waits up to 2 s for the Adapter to observe the deletion of Lease name.
func waitForgotten(t testing.TB, a *Adapter, name string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := a.cached(name); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("deletion of lease %s not observed in 2s", name)
		}
		time.Sleep(time.Millisecond)
	}
}

// actions returns the fake client's actions on Leases with verb, "" matching any.
func actions(client *fake.Clientset, verb string) []k8stesting.Action {
	var out []k8stesting.Action
	for _, x := range client.Actions() {
		if x.GetResource() == leasesResource && (verb == "" || x.GetVerb() == verb) {
			out = append(out, x)
		}
	}
	return out
}

// writes returns the fake client's create, update and delete actions on Leases.
func writes(client *fake.Clientset) []k8stesting.Action {
	var out []k8stesting.Action
	for _, verb := range []string{"create", "update", "delete"} {
		out = append(out, actions(client, verb)...)
	}
	return out
}

// TestK8S100InformerLifecycle checks the informer's lifecycle: none runs before a session, the
// first session starts one over the group's Leases and waits for its initial list, later
// sessions share it, and it stops within 1 s of the last session's end; a start whose initial
// list does not come in time returns ctx.Err() and holds nothing.
func TestK8S100InformerLifecycle(t *testing.T) {
	ctx := t.Context()

	t.Run("sessions hold it", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		if a.runningCache() != nil || len(client.Actions()) != 0 {
			t.Fatalf("before a session: cache %v, actions %v; want none", a.runningCache(), client.Actions())
		}
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("KeepAlive pod-0/1: %v", err)
		}
		c := a.runningCache()
		if c == nil || c.refs != 1 {
			t.Fatalf("after the first session: cache %+v, want one with 1 hold", c)
		}
		select {
		case <-c.synced:
		default:
			t.Error("KeepAlive returned before the initial list was observed")
		}
		for _, verb := range []string{"list", "watch"} {
			// The watch starts after the initial list, maybe after KeepAlive returned.
			deadline := time.Now().Add(time.Second)
			for len(actions(client, verb)) == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			got := actions(client, verb)
			if len(got) != 1 {
				t.Fatalf("%d %s actions, want 1: %v", len(got), verb, got)
			}
			var sel string
			switch x := got[0].(type) {
			case k8stesting.ListActionImpl:
				sel = x.GetListRestrictions().Labels.String()
			case k8stesting.WatchActionImpl:
				sel = x.GetWatchRestrictions().Labels.String()
			}
			if sel != "tenure.group=g" || got[0].GetNamespace() != "default" {
				t.Errorf("%s in %q with selector %q, want default and tenure.group=g", verb, got[0].GetNamespace(), sel)
			}
		}
		if err := a.KeepAlive(ctx, "pod-1/1", T); err != nil {
			t.Fatalf("KeepAlive pod-1/1: %v", err)
		}
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("renew pod-0/1: %v", err)
		}
		if a.runningCache() != c || c.refs != 2 || len(actions(client, "list")) != 1 || len(actions(client, "watch")) != 1 {
			t.Fatalf("after two sessions: cache %p (%d holds), %d lists, %d watches; want the first run, 2 holds, 1, 1",
				a.runningCache(), c.refs, len(actions(client, "list")), len(actions(client, "watch")))
		}
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Fatalf("CloseSession pod-0/1: %v", err)
		}
		if a.runningCache() != c || c.refs != 1 {
			t.Fatalf("after one close: cache %p with %d holds, want the run with 1", a.runningCache(), c.refs)
		}
		if err := a.CloseSession(ctx, "pod-1/1"); err != nil {
			t.Fatalf("CloseSession pod-1/1: %v", err)
		}
		if a.runningCache() != nil {
			t.Fatal("after the last close the informer still runs")
		}
		waitStopped(t, a)
		if _, ok := a.cached(memberName("g", "pod-1")); ok {
			t.Error("observations kept after the informer stopped")
		}

		// The next session starts a new run with a new list.
		if err := a.KeepAlive(ctx, "pod-2/1", T); err != nil {
			t.Fatalf("KeepAlive pod-2/1: %v", err)
		}
		if c2 := a.runningCache(); c2 == nil || c2 == c || len(actions(client, "list")) != 2 {
			t.Fatalf("after a new session: cache %p (first %p), %d lists; want a new run and 2", c2, c, len(actions(client, "list")))
		}
		if err := a.CloseSession(ctx, "pod-2/1"); err != nil {
			t.Fatalf("CloseSession pod-2/1: %v", err)
		}
	})

	t.Run("a lost session gives it back", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		if err := client.CoordinationV1().Leases("default").Delete(ctx, memberName("g", "pod-0"), metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := a.KeepAlive(ctx, "pod-0/1", T); err == nil {
			t.Fatal("KeepAlive after the Lease's deletion = nil, want the session lost")
		}
		if a.runningCache() != nil {
			t.Fatal("the informer still runs after the only session was lost")
		}
	})

	t.Run("initial list or ctx.Err", func(t *testing.T) {
		client := newFake()
		listed := make(chan struct{})
		client.PrependReactor("list", resourceLeases, func(k8stesting.Action) (bool, runtime.Object, error) {
			<-listed
			return false, nil, nil
		})
		a := fakeAdapter(t, client)
		tctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := a.KeepAlive(tctx, "pod-0/1", T)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
			t.Errorf("KeepAlive with a list that never ends = %v after %v, want context.DeadlineExceeded after 200ms", err, time.Since(start))
		}
		if a.runningCache() != nil {
			t.Error("the informer still holds after its start failed")
		}
		close(listed)
		waitStopped(t, a)
		if got := writes(client); len(got) != 0 {
			t.Errorf("writes %v, want none", got)
		}
		// The holder has no session: the next KeepAlive opens one.
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("KeepAlive after the failed start: %v", err)
		}
		if got := actions(client, "create"); len(got) != 1 {
			t.Errorf("creates %v, want 1", got)
		}
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
	})
}

// TestK8S110Observation checks the observations of the Adapter's own clock: rvSeenAt moves with
// every new resourceVersion, tokenSeenAt only with a new UID, holder or renewTime; a deleted
// member Lease leaves a tombstone with its last tokenSeenAt and its duration until that has
// passed; an older state is ignored; and a stopped informer's observations are discarded.
func TestK8S110Observation(t *testing.T) {
	ctx := t.Context()
	clk := newFakeClock()
	client := newFake()
	a := fakeAdapter(t, client, WithTiming(core.DefaultTiming), withClock(clk.now))
	leases := client.CoordinationV1().Leases("default")
	c, err := a.holdCache(ctx)
	if err != nil {
		t.Fatalf("holdCache: %v", err)
	}

	name := memberName("g", "pod-0")
	seconds := int32(10)
	renew := metav1.NewMicroTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	l := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: leaseLabels("g", kindMember, "")},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: new("pod-0/1"), RenewTime: new(renew), LeaseDurationSeconds: &seconds},
	}
	check := func(step string, l *coordinationv1.Lease, rvSeenAt, tokenSeenAt time.Time) {
		t.Helper()
		o := waitObserved(t, a, l.Name, l.ResourceVersion)
		if !o.rvSeenAt.Equal(rvSeenAt) || !o.tokenSeenAt.Equal(tokenSeenAt) {
			t.Errorf("%s: rvSeenAt %v, tokenSeenAt %v; want %v, %v", step, o.rvSeenAt, o.tokenSeenAt, rvSeenAt, tokenSeenAt)
		}
	}
	update := func(f func(l *coordinationv1.Lease)) {
		t.Helper()
		f(l)
		var err error
		if l, err = leases.Update(ctx, l, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update: %v", err)
		}
	}

	t0 := clk.now()
	if l, err = leases.Create(ctx, l, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	check("created", l, t0, t0)

	t1 := clk.advance(time.Second)
	update(func(l *coordinationv1.Lease) { l.Annotations = map[string]string{"x": "1"} })
	check("annotation changed", l, t1, t0)

	t2 := clk.advance(time.Second)
	update(func(l *coordinationv1.Lease) {
		l.Spec.RenewTime = new(metav1.NewMicroTime(renew.Add(time.Microsecond)))
	})
	check("renewTime changed", l, t2, t2)

	t3 := clk.advance(time.Second)
	update(func(l *coordinationv1.Lease) { l.Spec.HolderIdentity = new("pod-0/2") })
	check("holder changed", l, t3, t3)

	clk.advance(time.Second)
	update(func(l *coordinationv1.Lease) { l.Spec.LeaseTransitions = new(int32(1)) })
	check("transitions changed", l, t3.Add(time.Second), t3)

	// An older state of the Lease, or the same, changes nothing.
	clk.advance(time.Second)
	older := l.DeepCopy()
	older.ResourceVersion = "100"
	c.observe(older)
	c.observe(l.DeepCopy())
	check("older state", l, t3.Add(time.Second), t3)

	// A shard Lease is observed too; only member Leases leave tombstones.
	shard := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: shardName("g", "markets", 0), Labels: leaseLabels("g", kindShard, "markets")}}
	t5 := clk.now()
	if shard, err = leases.Create(ctx, shard, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create shard: %v", err)
	}
	check("shard", shard, t5, t5)
	if got := a.cachedLeases(kindMember); len(got) != 1 || got[0].lease.Name != name {
		t.Errorf("cachedLeases(member) = %v, want %s alone", got, name)
	}
	if got := a.cachedLeases(kindShard); len(got) != 1 || got[0].lease.Name != shard.Name {
		t.Errorf("cachedLeases(shard) = %v, want %s alone", got, shard.Name)
	}
	if err := leases.Delete(ctx, shard.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete shard: %v", err)
	}
	waitForgotten(t, a, shard.Name)
	if _, ok := a.tombstoneOf(shard.UID); ok {
		t.Error("a deleted shard Lease left a member tombstone")
	}

	// A deleted member Lease leaves a tombstone until its last tokenSeenAt + leaseDurationSeconds.
	clk.advance(time.Second)
	if err := leases.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForgotten(t, a, name)
	want := tombstone{tokenSeenAt: t3, wait: 10 * time.Second}
	if got, ok := a.tombstoneOf(l.UID); !ok || got != want {
		t.Errorf("tombstone = %+v, %v; want %+v", got, ok, want)
	}
	clk.advance(t3.Add(10*time.Second - time.Nanosecond).Sub(clk.now()))
	if _, ok := a.tombstoneOf(l.UID); !ok {
		t.Error("tombstone gone before tokenSeenAt + wait")
	}
	clk.advance(time.Nanosecond)
	if _, ok := a.tombstoneOf(l.UID); ok {
		t.Error("tombstone kept at tokenSeenAt + wait")
	}

	// Recreated: a new UID is a new Lease. Without leaseDurationSeconds the tombstone waits the
	// Adapter's LeaseDuration.
	t6 := clk.now()
	l = &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: leaseLabels("g", kindMember, "")}}
	if l, err = leases.Create(ctx, l, metav1.CreateOptions{}); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	check("recreated", l, t6, t6)
	clk.advance(time.Second)
	if err := leases.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForgotten(t, a, name)
	want = tombstone{tokenSeenAt: t6, wait: core.DefaultTiming.LeaseDuration}
	if got, ok := a.tombstoneOf(l.UID); !ok || got != want {
		t.Errorf("tombstone without leaseDurationSeconds = %+v, %v; want %+v", got, ok, want)
	}
	gone := l.UID

	// Stopping the informer discards observations and tombstones; a new run observes anew.
	l = &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: memberName("g", "pod-1"), Labels: leaseLabels("g", kindMember, "")}}
	if l, err = leases.Create(ctx, l, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod-1: %v", err)
	}
	check("pod-1", l, clk.now(), clk.now())
	a.releaseCache(c)
	waitStopped(t, a)
	if _, ok := a.cached(l.Name); ok {
		t.Error("observation kept after the informer stopped")
	}
	t7 := clk.advance(time.Second)
	if c, err = a.holdCache(ctx); err != nil {
		t.Fatalf("holdCache again: %v", err)
	}
	defer a.releaseCache(c)
	check("pod-1 after a restart", l, t7, t7)
	if _, ok := a.tombstoneOf(gone); ok {
		t.Error("tombstone kept after the informer stopped")
	}
}
