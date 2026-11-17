// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure"
)

// failingLists returns newFake's client whose Lease watches pass through gate, and whose Lease
// lists fail while failLists is set, each failure noted in failed.
func failingLists(gate *watchGate, failLists *atomic.Bool, failed *attempts) *fake.Clientset {
	client := newFake()
	gateWatches(client, gate)
	client.PrependReactor("list", resourceLeases, func(k8stesting.Action) (bool, runtime.Object, error) {
		if failLists.Load() {
			failed.add(time.Now())
			return true, nil, errors.New("injected list failure")
		}
		return false, nil, nil
	})
	return client
}

// attempts records instants.
type attempts struct {
	mu sync.Mutex
	at []time.Time
}

func (a *attempts) add(t time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.at = append(a.at, t)
}

func (a *attempts) get() []time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.at)
}

// leaseRenewer rewrites the member Leases of ids, in namespace default of client, every RetryPeriod of
// T, as their members' heartbeats do, until each is stopped.
type leaseRenewer struct {
	t      *testing.T
	client *fake.Clientset
	mu     sync.Mutex
	ids    map[string]bool
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func renewLeases(t *testing.T, client *fake.Clientset, g string, ids ...string) *leaseRenewer {
	ctx, cancel := context.WithCancel(context.Background())
	r := &leaseRenewer{t: t, client: client, ids: map[string]bool{}, cancel: cancel}
	for _, id := range ids {
		r.ids[id] = true
	}
	leases := client.CoordinationV1().Leases("default")
	r.wg.Go(func() {
		tick := time.NewTicker(T.RetryPeriod)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			r.mu.Lock()
			for id, on := range r.ids {
				if !on {
					continue
				}
				l, err := leases.Get(ctx, memberName(g, id), metav1.GetOptions{})
				if err != nil {
					continue // deleted
				}
				now := metav1.NewMicroTime(time.Now())
				l.Spec.RenewTime = &now
				if _, err := leases.Update(ctx, l, metav1.UpdateOptions{}); err != nil && ctx.Err() == nil {
					t.Errorf("renew %s: %v", id, err)
				}
			}
			r.mu.Unlock()
		}
	})
	t.Cleanup(r.close)
	return r
}

// stop stops renewing the Lease of id.
func (r *leaseRenewer) stop(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids[id] = false
}

func (r *leaseRenewer) close() {
	r.cancel()
	r.wg.Wait()
}

// waitStale waits up to d for a.Stale() to be want.
func waitStale(t *testing.T, a *Adapter, want bool, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for a.Stale() != want {
		if time.Now().After(deadline) {
			t.Fatalf("Stale() = %t after %v, want %t", !want, d, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestATRTR15Relist checks a watch-only Adapter whose watch stops delivering, on a fake client in
// real time with timing T: once no Lease event has come for RenewDeadline, it reports stale and
// relists; while its lists fail, with a backoff from RetryPeriod doubling up to LeaseDuration, it
// stays stale and keeps every member past LeaseDuration; once a list succeeds, it is fresh, the
// members whose Leases changed stay, a member whose Lease was deleted meanwhile and one whose
// Lease did not change leave; and with the watch still silent, a member that stops renewing
// expires LeaseDuration after its Lease last changed, while relists come at most once per
// quiet RenewDeadline. A writer Adapter is never stale. The acceptance test's steps were written
// from the issue's text: the original text is unavailable.
func TestATRTR15Relist(t *testing.T) {
	ctx := t.Context()
	gate := &watchGate{}
	var failLists atomic.Bool
	failed := &attempts{}
	client := failingLists(gate, &failLists, failed)
	leases := client.CoordinationV1().Leases("default")
	for _, id := range []string{"m0", "m1", "m2", "m3"} {
		if _, err := leases.Create(ctx, memberLease("g", id, encodeMember(member(id)), "", 3), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	a := fakeAdapter(t, client, WithWatchOnly())
	listedFrom := time.Now()
	w := watchView(t, ctx, a, "a")
	if !listing("m0", "m1", "m2", "m3")(w.last) {
		t.Fatalf("first view %s, want m0 to m3", fmtView(w.last))
	}
	if a.Stale() {
		t.Fatal("Stale() = true after the first list, want false")
	}
	writer := fakeAdapter(t, client)
	if writer.Stale() {
		t.Fatal("writer Stale() = true, want false")
	}

	// The watch stops delivering, and lists fail. m0, m1 and m3 go on renewing; m2 has stopped.
	open := gate.shut()
	t.Cleanup(open)
	failLists.Store(true)
	quietFrom := time.Now()
	r := renewLeases(t, client, "g", "m0", "m1", "m3")
	time.Sleep(T.RetryPeriod + T.RetryPeriod/2)
	r.stop("m3")
	if err := leases.Delete(ctx, memberName("g", "m3"), metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete m3: %v", err)
	}

	waitStale(t, a, true, T.RenewDeadline+time.Second)
	if d := time.Since(listedFrom); d < T.RenewDeadline-50*time.Millisecond {
		t.Errorf("stale after %v of quiet, want at least RenewDeadline %v", d, T.RenewDeadline)
	}
	// Past LeaseDuration since m2 last changed, every member stays.
	time.Sleep(T.LeaseDuration + T.RetryPeriod - time.Since(quietFrom))
	for {
		if _, ok := w.next(0); !ok {
			break
		}
	}
	if !listing("m0", "m1", "m2", "m3")(w.last) || !a.Stale() {
		t.Fatalf("view %d: %s, stale %t; want m0 to m3 kept while stale", w.last.Version, fmtView(w.last), a.Stale())
	}
	at := failed.get()
	if len(at) < 2 {
		t.Fatalf("%d failed lists, want at least 2", len(at))
	}
	for i := 1; i < len(at); i++ {
		gap, want := at[i].Sub(at[i-1]), min(T.RetryPeriod<<(i-1), T.LeaseDuration)
		if gap < want-100*time.Millisecond {
			t.Errorf("failed list %d came %v after the one before, want at least %v", i, gap, want)
		}
	}

	// A list succeeds: fresh; m0 and m1 changed and stay; m2 did not and leaves, as does m3,
	// deleted meanwhile.
	failLists.Store(false)
	waitStale(t, a, false, T.LeaseDuration+time.Second)
	w.waitFor(time.Second, "m0 and m1", listing("m0", "m1"))

	// With the watch still silent, m1 stops renewing: it leaves once its Lease has not changed for
	// LeaseDuration; m0 stays.
	lists := func() int { return len(actions(client, "list")) - len(failed.get()) }
	listsFrom, from := lists(), time.Now()
	r.stop("m1")
	stopped := time.Now()
	w.waitFor(T.LeaseDuration+T.RenewDeadline+time.Second, "m0 alone", listing("m0"))
	if d := time.Since(stopped); d < T.LeaseDuration-T.RetryPeriod {
		t.Errorf("m1 left %v after its last renewal, want about LeaseDuration %v", d, T.LeaseDuration)
	}
	w.quiet(T.RenewDeadline + T.RetryPeriod)
	if n, most := lists()-listsFrom, int(time.Since(from)/T.RenewDeadline)+1; n > most {
		t.Errorf("%d relists in %v, want at most one per RenewDeadline (%d)", n, time.Since(from), most)
	}
}

// TestRTR720RouterStale checks on a cluster that a Router on a watch-only Adapter keeps its
// table and reports stale while the Adapter's watch and lists are blocked, here in its client's
// transport (the recorder's stall), for longer than LeaseDuration, and that it is fresh again,
// with the same table, once they pass.
func TestRTR720RouterStale(t *testing.T) {
	c := newCluster(t)
	t.Parallel()
	user := applyReadonlyRBAC(t, c)
	g := group()

	a, _ := c.adapter(t, g)
	b, _ := c.adapter(t, g)
	mustJoin(t, a, member("m0"))
	mustJoin(t, b, member("m1"))
	wo, rec := watchOnlyAdapter(t, c, g, user)
	r, err := tenure.NewRouter(wo, []tenure.Space{{Name: "orders", Shards: 8}})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	routes := func() []string {
		var out []string
		for shard := range 8 {
			m, err := r.RouteShard("orders", shard)
			if err != nil {
				t.Fatalf("RouteShard(%d): %v", shard, err)
			}
			out = append(out, m.ID)
		}
		return out
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(r.Table().Members) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("Router members %v after 5s, want m0 and m1", r.Table().Members)
		}
		time.Sleep(10 * time.Millisecond)
	}
	before := routes()
	if r.Stale() {
		t.Fatal("Router stale before the stall")
	}

	release := rec.stall(func(x request) bool { return x.verb == verbWatch || x.verb == verbList })
	deadline = time.Now().Add(T.RenewDeadline + 3*time.Second)
	for !r.Stale() {
		if time.Now().After(deadline) {
			t.Fatal("Router not stale within RenewDeadline of the stall")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(T.LeaseDuration + T.RetryPeriod)
	if !r.Stale() || !wo.Stale() {
		t.Errorf("Router stale %t, Adapter stale %t after LeaseDuration of the stall, want both", r.Stale(), wo.Stale())
	}
	if got := routes(); !slices.Equal(got, before) || len(r.Table().Members) != 2 {
		t.Errorf("routes %v with members %v while stale, want %v with m0 and m1", got, r.Table().Members, before)
	}

	release()
	deadline = time.Now().Add(T.LeaseDuration + 3*time.Second)
	for r.Stale() {
		if time.Now().After(deadline) {
			t.Fatal("Router still stale after the stall ended")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := routes(); !slices.Equal(got, before) {
		t.Errorf("routes %v after the stall, want %v", got, before)
	}
}
