// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// fakeClock is a clock for WithClock that moves only when told.
type fakeClock struct {
	ns atomic.Int64 // nanoseconds after routerWall
}

func (c *fakeClock) now() time.Time          { return routerWall.Add(time.Duration(c.ns.Load())) }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }
func (c *fakeClock) option() RouterOption    { return WithClock(c.now) }

// shardOwnedBy returns a shard of orders whose route in r is id, failing t when there is none.
func shardOwnedBy(t *testing.T, r *Router, id string) int {
	t.Helper()
	for shard := range 8 {
		if m, err := r.RouteShard("orders", shard); err == nil && m.ID == id {
			return shard
		}
	}
	t.Fatalf("no shard of orders routes to %s", id)
	return -1
}

// keyOfShard returns a key whose shard among n is shard.
func keyOfShard(shard, n int) string {
	for i := 0; ; i++ {
		if k := "k" + strconv.Itoa(i); core.KeyShard(k, n) == shard {
			return k
		}
	}
}

// TestATRTR09RouterCorrect checks that a correction wins until a new snapshot, and until its TTL
// on a fake clock, and that Correct refuses an address not in the snapshot (AT-RTR-09).
func TestATRTR09RouterCorrect(t *testing.T) {
	clk := &fakeClock{}
	ttl := 5 * time.Second
	r := newTestRouter(t, routerSpaces(), clk.option(), WithCorrectionTTL(ttl))
	if r.Correct("orders", 0, "n1:80") {
		t.Fatal("Correct before the first view = true")
	}
	applyAt(r, plainView(1, "n1", "n2", "n3"), 0)
	shard := shardOwnedBy(t, r, "n1")
	key := keyOfShard(shard, 8)
	before := routesOf(r, "orders", 8)
	h, _ := r.Space("orders")

	if !r.Correct("orders", shard, "n2:80") {
		t.Fatal("Correct to n2 = false")
	}
	check := func(want string) {
		t.Helper()
		for name, f := range map[string]func() (Member, error){
			"Route":            func() (Member, error) { return r.Route("orders", key) },
			"RouteShard":       func() (Member, error) { return r.RouteShard("orders", shard) },
			"RouteSpace.Route": func() (Member, error) { return h.Route(key) },
		} {
			if m, err := f(); err != nil || m.ID != want {
				t.Fatalf("%s = %+v, %v, want %s", name, m, err, want)
			}
		}
	}
	check("n2")
	got := routesOf(r, "orders", 8)
	for s := range got {
		if s != shard && got[s] != before[s] {
			t.Fatalf("shard %d changed from %s to %s; only shard %d was corrected", s, before[s], got[s], shard)
		}
	}

	clk.advance(ttl - time.Nanosecond)
	check("n2")
	clk.advance(time.Nanosecond)
	check("n1")

	// A new snapshot ends a correction, even one well within its TTL.
	if !r.Correct("orders", shard, "n3:80") {
		t.Fatal("Correct to n3 = false")
	}
	check("n3")
	applyAt(r, plainView(2, "n1", "n2", "n3"), time.Second)
	check("n1")
	if n := r.byName["orders"].live.Load(); n != 0 {
		t.Fatalf("%d corrections set after a new snapshot, want 0", n)
	}

	// Refusals record nothing.
	for name, ok := range map[string]bool{
		"address not in the view": r.Correct("orders", shard, "n9:80"),
		"empty address":           r.Correct("orders", shard, ""),
		"unknown space":           r.Correct("carts", shard, "n2:80"),
		"negative shard":          r.Correct("orders", -1, "n2:80"),
		"shard outside":           r.Correct("orders", 8, "n2:80"),
	} {
		if ok {
			t.Errorf("Correct with %s = true", name)
		}
	}
	if n := r.byName["orders"].live.Load(); n != 0 {
		t.Fatalf("%d corrections set after refusals, want 0", n)
	}
	check("n1")

	// A member of the view that owns no shard can be named: the members' view may already give
	// it this one.
	v := plainView(3, "n1", "n2", "n3", "n4")
	v.Members[3].Draining = true
	applyAt(r, v, 2*time.Second)
	shard = shardOwnedBy(t, r, "n1")
	key = keyOfShard(shard, 8)
	if !r.Correct("orders", shard, "n4:80") {
		t.Fatal("Correct to a member that owns nothing = false")
	}
	check("n4")

	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.Correct("orders", shard, "n2:80") {
		t.Fatal("Correct after Close = true")
	}
}

// TestATRTR04RouterAllocsWithCorrection checks that lookups of a corrected shard, and of the
// other shards while a correction is set, allocate nothing (AT-RTR-04).
func TestATRTR04RouterAllocsWithCorrection(t *testing.T) {
	r := newTestRouter(t, routerSpaces())
	applyAt(r, plainView(1, "n1", "n2", "n3"), 0)
	shard := shardOwnedBy(t, r, "n1")
	other := shardOwnedBy(t, r, "n2")
	key := keyOfShard(shard, 8)
	if !r.Correct("orders", shard, "n3:80") {
		t.Fatal("Correct = false")
	}
	h, _ := r.Space("orders")
	for name, f := range map[string]func(){
		"Route corrected":            func() { _, _ = r.Route("orders", key) },
		"RouteShard corrected":       func() { _, _ = r.RouteShard("orders", shard) },
		"RouteShard other":           func() { _, _ = r.RouteShard("orders", other) },
		"RouteSpace.Route corrected": func() { _, _ = h.Route(key) },
	} {
		if n := testing.AllocsPerRun(100, f); n != 0 {
			t.Errorf("%s: %v allocations, want 0", name, n)
		}
	}
	if m, _ := r.RouteShard("orders", shard); m.ID != "n3" {
		t.Fatalf("corrected shard routes to %s", m.ID)
	}
}

// TestRouterCorrectConcurrent corrects, looks up and swaps snapshots at once, for the race
// detector.
func TestRouterCorrectConcurrent(t *testing.T) {
	r := newTestRouter(t, routerSpaces())
	applyAt(r, plainView(1, "n1", "n2", "n3"), 0)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				r.Correct("markets", (g+i)%64, "n"+strconv.Itoa(1+i%3)+":80")
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if m, err := r.RouteShard("markets", i%64); err != nil || m.ID == "" {
					t.Errorf("RouteShard = %+v, %v", m, err)
					return
				}
			}
		}()
	}
	for i := range 200 {
		applyAt(r, plainView(uint64(i+2), "n1", "n2", "n3"), time.Duration(i)*time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

// TestRouterCorrectionOutlivesPublish checks that a lookup racing with a new snapshot never
// returns the old snapshot's owner of a corrected shard: it returns the correction or the new
// snapshot's owner. Each round applies a view, corrects a shard of n1 to n2, then applies a view
// in which n1 drains, so that the new owner is not n1, while lookups of the shard run.
func TestRouterCorrectionOutlivesPublish(t *testing.T) {
	clk := &fakeClock{} // never moves, so no correction expires
	r := newTestRouter(t, routerSpaces(), clk.option())
	h, _ := r.Space("orders")
	// round is odd from the shard's correction until the view after it has been applied; shard
	// is the round's corrected shard, set before round turns odd.
	var round atomic.Uint64
	var shard atomic.Int64
	var stale atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				n := round.Load()
				if n%2 == 0 {
					continue
				}
				sh := int(shard.Load())
				var m Member
				var err error
				switch (g + i) % 3 {
				case 0:
					m, err = h.RouteShard(sh)
				case 1:
					m, err = r.RouteShard("orders", sh)
				default:
					m, err = h.Route(keyOfShard(sh, h.Shards()))
				}
				if round.Load() == n && (err != nil || m.ID == "n1") {
					stale.Add(1)
				}
			}
		}()
	}
	defer func() {
		close(stop)
		wg.Wait()
		if n := stale.Load(); n != 0 {
			t.Errorf("%d lookups returned the old snapshot's owner of the corrected shard, or an error", n)
		}
	}()

	for i := range 1000 {
		d := time.Duration(i) * time.Millisecond
		applyAt(r, plainView(uint64(2*i+1), "n1", "n2", "n3"), d)
		sh := shardOwnedBy(t, r, "n1")
		shard.Store(int64(sh))
		if !r.Correct("orders", sh, "n2:80") {
			t.Fatal("Correct = false")
		}
		round.Add(1)
		v := plainView(uint64(2*i+2), "n1", "n2", "n3")
		v.Members[0].Draining = true
		applyAt(r, v, d)
		if m, err := r.RouteShard("orders", sh); err != nil || m.ID == "n1" {
			t.Fatalf("round %d: shard %d routes to %+v, %v once n1 drains", i, sh, m, err)
		}
		round.Add(1)
	}
}
