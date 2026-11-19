// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// newSourceRouter returns a Router of spaces on a new routerSource, closed when t ends.
func newSourceRouter(t testing.TB, spaces []Space) (*Router, *routerSource) {
	t.Helper()
	src := &routerSource{}
	r, err := NewRouter(src, spaces)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, src
}

// waitApplied waits until r's snapshot comes from the view of the given version.
func waitApplied(t testing.TB, r *Router, version uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v := r.snap.Load().view; v != nil && v.Version == version {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("view %d not applied", version)
		}
		time.Sleep(time.Millisecond)
	}
}

// buildStats returns r's build counts.
func buildStats(r *Router) routerBuildStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.builds
}

// routesOf returns the member ID, or the error text, of every shard of space in r.
func routesOf(r *Router, space string, shards int) []string {
	out := make([]string, shards)
	for shard := range shards {
		m, err := r.RouteShard(space, shard)
		if err != nil {
			out[shard] = err.Error()
		} else {
			out[shard] = m.ID + "@" + strconv.Itoa(m.Slot)
		}
	}
	return out
}

func TestNewRouterWatchError(t *testing.T) {
	boom := errors.New("boom")
	r, err := NewRouter(&routerSource{failN: 1, err: boom}, routerSpaces())
	if r != nil || !errors.Is(err, boom) || err.Error() != "tenure: NewRouter: watch: boom" {
		t.Fatalf("NewRouter = %v, %v", r, err)
	}
}

// TestATRTR05RouterBurst checks that a burst of 100 views makes at most 2 builds, and that a
// lookup during a slow build reads the previous snapshot (AT-RTR-05).
func TestATRTR05RouterBurst(t *testing.T) {
	r, src := newSourceRouter(t, routerSpaces())
	src.put(plainView(1, "n1", "n2"))
	waitApplied(t, r, 1)
	before := routesOf(r, "markets", 64)

	entered, release := make(chan struct{}), make(chan struct{})
	start := buildStats(r).snapshots
	r.mu.Lock()
	r.testApplyHook = func(v core.View) {
		if v.Version == 2 {
			close(entered)
			<-release
		}
	}
	r.mu.Unlock()

	src.put(plainView(2, "n1", "n2", "n3"))
	<-entered
	// The builder holds view 2 and is slow: lookups read snapshot 1.
	if got := r.Version(); got != 1 {
		t.Fatalf("Version() during the build = %d, want 1", got)
	}
	if got := routesOf(r, "markets", 64); !slicesEqual(got, before) {
		t.Fatal("a lookup during the build did not read the previous snapshot")
	}
	for i := uint64(3); i <= 102; i++ {
		ids := []string{"n1", "n2", "n3", "n4"}[:2+i%3]
		src.put(plainView(i, ids...))
	}
	close(release)
	waitApplied(t, r, 102)
	if n := buildStats(r).snapshots - start; n > 2 {
		t.Fatalf("the burst made %d builds, the one in progress included; want at most 2", n)
	}
	checkLikeNode(t, r, routerSpaces(), plainView(102, []string{"n1", "n2", "n3", "n4"}[:2+102%3]...), routerWall)
}

// slicesEqual reports whether a and b hold the same strings.
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestATRTR06RouterSlotChurn checks that, with slots, replacing the members one by one under a
// constant slot count never reruns Balanced, while the table follows the new holders (AT-RTR-06).
func TestATRTR06RouterSlotChurn(t *testing.T) {
	spaces := routerSpaces()
	r, src := newSourceRouter(t, spaces)
	members := []string{"n1", "n2", "n3", "n4"}
	holders := []string{"n1", "n2", "n3", "n4"}
	version := uint64(1)
	put := func() core.View {
		t.Helper()
		v := slottedView(version, members, holders...)
		src.put(v)
		waitApplied(t, r, version)
		version++
		return v
	}
	put()
	if got := buildStats(r).balanced; got != 1 {
		t.Fatalf("first view: Balanced ran %d times, want 1", got)
	}
	for i, old := range []string{"n1", "n2", "n3", "n4"} {
		repl := "r" + strconv.Itoa(i)
		// The old member leaves: its slot is vacant. Then its replacement joins and holds it.
		members = slicesWithout(members, old)
		holders[i] = ""
		v := put()
		checkLikeNode(t, r, spaces, v, routerWall)
		members = append(members, repl)
		sortStrings(members)
		holders[i] = repl
		v = put()
		checkLikeNode(t, r, spaces, v, routerWall)
	}
	stats := buildStats(r)
	if stats.balanced != 1 {
		t.Fatalf("Balanced ran %d times over the rollout, want 1 (snapshots %d)", stats.balanced, stats.snapshots)
	}
	if stats.snapshots != 9 {
		t.Fatalf("%d snapshots, want 9", stats.snapshots)
	}
}

// slicesWithout returns a copy of s without x.
func slicesWithout(s []string, x string) []string {
	var out []string
	for _, e := range s {
		if e != x {
			out = append(out, e)
		}
	}
	return out
}

// sortStrings sorts s in place.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// TestATRTR07RouterTimers checks with testing/synctest that a slot's vacancy expiry changes the
// table at SlotGrace after the first view that showed it vacant, and that a JoinAt bound changes
// it with the next view, as on a member, whose machine recomputes only on a view or a vacancy
// (AT-RTR-07).
func TestATRTR07RouterTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spaces := routerSpaces()
		r, src := newSourceRouter(t, spaces)
		grace := DefaultSlotGrace

		src.put(slottedView(1, []string{"n1", "n2"}, "n1", "n2", ""))
		synctest.Wait()
		vacant := func() bool {
			for _, route := range routesOf(r, "markets", 64) {
				if len(route) > 9 && route[len(route)-9:] == "is vacant" {
					return true
				}
			}
			return false
		}
		if !vacant() {
			t.Fatal("no shard on the vacant slot after the first view")
		}
		time.Sleep(grace - time.Nanosecond)
		synctest.Wait()
		if !vacant() || r.Version() != 1 {
			t.Fatalf("at SlotGrace-1ns: vacant %v, version %d; want the slot kept", vacant(), r.Version())
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if vacant() || r.Version() != 2 {
			t.Fatalf("at SlotGrace: vacant %v, version %d; want the slot dropped", vacant(), r.Version())
		}

		// A JoinAt bound: the table waits for the next view, then takes n3.
		v := plainView(2, "n1", "n2", "n3")
		v.Members[2].JoinAt = time.Now().Add(5 * time.Second)
		src.put(v)
		synctest.Wait()
		hasN3 := func() bool {
			for _, route := range routesOf(r, "orders", 8) {
				if route == "n3@-1" {
					return true
				}
			}
			return false
		}
		if hasN3() {
			t.Fatal("n3 routed before its JoinAt")
		}
		time.Sleep(6 * time.Second)
		synctest.Wait()
		if hasN3() {
			t.Fatal("n3 routed after its JoinAt without a new view; a member's machine waits for one")
		}
		v.Version = 3
		src.put(v)
		synctest.Wait()
		if !hasN3() {
			t.Fatal("n3 not routed after the view that followed its JoinAt")
		}
		checkLikeNode(t, r, spaces, v, time.Now())
	})
}

// staleRouterSource is a routerSource that reports its own staleness.
type staleRouterSource struct {
	routerSource
	stale bool
}

func (s *staleRouterSource) Stale() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stale
}

// TestATRTR08RouterStale checks that a closed watch channel makes the Router stale, that it keeps
// routing, and that it is fresh again once the Watch it calls after RetryPeriod delivers a view;
// also a failing Watch, a Watch that returns ErrClosed, and a source that reports staleness
// (AT-RTR-08).
func TestATRTR08RouterStale(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		retry := core.DefaultTiming.RetryPeriod
		r, src := newSourceRouter(t, routerSpaces())
		v := plainView(1, "n1", "n2")
		src.put(v)
		synctest.Wait()
		if r.Stale() {
			t.Fatal("stale after the first view")
		}
		want := routesOf(r, "orders", 8)

		src.closeAll()
		synctest.Wait()
		if !r.Stale() {
			t.Fatal("not stale after the watch channel closed")
		}
		if got := routesOf(r, "orders", 8); !slicesEqual(got, want) {
			t.Fatal("a stale Router changed its table")
		}
		time.Sleep(retry - time.Nanosecond)
		synctest.Wait()
		if !r.Stale() || src.calls() != 1 {
			t.Fatalf("before RetryPeriod: stale %v, %d Watch calls", r.Stale(), src.calls())
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if r.Stale() || src.calls() != 2 {
			t.Fatalf("after RetryPeriod: stale %v, %d Watch calls; want fresh and 2", r.Stale(), src.calls())
		}

		// Two failing Watch calls keep it stale; the third brings it back.
		src.mu.Lock()
		src.failN, src.err = 2, errors.New("unavailable")
		src.mu.Unlock()
		src.closeAll()
		synctest.Wait()
		time.Sleep(2 * retry)
		synctest.Wait()
		if !r.Stale() || src.calls() != 4 {
			t.Fatalf("after two failed Watch calls: stale %v, %d calls", r.Stale(), src.calls())
		}
		time.Sleep(retry)
		synctest.Wait()
		if r.Stale() || src.calls() != 5 {
			t.Fatalf("after a good Watch: stale %v, %d calls", r.Stale(), src.calls())
		}

		// ErrClosed ends the watching for good, and the Router keeps its table, stale.
		src.mu.Lock()
		src.failN, src.err = 1000, ErrClosed
		src.mu.Unlock()
		src.closeAll()
		synctest.Wait()
		time.Sleep(10 * retry)
		synctest.Wait()
		if !r.Stale() || src.calls() != 6 {
			t.Fatalf("after ErrClosed: stale %v, %d calls; want stale and 6", r.Stale(), src.calls())
		}
		if got := routesOf(r, "orders", 8); !slicesEqual(got, want) {
			t.Fatal("the table changed after ErrClosed")
		}
	})

	// A source that reports itself stale.
	synctest.Test(t, func(t *testing.T) {
		src := &staleRouterSource{}
		r, err := NewRouter(src, routerSpaces())
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if r.Stale() {
			t.Fatal("stale before the source says so")
		}
		src.mu.Lock()
		src.stale = true
		src.mu.Unlock()
		if !r.Stale() {
			t.Fatal("not stale while the source says so")
		}
	})
}

// TestATRTR12RouterClose checks that Close returns once every goroutine of the Router has
// returned, and that lookups, Space, Ready and Subscribe then report the Router closed
// (AT-RTR-12).
func TestATRTR12RouterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := bubbleGoroutines(t)
		src := &routerSource{}
		r, err := NewRouter(src, routerSpaces())
		if err != nil {
			t.Fatal(err)
		}
		h, _ := r.Space("orders")
		sub := r.Subscribe()
		src.put(slottedView(1, []string{"n1"}, "n1", "")) // arms a vacancy timer
		synctest.Wait()
		if got := <-sub; got != 1 {
			t.Fatalf("Subscribe got %d, want 1", got)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close = %v", err)
		}
		synctest.Wait() // the source's own context.AfterFunc closes its channel
		if n := bubbleGoroutines(t); n != start {
			t.Fatalf("goroutines after Close = %d, want the starting %d", n, start)
		}
		for name, err := range map[string]error{
			"Route":            second(r.Route("orders", "k")),
			"Route unknown":    second(r.Route("carts", "k")),
			"RouteShard":       second(r.RouteShard("orders", 1)),
			"RouteSpace.Route": second(h.Route("k")),
			"RouteSpace.Shard": second(h.RouteShard(1)),
			"Space":            second(r.Space("orders")),
			"Ready":            r.Ready(context.Background()),
		} {
			if !errors.Is(err, ErrClosed) {
				t.Errorf("%s after Close = %v, want ErrClosed", name, err)
			}
		}
		if _, ok := <-sub; ok {
			t.Error("a Subscribe channel is open after Close")
		}
		if _, ok := <-r.Subscribe(); ok {
			t.Error("Subscribe after Close returned an open channel")
		}
		if err := r.Close(); err != nil {
			t.Errorf("second Close = %v", err)
		}
		if src.calls() != 1 {
			t.Errorf("%d Watch calls, want 1", src.calls())
		}
	})
}

// second returns the error of a two-value result.
func second[T any](_ T, err error) error {
	return err
}

func TestRouterSubscribe(t *testing.T) {
	r, src := newSourceRouter(t, routerSpaces())
	early := r.Subscribe()
	select {
	case v := <-early:
		t.Fatalf("Subscribe before a view got %d", v)
	default:
	}
	src.put(plainView(1, "n1"))
	waitApplied(t, r, 1)
	if got := <-early; got != 1 {
		t.Fatalf("got %d, want 1", got)
	}
	src.put(plainView(2, "n1", "n2"))
	waitApplied(t, r, 2)
	src.put(plainView(3, "n2"))
	waitApplied(t, r, 3)
	if got := <-early; got != 3 {
		t.Fatalf("a slow subscriber got %d, want the latest, 3", got)
	}
	if got := <-r.Subscribe(); got != 3 {
		t.Fatalf("a new subscriber got %d, want 3", got)
	}
}

// bubbleGoroutines returns the number of goroutines in the synctest bubble of its caller.
// runtime.NumGoroutine is no use for that: it counts the whole process, including goroutines
// outside the bubble that come and go at any time, such as the runtime's finalizer and cleanup
// goroutines while they run user code after a collection. The bubble of a goroutine is in its
// header in runtime.Stack, "goroutine 7 [chan receive (durable), synctest bubble 1]:", and the
// caller's own trace comes first. memstore's watch tests have the same helper.
func bubbleGoroutines(t *testing.T) int {
	t.Helper()
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	traces := strings.Split(string(buf), "\n\n")
	self, _, _ := strings.Cut(traces[0], "\n")
	_, after, ok := strings.Cut(self, ", synctest bubble ")
	id, _, _ := strings.Cut(after, "]")
	if !ok || id == "" {
		t.Fatalf("bubbleGoroutines outside a synctest bubble: %q", self)
	}
	tag := ", synctest bubble " + id + "]"
	n := 0
	for _, tr := range traces {
		if header, _, _ := strings.Cut(tr, "\n"); strings.Contains(header, tag) {
			n++
		}
	}
	return n
}
