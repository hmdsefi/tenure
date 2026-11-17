// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// routerWall is the wall-clock time of the Router tests' views.
var routerWall = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// routerSpaces returns the spaces of the Router tests: orders with 8 shards and markets with 64.
func routerSpaces() []Space {
	return []Space{{Name: "orders", Shards: 8}, {Name: "markets", Shards: 64}}
}

// routerSource is a Membership for Router tests: Watch returns a channel that holds the latest
// view put, as Membership requires, and the other methods do nothing.
type routerSource struct {
	mu      sync.Mutex
	watches []chan core.View
	watchN  int // calls of Watch
	failN   int // the next failN calls of Watch fail
	last    *core.View
	err     error // what a failing Watch returns
}

func (s *routerSource) Join(context.Context, core.Member) error { return nil }
func (s *routerSource) SetDraining(context.Context, bool) error { return nil }
func (s *routerSource) Leave(context.Context) error             { return nil }
func (s *routerSource) Watch(ctx context.Context) (<-chan core.View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watchN++
	if s.failN > 0 {
		s.failN--
		return nil, s.err
	}
	ch := make(chan core.View, 1)
	if s.last != nil {
		ch <- *s.last
	}
	s.watches = append(s.watches, ch)
	context.AfterFunc(ctx, func() { s.closeWatch(ch) })
	return ch, nil
}

// closeWatch closes ch and forgets it, once.
func (s *routerSource) closeWatch(ch chan core.View) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, w := range s.watches {
		if w == ch {
			s.watches = append(s.watches[:i], s.watches[i+1:]...)
			close(ch)
			return
		}
	}
}

// newTestRouter returns a Router of spaces on a routerSource, failing t on an error.
func newTestRouter(t testing.TB, spaces []Space, opts ...RouterOption) *Router {
	t.Helper()
	r, err := NewRouter(&routerSource{}, spaces, opts...)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r
}

// applyAt applies v to r at reading d past the epoch, as the builder does.
func applyAt(r *Router, v core.View, d time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.apply(v, d, routerWall.Add(d))
}

// plainView returns a view without slots of the members ids, each with address "<id>:80".
func plainView(version uint64, ids ...string) core.View {
	v := core.View{Version: version}
	for _, id := range ids {
		v.Members = append(v.Members, core.Member{ID: id, Addr: id + ":80", Slot: -1})
	}
	return v
}

// slottedView returns a view with one slot per entry of holders: the ID of the slot's holder,
// whose holder string is "<id>/s", or "" for a vacant slot. Members lists the members of the
// view, sorted; a member that holds a slot gets its lowest one.
func slottedView(version uint64, members []string, holders ...string) core.View {
	v := core.View{Version: version, Slots: len(holders), SlotHolders: make([]string, len(holders))}
	for s, id := range holders {
		if id != "" {
			v.SlotHolders[s] = id + "/s"
		}
	}
	return mergeView(plainView(version, members...), v.Slots, v.SlotHolders)
}

// checkLikeNode fails t unless r routes every shard of every space as a node's own Route does for
// the view v, applied at routerWall, and returns its errors with the same texts.
func checkLikeNode(t *testing.T, r *Router, spaces []Space, v core.View, wall time.Time) {
	t.Helper()
	node := newRoutes(&v, nil, assignments(v, wall, spaces))
	for _, sp := range spaces {
		for shard := range sp.Shards {
			want, wantErr := node.route(sp.Name, shard)
			got, err := r.RouteShard(sp.Name, shard)
			if !reflect.DeepEqual(got, want) || errText(err) != errText(wantErr) {
				t.Fatalf("RouteShard(%q, %d) = %+v, %v; Node.Route gives %+v, %v", sp.Name, shard, got, err, want, wantErr)
			}
			if wantErr != nil && !errors.Is(err, ErrNoMembers) {
				t.Fatalf("RouteShard(%q, %d) error %v does not match ErrNoMembers", sp.Name, shard, err)
			}
		}
	}
}

// errText returns err's text, or "" for nil.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestNewRouterErrors(t *testing.T) {
	src := &routerSource{}
	cases := []struct {
		name   string
		m      Membership
		spaces []Space
		opts   []RouterOption
		want   string
	}{
		{"nil membership", nil, nil, nil, "tenure: NewRouter: nil membership"},
		{"empty name", src, []Space{{Name: "", Shards: 1}}, nil, `tenure: NewRouter: Spaces[0].Name: invalid name ""`},
		{"slash", src, []Space{{Name: "a/b", Shards: 1}}, nil, `tenure: NewRouter: Spaces[0].Name: invalid name "a/b"`},
		{"reserved", src, []Space{{Name: "slots", Shards: 1}}, nil, `tenure: NewRouter: Spaces[0].Name: reserved name "slots"`},
		{"duplicate", src, []Space{{Name: "a", Shards: 1}, {Name: "a", Shards: 2}}, nil, `tenure: NewRouter: Spaces[1].Name: duplicate name "a"`},
		{"shards", src, []Space{{Name: "a", Shards: 0}}, nil, "tenure: NewRouter: Spaces[0].Shards: 0 is below 1"},
		{"timing", src, nil, []RouterOption{WithRouterTiming(core.Timing{LeaseDuration: time.Second})}, "tenure: NewRouter: timing: "},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := NewRouter(c.m, c.spaces, c.opts...)
			if r != nil || err == nil || len(err.Error()) < len(c.want) || err.Error()[:len(c.want)] != c.want {
				t.Fatalf("NewRouter = %v, %v, want an error starting %q", r, err, c.want)
			}
		})
	}
	if _, err := NewRouter(src, []Space{{Name: "a", Shards: 1}}, WithRouterTiming(core.Timing{LeaseDuration: time.Second})); !errors.Is(err, core.ErrInvalidTiming) {
		t.Fatalf("timing error %v does not match core.ErrInvalidTiming", err)
	}
}

func TestRouterOptions(t *testing.T) {
	r := newTestRouter(t, nil)
	if r.opts.timing != core.DefaultTiming || r.opts.correctionTTL != core.DefaultTiming.LeaseDuration {
		t.Fatalf("defaults: timing %+v, correction TTL %v", r.opts.timing, r.opts.correctionTTL)
	}
	timing := core.DefaultTiming
	timing.SlotGrace = 0
	now := func() time.Time { return routerWall }
	r = newTestRouter(t, nil, WithRouterTiming(timing), WithCorrectionTTL(3*time.Second), WithClock(now), WithClock(nil))
	if r.opts.timing.SlotGrace != DefaultSlotGrace || r.opts.correctionTTL != 3*time.Second || !r.epoch.Equal(routerWall) {
		t.Fatalf("options: timing %+v, correction TTL %v, epoch %v", r.opts.timing, r.opts.correctionTTL, r.epoch)
	}
}

func TestRouterBeforeFirstView(t *testing.T) {
	r := newTestRouter(t, routerSpaces())
	if r.Version() != 0 {
		t.Fatalf("Version() = %d, want 0", r.Version())
	}
	_, err := r.Route("orders", "k1")
	want := "tenure: no members: orders/" + strconv.Itoa(core.KeyShard("k1", 8)) + ": no view yet"
	if !errors.Is(err, ErrNoMembers) || err.Error() != want {
		t.Fatalf("Route before a view = %v, want %q", err, want)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := r.Ready(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ready before a view = %v", err)
	}
}

func TestRouterRoutesWithoutSlots(t *testing.T) {
	spaces := routerSpaces()
	r := newTestRouter(t, spaces)
	v := plainView(1, "n1", "n2", "n3")
	if !applyAt(r, v, 0) {
		t.Fatal("apply = false")
	}
	if r.Version() != 1 {
		t.Fatalf("Version() = %d, want 1", r.Version())
	}
	if err := r.Ready(context.Background()); err != nil {
		t.Fatalf("Ready: %v", err)
	}
	checkLikeNode(t, r, spaces, v, routerWall)

	for i := range 100 {
		key := "k" + strconv.Itoa(i)
		got, err := r.Route("markets", key)
		want, _ := r.RouteShard("markets", core.KeyShard(key, 64))
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("Route(markets, %q) = %+v, %v, want %+v", key, got, err, want)
		}
	}

	// A draining member drops out, and a view where none is eligible has no placement keys.
	v = plainView(2, "n1", "n2", "n3")
	v.Members[1].Draining = true
	applyAt(r, v, time.Second)
	checkLikeNode(t, r, spaces, v, routerWall.Add(time.Second))
	v = plainView(3)
	applyAt(r, v, 2*time.Second)
	checkLikeNode(t, r, spaces, v, routerWall.Add(2*time.Second))
	if _, err := r.RouteShard("orders", 3); errText(err) != "tenure: no members: orders/3: no placement keys" {
		t.Fatalf("no keys: %v", err)
	}
}

func TestRouterRoutesWithSlots(t *testing.T) {
	spaces := routerSpaces()
	r := newTestRouter(t, spaces)

	v := slottedView(1, []string{"n1", "n2", "n3"}, "n1", "n2", "n3")
	applyAt(r, v, 0)
	checkLikeNode(t, r, spaces, v, routerWall)

	// A vacant slot, and a holder whose member has left the view.
	v = slottedView(2, []string{"n1", "n3"}, "n1", "", "n4")
	applyAt(r, v, time.Second)
	checkLikeNode(t, r, spaces, v, routerWall)
	sawVacant, sawGone := false, false
	for shard := range 64 {
		m, err := r.RouteShard("markets", shard)
		switch errText(err) {
		case "":
			if m.ID != "n1" || m.Slot != 0 {
				t.Fatalf("shard %d routes to %+v, want n1 on slot 0", shard, m)
			}
		case "tenure: no members: markets/" + strconv.Itoa(shard) + ": slot-1 is vacant":
			sawVacant = true
		case "tenure: no members: markets/" + strconv.Itoa(shard) + ": n4 is not in the view":
			sawGone = true
		default:
			t.Fatalf("shard %d: unexpected error %v", shard, err)
		}
	}
	if !sawVacant || !sawGone {
		t.Fatalf("vacant slot seen %v, missing holder seen %v", sawVacant, sawGone)
	}
}

func TestRouterIgnoresViews(t *testing.T) {
	r := newTestRouter(t, routerSpaces())
	applyAt(r, plainView(5, "n1", "n2"), 0)
	if applyAt(r, plainView(4, "n1"), time.Second) {
		t.Fatal("older view applied")
	}
	if applyAt(r, plainView(6, "n2", "n1"), time.Second) {
		t.Fatal("invalid view applied")
	}
	if r.Version() != 1 {
		t.Fatalf("Version() = %d after ignored views, want 1", r.Version())
	}
	if !applyAt(r, plainView(5, "n1"), 2*time.Second) {
		t.Fatal("same version not applied")
	}
	if r.Version() != 2 {
		t.Fatalf("Version() = %d, want 2", r.Version())
	}
}

func TestRouterErrors(t *testing.T) {
	spaces := []Space{
		{Name: "orders", Shards: 8},
		{Name: "fixed", Shards: 4, ShardOf: func(key string) int { n, _ := strconv.Atoi(key); return n }},
	}
	r := newTestRouter(t, spaces)
	applyAt(r, plainView(1, "n1"), 0)

	if _, err := r.Route("carts", "k"); errText(err) != "tenure: unknown space" {
		t.Fatalf("Route(unknown space) = %v", err)
	}
	if _, err := r.RouteShard("carts", 0); errText(err) != "tenure: unknown space" {
		t.Fatalf("RouteShard(unknown space) = %v", err)
	}
	if _, err := r.Space("carts"); errText(err) != `tenure: unknown space "carts"` {
		t.Fatalf("Space(unknown) = %v", err)
	}
	if _, err := r.Route("fixed", "9"); errText(err) != "tenure: fixed: ShardOf result is outside [0, 4)" {
		t.Fatalf("Route(ShardOf outside) = %v", err)
	}
	if _, err := r.Route("fixed", "-1"); errText(err) != "tenure: fixed: ShardOf result is outside [0, 4)" {
		t.Fatalf("Route(ShardOf negative) = %v", err)
	}
	if m, err := r.Route("fixed", "3"); err != nil || m.ID != "n1" {
		t.Fatalf("Route(fixed, 3) = %+v, %v", m, err)
	}
	for _, shard := range []int{-1, 8} {
		if _, err := r.RouteShard("orders", shard); errText(err) != "tenure: orders: shard is outside [0, 8)" {
			t.Fatalf("RouteShard(orders, %d) = %v", shard, err)
		}
	}
}

func TestRouteSpace(t *testing.T) {
	spaces := []Space{
		{Name: "orders", Shards: 8},
		{Name: "fixed", Shards: 4, ShardOf: func(key string) int { n, _ := strconv.Atoi(key); return n }},
	}
	r := newTestRouter(t, spaces)
	h, err := r.Space("orders")
	if err != nil || h.Name() != "orders" || h.Shards() != 8 {
		t.Fatalf("Space(orders) = %v, %v", h, err)
	}
	if shard, err := h.Shard("k1"); err != nil || shard != core.KeyShard("k1", 8) {
		t.Fatalf("Shard(k1) = %d, %v", shard, err)
	}
	fixed, _ := r.Space("fixed")
	if shard, err := fixed.Shard("2"); err != nil || shard != 2 {
		t.Fatalf("fixed Shard(2) = %d, %v", shard, err)
	}
	if _, err := fixed.Shard("7"); errText(err) != "tenure: fixed: ShardOf result is outside [0, 4)" {
		t.Fatalf("fixed Shard(7) = %v", err)
	}

	// The handle reads each new snapshot.
	for i, ids := range [][]string{{"n1"}, {"n2"}, {"n1", "n2", "n3"}} {
		v := plainView(uint64(i+1), ids...)
		applyAt(r, v, time.Duration(i)*time.Second)
		for shard := range 8 {
			got, err := h.RouteShard(shard)
			want, wantErr := r.RouteShard("orders", shard)
			if !reflect.DeepEqual(got, want) || err != wantErr {
				t.Fatalf("view %d: handle RouteShard(%d) = %+v, %v, router gives %+v, %v", i, shard, got, err, want, wantErr)
			}
		}
		got, err := h.Route("k1")
		want, _ := r.Route("orders", "k1")
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("view %d: handle Route(k1) = %+v, %v, want %+v", i, got, err, want)
		}
	}
	if _, err := h.RouteShard(8); errText(err) != "tenure: orders: shard is outside [0, 8)" {
		t.Fatalf("handle RouteShard(8) = %v", err)
	}
}

// TestATRTR04RouterAllocs checks that no lookup allocates, on success and on every error
// (AT-RTR-04).
func TestATRTR04RouterAllocs(t *testing.T) {
	spaces := []Space{
		{Name: "orders", Shards: 8},
		{Name: "fixed", Shards: 4, ShardOf: func(key string) int { return len(key) }},
	}
	r := newTestRouter(t, spaces)
	h, _ := r.Space("orders")
	fixed, _ := r.Space("fixed")
	paths := map[string]func(){
		"Route before a view": func() { _, _ = r.Route("orders", "k1") },
	}
	checkAllocs := func() {
		t.Helper()
		for name, f := range paths {
			if n := testing.AllocsPerRun(100, f); n != 0 {
				t.Errorf("%s: %v allocations, want 0", name, n)
			}
		}
	}
	checkAllocs()

	applyAt(r, slottedView(1, []string{"n1", "n3"}, "n1", "", "n4"), 0)
	var okShard, vacant, gone int
	for shard := range 8 {
		_, err := r.RouteShard("orders", shard)
		switch {
		case err == nil:
			okShard = shard
		case errText(err) == "tenure: no members: orders/"+strconv.Itoa(shard)+": slot-1 is vacant":
			vacant = shard
		default:
			gone = shard
		}
	}
	paths = map[string]func(){
		"Route":                  func() { _, _ = r.Route("orders", "k1") },
		"RouteShard":             func() { _, _ = r.RouteShard("orders", okShard) },
		"RouteShard vacant":      func() { _, _ = r.RouteShard("orders", vacant) },
		"RouteShard not in view": func() { _, _ = r.RouteShard("orders", gone) },
		"RouteShard outside":     func() { _, _ = r.RouteShard("orders", 8) },
		"Route unknown space":    func() { _, _ = r.Route("carts", "k1") },
		"RouteShard unknown":     func() { _, _ = r.RouteShard("carts", 1) },
		"Route ShardOf outside":  func() { _, _ = r.Route("fixed", "xxxxxxx") },
		"RouteSpace.Route":       func() { _, _ = h.Route("k1") },
		"RouteSpace.RouteShard":  func() { _, _ = h.RouteShard(vacant) },
		"RouteSpace.Shard":       func() { _, _ = h.Shard("k1") },
		"RouteSpace.Shard out":   func() { _, _ = fixed.Shard("xxxxxxx") },
		"Version":                func() { _ = r.Version() },
	}
	checkAllocs()

	applyAt(r, plainView(2), time.Second)
	paths = map[string]func(){
		"Route no placement keys": func() { _, _ = r.Route("orders", "k1") },
	}
	checkAllocs()
}

// TestRouterConcurrentLookups looks up from 32 goroutines while snapshots are swapped, for the
// race detector.
func TestRouterConcurrentLookups(t *testing.T) {
	r := newTestRouter(t, routerSpaces())
	h, _ := r.Space("markets")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k" + strconv.Itoa(g)
			for {
				select {
				case <-stop:
					return
				default:
				}
				m, err := r.Route("orders", key)
				if err == nil && m.ID == "" {
					t.Error("Route returned an empty member without an error")
					return
				}
				_, _ = h.Route(key)
				_ = r.Version()
			}
		}()
	}
	views := []core.View{
		plainView(0, "n1", "n2", "n3"),
		slottedView(0, []string{"n1", "n2"}, "n1", "n2", ""),
		plainView(0, "n2"),
	}
	for i := range 300 {
		v := views[i%len(views)]
		v.Version = uint64(i + 1)
		applyAt(r, v, time.Duration(i)*time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if r.Version() != 300 {
		t.Fatalf("Version() = %d, want 300", r.Version())
	}
}
