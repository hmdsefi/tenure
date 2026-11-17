// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// eventSink collects a Router's events.
type eventSink struct {
	mu     sync.Mutex
	events []RouterEvent
}

func (s *eventSink) add(e RouterEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

// take returns the events collected since the last take, with Seq and Time cleared.
func (s *eventSink) take() []RouterEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.events
	s.events = nil
	for i := range out {
		out[i].Seq, out[i].Time = 0, time.Time{}
	}
	return out
}

// checkEvents fails t unless got equals want, ignoring Duration and comparing errors by text.
func checkEvents(t *testing.T, step string, got, want []RouterEvent) {
	t.Helper()
	same := len(got) == len(want)
	for i := 0; same && i < len(got); i++ {
		g, w := got[i], want[i]
		g.Duration, w.Duration = 0, 0
		ge, we := errText(g.Err), errText(w.Err)
		g.Err, w.Err = nil, nil
		same = g == w && ge == we
	}
	if !same {
		t.Fatalf("%s: events\n%+v\nwant\n%+v", step, got, want)
	}
}

// TestRouterEvents checks every event kind a Router reports, in order (RTR-600).
func TestRouterEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &eventSink{}
		src := &routerSource{}
		r, err := NewRouter(src, routerSpaces(), WithRouterEvents(sink.add))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		rebuilt := func(version uint64, space string, moved int, balanced bool) RouterEvent {
			return RouterEvent{Kind: RouterEventRebuilt, Version: version, Space: space, Shard: -1, Count: moved, Balanced: balanced}
		}

		// The first view: n2 advertises 16 shards for orders.
		v := advertising(plainView(1, "n1", "n2"), "orders:16", "n2")
		src.put(v)
		synctest.Wait()
		checkEvents(t, "first view", sink.take(), []RouterEvent{
			{Kind: RouterEventView, Version: 1, ViewVersion: 1, Count: 2, Shard: -1},
			rebuilt(1, "orders", 8, true),
			{Kind: RouterEventMismatch, Version: 1, Space: "orders", Shard: -1,
				Err: errors.New("tenure: space mismatch: orders has 8 shards here and 16 on member n2")},
			rebuilt(1, "markets", 64, true),
		})

		// The members agree again; only Addr changes, so nothing is recomputed.
		v = advertising(plainView(2, "n1", "n2"), "orders:8", "n2")
		v.Members[0].Addr = "moved:80"
		src.put(v)
		synctest.Wait()
		checkEvents(t, "matched", sink.take(), []RouterEvent{
			{Kind: RouterEventView, Version: 2, ViewVersion: 2, Count: 2, Shard: -1},
			rebuilt(2, "orders", 0, false),
			{Kind: RouterEventMatched, Version: 2, Space: "orders", Shard: -1},
			rebuilt(2, "markets", 0, false),
		})

		if !r.Correct("orders", 3, "moved:80") {
			t.Fatal("Correct = false")
		}
		synctest.Wait()
		checkEvents(t, "corrected", sink.take(), []RouterEvent{
			{Kind: RouterEventCorrected, Version: 2, Space: "orders", Shard: 3, Addr: "moved:80"},
		})

		// The watch ends: stale; the next Watch's view makes it fresh.
		src.closeAll()
		synctest.Wait()
		checkEvents(t, "stale", sink.take(), []RouterEvent{
			{Kind: RouterEventStale, Shard: -1, Err: errors.New("tenure: watch channel closed")},
		})
		time.Sleep(core.DefaultTiming.RetryPeriod)
		synctest.Wait()
		checkEvents(t, "fresh", sink.take(), []RouterEvent{
			{Kind: RouterEventView, Version: 3, ViewVersion: 2, Count: 2, Shard: -1},
			rebuilt(3, "orders", 0, false),
			rebuilt(3, "markets", 0, false),
			{Kind: RouterEventFresh, Version: 3, Shard: -1},
		})

		// A vacancy expiry rebuilds without a view.
		src.put(slottedView(3, []string{"n1"}, "n1", ""))
		synctest.Wait()
		sink.take()
		time.Sleep(DefaultSlotGrace)
		synctest.Wait()
		got := sink.take()
		if len(got) != 2 || got[0].Kind != RouterEventRebuilt || !got[0].Balanced || got[0].Version != 5 || got[0].Count == 0 {
			t.Fatalf("vacancy expiry: events %+v, want two rebuilt with Balanced at version 5", got)
		}

		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, "close", sink.take(), []RouterEvent{{Kind: RouterEventClosed, Version: 5, Shard: -1}})
	})
}

// TestRouterEventsSeq checks that events are numbered 1, 2, … without gaps, stamped with the
// clock in UTC, and that router.closed comes last.
func TestRouterEventsSeq(t *testing.T) {
	var mu sync.Mutex
	var events []RouterEvent
	clk := &fakeClock{}
	src := &routerSource{}
	r, err := NewRouter(src, routerSpaces(), clk.option(), WithRouterEvents(func(e RouterEvent) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}))
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		src.put(plainView(i, "n1"))
		waitApplied(t, r, i)
	}
	_ = r.Close()
	mu.Lock()
	defer mu.Unlock()
	for i, e := range events {
		if e.Seq != uint64(i+1) || !e.Time.Equal(routerWall) || e.Time.Location() != time.UTC {
			t.Fatalf("event %d: Seq %d, Time %v", i, e.Seq, e.Time)
		}
	}
	if len(events) == 0 || events[len(events)-1].Kind != RouterEventClosed {
		t.Fatalf("last event is not router.closed: %+v", events)
	}
}

// TestRouterSlowEventHandler checks that a handler that blocks never blocks the builder: the
// Router keeps applying views, and the events come once the handler returns, in order.
func TestRouterSlowEventHandler(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var kinds []RouterEventKind
	src := &routerSource{}
	r, err := NewRouter(src, routerSpaces(), WithRouterEvents(func(e RouterEvent) {
		<-release
		mu.Lock()
		defer mu.Unlock()
		kinds = append(kinds, e.Kind)
	}))
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 50; i++ {
		src.put(plainView(i, "n1", "n2"))
		waitApplied(t, r, i)
	}
	if !r.Correct("orders", 1, "n1:80") {
		t.Fatal("Correct = false")
	}
	close(release)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(kinds) != 50*3+2 || kinds[0] != RouterEventView || kinds[len(kinds)-1] != RouterEventClosed {
		t.Fatalf("%d events, first %v, last %v; want 152, router.view first and router.closed last", len(kinds), kinds[0], kinds[len(kinds)-1])
	}
}

func TestRouterEventLog(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelDebug}))
	src := &routerSource{}
	r, err := NewRouter(src, []Space{{Name: "orders", Shards: 8}}, WithRouterLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	src.put(advertising(plainView(1, "n1"), "orders:4", "n1"))
	waitApplied(t, r, 1)
	_ = r.Close()
	mu.Lock()
	defer mu.Unlock()
	out := buf.String()
	for _, want := range []string{
		"level=INFO msg=router.view seq=1 version=1 view_version=1 count=1",
		"level=DEBUG msg=router.rebuilt seq=2 version=1 space=orders count=8 balanced=true duration=",
		`level=WARN msg=router.mismatch seq=3 version=1 space=orders err="tenure: space mismatch: orders has 8 shards here and 4 on member n1"`,
		"level=INFO msg=router.closed seq=4 version=1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
}

// writerFunc is an io.Writer made of a function.
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestRouterEventKindString(t *testing.T) {
	for k, want := range map[RouterEventKind]string{
		RouterEventView: "router.view", RouterEventRebuilt: "router.rebuilt", RouterEventStale: "router.stale",
		RouterEventFresh: "router.fresh", RouterEventCorrected: "router.corrected", RouterEventMismatch: "router.mismatch",
		RouterEventMatched: "router.matched", RouterEventClosed: "router.closed", 0: "RouterEventKind(0)", 9: "RouterEventKind(9)",
	} {
		if got := k.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", k, got, want)
		}
	}
}

// TestRouterTable checks that Table is a copy of the snapshot: version, stale and closed flags,
// members, and each shard's route, error, mismatch and live correction (RTR-610).
func TestRouterTable(t *testing.T) {
	clk := &fakeClock{}
	r := newTestRouter(t, routerSpaces(), clk.option(), WithCorrectionTTL(time.Second))
	tab := r.Table()
	if tab.Version != 0 || tab.Members != nil || tab.Spaces[0].Routes[0].Err != "tenure: no members: orders/0: no view yet" {
		t.Fatalf("before a view: %+v", tab.Spaces[0].Routes[0])
	}

	v := advertising(slottedView(1, []string{"n1", "n2"}, "n1", "", "n2"), "markets:32", "n2")
	v.Members[0].Meta = map[string]string{"zone": "a"}
	applyAt(r, v, 0)
	shard := shardOwnedBy(t, r, "n1")
	r.Correct("orders", shard, "n2:80")
	tab = r.Table()
	if tab.Version != 1 || tab.ViewVersion != 1 || tab.Slots != 3 || tab.Stale || tab.Closed || !tab.Taken.Equal(routerWall) {
		t.Fatalf("table header: %+v", tab)
	}
	if tab.Spaces[1].Name != "markets" || !strings.Contains(tab.Spaces[1].Mismatch, "64 shards here and 32 on member n2") {
		t.Fatalf("markets: %q", tab.Spaces[1].Mismatch)
	}
	orders := tab.Spaces[0]
	for s, route := range orders.Routes {
		m, err := r.snap.Load().route(0, s)
		want := RouterRoute{Member: m.ID, Addr: m.Addr, Slot: -1, Err: errText(err)}
		if err == nil {
			want.Slot = m.Slot
		}
		if s == shard {
			want.Corrected = "n2:80"
		}
		if route != want {
			t.Fatalf("orders/%d: %+v, want %+v", s, route, want)
		}
	}

	// The table is a copy.
	tab.Members[0].Meta["zone"] = "b"
	tab.SlotHolders[0] = "x"
	if v := r.snap.Load().view; v.Members[0].Meta["zone"] != "a" || v.SlotHolders[0] != "n1/s" {
		t.Fatal("changing the table changed the snapshot")
	}

	clk.advance(time.Second)
	if r.Table().Spaces[0].Routes[shard].Corrected != "" {
		t.Fatal("an expired correction is in the table")
	}
	_ = r.Close()
	if tab := r.Table(); !tab.Closed || tab.Version != 1 {
		t.Fatalf("after Close: closed %v, version %d", tab.Closed, tab.Version)
	}
	if err := r.Ready(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
