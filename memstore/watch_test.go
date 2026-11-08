// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package memstore_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

var _ tenure.Watcher = (*memstore.Store)(nil)

// watch opens a watch channel on a context that never ends, so no goroutine is left behind.
func watch(t *testing.T, s *memstore.Store) <-chan tenure.Holding {
	t.Helper()
	ch, err := s.WatchHolders(ctx)
	if err != nil || ch == nil {
		t.Fatalf("WatchHolders() = %v, %v, want a channel, nil", ch, err)
	}
	return ch
}

// drain returns the values buffered in ch without waiting, and whether ch is closed.
func drain(ch <-chan tenure.Holding) (got []tenure.Holding, closed bool) {
	for {
		select {
		case h, ok := <-ch:
			if !ok {
				return got, true
			}
			got = append(got, h)
		default:
			return got, false
		}
	}
}

// wantValues checks that ch holds exactly want, already delivered, and is still open.
func wantValues(t *testing.T, name string, ch <-chan tenure.Holding, want ...tenure.Holding) {
	t.Helper()
	got, closed := drain(ch)
	if closed {
		t.Fatalf("%s: channel closed after %+v, want %+v and open", name, got, want)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s: values %+v, want %+v", name, got, want)
	}
}

// grant acquires the shard for h and returns the record after the grant.
func grant(t *testing.T, s *memstore.Store, h, space string, shard int) tenure.Holding {
	t.Helper()
	acquire(t, s, h, space, shard)
	return confirm(t, s, h, space, shard)
}

func closeSession(t *testing.T, s *memstore.Store, h string) {
	t.Helper()
	if err := s.CloseSession(ctx, h); err != nil {
		t.Fatalf("CloseSession(%q) = %v, want nil", h, err)
	}
}

// TestWatchRules checks delivery: each record write reaches every channel before the writing
// method returns, as the record after the write; CloseSession delivers one value per cleared
// record in space-then-shard order; a full channel closes without holding up the others.
func TestWatchRules(t *testing.T) {
	t.Run("every write, as the record after it", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		w1, w2 := watch(t, s), watch(t, s)
		g := grant(t, s, h1, "a", 0)
		wantValues(t, "grant w1", w1, g)
		wantValues(t, "grant w2", w2, g)
		if err := s.Release(ctx, h1, "a", 0, g.Term); err != nil {
			t.Fatal(err)
		}
		cleared := tenure.Holding{Space: "a", Shard: 0, Term: g.Term, Version: g.Version + 1}
		wantValues(t, "release w1", w1, cleared)
		wantValues(t, "release w2", w2, cleared)
	})

	t.Run("CloseSession in space-then-shard order", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		var held []tenure.Holding
		for _, k := range []struct {
			space string
			shard int
		}{{"b", 7}, {"a", 3}, {"c", 0}, {"a", 1}, {"b", 2}, {"a", 10}} {
			held = append(held, grant(t, s, h1, k.space, k.shard))
		}
		last := grant(t, s, h2, "a", 2)
		ch := watch(t, s)
		closeSession(t, s, h1)

		slices.SortFunc(held, func(x, y tenure.Holding) int {
			return cmp.Or(strings.Compare(x.Space, y.Space), cmp.Compare(x.Shard, y.Shard))
		})
		want := make([]tenure.Holding, len(held))
		for i, h := range held {
			want[i] = tenure.Holding{Space: h.Space, Shard: h.Shard, Term: h.Term, Version: last.Version + 1 + uint64(i)}
		}
		wantValues(t, "CloseSession", ch, want...)
	})

	t.Run("a full channel closes alone", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		full, kept := watch(t, s), watch(t, s)
		var got []tenure.Holding
		for i := range 1100 {
			acquire(t, s, h1, "a", i)
			v, _ := drain(kept)
			got = append(got, v...)
		}
		if len(got) != 1100 {
			t.Errorf("kept channel got %d values, want 1100", len(got))
		}
		if v, closed := drain(full); len(v) != 1024 || !closed {
			t.Errorf("full channel got %d values, closed %v, want 1024, true", len(v), closed)
		}
		g := grant(t, s, h1, "b", 0)
		wantValues(t, "kept after full", kept, g)
	})

	t.Run("context end closes and unregisters the channel", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		wctx, cancel := context.WithCancel(ctx)
		ch, err := s.WatchHolders(wctx)
		if err != nil {
			t.Fatal(err)
		}
		kept := watch(t, s)
		g := grant(t, s, h1, "a", 0)
		cancel()
		if got := waitClosed(t, ch); !slices.Equal(got, []tenure.Holding{g}) {
			t.Fatalf("values before close %+v, want %+v", got, []tenure.Holding{g})
		}

		// A write after the close sends nothing to the closed channel.
		g2 := grant(t, s, h1, "a", 1)
		wantValues(t, "kept after cancel", kept, g, g2)
	})

	// In a bubble, so that every context.AfterFunc goroutine has ended when the subtest does.
	t.Run("context end racing a full buffer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s, _ := newStore(t)
			keepAlive(t, s, h1, timing)
			for range 8 {
				wctx, cancel := context.WithCancel(ctx)
				chans := make([]<-chan tenure.Holding, 32)
				for i := range chans {
					ch, err := s.WatchHolders(wctx)
					if err != nil {
						t.Fatal(err)
					}
					chans[i] = ch
				}
				for range 1024 {
					acquire(t, s, h1, "a", 0)
				}

				// Each channel is now full: the next write and the context's end race to close it,
				// and it must be closed exactly once.
				start := make(chan struct{})
				var wg sync.WaitGroup
				wg.Go(func() {
					<-start
					cancel()
				})
				wg.Go(func() {
					<-start
					for range 4 {
						if _, err := s.Acquire(ctx, h1, "a", 0); err != nil {
							t.Error(err)
						}
					}
				})
				close(start)
				wg.Wait()
				for i, ch := range chans {
					if got, closed := drain(ch); len(got) != 1024 || !closed {
						t.Fatalf("channel %d got %d values, closed %v, want 1024, true", i, len(got), closed)
					}
				}
			}
		})
	})
}

// waitClosed receives from ch until it is closed and returns the values received; it fails the
// test when ch is not closed within 10 seconds.
func waitClosed(t *testing.T, ch <-chan tenure.Holding) []tenure.Holding {
	t.Helper()
	timeout := time.After(10 * time.Second)
	var got []tenure.Holding
	for {
		select {
		case h, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, h)
		case <-timeout:
			t.Fatalf("channel not closed after 10s, got %+v", got)
			return nil
		}
	}
}

// TestATSTO16NoWriteNoValue checks that a call which writes no shard record delivers no value:
// renewals, reads, refused grants, mismatched releases, slot calls, failed calls and deletions
// of absent records (AT-STO-16; steps written from issue #36).
func TestATSTO16NoWriteNoValue(t *testing.T) {
	s, c := newStore(t)
	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h2, timing)
	g := grant(t, s, h1, "a", 0)
	ch := watch(t, s)

	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h3, timing)
	wantHeld(t, s, h2, "a", h1)
	confirm(t, s, h1, "a", 0)
	confirm(t, s, h1, "a", 5)
	holdings(t, s, "a")
	holdings(t, s, "zz")
	for _, rel := range []struct {
		h     string
		shard int
		term  core.Term
	}{{h2, 0, g.Term}, {h1, 0, g.Term + 1}, {h1, 1, g.Term}} {
		if err := s.Release(ctx, rel.h, "a", rel.shard, rel.term); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Acquire(ctx, holder(4, 1), "a", 1); !errors.Is(err, tenure.ErrSessionLost) {
		t.Fatalf("Acquire(unknown) = %v, want %v", err, tenure.ErrSessionLost)
	}
	if _, err := s.Acquire(ctx, h1, "a/b", 1); err == nil {
		t.Fatal("Acquire(bad space) = nil, want an error")
	}
	closeSession(t, s, h3)
	s.DeleteRecord("a", 1)
	s.DeleteRecord("b", 0)
	setSlots(t, s, 2)
	claimSlot(t, s, h1, 0)
	claimSlot(t, s, h2, 1)
	releaseSlot(t, s, h2, 1)
	wantSlots(t, s, 2, []string{h1, ""})
	c.advance(time.Second)
	if _, err := s.WatchHolders(ctx); err != nil {
		t.Fatal(err)
	}
	wantValues(t, "no record write", ch)

	// The channel is still open and gets the next write.
	g2 := grant(t, s, h2, "a", 1)
	wantValues(t, "next write", ch, g2)
}

// TestATSTO24Interleaved checks that writes made concurrently by several holders reach every
// channel in commit order: versions 1, 2, 3, ... with no gap, the same sequence on each channel,
// and replaying it gives the store's records (AT-STO-24, memstore parts; steps written from
// issue #36).
func TestATSTO24Interleaved(t *testing.T) {
	s, _ := newStore(t)
	holders := []string{h1, h2, h3}
	for _, h := range holders {
		keepAlive(t, s, h, timing)
	}
	w1, w2 := watch(t, s), watch(t, s)

	const shards = 50
	var wg sync.WaitGroup
	errs := make(chan error, len(holders))
	for i, h := range holders {
		space := fmt.Sprintf("s%d", i)
		wg.Go(func() {
			for shard := range shards {
				term, err := s.Acquire(ctx, h, space, shard)
				if err == nil {
					err = s.Release(ctx, h, space, shard, term)
				}
				if err == nil && shard%2 == 0 {
					_, err = s.Acquire(ctx, h, "shared", shard)
					var held *tenure.HeldError
					if errors.As(err, &held) {
						err = nil
					}
				}
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	got1, closed1 := drain(w1)
	got2, closed2 := drain(w2)
	if closed1 || closed2 {
		t.Fatalf("channels closed (%v, %v), want open", closed1, closed2)
	}
	if !slices.Equal(got1, got2) {
		t.Fatalf("channels got different sequences:\n%+v\n%+v", got1, got2)
	}
	want := len(holders)*shards*2 + shards/2
	if len(got1) != want {
		t.Fatalf("got %d values, want %d", len(got1), want)
	}
	replay := map[string]map[int]tenure.Holding{}
	for i, h := range got1 {
		if h.Version != uint64(i+1) {
			t.Fatalf("value %d has Version %d, want %d: not in commit order", i, h.Version, i+1)
		}
		if replay[h.Space] == nil {
			replay[h.Space] = map[int]tenure.Holding{}
		}
		replay[h.Space][h.Shard] = h
	}
	for space, recs := range replay {
		var fromReplay []tenure.Holding
		for shard := range shards {
			if h, ok := recs[shard]; ok {
				fromReplay = append(fromReplay, h)
			}
		}
		if got := holdings(t, s, space); !slices.Equal(got, fromReplay) {
			t.Errorf("Holdings(%q) = %+v, want the replayed %+v", space, got, fromReplay)
		}
	}
}

// TestATSTO25CatchUp checks that a watcher started late catches up: a Holdings read after
// WatchHolders returns, plus the values that follow, gives the store's records, and the first
// value is the write right after the last one the read shows (AT-STO-25, memstore parts; steps
// written from issue #36).
func TestATSTO25CatchUp(t *testing.T) {
	s, _ := newStore(t)
	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h2, timing)
	for shard := range 5 {
		grant(t, s, h1, "a", shard)
	}
	g := confirm(t, s, h1, "a", 2)
	if err := s.Release(ctx, h1, "a", 2, g.Term); err != nil {
		t.Fatal(err)
	}

	ch := watch(t, s)
	snapshot := holdings(t, s, "a")
	var last uint64
	for _, h := range snapshot {
		last = max(last, h.Version)
	}

	grant(t, s, h2, "a", 2)
	grant(t, s, h1, "a", 7)
	closeSession(t, s, h1)
	s.DeleteRecord("a", 4)

	got, closed := drain(ch)
	if closed || len(got) == 0 {
		t.Fatalf("got %+v, closed %v, want values and open", got, closed)
	}
	if got[0].Version != last+1 {
		t.Errorf("first value has Version %d, want %d, the write after the snapshot", got[0].Version, last+1)
	}
	recs := map[int]tenure.Holding{}
	for _, h := range snapshot {
		recs[h.Shard] = h
	}
	for _, h := range got {
		if h.Space != "a" {
			continue
		}
		if h.Holder == "" && h.Term == 0 {
			delete(recs, h.Shard)
			continue
		}
		recs[h.Shard] = h
	}
	var want []tenure.Holding
	for shard := range 10 {
		if h, ok := recs[shard]; ok {
			want = append(want, h)
		}
	}
	if final := holdings(t, s, "a"); !slices.Equal(final, want) {
		t.Errorf("Holdings(a) = %+v, want the caught-up %+v", final, want)
	}
}

// TestATSTO27Interfaces checks that a Store, also on the system clock, is a tenure.Store, a
// tenure.SlotStore and a tenure.Watcher, and works through each interface (AT-STO-27; steps
// written from issue #36).
func TestATSTO27Interfaces(t *testing.T) {
	var v any = memstore.New(nil)
	st, ok1 := v.(tenure.Store)
	ss, ok2 := v.(tenure.SlotStore)
	w, ok3 := v.(tenure.Watcher)
	if !ok1 || !ok2 || !ok3 {
		t.Fatalf("*memstore.Store implements Store %v, SlotStore %v, Watcher %v, want all true", ok1, ok2, ok3)
	}

	ch, err := w.WatchHolders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.KeepAlive(ctx, h1, timing); err != nil {
		t.Fatal(err)
	}
	if err := ss.SetSlots(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if slot, err := ss.ClaimSlot(ctx, h1); slot != 0 || err != nil {
		t.Fatalf("ClaimSlot = %d, %v, want 0, nil", slot, err)
	}
	term, err := st.Acquire(ctx, h1, "a", 0)
	if err != nil {
		t.Fatal(err)
	}
	wantValues(t, "grant", ch, tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: term, Version: 1})
}

// TestATSTO30Buffer checks the 1024-value buffer: 1025 grants with nobody receiving give
// exactly the first 1024 values, shards 0 to 1023, and then a closed channel (AT-STO-30; steps
// written from issue #36).
func TestATSTO30Buffer(t *testing.T) {
	s, _ := newStore(t)
	keepAlive(t, s, h1, timing)
	ch := watch(t, s)
	for shard := range 1025 {
		acquire(t, s, h1, "a", shard)
	}
	if n := cap(ch); n != 1024 {
		t.Errorf("cap = %d, want 1024", n)
	}
	got, closed := drain(ch)
	if len(got) != 1024 || !closed {
		t.Fatalf("got %d values, closed %v, want 1024 values and closed", len(got), closed)
	}
	for i, h := range got {
		if h.Space != "a" || h.Shard != i || h.Holder != h1 || h.Version != uint64(i+1) {
			t.Fatalf("value %d = %+v, want shard %d of a held by %q at Version %d", i, h, i, h1, i+1)
		}
	}

	// Later writes do not reopen or panic, and a new channel works.
	acquire(t, s, h1, "a", 2000)
	if v, ok := <-ch; ok {
		t.Errorf("closed channel received %+v", v)
	}
	ch2 := watch(t, s)
	g := grant(t, s, h1, "a", 2001)
	wantValues(t, "new channel", ch2, g)
}

// TestATSTO31DeleteAbsent checks that DeleteRecord of an absent record delivers nothing and
// takes no version (AT-STO-31; steps written from issue #36).
func TestATSTO31DeleteAbsent(t *testing.T) {
	s, _ := newStore(t)
	keepAlive(t, s, h1, timing)
	g := grant(t, s, h1, "a", 0)
	ch := watch(t, s)
	s.DeleteRecord("a", 1)
	s.DeleteRecord("b", 0)
	wantValues(t, "absent", ch)

	s.DeleteRecord("a", 0)
	wantValues(t, "existing", ch, tenure.Holding{Space: "a", Shard: 0, Version: g.Version + 1})
	s.DeleteRecord("a", 0)
	wantValues(t, "deleted twice", ch)
	if got := version(t, s); got != g.Version+2 {
		t.Errorf("next version = %d, want %d", got, g.Version+2)
	}
}

// TestATSTO32Deterministic checks that the same calls on the same clock readings give the same
// results and the same values on every channel, run after run (AT-STO-32; steps written from
// issue #36).
func TestATSTO32Deterministic(t *testing.T) {
	run := func() []string {
		s, c := newStore(t)
		ch1, ch2 := watch(t, s), watch(t, s)
		var log []string
		note := func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) }
		if err := s.SetSlots(ctx, 3); err != nil {
			t.Fatal(err)
		}
		for i := 1; i <= 6; i++ {
			h := holder(i, 1)
			note("KeepAlive(%s) %v", h, s.KeepAlive(ctx, h, timing))
			slot, err := s.ClaimSlot(ctx, h)
			note("ClaimSlot(%s) %d %v", h, slot, err)
			for _, space := range []string{"b", "a", "c"} {
				term, err := s.Acquire(ctx, h, space, i%4)
				note("Acquire(%s, %s, %d) %d %v", h, space, i%4, term, err)
			}
			c.advance(timing.RetryPeriod)
		}
		note("CloseSession %v", s.CloseSession(ctx, holder(2, 1)))
		c.advance(lease)
		note("KeepAlive %v", s.KeepAlive(ctx, holder(7, 1), timing))
		for shard := range 4 {
			term, err := s.Acquire(ctx, holder(7, 1), "a", shard)
			note("Acquire(m7, a, %d) %d %v", shard, term, err)
		}
		s.DeleteRecord("c", 1)
		count, holders, err := s.Slots(ctx)
		note("Slots %d %q %v", count, holders, err)
		v1, _ := drain(ch1)
		v2, _ := drain(ch2)
		if !slices.Equal(v1, v2) {
			t.Fatalf("channels differ:\n%+v\n%+v", v1, v2)
		}
		for _, h := range v1 {
			note("value %+v", h)
		}
		return log
	}
	first := run()
	for range 5 {
		if again := run(); !slices.Equal(again, first) {
			t.Fatalf("run differs:\n%q\nwant\n%q", again, first)
		}
	}
}

// TestATSTO42DeleteDelivery checks that deleting an existing record delivers a Holding with
// only Space, Shard and the deletion's Version set (AT-STO-42, delete delivery; steps written
// from issue #36).
func TestATSTO42DeleteDelivery(t *testing.T) {
	s, c := newStore(t)
	keepAlive(t, s, h1, timing)
	grant(t, s, h1, "a", 3)
	g := grant(t, s, h1, "a", 3)
	ch := watch(t, s)
	s.DeleteRecord("a", 3)
	wantValues(t, "delete", ch, tenure.Holding{Space: "a", Shard: 3, Version: g.Version + 1})

	// The record and its term history are gone: the next grant computes its term from 0.
	c.advance(time.Second)
	next := grant(t, s, h1, "a", 3)
	if want := core.NextTerm(0, c.Wall()); next.Term != want {
		t.Errorf("term after delete = %d, want %d", next.Term, want)
	}
	wantValues(t, "grant after delete", ch, next)
}

// TestATSTO43Synctest checks that a Store on the system clock works inside a testing/synctest
// bubble: sessions expire on the bubble's clock, a receiver blocked on a watch channel wakes for
// each write, ending the watch context closes the channel, and the bubble ends with no deadlock
// and the goroutine count it started with (AT-STO-43; steps written from issue #36).
func TestATSTO43Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := runtime.NumGoroutine()
		s := memstore.New(nil)
		wctx, cancel := context.WithCancel(context.Background())
		ch, err := s.WatchHolders(wctx)
		if err != nil {
			t.Fatal(err)
		}
		var got []tenure.Holding
		done := make(chan struct{})
		go func() {
			defer close(done)
			for h := range ch {
				got = append(got, h)
			}
		}()

		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		t1 := acquire(t, s, h1, "a", 0)
		synctest.Wait()
		time.Sleep(lease - time.Millisecond)
		keepAlive(t, s, h2, timing)
		wantHeld(t, s, h2, "a", h1)
		time.Sleep(time.Millisecond)
		t2 := acquire(t, s, h2, "a", 0)
		if t2 <= t1 {
			t.Errorf("takeover term %d, want above %d", t2, t1)
		}
		synctest.Wait()

		cancel()
		synctest.Wait()
		<-done
		want := []tenure.Holding{
			{Space: "a", Shard: 0, Holder: h1, Term: t1, Version: 1},
			{Space: "a", Shard: 0, Holder: h2, Term: t2, Version: 2},
		}
		if !slices.Equal(got, want) {
			t.Errorf("values %+v, want %+v", got, want)
		}
		if n := runtime.NumGoroutine(); n != start {
			t.Errorf("goroutines = %d, want the starting %d", n, start)
		}
	})
}
