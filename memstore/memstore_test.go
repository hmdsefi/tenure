// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package memstore_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

var _ tenure.Store = (*memstore.Store)(nil)

// fakeClock is a Clock whose monotonic and wall readings the test sets.
type fakeClock struct {
	mu   sync.Mutex
	mono time.Duration
	wall time.Time
}

// epoch is the fake clock's starting wall time.
var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newFakeClock() *fakeClock { return &fakeClock{wall: epoch} }

func (c *fakeClock) Monotonic() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

func (c *fakeClock) Wall() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wall
}

// advance moves both readings forward by d.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mono += d
	c.wall = c.wall.Add(d)
}

// setMono sets the monotonic reading, also to a value before an earlier one.
func (c *fakeClock) setMono(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mono = d
}

// setWall sets the wall reading.
func (c *fakeClock) setWall(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wall = t
}

// holder returns incarnation k of member i.
func holder(i, k int) string { return fmt.Sprintf("m%d/%032x", i, k) }

var (
	ctx = context.Background()

	timing = core.DefaultTiming
	lease  = timing.LeaseDuration

	// doubled is timing with twice its LeaseDuration.
	doubled = core.Timing{LeaseDuration: 2 * lease, RenewDeadline: timing.RenewDeadline,
		RetryPeriod: timing.RetryPeriod, SlotGrace: 2 * lease}

	h1, h2, h3 = holder(1, 1), holder(2, 1), holder(3, 1)
)

func newStore(t *testing.T) (*memstore.Store, *fakeClock) {
	t.Helper()
	c := newFakeClock()
	return memstore.New(c), c
}

func keepAlive(t *testing.T, s *memstore.Store, h string, tm core.Timing) {
	t.Helper()
	if err := s.KeepAlive(ctx, h, tm); err != nil {
		t.Fatalf("KeepAlive(%q) = %v, want nil", h, err)
	}
}

func acquire(t *testing.T, s *memstore.Store, h, space string, shard int) core.Term {
	t.Helper()
	term, err := s.Acquire(ctx, h, space, shard)
	if err != nil || term == 0 {
		t.Fatalf("Acquire(%q, %q, %d) = %d, %v, want a term, nil", h, space, shard, term, err)
	}
	return term
}

// wantHeld checks that h's Acquire of shard 0 of space is refused as held by by.
func wantHeld(t *testing.T, s *memstore.Store, h, space, by string) {
	t.Helper()
	const shard = 0
	term, err := s.Acquire(ctx, h, space, shard)
	var held *tenure.HeldError
	if term != 0 || !errors.As(err, &held) || held.Holder != by {
		t.Fatalf("Acquire(%q, %q, %d) = %d, %v, want 0, held by %q", h, space, shard, term, err, by)
	}
}

func wantLost(t *testing.T, name string, err error) {
	t.Helper()
	if !errors.Is(err, tenure.ErrSessionLost) {
		t.Fatalf("%s = %v, want %v", name, err, tenure.ErrSessionLost)
	}
}

func confirm(t *testing.T, s *memstore.Store, h, space string, shard int) tenure.Holding {
	t.Helper()
	got, err := s.Confirm(ctx, h, space, shard, 1)
	if err != nil {
		t.Fatalf("Confirm(%q, %q, %d) = %v, want nil error", h, space, shard, err)
	}
	return got
}

func holdings(t *testing.T, s *memstore.Store, space string) []tenure.Holding {
	t.Helper()
	got, err := s.Holdings(ctx, space)
	if err != nil {
		t.Fatalf("Holdings(%q) = %v, want nil error", space, err)
	}
	return got
}

// version returns the store-wide version a new write gets, by making one on a probe space with
// a probe holder; the probe's own write takes that version.
func version(t *testing.T, s *memstore.Store) uint64 {
	t.Helper()
	const probe = "m99/probe"
	if err := s.KeepAlive(ctx, probe, timing); err != nil && !errors.Is(err, tenure.ErrSessionLost) {
		t.Fatalf("KeepAlive(%q) = %v", probe, err)
	}
	acquire(t, s, probe, "probe", 0)
	return confirm(t, s, probe, "probe", 0).Version
}

func TestRecordRules(t *testing.T) {
	t.Run("held at L-1ms, granted at L", func(t *testing.T) {
		s, c := newStore(t)
		keepAlive(t, s, h1, timing)
		first := acquire(t, s, h1, "a", 0)
		c.advance(lease - time.Millisecond)
		keepAlive(t, s, h2, timing)
		wantHeld(t, s, h2, "a", h1)
		c.advance(time.Millisecond)
		if got := acquire(t, s, h2, "a", 0); got <= first {
			t.Errorf("Acquire(h2) at L = %d, want a term above %d", got, first)
		}
	})

	t.Run("the holder's own lease counts", func(t *testing.T) {
		s, c := newStore(t)
		keepAlive(t, s, h1, doubled)
		acquire(t, s, h1, "a", 0)
		keepAlive(t, s, h2, timing)
		c.advance(lease - time.Millisecond)
		keepAlive(t, s, h2, timing)
		c.advance(time.Millisecond)
		wantHeld(t, s, h2, "a", h1)
		keepAlive(t, s, h2, timing)
		c.advance(lease - time.Millisecond)
		keepAlive(t, s, h2, timing)
		wantHeld(t, s, h2, "a", h1)
		c.advance(time.Millisecond)
		acquire(t, s, h2, "a", 0)
	})

	t.Run("re-grant gives a larger term and Version", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		t1 := acquire(t, s, h1, "a", 0)
		v1 := confirm(t, s, h1, "a", 0).Version
		t2 := acquire(t, s, h1, "a", 0)
		got := confirm(t, s, h1, "a", 0)
		if t2 <= t1 || got.Term != t2 || got.Version <= v1 || got.Holder != h1 {
			t.Errorf("re-grant: term %d, record %+v; want a term above %d and Version above %d", t2, got, t1, v1)
		}
	})

	t.Run("every grant builds on the record's term", func(t *testing.T) {
		// The wall clock stays put, so each term is the record's term plus 1.
		s, c := newStore(t)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		first := acquire(t, s, h1, "a", 0)
		if err := s.Release(ctx, h1, "a", 0, first); err != nil {
			t.Fatalf("Release(h1, a, 0, %d) = %v, want nil", first, err)
		}
		if got := acquire(t, s, h2, "a", 0); got != first+1 {
			t.Errorf("grant of a cleared record = %d, want %d", got, first+1)
		}
		c.setMono(lease - time.Millisecond)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h3, timing)
		c.setMono(lease)
		if got := acquire(t, s, h3, "a", 0); got != first+2 {
			t.Errorf("grant over an expired holder = %d, want %d", got, first+2)
		}
		keepAlive(t, s, holder(3, 2), timing)
		if got := acquire(t, s, h1, "a", 0); got != first+3 {
			t.Errorf("grant over a replaced holder = %d, want %d", got, first+3)
		}
	})

	t.Run("of racing holders exactly one is granted", func(t *testing.T) {
		s, _ := newStore(t)
		const n = 8
		holders := make([]string, n)
		for i := range holders {
			holders[i] = holder(10+i, 1)
			keepAlive(t, s, holders[i], timing)
		}
		// Each holder tries again and again while others read, so the race detector sees every
		// method run alongside the writes; only the first winner is ever granted, re-grants
		// included.
		var wg sync.WaitGroup
		grants := make([]int, n)
		errs := make([]error, 2*n)
		for i, h := range holders {
			wg.Go(func() {
				for range 20 {
					term, err := s.Acquire(ctx, h, "a", 0)
					var held *tenure.HeldError
					switch {
					case err == nil && term != 0:
						grants[i]++
					case errors.As(err, &held):
					default:
						errs[2*i] = fmt.Errorf("Acquire(%q) = %d, %v, want a term or a *tenure.HeldError", h, term, err)
						return
					}
				}
			})
			wg.Go(func() {
				for range 20 {
					if _, err := s.Holdings(ctx, "a"); err != nil {
						errs[2*i+1] = err
						return
					}
					if _, err := s.Confirm(ctx, h, "a", 0, 1); err != nil {
						errs[2*i+1] = err
						return
					}
				}
			})
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Error(err)
			}
		}
		winners := 0
		for _, g := range grants {
			if g > 0 {
				winners++
			}
		}
		if winners != 1 {
			t.Errorf("%d racing holders were granted the shard, want 1", winners)
		}
	})

	t.Run("mismatched Release changes nothing", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		term := acquire(t, s, h1, "a", 0)
		before := holdings(t, s, "a")
		for _, r := range []struct {
			holder, space string
			shard         int
			term          core.Term
		}{
			{h2, "a", 0, term},
			{h1, "a", 0, term + 1},
			{h1, "a", 1, term},
			{h1, "b", 0, term},
		} {
			if err := s.Release(ctx, r.holder, r.space, r.shard, r.term); err != nil {
				t.Errorf("Release(%q, %q, %d, %d) = %v, want nil", r.holder, r.space, r.shard, r.term, err)
			}
		}
		if got := holdings(t, s, "a"); !slices.Equal(got, before) {
			t.Errorf("Holdings(a) after mismatched releases = %+v, want %+v", got, before)
		}
		if got := holdings(t, s, "b"); len(got) != 0 {
			t.Errorf("Holdings(b) = %+v, want none", got)
		}
		if got, want := version(t, s), before[0].Version+1; got != want {
			t.Errorf("next version = %d, want %d", got, want)
		}

		if err := s.Release(ctx, h1, "a", 0, term); err != nil {
			t.Fatalf("Release(h1, a, 0, %d) = %v, want nil", term, err)
		}
		got := confirm(t, s, h1, "a", 0)
		if got.Holder != "" || got.Term != term || got.Version != before[0].Version+2 {
			t.Errorf("record after Release = %+v, want no holder, term %d, version %d", got, term, before[0].Version+2)
		}
	})

	t.Run("CloseSession clears only the holder's records and keeps their terms", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		keepAlive(t, s, h2, timing)
		ta0 := acquire(t, s, h1, "a", 0)
		ta1 := acquire(t, s, h2, "a", 1)
		ta2 := acquire(t, s, h1, "a", 2)
		tb0 := acquire(t, s, h1, "b", 0)
		v := confirm(t, s, h1, "b", 0).Version
		if err := s.CloseSession(ctx, h1); err != nil {
			t.Fatalf("CloseSession(h1) = %v, want nil", err)
		}
		want := []tenure.Holding{
			{Space: "a", Shard: 0, Holder: "", Term: ta0, Version: v + 1},
			{Space: "a", Shard: 1, Holder: h2, Term: ta1, Version: 2},
			{Space: "a", Shard: 2, Holder: "", Term: ta2, Version: v + 2},
		}
		if got := holdings(t, s, "a"); !slices.Equal(got, want) {
			t.Errorf("Holdings(a) = %+v, want %+v", got, want)
		}
		wantB := []tenure.Holding{{Space: "b", Shard: 0, Holder: "", Term: tb0, Version: v + 3}}
		if got := holdings(t, s, "b"); !slices.Equal(got, wantB) {
			t.Errorf("Holdings(b) = %+v, want %+v", got, wantB)
		}
		wantLost(t, "KeepAlive(h1) after CloseSession", s.KeepAlive(ctx, h1, timing))

		// CloseSession of a holder the store has never heard of blocks it for good.
		if err := s.CloseSession(ctx, h3); err != nil {
			t.Fatalf("CloseSession(h3) = %v, want nil", err)
		}
		wantLost(t, "KeepAlive(h3) after CloseSession", s.KeepAlive(ctx, h3, timing))
	})

	t.Run("Confirm of an absent record", func(t *testing.T) {
		s, _ := newStore(t)
		keepAlive(t, s, h1, timing)
		want := tenure.Holding{Space: "a", Shard: 7}
		if got := confirm(t, s, h1, "a", 7); got != want {
			t.Errorf("Confirm(h1, a, 7) = %+v, want %+v", got, want)
		}
	})
}

// TestATSTO02NoEffect checks that a call returning an error changes nothing: a done context,
// a bad argument, a lost session or a held shard leaves sessions, records and the version
// counter as they were (AT-STO-02, record calls; steps written from issue #35).
func TestATSTO02NoEffect(t *testing.T) {
	s, _ := newStore(t)
	keepAlive(t, s, h1, timing)
	term := acquire(t, s, h1, "a", 0)
	before := holdings(t, s, "a")

	done, cancel := context.WithCancel(ctx)
	cancel()
	calls := []struct {
		name string
		call func() error
	}{
		{"KeepAlive(done, h2)", func() error { return s.KeepAlive(done, h2, timing) }},
		{"KeepAlive(done, m1/2)", func() error { return s.KeepAlive(done, holder(1, 2), timing) }},
		{"KeepAlive(m1/2, bad timing)", func() error { return s.KeepAlive(ctx, holder(1, 2), core.Timing{}) }},
		{"Acquire(done)", func() error { _, err := s.Acquire(done, h1, "a", 1); return err }},
		{"Acquire(bad space)", func() error { _, err := s.Acquire(ctx, h1, "a/b", 1); return err }},
		{"Acquire(unknown holder)", func() error { _, err := s.Acquire(ctx, h2, "a", 1); return err }},
		{"Release(done)", func() error { return s.Release(done, h1, "a", 0, term) }},
		{"Release(term 0)", func() error { return s.Release(ctx, h1, "a", 0, 0) }},
		{"Release(negative shard)", func() error { return s.Release(ctx, h1, "a", -1, term) }},
		{"Confirm(done)", func() error { _, err := s.Confirm(done, h1, "a", 0, term); return err }},
		{"CloseSession(done)", func() error { return s.CloseSession(done, h1) }},
		{"CloseSession(bad holder)", func() error { return s.CloseSession(ctx, "/1") }},
		{"Holdings(done)", func() error { _, err := s.Holdings(done, "a"); return err }},
	}
	for _, cl := range calls {
		if err := cl.call(); err == nil {
			t.Errorf("%s = nil, want an error", cl.name)
		}
	}

	// h1 is still live, its record unchanged, and neither h2 nor m1/2 has a session.
	if got := holdings(t, s, "a"); !slices.Equal(got, before) {
		t.Errorf("Holdings(a) = %+v, want %+v", got, before)
	}
	_, err := s.Acquire(ctx, h2, "a", 0)
	wantLost(t, "Acquire(h2)", err)
	_, err = s.Acquire(ctx, holder(1, 2), "a", 0)
	wantLost(t, "Acquire(m1/2)", err)
	keepAlive(t, s, h1, timing)

	// A held shard and a lost session refuse without writing.
	keepAlive(t, s, h2, timing)
	wantHeld(t, s, h2, "a", h1)
	if err := s.CloseSession(ctx, h3); err != nil {
		t.Fatalf("CloseSession(h3) = %v, want nil", err)
	}
	_, err = s.Acquire(ctx, h3, "a", 1)
	wantLost(t, "Acquire(h3) after CloseSession", err)
	if got := holdings(t, s, "a"); !slices.Equal(got, before) {
		t.Errorf("Holdings(a) after refusals = %+v, want %+v", got, before)
	}
	if got, want := version(t, s), before[0].Version+1; got != want {
		t.Errorf("next version = %d, want %d: a failed call took a version", got, want)
	}
}

// TestATSTO04Messages checks the error of every record call: ctx.Err() itself for a done
// context, then the first bad argument in parameter order with its exact text, then the
// session and held errors (AT-STO-04, record calls; steps written from issue #35).
func TestATSTO04Messages(t *testing.T) {
	s, _ := newStore(t)
	keepAlive(t, s, h1, timing)
	term := acquire(t, s, h1, "a", 0)
	keepAlive(t, s, h2, timing)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	expired, cancel2 := context.WithDeadline(ctx, time.Unix(0, 0))
	defer cancel2()
	bad := core.Timing{}

	tests := []struct {
		name string
		call func() error
		want string
	}{
		{"KeepAlive canceled", func() error { return s.KeepAlive(canceled, "", bad) }, context.Canceled.Error()},
		{"KeepAlive expired", func() error { return s.KeepAlive(expired, "", bad) }, context.DeadlineExceeded.Error()},
		{"KeepAlive empty holder", func() error { return s.KeepAlive(ctx, "", bad) }, "memstore: empty holder"},
		{"KeepAlive zero byte", func() error { return s.KeepAlive(ctx, "m1\x00/1", bad) }, `memstore: holder "m1\x00/1" contains a zero byte`},
		{"KeepAlive empty member", func() error { return s.KeepAlive(ctx, "/1", bad) }, `memstore: holder "/1" has an empty member ID`},
		{"KeepAlive timing", func() error { return s.KeepAlive(ctx, h3, bad) }, "memstore: " + bad.Validate().Error()},

		{"Acquire canceled", func() error { _, err := s.Acquire(canceled, "", "", -1); return err }, context.Canceled.Error()},
		{"Acquire empty holder", func() error { _, err := s.Acquire(ctx, "", "", -1); return err }, "memstore: empty holder"},
		{"Acquire empty space", func() error { _, err := s.Acquire(ctx, h3, "", -1); return err }, "memstore: empty space"},
		{"Acquire slash", func() error { _, err := s.Acquire(ctx, h3, "a/b", -1); return err }, `memstore: space "a/b" contains '/'`},
		{"Acquire space zero byte", func() error { _, err := s.Acquire(ctx, h3, "a\x00", -1); return err }, `memstore: space "a\x00" contains a zero byte`},
		{"Acquire negative shard", func() error { _, err := s.Acquire(ctx, h3, "a", -1); return err }, "memstore: shard -1 is negative"},
		{"Acquire no session", func() error { _, err := s.Acquire(ctx, h3, "a", 0); return err }, tenure.ErrSessionLost.Error()},
		{"Acquire held", func() error { _, err := s.Acquire(ctx, h2, "a", 0); return err }, "tenure: held by " + h1 + ", retry after 2s"},

		{"Release canceled", func() error { return s.Release(canceled, "", "", -1, 0) }, context.Canceled.Error()},
		{"Release empty holder", func() error { return s.Release(ctx, "", "", -1, 0) }, "memstore: empty holder"},
		{"Release empty space", func() error { return s.Release(ctx, h1, "", -1, 0) }, "memstore: empty space"},
		{"Release negative shard", func() error { return s.Release(ctx, h1, "a", -1, 0) }, "memstore: shard -1 is negative"},
		{"Release term 0", func() error { return s.Release(ctx, h1, "a", 0, 0) }, "memstore: term 0"},

		{"Confirm canceled", func() error { _, err := s.Confirm(canceled, "", "", -1, 0); return err }, context.Canceled.Error()},
		{"Confirm empty member", func() error { _, err := s.Confirm(ctx, "/1", "", -1, 0); return err }, `memstore: holder "/1" has an empty member ID`},
		{"Confirm slash", func() error { _, err := s.Confirm(ctx, h1, "/", -1, 0); return err }, `memstore: space "/" contains '/'`},
		{"Confirm negative shard", func() error { _, err := s.Confirm(ctx, h1, "a", -2, 0); return err }, "memstore: shard -2 is negative"},
		{"Confirm term 0", func() error { _, err := s.Confirm(ctx, h3, "a", 0, 0); return err }, "memstore: term 0"},
		{"Confirm no session", func() error { _, err := s.Confirm(ctx, h3, "a", 0, term); return err }, tenure.ErrSessionLost.Error()},

		{"CloseSession canceled", func() error { return s.CloseSession(canceled, "") }, context.Canceled.Error()},
		{"CloseSession empty holder", func() error { return s.CloseSession(ctx, "") }, "memstore: empty holder"},
		{"CloseSession zero byte", func() error { return s.CloseSession(ctx, "\x00") }, `memstore: holder "\x00" contains a zero byte`},

		{"Holdings canceled", func() error { _, err := s.Holdings(canceled, ""); return err }, context.Canceled.Error()},
		{"Holdings empty space", func() error { _, err := s.Holdings(ctx, ""); return err }, "memstore: empty space"},
		{"Holdings zero byte", func() error { _, err := s.Holdings(ctx, "\x00"); return err }, `memstore: space "\x00" contains a zero byte`},
	}
	for _, tt := range tests {
		err := tt.call()
		if err == nil || err.Error() != tt.want {
			t.Errorf("%s = %v, want %s", tt.name, err, tt.want)
		}
	}

	// A done context's error is ctx.Err() itself, and the timing error wraps Validate's.
	if err := s.KeepAlive(canceled, "", bad); err != context.Canceled {
		t.Errorf("KeepAlive(canceled) = %v, want context.Canceled itself", err)
	}
	if _, err := s.Acquire(expired, "", "", -1); err != context.DeadlineExceeded {
		t.Errorf("Acquire(expired) = %v, want context.DeadlineExceeded itself", err)
	}
	if err := s.KeepAlive(ctx, h3, bad); !errors.Is(err, core.ErrInvalidTiming) {
		t.Errorf("KeepAlive(bad timing) = %v, want an error wrapping %v", err, core.ErrInvalidTiming)
	}
}

// TestATSTO05Replace checks that a new incarnation of a member ends the old session at once and
// for good, that the old session's records stay until someone takes them, and that its own
// Release still works (AT-STO-05; steps written from issue #35).
func TestATSTO05Replace(t *testing.T) {
	s, c := newStore(t)
	old, next := holder(1, 1), holder(1, 2)
	keepAlive(t, s, old, timing)
	keepAlive(t, s, h2, timing)
	t0 := acquire(t, s, old, "a", 0)
	t1 := acquire(t, s, old, "a", 1)
	wantHeld(t, s, h2, "a", old)

	keepAlive(t, s, next, timing)
	if got := confirm(t, s, next, "a", 0); got.Holder != old || got.Term != t0 {
		t.Errorf("record a/0 after replacement = %+v, want it still naming %q at %d", got, old, t0)
	}
	if got := acquire(t, s, h2, "a", 0); got <= t0 {
		t.Errorf("Acquire(h2, a, 0) after replacement = %d, want a term above %d", got, t0)
	}

	wantLost(t, "KeepAlive(old)", s.KeepAlive(ctx, old, timing))
	_, err := s.Acquire(ctx, old, "a", 2)
	wantLost(t, "Acquire(old)", err)
	_, err = s.Confirm(ctx, old, "a", 1, t1)
	wantLost(t, "Confirm(old)", err)

	if err := s.Release(ctx, old, "a", 1, t1); err != nil {
		t.Fatalf("Release(old, a, 1) = %v, want nil", err)
	}
	if got := confirm(t, s, next, "a", 1); got.Holder != "" || got.Term != t1 {
		t.Errorf("record a/1 after the old session's Release = %+v, want no holder at %d", got, t1)
	}

	c.advance(10 * lease)
	wantLost(t, "KeepAlive(old) later", s.KeepAlive(ctx, old, timing))
	keepAlive(t, s, holder(1, 3), timing)
	wantLost(t, "KeepAlive(next) after a third incarnation", s.KeepAlive(ctx, next, timing))
}

// TestATSTO09LateRenewal checks that a renewal at or after expiry ends the session for good,
// and that a renewal in time extends it and stores the new timing (AT-STO-09; steps written
// from issue #35).
func TestATSTO09LateRenewal(t *testing.T) {
	s, c := newStore(t)
	keepAlive(t, s, h1, timing)
	acquire(t, s, h1, "a", 0)
	keepAlive(t, s, h2, doubled)

	c.advance(lease - time.Millisecond)
	keepAlive(t, s, h1, doubled)
	c.advance(lease)
	wantHeld(t, s, h2, "a", h1)

	c.advance(lease)
	wantLost(t, "KeepAlive(h1) at expiry", s.KeepAlive(ctx, h1, doubled))
	wantLost(t, "KeepAlive(h1) again", s.KeepAlive(ctx, h1, doubled))
	_, err := s.Acquire(ctx, h1, "a", 0)
	wantLost(t, "Acquire(h1)", err)
	c.setMono(0)
	wantLost(t, "KeepAlive(h1) with the clock read back", s.KeepAlive(ctx, h1, timing))
}

// TestATSTO12RetryAfter checks that HeldError.RetryAfter is the acquirer's RetryPeriod, not the
// holder's (AT-STO-12; steps written from issue #35).
func TestATSTO12RetryAfter(t *testing.T) {
	s, _ := newStore(t)
	slow := core.Timing{LeaseDuration: lease, RenewDeadline: timing.RenewDeadline,
		RetryPeriod: 5 * time.Second, SlotGrace: timing.SlotGrace}
	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h2, slow)
	acquire(t, s, h1, "a", 0)

	_, err := s.Acquire(ctx, h2, "a", 0)
	var held *tenure.HeldError
	if !errors.As(err, &held) {
		t.Fatalf("Acquire(h2) = %v, want a *tenure.HeldError", err)
	}
	if want := (tenure.HeldError{Holder: h1, RetryAfter: slow.RetryPeriod}); *held != want {
		t.Errorf("Acquire(h2) error = %+v, want %+v", *held, want)
	}
	acquire(t, s, h2, "a", 1)
	_, err = s.Acquire(ctx, h1, "a", 1)
	if !errors.As(err, &held) || held.RetryAfter != timing.RetryPeriod {
		t.Errorf("Acquire(h1, a, 1) = %v, want RetryAfter %v", err, timing.RetryPeriod)
	}
}

// TestATSTO15LostRecordTerm checks that a lost record takes its term history with it: the next
// grant's term is core.NextTerm(0, wall), and the deletion counts as a write (AT-STO-15; steps
// written from issue #35).
func TestATSTO15LostRecordTerm(t *testing.T) {
	s, c := newStore(t)
	keepAlive(t, s, h1, timing)
	acquire(t, s, h1, "a", 0)
	acquire(t, s, h1, "a", 0)
	last := acquire(t, s, h1, "a", 0)
	if want := core.NextTerm(0, epoch) + 2; last != want {
		t.Fatalf("third grant = %d, want %d", last, want)
	}
	v := confirm(t, s, h1, "a", 0).Version

	s.DeleteRecord("a", 0)
	if got, want := confirm(t, s, h1, "a", 0), (tenure.Holding{Space: "a", Shard: 0}); got != want {
		t.Errorf("Confirm after DeleteRecord = %+v, want %+v", got, want)
	}
	if got := holdings(t, s, "a"); len(got) != 0 {
		t.Errorf("Holdings(a) after DeleteRecord = %+v, want none", got)
	}
	if got, want := acquire(t, s, h1, "a", 0), core.NextTerm(0, epoch); got != want {
		t.Errorf("grant after DeleteRecord = %d, want %d", got, want)
	}
	if got := confirm(t, s, h1, "a", 0).Version; got != v+2 {
		t.Errorf("Version after DeleteRecord and a grant = %d, want %d", got, v+2)
	}

	s.DeleteRecord("a", 0)
	c.advance(time.Second)
	if got, want := acquire(t, s, h1, "a", 0), core.NextTerm(0, epoch.Add(time.Second)); got != want {
		t.Errorf("grant a second later = %d, want %d", got, want)
	}

	s.DeleteRecord("a", 5)
	s.DeleteRecord("z", 0)
	if got, want := version(t, s), v+5; got != want {
		t.Errorf("next version after deleting absent records = %d, want %d", got, want)
	}
}

// TestATSTO19HoldingsCopy checks that Holdings returns every record of the space, cleared ones
// too, sorted by shard, in a slice the caller owns (AT-STO-19; steps written from issue #35).
func TestATSTO19HoldingsCopy(t *testing.T) {
	s, _ := newStore(t)
	keepAlive(t, s, h1, timing)
	t5 := acquire(t, s, h1, "a", 5)
	t1 := acquire(t, s, h1, "a", 1)
	t3 := acquire(t, s, h1, "a", 3)
	acquire(t, s, h1, "b", 0)
	if err := s.Release(ctx, h1, "a", 3, t3); err != nil {
		t.Fatal(err)
	}

	want := []tenure.Holding{
		{Space: "a", Shard: 1, Holder: h1, Term: t1, Version: 2},
		{Space: "a", Shard: 3, Holder: "", Term: t3, Version: 5},
		{Space: "a", Shard: 5, Holder: h1, Term: t5, Version: 1},
	}
	got := holdings(t, s, "a")
	if !slices.Equal(got, want) {
		t.Fatalf("Holdings(a) = %+v, want %+v", got, want)
	}
	got[0].Holder = "x"
	got[1].Term = 0
	_ = append(got[:1], tenure.Holding{Space: "a", Shard: 9})
	if again := holdings(t, s, "a"); !slices.Equal(again, want) {
		t.Errorf("Holdings(a) after changing the first result = %+v, want %+v", again, want)
	}
	if got := holdings(t, s, "c"); len(got) != 0 {
		t.Errorf("Holdings(c) = %+v, want none", got)
	}
}

// TestATSTO27SystemClock checks a Store on the system clock: a grant's term is the wall time in
// Unix milliseconds, between readings taken around the call (AT-STO-27, term bounds; steps
// written from issue #35).
func TestATSTO27SystemClock(t *testing.T) {
	s := memstore.New(nil)
	keepAlive(t, s, h1, timing)
	before := core.NextTerm(0, time.Now())
	term := acquire(t, s, h1, "a", 0)
	after := core.NextTerm(0, time.Now())
	if term < before || term > after {
		t.Errorf("Acquire = %d, want a term in [%d, %d], the wall time in Unix milliseconds", term, before, after)
	}
	if next := acquire(t, s, h1, "a", 0); next <= term {
		t.Errorf("re-grant = %d, want a term above %d", next, term)
	}
	keepAlive(t, s, h2, timing)
	wantHeld(t, s, h2, "a", h1)
}

// TestATSTO28MonotonicNeverBack checks that the Store's time is the largest monotonic reading
// seen: a reading back in time neither revives an expired session nor shortens a live one
// (AT-STO-28; steps written from issue #35).
func TestATSTO28MonotonicNeverBack(t *testing.T) {
	s, c := newStore(t)
	keepAlive(t, s, h1, timing)
	acquire(t, s, h1, "a", 0)
	c.setMono(lease)
	keepAlive(t, s, h2, timing)

	c.setMono(time.Second)
	acquire(t, s, h2, "a", 0)
	wantLost(t, "KeepAlive(h1)", s.KeepAlive(ctx, h1, timing))

	// h2 was renewed at the Store's time L, not at the reading 1s.
	keepAlive(t, s, h2, timing)
	c.setMono(2*lease - time.Millisecond)
	keepAlive(t, s, h3, timing)
	wantHeld(t, s, h3, "a", h2)
	c.setMono(2 * lease)
	acquire(t, s, h3, "a", 0)
}

// TestATSTO29WallStepsDontExpire checks that steps of the wall clock never expire a session,
// and that a wall step back still gives a larger term (AT-STO-29; steps written from issue #35).
func TestATSTO29WallStepsDontExpire(t *testing.T) {
	s, c := newStore(t)
	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h2, timing)
	first := acquire(t, s, h1, "a", 0)

	c.setWall(epoch.Add(24 * time.Hour))
	wantHeld(t, s, h2, "a", h1)
	keepAlive(t, s, h1, timing)
	ahead := acquire(t, s, h1, "a", 0)
	if want := core.NextTerm(first, epoch.Add(24*time.Hour)); ahead != want {
		t.Errorf("grant after a wall step forward = %d, want %d", ahead, want)
	}

	c.setWall(epoch.Add(-24 * time.Hour))
	wantHeld(t, s, h2, "a", h1)
	if back := acquire(t, s, h1, "a", 0); back != ahead+1 {
		t.Errorf("grant after a wall step back = %d, want %d", back, ahead+1)
	}
}

// TestATSTO41ConfirmReads checks that Confirm returns the current record, shows a takeover, and
// never writes (AT-STO-41; steps written from issue #35).
func TestATSTO41ConfirmReads(t *testing.T) {
	s, c := newStore(t)
	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h2, timing)
	term := acquire(t, s, h1, "a", 0)
	want := tenure.Holding{Space: "a", Shard: 0, Holder: h1, Term: term, Version: 1}
	for range 3 {
		if got := confirm(t, s, h1, "a", 0); got != want {
			t.Fatalf("Confirm(h1) = %+v, want %+v", got, want)
		}
	}
	if got, err := s.Confirm(ctx, h1, "a", 0, term+7); err != nil || got != want {
		t.Errorf("Confirm(h1) with another term = %+v, %v, want %+v, nil", got, err, want)
	}
	if got := confirm(t, s, h2, "a", 0); got != want {
		t.Errorf("Confirm(h2) = %+v, want %+v", got, want)
	}
	if got := version(t, s); got != 2 {
		t.Errorf("next version after Confirms = %d, want 2", got)
	}

	c.advance(lease - time.Millisecond)
	keepAlive(t, s, h2, timing)
	c.advance(time.Millisecond)
	taken := acquire(t, s, h2, "a", 0)
	_, err := s.Confirm(ctx, h1, "a", 0, term)
	wantLost(t, "Confirm(h1) after expiry", err)
	want = tenure.Holding{Space: "a", Shard: 0, Holder: h2, Term: taken, Version: 3}
	if got := confirm(t, s, h2, "a", 0); got != want {
		t.Errorf("Confirm(h2) after the takeover = %+v, want %+v", got, want)
	}
}

// TestATSTO42Versions checks the one store-wide version counter: each grant, clear and
// deletion of an existing record takes the next value, across spaces, and nothing else does
// (AT-STO-42, memstore parts without a watch channel; steps written from issue #35).
func TestATSTO42Versions(t *testing.T) {
	s, c := newStore(t)
	keepAlive(t, s, h1, timing)
	keepAlive(t, s, h2, timing)
	ver := func(h, space string, shard int) uint64 {
		t.Helper()
		return confirm(t, s, h, space, shard).Version
	}
	acquire(t, s, h1, "a", 0)
	acquire(t, s, h2, "b", 0)
	acquire(t, s, h1, "a", 0)
	if got := ver(h1, "a", 0); got != 3 {
		t.Errorf("a/0 Version = %d, want 3", got)
	}
	if got := ver(h1, "b", 0); got != 2 {
		t.Errorf("b/0 Version = %d, want 2", got)
	}

	term := confirm(t, s, h1, "a", 0).Term
	if err := s.Release(ctx, h1, "a", 0, term); err != nil {
		t.Fatal(err)
	}
	if got := ver(h1, "a", 0); got != 4 {
		t.Errorf("a/0 Version after Release = %d, want 4", got)
	}

	// None of these writes a record.
	keepAlive(t, s, h1, timing)
	c.advance(time.Second)
	wantHeld(t, s, h1, "b", h2)
	if err := s.Release(ctx, h1, "b", 0, term); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseSession(ctx, h3); err != nil {
		t.Fatal(err)
	}
	s.DeleteRecord("a", 9)
	if _, err := s.Holdings(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	if err := s.CloseSession(ctx, h2); err != nil {
		t.Fatal(err)
	}
	if got := ver(h1, "b", 0); got != 5 {
		t.Errorf("b/0 Version after CloseSession = %d, want 5", got)
	}
	s.DeleteRecord("b", 0)
	if got := ver(h1, "b", 0); got != 0 {
		t.Errorf("b/0 Version after DeleteRecord = %d, want 0", got)
	}
	if got := version(t, s); got != 7 {
		t.Errorf("next version after DeleteRecord = %d, want 7", got)
	}
}
