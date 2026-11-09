// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of recovery from a lost session (RUN-180, RUN-190). Steps carrying an AT-RUN number
// were written from the issue's text.

// holder02 is the holder of member n1 whose incarnation is the bytes 16 to 31: its incarnation
// after holder01 with recoveryRand.
const holder02 = "n1/101112131415161718191a1b1c1d1e1f"

// recoveryRand returns a reader of the bytes 0, 1, 2, … 31: holder01, then holder02.
func recoveryRand() io.Reader {
	return &countingReader{}
}

// countingReader is a Rand that reads the bytes 0, 1, 2, … in order. Each Read is recorded, and
// after the first, the first fails reads return errBoom and read nothing.
type countingReader struct {
	mu    sync.Mutex
	next  byte
	fails int
	reads []time.Time
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, time.Now())
	if len(r.reads) > 1 && r.fails > 0 {
		r.fails--
		return 0, errBoom
	}
	for i := range p {
		p[i] = r.next
		r.next++
	}
	return len(p), nil
}

// Reads returns the time of each Read, in order.
func (r *countingReader) Reads() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reads)
}

// loseSession makes every later KeepAlive of holder01 return ErrSessionLost; the other holders
// calls go on to the memstore.
func loseSession(fs *recStore) {
	fs.Answer("KeepAlive", func(ctx context.Context, args []any) ([]any, error) {
		if argAt[string](args, 0) == holder01 {
			return nil, tenure.ErrSessionLost
		}
		return fs.Forward(ctx, "KeepAlive", args)
	})
}

// holderCalls are the store methods whose first argument is the holder.
var holderCalls = []string{"KeepAlive", "Acquire", "Release", "Confirm", "CloseSession", "ClaimSlot", "ReleaseSlot"}

// callsOf returns the calls among calls of holderCalls' methods whose holder is holder, in order.
func callsOf(calls []call, holder string) []call {
	var out []call
	for _, c := range calls {
		if slices.Contains(holderCalls, c.Method) && argAt[string](c.Args, 0) == holder {
			out = append(out, c)
		}
	}
	return out
}

// lateRun returns a runScript that blocks until ctx is done and returns its cause, except that
// the first call for shard id ignores ctx until wait has passed after ctx was done.
func lateRun(id int, wait time.Duration) runScript {
	return func(ctx context.Context, sh *tenure.Shard, earlier int) error {
		<-ctx.Done()
		if sh.ID == id && earlier == 0 {
			time.Sleep(wait)
		}
		return context.Cause(ctx)
	}
}

// wantLossAt fails t unless the last KeepAlive of holder01 among calls started at at and returned
// ErrSessionLost, and no call of holder01 started after it.
func wantLossAt(t *testing.T, calls []call, at time.Time) {
	t.Helper()
	h1 := callsOf(calls, holder01)
	var loss call
	for _, c := range h1 {
		if c.Method == "KeepAlive" {
			loss = c
		}
	}
	if !loss.Start.Equal(at) || !errors.Is(loss.Err, tenure.ErrSessionLost) {
		t.Fatalf("last KeepAlive(%s): %v at %v, want ErrSessionLost at %v", holder01, loss, loss.Start, at)
	}
	for _, c := range h1 {
		if c.Seq > loss.EndSeq {
			t.Errorf("%v at %v: a call of %s after the loss", c, c.Start, holder01)
		}
	}
}

// TestATRUN19SessionLostRecovery checks RUN-180 and RUN-190. n1 owns the 8 shards of orders
// from T0, and its KeepAlive at T0+10s returns ErrSessionLost. (a) No Release or CloseSession of
// H1, and no other call of it, goes out after the loss. (b) The first Run of orders/3 ignores
// ctx for 3 s and returns at T0+13s: KeepAlive of H2 goes out at that instant, then Acquire of
// every shard and Holdings("orders"), and every shard runs again under H2. (c) Every Shard of H1
// returns "…: lost" from the loss on. (d) n1 joined once and does not leave until Close; and
// when a drain started at T0+10s is in progress at a loss at T0+12s, with the first Run of
// orders/3 returning at T0+40s, Drain returns nil at T0+40s, no CloseSession goes out, and no new
// incarnation starts.
func TestATRUN19SessionLostRecovery(t *testing.T) {
	t.Run("recovery", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			cfg := r.config()
			cfg.Rand = recoveryRand()
			r.runs.Script(lateRun(3, 3*time.Second))
			t0 := time.Now()
			n := start(t, context.Background(), cfg)
			sleepUntil(t0.Add(9 * time.Second))
			loseSession(r.fs)
			sleepUntil(t0.Add(20 * time.Second))
			synctest.Wait()

			calls := r.fs.Calls()
			// (a)
			wantLossAt(t, calls, t0.Add(10*time.Second))
			for _, c := range callsOf(calls, holder01) {
				if c.Method == "Release" || c.Method == "CloseSession" {
					t.Errorf("%v: H1 released or closed", c)
				}
			}

			// (b)
			last := r.runs.Of("orders", 3)[0]
			if !last.Done || !last.End.Equal(t0.Add(13*time.Second)) {
				t.Fatalf("%v ended at %v, want T0+13s", last, last.End)
			}
			h2 := callsOf(calls, holder02)
			if len(h2) == 0 || h2[0].Method != "KeepAlive" || !h2[0].Start.Equal(last.End) ||
				h2[0].Seq < last.EndSeq || h2[0].Err != nil {
				t.Fatalf("calls of H2 %v, the first at %v; want KeepAlive = nil at T0+13s, when %v returned",
					h2, startsAt(h2, t0), last)
			}
			if got := n.Holder(); got != holder02 {
				t.Errorf("Holder() = %q, want %q", got, holder02)
			}
			ka := h2[0]
			var acquired []int
			for _, c := range h2[1:] {
				if c.Method != "Acquire" {
					continue
				}
				if !c.Start.Equal(ka.Start) || c.Seq < ka.EndSeq || c.Err != nil {
					t.Errorf("%v at %v, want a grant at T0+13s after %v", c, c.Start.Sub(t0), ka)
				}
				acquired = append(acquired, argAt[int](c.Args, 2))
			}
			if slices.Sort(acquired); !slices.Equal(acquired, allShards) {
				t.Errorf("H2 acquired %v, want %v", acquired, allShards)
			}
			hs := holdingsOf(calls, "orders")
			if len(hs) != 1 || !hs[0].Start.Equal(ka.Start) || hs[0].Seq < ka.EndSeq {
				t.Errorf("Holdings calls %v at %v, want one at T0+13s after %v", hs, startsAt(hs, t0), ka)
			}
			for _, id := range allShards {
				runs := r.runs.Of("orders", id)
				if len(runs) != 2 || !runs[1].Start.Equal(ka.Start) || runs[1].Shard.Term <= runs[0].Shard.Term {
					t.Errorf("orders/%d runs %v at %v, want a second at T0+13s with a greater term", id, runs,
						runStarts(runs, t0))
					continue
				}
				wantRunning(t, runs[1:])
			}

			// (c)
			for _, id := range allShards {
				c := r.runs.Of("orders", id)[0]
				wantStopped(t, c, t0.Add(10*time.Second), "lost")
				if err := c.Shard.Check(); err == nil || err.Error() != stopText(c.Shard, "lost") {
					t.Errorf("%v: Check() = %v after recovery, want %q", c, err, stopText(c.Shard, "lost"))
				}
			}

			// (d)
			if j, l := r.sm.Calls("Join"), r.sm.Calls("Leave"); len(j) != 1 || len(l) != 0 {
				t.Errorf("Join calls %v, Leave calls %v; want one Join and no Leave", j, l)
			}

			closeNode(t, n)
			if l := r.sm.Calls("Leave"); len(l) != 1 {
				t.Errorf("Leave calls %v after Close, want one", l)
			}
			for _, c := range callsOf(r.fs.Calls(), holder01) {
				if c.Method == "Release" || c.Method == "CloseSession" {
					t.Errorf("%v: H1 released or closed", c)
				}
			}
			check()
		})
	})

	t.Run("drain", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			cfg := r.config()
			cfg.Rand = recoveryRand()
			r.runs.Script(lateRun(3, 30*time.Second))
			t0 := time.Now()
			n := start(t, context.Background(), cfg)
			sleepUntil(t0.Add(10 * time.Second))
			var drainErr error
			drained := make(chan time.Time, 1)
			go func() {
				drainErr = n.Drain(context.Background())
				drained <- time.Now()
			}()
			sleepUntil(t0.Add(11 * time.Second))
			loseSession(r.fs)

			at := <-drained
			if drainErr != nil || !at.Equal(t0.Add(40*time.Second)) {
				t.Errorf("Drain() = %v at T0+%v, want nil at T0+40s", drainErr, at.Sub(t0))
			}
			calls := r.fs.Calls()
			wantLossAt(t, calls, t0.Add(12*time.Second))
			for _, c := range r.runs.Calls() {
				if c.Shard.ID == 3 && !c.End.Equal(t0.Add(40*time.Second)) {
					t.Errorf("%v ended at %v, want T0+40s", c, c.End.Sub(t0))
				}
			}
			var released []int
			for _, c := range r.fs.Calls("Release") {
				released = append(released, argAt[int](c.Args, 2))
				if !c.Start.Equal(t0.Add(10*time.Second)) || argAt[string](c.Args, 0) != holder01 {
					t.Errorf("%v at %v, want a Release of H1 at T0+10s", c, c.Start.Sub(t0))
				}
			}
			if slices.Sort(released); !slices.Equal(released, []int{0, 1, 2, 4, 5, 6, 7}) {
				t.Errorf("released %v, want every shard but orders/3", released)
			}

			closeNode(t, n)
			calls = r.fs.Calls()
			if cs := r.fs.Calls("CloseSession"); len(cs) != 0 {
				t.Errorf("CloseSession calls %v, want none", cs)
			}
			for _, c := range calls {
				if slices.Contains(holderCalls, c.Method) && argAt[string](c.Args, 0) != holder01 {
					t.Errorf("%v: a call of a new incarnation", c)
				}
			}
			if len(r.runs.Calls()) != 8 {
				t.Errorf("runs %v, want only the 8 of H1", r.runs.Calls())
			}
			if j, l := r.sm.Calls("Join"), r.sm.Calls("Leave"); len(j) != 1 || len(l) != 1 {
				t.Errorf("Join calls %v, Leave calls %v; want one of each", j, l)
			}
			check()
		})
	})
}

// TestATRUN40RunAcrossIncarnations checks RUN-300 across incarnations: the first Run of
// orders/5 ignores ctx for 7 s after the loss at T0+10s, and the second Run of orders/5, under
// the new incarnation, starts at the instant the first returns, T0+17s. No Run of the new
// incarnation starts before then.
func TestATRUN40RunAcrossIncarnations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		cfg := r.config()
		cfg.Rand = recoveryRand()
		r.runs.Script(lateRun(5, 7*time.Second))
		t0 := time.Now()
		n := start(t, context.Background(), cfg)
		sleepUntil(t0.Add(9 * time.Second))
		loseSession(r.fs)
		sleepUntil(t0.Add(20 * time.Second))
		synctest.Wait()

		runs := r.runs.Of("orders", 5)
		if len(runs) != 2 {
			t.Fatalf("orders/5 runs %v, want 2", runs)
		}
		first, second := runs[0], runs[1]
		if !first.End.Equal(t0.Add(17*time.Second)) || !second.Start.Equal(first.End) || second.Seq < first.EndSeq {
			t.Errorf("first Run of orders/5 returned at T0+%v, second started at T0+%v; want both at T0+17s, in order",
				first.End.Sub(t0), second.Start.Sub(t0))
		}
		if second.Shard.Term <= first.Shard.Term {
			t.Errorf("second Run of orders/5 at term %d, want above %d", second.Shard.Term, first.Shard.Term)
		}
		for _, id := range allShards {
			runs := r.runs.Of("orders", id)
			if len(runs) != 2 || !runs[1].Start.Equal(first.End) {
				t.Errorf("orders/%d runs started at %v, want the second at T0+17s", id, runStarts(runs, t0))
			}
		}
		closeNode(t, n)
		check()
	})
}

// TestRecoveryRandRetry checks the draw of a new incarnation: after the loss at T0+10s, with every
// Run returned at once, the reads of Rand at T0+10s and T0+12s fail and the one at T0+14s
// succeeds, so KeepAlive of H2 goes out at T0+14s and no call of H2 before.
func TestRecoveryRandRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		cfg := r.config()
		rnd := &countingReader{fails: 2}
		cfg.Rand = rnd
		t0 := time.Now()
		n := start(t, context.Background(), cfg)
		sleepUntil(t0.Add(9 * time.Second))
		loseSession(r.fs)
		sleepUntil(t0.Add(20 * time.Second))
		synctest.Wait()

		var reads []time.Duration
		for _, at := range rnd.Reads() {
			reads = append(reads, at.Sub(t0))
		}
		if want := secs(0, 10, 12, 14); !slices.Equal(reads, want) {
			t.Errorf("Rand reads at %v, want %v", reads, want)
		}
		calls := r.fs.Calls()
		wantLossAt(t, calls, t0.Add(10*time.Second))
		h2 := callsOf(calls, holder02)
		if len(h2) == 0 || h2[0].Method != "KeepAlive" || !h2[0].Start.Equal(t0.Add(14*time.Second)) {
			t.Errorf("calls of H2 %v at %v, want KeepAlive first, at T0+14s", h2, startsAt(h2, t0))
		}
		for _, id := range allShards {
			runs := r.runs.Of("orders", id)
			if len(runs) != 2 || !runs[1].Start.Equal(t0.Add(14*time.Second)) {
				t.Errorf("orders/%d runs started at %v, want the second at T0+14s", id, runStarts(runs, t0))
			}
		}
		closeNode(t, n)
		check()
	})
}

// holder03 is the holder of member n1 whose incarnation is the bytes 32 to 47: its incarnation
// after holder02 with recoveryRand.
const holder03 = "n1/202122232425262728292a2b2c2d2e2f"

// TestRecoveryTwice checks a loss while recovering. H1's KeepAlive at T0+10s returns
// ErrSessionLost, and so does the first KeepAlive of H2, at T0+10s. H3's KeepAlive goes out
// RetryPeriod after H2's, at T0+12s, and H3 acquires every shard. n2 joins at T0+15s: the new
// machine takes that view, n1 then runs only the shards Balanced gives it, and Route sends every
// shard to its owner. Route answers after every step from Start on, so routing keeps the earlier
// assignment until a new machine has taken a view.
func TestRecoveryTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		cfg := r.config()
		cfg.Rand = recoveryRand()

		var (
			node    atomic.Pointer[tenure.Node]
			mu      sync.Mutex
			unroute []string
		)
		ctx := tenure.WithStepHook(context.Background(), func(rec tenure.StepRecord) {
			n := node.Load()
			if n == nil {
				return
			}
			for _, id := range allShards {
				if _, err := n.Route("orders", keyOf(id)); err != nil {
					mu.Lock()
					unroute = append(unroute, fmt.Sprintf("generation %d, %T: %v", rec.Gen, rec.Input, err))
					mu.Unlock()
				}
			}
		})
		t0 := time.Now()
		n := start(t, ctx, cfg)
		node.Store(n)
		sleepUntil(t0.Add(9 * time.Second))
		r.fs.Answer("KeepAlive", func(ctx context.Context, args []any) ([]any, error) {
			if h := argAt[string](args, 0); h == holder01 || h == holder02 {
				return nil, tenure.ErrSessionLost
			}
			return r.fs.Forward(ctx, "KeepAlive", args)
		})
		sleepUntil(t0.Add(15 * time.Second))
		r.g.Join(tenure.Member{ID: "n2", Addr: "a2"})
		sleepUntil(t0.Add(20 * time.Second))
		synctest.Wait()

		calls := r.fs.Calls()
		wantLossAt(t, calls, t0.Add(10*time.Second))
		h2 := callsOf(calls, holder02)
		if len(h2) != 1 || h2[0].Method != "KeepAlive" || !h2[0].Start.Equal(t0.Add(10*time.Second)) {
			t.Errorf("calls of H2 %v at %v, want one KeepAlive at T0+10s", h2, startsAt(h2, t0))
		}
		h3 := callsOf(calls, holder03)
		if len(h3) == 0 || h3[0].Method != "KeepAlive" || !h3[0].Start.Equal(t0.Add(12*time.Second)) {
			t.Errorf("calls of H3 %v at %v, want KeepAlive first, at T0+12s", h3, startsAt(h3, t0))
		}
		if got := n.Holder(); got != holder03 {
			t.Errorf("Holder() = %q, want %q", got, holder03)
		}

		owner := ownerOf(bal("orders", 8, "n1", "n2"))
		var running, want []int
		for _, c := range r.runs.Calls() {
			if !c.Done {
				running = append(running, c.Shard.ID)
			}
		}
		for _, id := range allShards {
			if owner[id] == "n1" {
				want = append(want, id)
			}
			wantRoute(t, n, "orders", keyOf(id), owner[id])
		}
		if slices.Sort(running); !slices.Equal(running, want) {
			t.Errorf("n1 runs %v after n2 joined, want %v", running, want)
		}
		mu.Lock()
		for _, u := range unroute {
			t.Errorf("Route failed after a step: %s", u)
		}
		mu.Unlock()

		node.Store(nil)
		closeNode(t, n)
		check()
	})
}

// TestLossDropsWaitingCalls checks that no call of a lost holder starts once the step of the
// loss is done (MCH O7), also a call that waited for a permit then: found by
// TestDriverContractO7Closing, seed 2 of a wider sweep, where a queued Release went out after the
// loss. n1 serves 17 shards; the first 16 Acquire calls hang in the store and hold every permit,
// so the 17th waits for one. The KeepAlive at T0+2s loses the session, and the step hook of the
// loss lets the 16 calls return. The 17th Acquire must not go out.
func TestLossDropsWaitingCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		cfg := r.config()
		cfg.Rand = recoveryRand()
		cfg.Spaces[0].Shards = 17
		release := r.fs.Block("Acquire")
		var lossSeq atomic.Uint64
		ctx := tenure.WithStepHook(context.Background(), func(rec tenure.StepRecord) {
			if rec.Before.Session != core.SessionStateLost && rec.After.Session == core.SessionStateLost {
				lossSeq.Store(seq())
				release()
				time.Sleep(time.Millisecond) // the lanes run on meanwhile
			}
		})
		t0 := time.Now()
		n := start(t, ctx, cfg)
		loseSession(r.fs)
		sleepUntil(t0.Add(5 * time.Second))
		synctest.Wait()

		at := lossSeq.Load()
		if at == 0 {
			t.Fatal("the session was not lost")
		}
		acquires := 0
		for _, c := range callsOf(r.fs.Calls(), holder01) {
			if c.Method == "Acquire" {
				acquires++
			}
			if c.Seq > at {
				t.Errorf("%v started after the loss", c)
			}
		}
		if acquires != 16 {
			t.Errorf("%d Acquire calls of %s, want 16: the 17th waits for a permit", acquires, holder01)
		}
		closeNode(t, n)
		check()
	})
}
