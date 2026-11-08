// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// The tests of a node that runs shards through the store: Acquire and Release through the
// executor, one supervised Run per StartRun, StopRun's cancel with the stop error, and RunStopped
// before every Release. Steps carrying an AT-RUN number were written from the issue's text.

// shardRig is member n1's doubles for the shard tests: fs over a memstore, a group that lists no
// member, sm for n1, and the runLog that serves orders.
type shardRig struct {
	ms   *memstore.Store
	fs   *recStore
	g    *group
	sm   *recMembership
	runs *runLog
}

// newShardRig returns a shardRig. n1 is alone in its views, so it owns every shard of orders.
func newShardRig(t *testing.T) shardRig {
	t.Helper()
	ms := memstore.New(nil)
	g := newGroup(t)
	return shardRig{ms: ms, fs: newRecStore(ms), g: g, sm: g.Absent("n1"), runs: &runLog{}}
}

// config returns the Config of member n1 over r's doubles, with orders and the counting Rand.
func (r shardRig) config() tenure.Config {
	return tenure.Config{
		Self:       tenure.Member{ID: "n1", Addr: "a1"},
		Membership: r.sm,
		Store:      r.fs,
		Spaces:     []tenure.Space{orders(r.runs.Run)},
		Rand:       countingRand(),
	}
}

// sleepUntil sleeps until the bubble time t.
func sleepUntil(t time.Time) { time.Sleep(time.Until(t)) }

// allShards are the shards of orders.
var allShards = []int{0, 1, 2, 3, 4, 5, 6, 7}

// runStarts returns the start of each call, relative to t0.
func runStarts(calls []runCall, t0 time.Time) []time.Duration {
	out := make([]time.Duration, len(calls))
	for i, c := range calls {
		out[i] = c.Start.Sub(t0)
	}
	return out
}

// shardCalls returns the calls among calls whose arguments name shard id of orders, in order.
func shardCalls(calls []call, id int) []call {
	var out []call
	for _, c := range calls {
		if argAt[string](c.Args, 1) == "orders" && argAt[int](c.Args, 2) == id {
			out = append(out, c)
		}
	}
	return out
}

// stopText returns the text of the stop error of sh with reason.
func stopText(sh *tenure.Shard, reason string) string {
	return fmt.Sprintf("tenure: not owner of %s/%d (term %d): %s", sh.Space, sh.ID, sh.Term, reason)
}

// wantStopped fails t unless c is a call whose ctx was done at at with the stop error of reason
// as its cause, which Check already returned at that instant.
func wantStopped(t *testing.T, c runCall, at time.Time, reason string) {
	t.Helper()
	want := stopText(c.Shard, reason)
	if !c.CtxDone.Equal(at) {
		t.Errorf("%v: ctx done at %v, want %v", c, c.CtxDone, at)
	}
	if c.Cause == nil || c.Cause.Error() != want || !errors.Is(c.Cause, tenure.ErrNotOwner) {
		t.Errorf("%v: cause %v, want %q matching ErrNotOwner", c, c.Cause, want)
	}
	if c.CheckAtDone != c.Cause {
		t.Errorf("%v: Check at ctx done = %v, want the cause %v", c, c.CheckAtDone, c.Cause)
	}
}

// maxInFlight returns the largest number of calls among calls in flight at once, by their stamps.
func maxInFlight(calls []call) int {
	type edge struct {
		at    uint64
		delta int
	}
	var edges []edge
	for _, c := range calls {
		edges = append(edges, edge{c.Seq, 1})
		if c.Done {
			edges = append(edges, edge{c.EndSeq, -1})
		}
	}
	slices.SortFunc(edges, func(a, b edge) int { return int(a.at) - int(b.at) })
	most, cur := 0, 0
	for _, e := range edges {
		cur += e.delta
		most = max(most, cur)
	}
	return most
}

// TestATRUN10PermitsAndRenewals checks that at most 16 shard calls are in flight at once while
// renewals, which need no permit, keep their cadence: 40 Acquire calls block from T0 to T0+7s.
func TestATRUN10PermitsAndRenewals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		cfg := r.config()
		cfg.Spaces[0].Shards = 40
		release := r.fs.Block("Acquire")
		t0 := time.Now()
		n := start(t, context.Background(), cfg)
		synctest.Wait()
		if got := len(r.fs.Calls("Acquire")); got != 16 {
			t.Errorf("%d Acquire calls at T0, want 16", got)
		}

		sleepUntil(t0.Add(7 * time.Second))
		ka := r.fs.Calls("KeepAlive")
		if got, want := startsAt(ka, t0), secs(0, 2, 4, 6); !slices.Equal(got, want) {
			t.Errorf("KeepAlive calls at %v, want %v", got, want)
		}
		for _, c := range ka {
			if c.Err != nil {
				t.Errorf("%v, want nil", c)
			}
		}
		acq := r.fs.Calls("Acquire")
		if len(acq) != 16 {
			t.Errorf("%d Acquire calls at T0+7s, want 16", len(acq))
		}
		for _, c := range acq {
			if c.Done {
				t.Errorf("%v returned while blocked", c)
			}
		}
		if c := r.runs.Calls(); len(c) != 0 {
			t.Errorf("runs before any Acquire returned: %v", c)
		}

		// The blocked calls return past their deadline, and the machine tries those shards again
		// RetryPeriod later; the 24 calls that waited for a permit go out now and succeed.
		release()
		sleepUntil(t0.Add(9500 * time.Millisecond))
		acq = r.fs.Calls("Acquire")
		if got := len(acq); got != 16+24+16 {
			t.Errorf("%d Acquire calls, want 56", got)
		}
		if got := maxInFlight(acq); got != 16 {
			t.Errorf("at most %d Acquire calls in flight at once, want 16", got)
		}
		for _, c := range acq[:16] {
			if !errors.Is(c.Err, context.DeadlineExceeded) {
				t.Errorf("blocked %v, want DeadlineExceeded", c)
			}
		}
		starts := map[time.Duration]int{}
		for id := range 40 {
			calls := r.runs.Of("orders", id)
			if len(calls) != 1 {
				t.Errorf("orders/%d has runs %v, want one", id, calls)
				continue
			}
			starts[calls[0].Start.Sub(t0)]++
		}
		if want := map[time.Duration]int{7 * time.Second: 24, 9 * time.Second: 16}; !mapsEqual(starts, want) {
			t.Errorf("runs started %v, want %v", starts, want)
		}
		if got, want := startsAt(r.fs.Calls("KeepAlive"), t0), secs(0, 2, 4, 6, 8); !slices.Equal(got, want) {
			t.Errorf("KeepAlive calls at %v, want %v", got, want)
		}
		closeNode(t, n)
		if got := len(r.fs.Calls("Release")); got != 40 {
			t.Errorf("%d Release calls at Close, want 40", got)
		}
		check()
	})
}

// mapsEqual reports whether a and b hold the same entries.
func mapsEqual[K comparable, V comparable](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// TestATRUN11UnfencedExpiry checks that the runs of an unfenced space end at the session
// deadline: after the last successful renewal S at T0, every Run's ctx is done at exactly S+10s
// with the stop error "…: expired" as its cause, and no Release goes out before.
func TestATRUN11UnfencedExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		s := time.Now()
		n := start(t, context.Background(), r.config())
		r.fs.Fail("KeepAlive", errors.New("boom"))
		synctest.Wait()
		calls := r.runs.Calls()
		if len(calls) != 8 {
			t.Fatalf("runs = %v, want one per shard", calls)
		}

		sleepUntil(s.Add(10*time.Second - time.Nanosecond))
		for _, c := range r.runs.Calls() {
			if !c.CtxDone.IsZero() || c.Shard.Check() != nil {
				t.Errorf("%v: ctx done at %v, Check() = %v before S+10s", c, c.CtxDone, c.Shard.Check())
			}
		}
		if rel := r.fs.Calls("Release"); len(rel) != 0 {
			t.Errorf("Release calls before S+10s: %v", rel)
		}

		sleepUntil(s.Add(10 * time.Second))
		synctest.Wait()
		for _, c := range r.runs.Calls() {
			wantStopped(t, c, s.Add(10*time.Second), "expired")
			if !c.Done || !c.End.Equal(s.Add(10*time.Second)) {
				t.Errorf("%v returned at %v, want S+10s", c, c.End)
			}
			rel := shardCalls(r.fs.Calls("Release"), c.Shard.ID)
			if len(rel) != 1 || rel[0].Args[3] != c.Shard.Term || rel[0].Err != nil || !rel[0].Start.Equal(s.Add(10*time.Second)) {
				t.Errorf("orders/%d: Release calls %v, want one at the run's term at S+10s", c.Shard.ID, rel)
			}
			if p := n.PublishedShard("orders", c.Shard.ID); p != nil {
				t.Errorf("orders/%d is still published after its RunStopped", c.Shard.ID)
			}
		}
		if acq := r.fs.Calls("Acquire"); len(acq) != 8 {
			t.Errorf("Acquire calls = %v, want only the first 8: the session is late", acq)
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN12ExpiryAtDeadline checks, 20 times over, that a Run checking its Shard right at the
// session deadline is no longer the owner, even before the node stops the shard: Check is nil one
// nanosecond before and the expired error at the deadline, the same error that becomes ctx's
// cause.
func TestATRUN12ExpiryAtDeadline(t *testing.T) {
	for i := range 20 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				r := newShardRig(t)
				type seen struct{ before, at, cause error }
				results := make(chan seen, 8)
				t0 := time.Now()
				deadline := t0.Add(10 * time.Second)
				r.runs.Script(func(ctx context.Context, sh *tenure.Shard, _ int) error {
					var s seen
					sleepUntil(deadline.Add(-time.Nanosecond))
					s.before = sh.Check()
					sleepUntil(deadline)
					s.at = sh.Check()
					<-ctx.Done()
					s.cause = context.Cause(ctx)
					results <- s
					return s.cause
				})
				n := start(t, context.Background(), r.config())
				r.fs.Fail("KeepAlive", errors.New("boom"))
				sleepUntil(deadline)
				synctest.Wait()
				if len(results) != 8 {
					t.Fatalf("%d runs ended at the deadline, want 8", len(results))
				}
				for range 8 {
					s := <-results
					if s.before != nil {
						t.Errorf("Check() 1ns before the deadline = %v, want nil", s.before)
					}
					if s.at == nil || !strings.HasSuffix(s.at.Error(), "): expired") || s.at != s.cause {
						t.Errorf("Check() at the deadline = %v, cause %v, want the same expired error", s.at, s.cause)
					}
				}
				for _, c := range r.runs.Calls() {
					wantStopped(t, c, deadline, "expired")
				}
				closeNode(t, n)
				check()
			})
		})
	}
}

// TestATRUN15AcquireRetry checks that a shard the store does not grant is acquired again: after a
// HeldError at the RetryAfter the error gives (raised to RetryPeriod), after any other error
// RetryPeriod later; its Run starts once an Acquire succeeds.
func TestATRUN15AcquireRetry(t *testing.T) {
	t.Run("held", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			o := newOutside(t, r.ms, core.DefaultTiming)
			if _, err := o.Acquire("orders", 3); err != nil {
				t.Fatalf("outside Acquire(orders/3) = %v", err)
			}
			t0 := time.Now()
			n := start(t, context.Background(), r.config())
			sleepUntil(t0.Add(5 * time.Second))
			if err := o.Release("orders", 3); err != nil {
				t.Fatalf("outside Release(orders/3) = %v", err)
			}
			sleepUntil(t0.Add(7 * time.Second))

			acq := shardCalls(r.fs.Calls("Acquire"), 3)
			if got, want := startsAt(acq, t0), secs(0, 2, 4, 6); !slices.Equal(got, want) {
				t.Fatalf("Acquire(orders/3) calls at %v, want %v", got, want)
			}
			for _, c := range acq[:3] {
				var held *tenure.HeldError
				if !errors.As(c.Err, &held) || held.Holder != outsideHolder {
					t.Errorf("%v, want a HeldError naming %s", c, outsideHolder)
				}
			}
			if acq[3].Err != nil {
				t.Errorf("%v, want a term", acq[3])
			}
			if got, want := runStarts(r.runs.Of("orders", 3), t0), secs(6); !slices.Equal(got, want) {
				t.Errorf("orders/3 runs started at %v, want %v", got, want)
			}
			for _, id := range allShards {
				if id != 3 && len(shardCalls(r.fs.Calls("Acquire"), id)) != 1 {
					t.Errorf("orders/%d: Acquire calls %v, want one", id, shardCalls(r.fs.Calls("Acquire"), id))
				}
			}
			o.Stop()
			closeNode(t, n)
			check()
		})
	})

	t.Run("failed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			boom := errors.New("boom")
			t0 := time.Now()
			r.fs.Answer("Acquire", func(ctx context.Context, args []any) ([]any, error) {
				if argAt[int](args, 2) == 5 && time.Now().Before(t0.Add(3*time.Second)) {
					return []any{core.Term(0)}, boom
				}
				return r.fs.Forward(ctx, "Acquire", args)
			})
			n := start(t, context.Background(), r.config())
			sleepUntil(t0.Add(5 * time.Second))

			acq := shardCalls(r.fs.Calls("Acquire"), 5)
			if got, want := startsAt(acq, t0), secs(0, 2, 4); !slices.Equal(got, want) {
				t.Fatalf("Acquire(orders/5) calls at %v, want %v", got, want)
			}
			if acq[0].Err != boom || acq[1].Err != boom || acq[2].Err != nil {
				t.Errorf("Acquire(orders/5) calls %v, want boom, boom, a term", acq)
			}
			if got, want := runStarts(r.runs.Of("orders", 5), t0), secs(4); !slices.Equal(got, want) {
				t.Errorf("orders/5 runs started at %v, want %v", got, want)
			}
			closeNode(t, n)
			check()
		})
	})
}

// TestATRUN16ReleaseRetries checks that a Release the store does not answer with nil is repeated
// every RetryPeriod until one succeeds, and that the drain completes then: (a) a failing Release,
// (b) a Release that blocks past its deadline, during which the repeats are dropped.
func TestATRUN16ReleaseRetries(t *testing.T) {
	tests := []struct {
		name   string
		script func(r shardRig, t0 time.Time) // from T0+1s
		starts []time.Duration
	}{
		{"a: fails", func(r shardRig, t0 time.Time) {
			r.fs.Fail("Release", errors.New("boom"))
			go func() {
				sleepUntil(t0.Add(6 * time.Second))
				r.fs.Fail("Release", nil)
			}()
		}, secs(1, 3, 5, 7)},
		{"b: blocks", func(r shardRig, t0 time.Time) {
			release := r.fs.Block("Release")
			go func() {
				sleepUntil(t0.Add(6 * time.Second))
				release()
			}()
		}, secs(1, 7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				r := newShardRig(t)
				t0 := time.Now()
				n := start(t, context.Background(), r.config())
				sleepUntil(t0.Add(time.Second))
				tt.script(r, t0)
				if err := n.Drain(context.Background()); err != nil {
					t.Fatalf("Drain() = %v, want nil", err)
				}
				if d := time.Since(t0); d != 7*time.Second {
					t.Errorf("Drain returned at T0+%v, want T0+7s", d)
				}
				for _, c := range r.runs.Calls() {
					wantStopped(t, c, t0.Add(time.Second), "draining")
					rel := shardCalls(r.fs.Calls("Release"), c.Shard.ID)
					if got := startsAt(rel, t0); !slices.Equal(got, tt.starts) {
						t.Errorf("orders/%d: Release calls at %v, want %v", c.Shard.ID, got, tt.starts)
					}
					if len(rel) > 0 && rel[len(rel)-1].Err != nil {
						t.Errorf("last %v, want nil", rel[len(rel)-1])
					}
					if strings.HasPrefix(tt.name, "b") && len(rel) > 0 && !errors.Is(rel[0].Err, context.DeadlineExceeded) {
						t.Errorf("blocked %v, want DeadlineExceeded", rel[0])
					}
				}
				if got := startsAt(r.fs.Calls("CloseSession"), t0); !slices.Equal(got, secs(7)) {
					t.Errorf("CloseSession calls at %v, want [7s]", got)
				}
				closeNode(t, n)
				check()
			})
		})
	}
}

// TestATRUN31Moved checks a move: when n2 joins, the runs of the shards it gets are stopped with
// "…: moved", released once they have returned and unpublished, while n1's other runs go on.
func TestATRUN31Moved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		t0 := time.Now()
		n := start(t, context.Background(), r.config())
		sleepUntil(t0.Add(time.Second))
		r.g.Join(tenure.Member{ID: "n2", Addr: "a2"})
		synctest.Wait()

		moved := bal("orders", 8, "n1", "n2")["n2"]
		for _, id := range allShards {
			calls := r.runs.Of("orders", id)
			if len(calls) != 1 {
				t.Fatalf("orders/%d runs = %v, want one", id, calls)
			}
			c := calls[0]
			rel := shardCalls(r.fs.Calls("Release"), id)
			if !slices.Contains(moved, id) {
				if !c.CtxDone.IsZero() || !c.Shard.Valid() || n.PublishedShard("orders", id) != c.Shard || len(rel) != 0 {
					t.Errorf("kept orders/%d: %v, published %v, Release calls %v; want it running", id, c,
						n.PublishedShard("orders", id), rel)
				}
				continue
			}
			wantStopped(t, c, t0.Add(time.Second), "moved")
			if c.Shard.Check() != c.Cause {
				t.Errorf("orders/%d: Check() = %v, want the cause", id, c.Shard.Check())
			}
			if len(rel) != 1 || rel[0].Args[3] != c.Shard.Term || rel[0].Err != nil || rel[0].Seq < c.EndSeq {
				t.Errorf("moved orders/%d: Release calls %v, want one at term %d after Run returned", id, rel, c.Shard.Term)
			}
			if p := n.PublishedShard("orders", id); p != nil {
				t.Errorf("moved orders/%d is still published", id)
			}
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN34RunRestarts checks the restart backoff of a Run that keeps returning at once:
// min(RetryPeriod·2^(c−1), LeaseDuration), so calls at T0+{0,2,6,14,29,44}s, each with the same
// Shard.
func TestATRUN34RunRestarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		boom := errors.New("boom")
		r.runs.Script(func(context.Context, *tenure.Shard, int) error { return boom })
		t0 := time.Now()
		n := start(t, context.Background(), r.config())
		sleepUntil(t0.Add(45 * time.Second))
		for _, id := range allShards {
			calls := r.runs.Of("orders", id)
			if got, want := runStarts(calls, t0), secs(0, 2, 6, 14, 29, 44); !slices.Equal(got, want) {
				t.Errorf("orders/%d runs at %v, want %v", id, got, want)
			}
			for _, c := range calls {
				if c.Shard != calls[0].Shard || c.Err != boom {
					t.Errorf("%v, want the first call's Shard and boom", c)
				}
			}
		}
		if rel := r.fs.Calls("Release"); len(rel) != 0 {
			t.Errorf("Release calls while the member owns the shards: %v", rel)
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN35RestartAfterLongRun checks that a Run that lasted at least LeaseDuration resets the
// count: a 20 s call from T0, then quick returns, restarts at T0+22s, +26s and +34s.
func TestATRUN35RestartAfterLongRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		boom := errors.New("boom")
		r.runs.Script(func(ctx context.Context, _ *tenure.Shard, earlier int) error {
			if earlier == 0 {
				select {
				case <-time.After(20 * time.Second):
				case <-ctx.Done():
				}
			}
			return boom
		})
		t0 := time.Now()
		n := start(t, context.Background(), r.config())
		sleepUntil(t0.Add(35 * time.Second))
		for _, id := range allShards {
			if got, want := runStarts(r.runs.Of("orders", id), t0), secs(0, 22, 26, 34); !slices.Equal(got, want) {
				t.Errorf("orders/%d runs at %v, want %v", id, got, want)
			}
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN36MoveDuringBackoff checks that a shard that moves while its Run waits to be restarted
// is not run again: it is released at once and unpublished, while the shards n1 keeps restart.
func TestATRUN36MoveDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		boom := errors.New("boom")
		r.runs.Script(func(context.Context, *tenure.Shard, int) error { return boom })
		t0 := time.Now()
		n := start(t, context.Background(), r.config())
		sleepUntil(t0.Add(time.Second))
		r.g.Join(tenure.Member{ID: "n2", Addr: "a2"})
		sleepUntil(t0.Add(7 * time.Second))

		moved := bal("orders", 8, "n1", "n2")["n2"]
		for _, id := range allShards {
			calls := r.runs.Of("orders", id)
			rel := shardCalls(r.fs.Calls("Release"), id)
			if !slices.Contains(moved, id) {
				if got, want := runStarts(calls, t0), secs(0, 2, 6); !slices.Equal(got, want) || len(rel) != 0 {
					t.Errorf("kept orders/%d: runs at %v and Release calls %v, want runs at %v and no Release", id, got, rel, want)
				}
				continue
			}
			if got, want := runStarts(calls, t0), secs(0); !slices.Equal(got, want) {
				t.Errorf("moved orders/%d runs at %v, want %v", id, got, want)
			}
			if got, want := startsAt(rel, t0), secs(1); !slices.Equal(got, want) || rel[0].Err != nil {
				t.Errorf("moved orders/%d: Release calls %v at %v, want one at %v", id, rel, got, want)
			}
			if n.PublishedShard("orders", id) != nil {
				t.Errorf("moved orders/%d is still published", id)
			}
			if err := calls[0].Shard.Check(); err == nil || err.Error() != stopText(calls[0].Shard, "moved") {
				t.Errorf("moved orders/%d: Check() = %v, want the moved error", id, err)
			}
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN39StuckRun checks a Run that ignores its ctx: stopped at M, it is stuck from
// M+RenewDeadline, the shard stays published with its stop error, and it is released only once
// Run returns at M+30s.
func TestATRUN39StuckRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		moved := bal("orders", 8, "n1", "n2")["n2"]
		r.runs.Script(func(ctx context.Context, sh *tenure.Shard, _ int) error {
			<-ctx.Done()
			if slices.Contains(moved, sh.ID) {
				time.Sleep(30 * time.Second)
			}
			return nil
		})
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.config())
		m := t0.Add(time.Second)
		sleepUntil(m)
		r.g.Join(tenure.Member{ID: "n2", Addr: "a2"})

		sleepUntil(m.Add(30*time.Second - time.Nanosecond))
		for _, id := range moved {
			c := r.runs.Of("orders", id)[0]
			if c.Done {
				t.Errorf("stuck %v returned early", c)
			}
			if rel := shardCalls(r.fs.Calls("Release"), id); len(rel) != 0 {
				t.Errorf("orders/%d released before M+30s: %v", id, rel)
			}
			if p := n.PublishedShard("orders", id); p != c.Shard || p.Check() == nil || p.Check().Error() != stopText(p, "moved") {
				t.Errorf("stuck orders/%d: published %v, want its Shard with the moved error", id, p)
			}
		}

		sleepUntil(m.Add(30 * time.Second))
		synctest.Wait()
		for _, id := range moved {
			c := r.runs.Of("orders", id)[0]
			wantStopped(t, c, m, "moved")
			rel := shardCalls(r.fs.Calls("Release"), id)
			if got, want := startsAt(rel, t0), secs(31); !slices.Equal(got, want) || rel[0].Err != nil || rel[0].Seq < c.EndSeq {
				t.Errorf("stuck orders/%d: Release calls %v at %v, want one at %v after Run returned", id, rel, got, want)
			}
			if n.PublishedShard("orders", id) != nil {
				t.Errorf("stuck orders/%d still published after Run returned", id)
			}
		}

		// The machine marks the runs stuck RenewDeadline after their StopRun, at M+10s.
		recs, _ := log.records()
		for _, id := range moved {
			var stuckAt time.Duration = -1
			for _, rec := range recs {
				if rec.After.Spaces[0].Shards[id].Stuck {
					stuckAt = rec.Now
					break
				}
			}
			if want := 11 * time.Second; stuckAt != want {
				t.Errorf("orders/%d first stuck at %v, want %v", id, stuckAt, want)
			}
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN41RunPanics checks that a panic in Run is not recovered: the child process, whose Run
// panics, crashes with Run's panic on stderr. stderr is matched by substring, since a coverage
// build adds lines of its own.
func TestATRUN41RunPanics(t *testing.T) {
	if os.Getenv("TENURE_RUN41_CHILD") == "1" {
		t.Skip("in the child process")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestATRUN41Child$") //nolint:gosec // the test binary itself
	cmd.Env = append(os.Environ(), "TENURE_RUN41_CHILD=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("child exited with %v, want a non-zero exit; stderr:\n%s", err, stderr.String())
	}
	if !strings.Contains(stderr.String(), "panic: kaboom") {
		t.Errorf("child's stderr does not contain %q:\n%s", "panic: kaboom", stderr.String())
	}
}

// TestATRUN41Child is TestATRUN41RunPanics's child process: a node whose Run panics. It runs only
// with TENURE_RUN41_CHILD=1.
func TestATRUN41Child(t *testing.T) {
	if os.Getenv("TENURE_RUN41_CHILD") != "1" {
		t.Skip("runs only as TestATRUN41RunPanics's child process")
	}
	synctest.Test(t, func(t *testing.T) {
		r := newShardRig(t)
		r.runs.Script(func(context.Context, *tenure.Shard, int) error { panic("kaboom") })
		n := start(t, context.Background(), r.config())
		time.Sleep(time.Second)
		closeNode(t, n)
		t.Error("Run's panic did not crash the process")
	})
}

// TestATRUN71DrainHandsOver checks a handover between two nodes: n2 joins and takes its share,
// then n1 drains and n2 takes the rest. Each shard has one Run at a time across both nodes, n1's
// runs end with "…: draining", and n1 releases every shard it ran.
func TestATRUN71DrainHandsOver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ms := memstore.New(nil)
		g := newGroup(t)
		fs1, fs2 := newRecStore(ms), newRecStore(ms)
		var runs1, runs2 runLog
		cfg := func(id string, m tenure.Membership, fs *recStore, runs *runLog) tenure.Config {
			return tenure.Config{
				Self:       tenure.Member{ID: id, Addr: "a-" + id},
				Membership: m,
				Store:      fs,
				Spaces:     []tenure.Space{orders(runs.Run)},
				Rand:       countingRand(),
			}
		}
		t0 := time.Now()
		n1 := start(t, context.Background(), cfg("n1", g.Absent("n1"), fs1, &runs1))
		sleepUntil(t0.Add(time.Second))
		n2 := start(t, context.Background(), cfg("n2", g.Absent("n2"), fs2, &runs2))
		sleepUntil(t0.Add(10 * time.Second))

		share := bal("orders", 8, "n1", "n2")
		running := func(l *runLog) []int {
			var ids []int
			for _, c := range l.Calls() {
				if !c.Done {
					ids = append(ids, c.Shard.ID)
				}
			}
			slices.Sort(ids)
			return ids
		}
		if got := running(&runs1); !slices.Equal(got, share["n1"]) {
			t.Errorf("n1 runs %v after n2 joined, want %v", got, share["n1"])
		}
		if got := running(&runs2); !slices.Equal(got, share["n2"]) {
			t.Errorf("n2 runs %v after it joined, want %v", got, share["n2"])
		}

		if err := n1.Drain(context.Background()); err != nil {
			t.Fatalf("n1.Drain() = %v, want nil", err)
		}
		drained := time.Now()
		sleepUntil(drained.Add(5 * time.Second))
		if got := running(&runs1); len(got) != 0 {
			t.Errorf("n1 still runs %v after its drain", got)
		}
		if got := running(&runs2); !slices.Equal(got, allShards) {
			t.Errorf("n2 runs %v after n1 drained, want every shard", got)
		}
		for _, c := range runs1.Calls() {
			want := "draining"
			if slices.Contains(share["n2"], c.Shard.ID) {
				want = "moved"
			}
			if c.Cause == nil || c.Cause.Error() != stopText(c.Shard, want) {
				t.Errorf("n1's %v, want cause %q", c, stopText(c.Shard, want))
			}
			rel := shardCalls(fs1.Calls("Release"), c.Shard.ID)
			if len(rel) != 1 || rel[0].Err != nil || rel[0].Args[3] != c.Shard.Term {
				t.Errorf("n1 orders/%d: Release calls %v, want one at term %d", c.Shard.ID, rel, c.Shard.Term)
			}
		}
		for _, id := range allShards {
			all := slices.Concat(runs1.Of("orders", id), runs2.Of("orders", id))
			slices.SortFunc(all, func(a, b runCall) int { return int(a.Seq) - int(b.Seq) })
			for i := 1; i < len(all); i++ {
				if !all[i-1].Done || all[i-1].EndSeq > all[i].Seq {
					t.Errorf("orders/%d: %v started before %v returned", id, all[i], all[i-1])
				}
				if all[i].Shard.Term <= all[i-1].Shard.Term {
					t.Errorf("orders/%d: %v does not have a later term than %v", id, all[i], all[i-1])
				}
			}
		}
		if cs := fs1.Calls("CloseSession"); len(cs) != 1 || cs[0].Err != nil {
			t.Errorf("n1 CloseSession calls = %v, want one", cs)
		}
		closeNode(t, n1)
		closeNode(t, n2)
		check()
	})
}
