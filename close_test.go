// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of Drain and Close bounded by their contexts: Drain's deadline error, Close's
// sequence and ErrClosed afterwards, and the forced Close of a ctx that ends first, which neither
// closes the session nor leaves. Steps carrying an AT-RUN number were written from the issue's
// text.

// stuckRun makes r's Run of orders/3 ignore its ctx and return nil once free is closed; every
// other Run blocks until its ctx is done, as the runLog's default does.
func stuckRun(r shardRig, free <-chan struct{}) {
	r.runs.Script(func(ctx context.Context, sh *tenure.Shard, _ int) error {
		if sh.ID == 3 {
			<-free
			return nil
		}
		<-ctx.Done()
		return context.Cause(ctx)
	})
}

// wantErrText fails t unless err's text is want and err matches target.
func wantErrText(t *testing.T, what string, err error, want string, target error) {
	t.Helper()
	if err == nil || err.Error() != want || !errors.Is(err, target) {
		t.Errorf("%s = %v, want %q matching %v", what, err, want, target)
	}
}

// wantAllClosed fails t unless every method RUN-540 names returns ErrClosed on n, and the Resolver
// res that n returned before Close looks keys up no more.
func wantAllClosed(t *testing.T, n *tenure.Node, res *tenure.Resolver) {
	t.Helper()
	if _, err := n.Route("orders", keyOf(3)); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Route() after Close = %v, want ErrClosed", err)
	}
	if _, err := n.Local("orders", keyOf(3)); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Local() after Close = %v, want ErrClosed", err)
	}
	if err := n.Drain(context.Background()); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Drain() after Close = %v, want ErrClosed", err)
	}
	if err := n.SetSlots(context.Background(), 2); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("SetSlots() after Close = %v, want ErrClosed", err)
	}
	if _, err := res.Lookup("k"); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("node resolver's Lookup() after Close = %v, want ErrClosed", err)
	}
	if _, err := n.Resolver("").Lookup("k"); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Lookup() of a Resolver made after Close = %v, want ErrClosed", err)
	}
	if err := n.Close(context.Background()); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("second Close() = %v, want ErrClosed", err)
	}
}

// wantStopErrors fails t unless every Shard that l's Run got returns the stop error of reason
// from Check.
func wantStopErrors(t *testing.T, l *runLog, reason string) {
	t.Helper()
	for _, c := range l.Calls() {
		err := c.Shard.Check()
		if err == nil || err.Error() != stopText(c.Shard, reason) || !errors.Is(err, tenure.ErrNotOwner) {
			t.Errorf("%v: Check() = %v, want %q", c, err, stopText(c.Shard, reason))
		}
	}
}

// TestATRUN72DrainDeadline checks Drain's deadline error: 5 s after the call, it names the shards
// not released yet, or says that the session is not closed, and wraps ctx's error; the drain goes
// on, and a later Drain returns nil once it completes.
func TestATRUN72DrainDeadline(t *testing.T) {
	const timeout = 5 * time.Second
	tests := []struct {
		name string
		// block makes the drain stall and returns what lets it go on.
		block func(r shardRig) (free func())
		want  string
	}{{
		name: "run",
		block: func(r shardRig) func() {
			free := make(chan struct{})
			stuckRun(r, free)
			return func() { close(free) }
		},
		want: "tenure: drain: context deadline exceeded (not released: orders/3)",
	}, {
		// The Run of orders/3 has returned and the shard is no longer published, but its
		// Release is still in flight.
		name: "release",
		block: func(r shardRig) func() {
			free := make(chan struct{})
			r.fs.Answer("Release", func(ctx context.Context, args []any) ([]any, error) {
				if argAt[int](args, 2) == 3 {
					<-free
					ctx = context.WithoutCancel(ctx)
				}
				return r.fs.Forward(ctx, "Release", args)
			})
			return func() { close(free) }
		},
		want: "tenure: drain: context deadline exceeded (not released: orders/3)",
	}, {
		name: "session",
		block: func(r shardRig) func() {
			return r.fs.Block("CloseSession")
		},
		want: "tenure: drain: context deadline exceeded (session not closed)",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				r := newShardRig(t)
				free := tt.block(r)
				t0 := time.Now()
				n := start(t, context.Background(), r.config())
				synctest.Wait()
				if got := running(r.runs); !slices.Equal(got, allShards) {
					t.Fatalf("runs %v, want every shard", got)
				}

				sleepUntil(t0.Add(time.Second))
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				err := n.Drain(ctx)
				wantErrText(t, "Drain(5s ctx)", err, tt.want, context.DeadlineExceeded)
				if d := time.Since(t0); d != time.Second+timeout {
					t.Errorf("Drain(5s ctx) returned at T0+%v, want T0+6s", d)
				}

				free()
				if err := n.Drain(context.Background()); err != nil {
					t.Errorf("later Drain() = %v, want nil", err)
				}
				if cs := r.fs.Calls("CloseSession"); len(cs) == 0 || cs[len(cs)-1].Err != nil {
					t.Errorf("CloseSession calls = %v, want the last to succeed", cs)
				}
				closeNode(t, n)
				check()
			})
		})
	}
}

// TestATRUN73CloseSequence closes a member that holds a slot with a ctx that does not end: Close
// releases every shard, then the slot once the last Release has returned, then closes the
// session once ReleaseSlot has, as AT-RUN-70 drains, and then leaves once CloseSession has.
// Afterwards Route, Local, Drain, SetSlots, a node resolver's Lookup and a second Close return
// ErrClosed, and the Shards that Run got keep their stop error.
func TestATRUN73CloseSequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		t0 := time.Now()
		n := start(t, context.Background(), r.slotConfig(1))
		synctest.Wait()
		if got := running(r.runs); !slices.Equal(got, allShards) {
			t.Fatalf("runs %v, want every shard on slot 0", got)
		}
		res := n.Resolver("")

		sleepUntil(t0.Add(time.Second))
		r.fs.Delay("Release", 500*time.Millisecond)
		r.fs.Delay("ReleaseSlot", 500*time.Millisecond)
		r.fs.Delay("CloseSession", 500*time.Millisecond)
		closeNode(t, n)
		if d := time.Since(t0); d != 2500*time.Millisecond {
			t.Errorf("Close returned at T0+%v, want T0+2.5s", d)
		}

		calls := callsInOrder(r.fs.Calls("Release", "ReleaseSlot", "CloseSession"), r.sm.Calls("Leave"))
		want := []string{"Release", "Release", "Release", "Release", "Release", "Release", "Release",
			"Release", "ReleaseSlot", "CloseSession", "Leave"}
		if got := callMethods(calls); !slices.Equal(got, want) {
			t.Fatalf("calls %v, want %v", got, want)
		}
		// Each step starts only once every call of the step before has returned.
		var ended uint64
		for i, c := range calls {
			if c.Err != nil || !c.Done {
				t.Errorf("%v, want it to have succeeded", c)
			}
			if i >= 8 && c.Seq < ended {
				t.Errorf("%v started before %v returned", c, calls[i-1])
			}
			ended = max(ended, c.EndSeq)
		}
		if got := startsAt(calls[7:], t0); !slices.Equal(got, secs(1, 1.5, 2, 2.5)) {
			t.Errorf("last Release, ReleaseSlot, CloseSession and Leave started at %v, want T0+%v", got, secs(1, 1.5, 2, 2.5))
		}

		wantAllClosed(t, n, res)
		wantStopErrors(t, r.runs, "draining")
		check()
	})
}

// TestATRUN74ForcedClose closes a member whose Run of orders/3 ignores its ctx, with a ctx that
// ends first: Close returns 5 s after the call with an error that names orders/3, without
// closing the session or leaving, and no call of the store or the membership starts or is still
// out once Close has returned. The supervisor of orders/3 outlives Close until its Run returns.
func TestATRUN74ForcedClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		free := make(chan struct{})
		stuckRun(r, free)
		t0 := time.Now()
		n := start(t, context.Background(), r.config())
		synctest.Wait()
		if got := running(r.runs); !slices.Equal(got, allShards) {
			t.Fatalf("runs %v, want every shard", got)
		}
		res := n.Resolver("")

		sleepUntil(t0.Add(time.Second))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := n.Close(ctx)
		closed := time.Now()
		wantErrText(t, "Close(5s ctx)", err, "tenure: close: context deadline exceeded (not released: orders/3)",
			context.DeadlineExceeded)
		if d := closed.Sub(t0); d != 6*time.Second {
			t.Errorf("Close(5s ctx) returned at T0+%v, want T0+6s", d)
		}

		stores, members := len(r.fs.Calls()), len(r.sm.Calls())
		for _, c := range callsInOrder(r.fs.Calls(), r.sm.Calls()) {
			if !c.Done || c.End.After(closed) {
				t.Errorf("%v still out when Close returned at T0+%v", c, closed.Sub(t0))
			}
		}
		if cs := r.fs.Calls("CloseSession"); len(cs) != 0 {
			t.Errorf("CloseSession calls = %v, want none", cs)
		}
		if ls := r.sm.Calls("Leave"); len(ls) != 0 {
			t.Errorf("Leave calls = %v, want none", ls)
		}
		hs, err := r.ms.Holdings(context.Background(), "orders")
		if err != nil {
			t.Fatalf("Holdings(orders) = %v", err)
		}
		for _, h := range hs {
			if want := h.Shard == 3; (h.Holder == holder01) != want {
				t.Errorf("after Close, %+v; want only orders/3 still held by %s", h, holder01)
			}
		}

		wantAllClosed(t, n, res)
		wantStopErrors(t, r.runs, "draining")
		if c := r.runs.Of("orders", 3); len(c) != 1 || c[0].Done {
			t.Fatalf("orders/3 runs %v, want one still running", c)
		}

		time.Sleep(10 * time.Second)
		close(free)
		synctest.Wait()
		time.Sleep(10 * time.Second)
		if c := r.runs.Of("orders", 3); len(c) != 1 || !c[0].Done {
			t.Errorf("orders/3 runs %v after free, want one that returned", c)
		}
		if got := len(r.fs.Calls()); got != stores {
			t.Errorf("store calls after Close: %v", r.fs.Calls()[stores:])
		}
		if got := len(r.sm.Calls()); got != members {
			t.Errorf("membership calls after Close: %v", r.sm.Calls()[members:])
		}
		check()
	})
}

// TestCloseCanceledContext closes a node with a ctx that is done already: Close skips the drain,
// so it neither marks the member as draining nor releases a shard, and every Run is canceled with
// the stop error "closed", which Check returns from then on, in a space with fencing and in one
// without.
func TestCloseCanceledContext(t *testing.T) {
	for _, tt := range []struct {
		name   string
		fenced bool
	}{{"unfenced", false}, {"fenced", true}} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				r := newShardRig(t)
				cfg := r.config()
				cfg.Spaces[0].Fenced = tt.fenced
				n := start(t, context.Background(), cfg)
				synctest.Wait()
				if got := running(r.runs); !slices.Equal(got, allShards) {
					t.Fatalf("runs %v, want every shard", got)
				}

				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				at := time.Now()
				err := n.Close(ctx)
				wantErrText(t, "Close(canceled ctx)", err, "tenure: close: context canceled (not released: orders/0, "+
					"orders/1, orders/2, orders/3, orders/4, orders/5, orders/6, orders/7)", context.Canceled)
				if d := time.Since(at); d != 0 {
					t.Errorf("Close(canceled ctx) took %v, want 0", d)
				}
				for _, m := range []string{"SetDraining", "Leave"} {
					if cs := r.sm.Calls(m); len(cs) != 0 {
						t.Errorf("%s calls = %v, want none", m, cs)
					}
				}
				for _, m := range []string{"Release", "CloseSession"} {
					if cs := r.fs.Calls(m); len(cs) != 0 {
						t.Errorf("%s calls = %v, want none", m, cs)
					}
				}

				synctest.Wait()
				calls := r.runs.Calls()
				if len(calls) != len(allShards) {
					t.Errorf("runs %v, want one per shard", calls)
				}
				for _, c := range calls {
					if !c.Done || c.Err != c.Cause {
						t.Errorf("%v, want it to have returned its cause", c)
					}
					wantStopped(t, c, at, "closed")
				}
				wantStopErrors(t, r.runs, "closed")
				wantAllClosed(t, n, n.Resolver(""))
				check()
			})
		})
	}
}

// TestCloseCanceledContextDrain checks Drain around a Close whose ctx is done at the call. A Drain
// during such a Close, which skips the drain, returns ErrClosed and starts no drain; a drain that
// a Drain started before is cut off by it, and that Drain returns ErrClosed; a drain that has
// completed before lets it take the normal path, which leaves the group.
func TestCloseCanceledContextDrain(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	all := "orders/0, orders/1, orders/2, orders/3, orders/4, orders/5, orders/6, orders/7"

	t.Run("during", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			n := start(t, context.Background(), r.config())
			synctest.Wait()
			// A renewal that ignores its ctx holds Close up once it has stopped the node.
			release := r.fs.Block("KeepAlive")
			time.Sleep(core.DefaultTiming.RetryPeriod + time.Second)
			if ka := r.fs.Calls("KeepAlive"); len(ka) < 2 || ka[len(ka)-1].Done {
				t.Fatalf("KeepAlive calls %v, want the last in flight", ka)
			}

			errc := make(chan error, 1)
			go func() { errc <- n.Close(canceled) }()
			synctest.Wait()
			select {
			case err := <-errc:
				t.Fatalf("Close(canceled ctx) = %v before the renewal returned", err)
			default:
			}
			if err := n.Drain(context.Background()); !errors.Is(err, tenure.ErrClosed) {
				t.Errorf("Drain() during Close = %v, want ErrClosed", err)
			}
			if err := n.Drain(canceled); !errors.Is(err, tenure.ErrClosed) {
				t.Errorf("Drain(canceled ctx) during Close = %v, want ErrClosed", err)
			}
			synctest.Wait()
			if cs := r.sm.Calls("SetDraining"); len(cs) != 0 {
				t.Errorf("SetDraining calls = %v, want none", cs)
			}

			release()
			wantErrText(t, "Close(canceled ctx)", <-errc,
				"tenure: close: context canceled (not released: "+all+")", context.Canceled)
			for _, m := range []string{"Release", "CloseSession"} {
				if cs := r.fs.Calls(m); len(cs) != 0 {
					t.Errorf("%s calls = %v, want none", m, cs)
				}
			}
			if ls := r.sm.Calls("Leave"); len(ls) != 0 {
				t.Errorf("Leave calls = %v, want none", ls)
			}
			wantStopErrors(t, r.runs, "closed")
			wantAllClosed(t, n, n.Resolver(""))
			check()
		})
	})

	t.Run("before", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			free := make(chan struct{})
			stuckRun(r, free)
			n := start(t, context.Background(), r.config())
			synctest.Wait()

			drained := make(chan error, 1)
			go func() { drained <- n.Drain(context.Background()) }()
			time.Sleep(time.Second)
			if got := running(r.runs); !slices.Equal(got, []int{3}) {
				t.Fatalf("runs %v during the drain, want orders/3 only", got)
			}
			wantErrText(t, "Close(canceled ctx)", n.Close(canceled),
				"tenure: close: context canceled (not released: orders/3)", context.Canceled)
			if err := <-drained; !errors.Is(err, tenure.ErrClosed) {
				t.Errorf("Drain() cut off by Close = %v, want ErrClosed", err)
			}
			if cs := r.fs.Calls("CloseSession"); len(cs) != 0 {
				t.Errorf("CloseSession calls = %v, want none", cs)
			}
			if ls := r.sm.Calls("Leave"); len(ls) != 0 {
				t.Errorf("Leave calls = %v, want none", ls)
			}
			wantStopErrors(t, r.runs, "draining")
			wantAllClosed(t, n, n.Resolver(""))

			close(free)
			check()
		})
	})

	t.Run("completed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			n := start(t, context.Background(), r.config())
			synctest.Wait()
			if err := n.Drain(context.Background()); err != nil {
				t.Fatalf("Drain() = %v, want nil", err)
			}
			if err := n.Close(canceled); err != nil {
				t.Errorf("Close(canceled ctx) after a completed drain = %v, want nil", err)
			}
			if ls := r.sm.Calls("Leave"); len(ls) != 1 {
				t.Errorf("Leave calls = %v, want one", ls)
			}
			wantStopErrors(t, r.runs, "draining")
			wantAllClosed(t, n, n.Resolver(""))
			check()
		})
	})
}
