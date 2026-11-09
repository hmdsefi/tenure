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
	"github.com/hmdsefi/tenure/memstore"
)

// The tests of holder observations: the store watch, holdings resyncs, the freshness filter on
// the way to the machine, and fenced spaces that run through an outage. Steps carrying an AT-RUN
// number were written from the issue's text.

// errBoom is the error a scripted call fails with.
var errBoom = errors.New("boom")

// holderRig is member n1's doubles for the holder tests: fs over a memstore, a group that lists
// no member, sm for n1, and the runLog that serves every space.
type holderRig struct {
	ms   *memstore.Store
	fs   *recStore
	sm   *recMembership
	runs *runLog
}

// newHolderRig returns a holderRig. n1 is alone in its views, so it owns every shard.
func newHolderRig(t *testing.T) holderRig {
	t.Helper()
	ms := memstore.New(nil)
	g := newGroup(t)
	return holderRig{ms: ms, fs: newRecStore(ms), sm: g.Absent("n1"), runs: &runLog{}}
}

// config returns the Config of member n1 over r's doubles with store, the counting Rand and one
// space per name in names, each of 2 shards, fenced when its name starts with "f".
func (r holderRig) config(store tenure.Store, names ...string) tenure.Config {
	spaces := make([]tenure.Space, len(names))
	for i, name := range names {
		spaces[i] = tenure.Space{Name: name, Shards: 2, Fenced: name[0] == 'f', Run: r.runs.Run}
	}
	return tenure.Config{
		Self:       tenure.Member{ID: "n1", Addr: "a1"},
		Membership: r.sm,
		Store:      store,
		Spaces:     spaces,
		Rand:       countingRand(),
	}
}

// onShard returns the calls among calls whose arguments name shard id of space, in order. The
// calls are of methods whose arguments are holder, space and shard.
func onShard(calls []call, space string, id int) []call {
	var out []call
	for _, c := range calls {
		if argAt[string](c.Args, 1) == space && argAt[int](c.Args, 2) == id {
			out = append(out, c)
		}
	}
	return out
}

// holdingsOf returns the Holdings calls among calls for space, in order.
func holdingsOf(calls []call, space string) []call {
	var out []call
	for _, c := range calls {
		if c.Method == "Holdings" && argAt[string](c.Args, 0) == space {
			out = append(out, c)
		}
	}
	return out
}

// stepAt is one step of a stepLog that a test looks for: its reading and input, and the actions
// it returned.
type stepAt struct {
	Now     time.Duration
	Input   core.Input
	Actions []core.Action
}

// holderSteps returns the steps of recs that stepped HolderChanged for shard id of space f, with
// Now cleared from the input.
func holderSteps(recs []tenure.StepRecord, id int) []stepAt {
	var out []stepAt
	for _, r := range recs {
		if hc, ok := r.Input.(core.HolderChanged); ok && hc.Space == "f" && hc.Shard == id {
			hc.Now = 0
			out = append(out, stepAt{Now: r.Now, Input: hc, Actions: r.Actions})
		}
	}
	return out
}

// actionsAt returns the actions of type T that recs returned, with the reading of their step.
func actionsAt[T core.Action](recs []tenure.StepRecord) []stepAt {
	var out []stepAt
	for _, r := range recs {
		for _, a := range r.Actions {
			if _, ok := a.(T); ok {
				out = append(out, stepAt{Now: r.Now, Actions: []core.Action{a}})
			}
		}
	}
	return out
}

// only returns the one action of each step, in order.
func only(steps []stepAt) []core.Action {
	out := make([]core.Action, len(steps))
	for i, s := range steps {
		out[i] = s.Actions[0]
	}
	return out
}

// readings returns the Now of each step, in order.
func readings(steps []stepAt) []time.Duration {
	out := make([]time.Duration, len(steps))
	for i, s := range steps {
		out[i] = s.Now
	}
	return out
}

// hasAction reports whether acts holds a.
func hasAction(acts []core.Action, a core.Action) bool {
	return slices.Contains(acts, a)
}

// termOf returns the term the first successful call among acquires, Acquire calls, got for shard
// id of space f, failing t when there is none.
func termOf(t *testing.T, acquires []call, id int) core.Term {
	t.Helper()
	for _, c := range onShard(acquires, "f", id) {
		if c.Done && c.Err == nil {
			return resultAt[core.Term](c.Result, 0)
		}
	}
	t.Fatalf("no successful Acquire of f/%d in %v", id, acquires)
	return 0
}

// wantRunning fails t unless every call in calls is still running with its ctx not done.
func wantRunning(t *testing.T, calls []runCall) {
	t.Helper()
	for _, c := range calls {
		if c.Done || !c.CtxDone.IsZero() || c.Shard.Check() != nil {
			t.Errorf("%v: ctx done at %v, Check() = %v, want running", c, c.CtxDone, c.Shard.Check())
		}
	}
}

// TestATRUN17HoldingsResync checks the resyncs of RUN-170: (a) the first renewal after failed
// ones reads the holdings of every space, by name; (b) a failed Holdings is retried every
// RetryPeriod; (c) a shard held by another holder is acquired at the instant the watch shows its
// release; (d) a closed store watch starts again RetryPeriod later, at T0+32s, and a failed
// restart RetryPeriod after that, each followed by a resync.
func TestATRUN17HoldingsResync(t *testing.T) {
	t.Run("a", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newHolderRig(t)
			t0 := time.Now()
			n := start(t, context.Background(), r.config(r.fs, "b", "a"))
			r.fs.Fail("KeepAlive", errBoom)
			sleepUntil(t0.Add(5 * time.Second))
			if h := r.fs.Calls("Holdings"); len(h) != 0 {
				t.Errorf("Holdings calls during the outage: %v", h)
			}
			r.fs.Fail("KeepAlive", nil)
			sleepUntil(t0.Add(20 * time.Second))

			ka := r.fs.Calls("KeepAlive")
			if got, want := startsAt(ka, t0)[:5], secs(0, 2, 4, 6, 8); !slices.Equal(got, want) {
				t.Errorf("KeepAlive calls at %v, want %v first", got, want)
			}
			h := r.fs.Calls("Holdings")
			if got, want := callArgs(h), []string{"a", "b"}; !slices.Equal(got, want) {
				t.Fatalf("Holdings calls %v, want a then b", h)
			}
			if got, want := startsAt(h, t0), secs(6, 6); !slices.Equal(got, want) {
				t.Errorf("Holdings calls at %v, want %v", got, want)
			}
			if h[1].Seq < h[0].EndSeq {
				t.Errorf("Holdings(b) started before Holdings(a) returned: %v", h)
			}
			for _, c := range h {
				if c.Err != nil {
					t.Errorf("%v, want nil", c)
				}
			}
			wantDeadlines(t, h)
			closeNode(t, n)
			check()
		})
	})

	t.Run("b", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newHolderRig(t)
			t0 := time.Now()
			n := start(t, context.Background(), r.config(r.fs, "b", "a"))
			r.fs.Fail("KeepAlive", errBoom)
			r.fs.Fail("Holdings", errBoom)
			sleepUntil(t0.Add(5 * time.Second))
			r.fs.Fail("KeepAlive", nil)
			sleepUntil(t0.Add(9 * time.Second))
			r.fs.Fail("Holdings", nil)
			sleepUntil(t0.Add(20 * time.Second))

			h := r.fs.Calls("Holdings")
			if got, want := startsAt(holdingsOf(h, "a"), t0), secs(6, 8, 10); !slices.Equal(got, want) {
				t.Errorf("Holdings(a) calls at %v, want %v", got, want)
			}
			if got, want := startsAt(holdingsOf(h, "b"), t0), secs(10); !slices.Equal(got, want) {
				t.Errorf("Holdings(b) calls at %v, want %v", got, want)
			}
			for i, c := range holdingsOf(h, "a") {
				if want := i < 2; (c.Err != nil) != want {
					t.Errorf("%v: failed %t, want %t", c, c.Err != nil, want)
				}
			}
			wantDeadlines(t, h)
			closeNode(t, n)
			check()
		})
	})

	t.Run("c", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newHolderRig(t)
			o := newOutside(t, r.ms, core.DefaultTiming)
			if _, err := o.Acquire("a", 0); err != nil {
				t.Fatalf("outside Acquire(a/0) = %v", err)
			}
			t0 := time.Now()
			n := start(t, context.Background(), r.config(r.fs.Watching(), "a"))
			sleepUntil(t0.Add(3 * time.Second))
			if err := o.Release("a", 0); err != nil {
				t.Fatalf("outside Release(a/0) = %v", err)
			}
			synctest.Wait()

			if got, want := startsAt(r.fs.Calls("WatchHolders"), t0), secs(0); !slices.Equal(got, want) {
				t.Errorf("WatchHolders calls at %v, want %v", got, want)
			}
			acq := onShard(r.fs.Calls("Acquire"), "a", 0)
			if got, want := startsAt(acq, t0), secs(0, 2, 3); !slices.Equal(got, want) {
				t.Errorf("Acquire(a/0) calls at %v, want %v", got, want)
			}
			for i, c := range acq {
				var held *tenure.HeldError
				if want := i < 2; errors.As(c.Err, &held) != want {
					t.Errorf("%v: held %t, want %t", c, !want, want)
				}
			}
			if got, want := runStarts(r.runs.Of("a", 0), t0), secs(3); !slices.Equal(got, want) {
				t.Errorf("a/0 runs start at %v, want %v", got, want)
			}
			if h := r.fs.Calls("Holdings"); len(h) != 0 {
				t.Errorf("Holdings calls %v, want none", h)
			}
			o.Stop()
			closeNode(t, n)
			check()
		})
	})

	t.Run("d", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newHolderRig(t)
			st := r.fs.Injected()
			t0 := time.Now()
			n := start(t, context.Background(), r.config(st, "b", "a"))
			sleepUntil(t0.Add(30 * time.Second))
			if h := r.fs.Calls("Holdings"); len(h) != 0 {
				t.Errorf("Holdings calls before the watch closed: %v", h)
			}
			st.CloseWatch()
			sleepUntil(t0.Add(40 * time.Second))
			r.fs.Fail("WatchHolders", errBoom)
			st.CloseWatch()
			sleepUntil(t0.Add(43 * time.Second))
			r.fs.Fail("WatchHolders", nil)
			sleepUntil(t0.Add(50 * time.Second))

			w := r.fs.Calls("WatchHolders")
			if got, want := startsAt(w, t0), secs(0, 32, 42, 44); !slices.Equal(got, want) {
				t.Fatalf("WatchHolders calls at %v, want %v", got, want)
			}
			if w[2].Err == nil || w[3].Err != nil {
				t.Errorf("WatchHolders calls %v, want the one at T0+42s to fail", w)
			}
			h := r.fs.Calls("Holdings")
			if got, want := callArgs(h), []string{"a", "b", "a", "b"}; !slices.Equal(got, want) {
				t.Fatalf("Holdings calls %v, want a, b twice", h)
			}
			if got, want := startsAt(h, t0), secs(32, 32, 44, 44); !slices.Equal(got, want) {
				t.Errorf("Holdings calls at %v, want %v", got, want)
			}
			if h[0].Seq < w[1].EndSeq || h[2].Seq < w[3].EndSeq {
				t.Errorf("a resync started before its watch: watch %v, holdings %v", w, h)
			}
			closeNode(t, n)
			check()
		})
	})
}

// callArgs returns the first argument of each call, a string.
func callArgs(calls []call) []string {
	out := make([]string, len(calls))
	for i, c := range calls {
		out[i] = argAt[string](c.Args, 0)
	}
	return out
}

// TestATRUN18RecordFreshness checks, through the step hook, which records of the store watch,
// Confirm answers and Holdings answers reach the machine (RUN-175): (a) a record at or below the
// version already stepped does not; (b) a record older than the shard's grant does not; (c) while
// the shard is degraded, a record naming n1 at the run's term is held back until the latest
// Confirm call went out, and the Confirm answers restore the shards; (d) a shard missing from a
// Holdings answer steps no holder only when the read started after the last record stepped.
func TestATRUN18RecordFreshness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newHolderRig(t)
		st := r.fs.Injected()
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.config(st, "f"))
		synctest.Wait()
		term0, term1 := termOf(t, r.fs.Calls("Acquire"), 0), termOf(t, r.fs.Calls("Acquire"), 1)
		us := n.Holder()

		// (a) Versions: the first record steps, an equal or lower version does not.
		sleepUntil(t0.Add(time.Second))
		st.Inject(tenure.Holding{Space: "f", Shard: 0, Holder: us, Term: term0, Version: 50})
		st.Inject(tenure.Holding{Space: "f", Shard: 0, Holder: "x/1", Term: term0 + 1, Version: 50})
		st.Inject(tenure.Holding{Space: "f", Shard: 0, Holder: "x/1", Term: term0 + 1, Version: 49})
		synctest.Wait()
		recs, _ := log.records()
		want0 := []stepAt{{Now: time.Second, Input: core.HolderChanged{Space: "f", Shard: 0, Holder: us, Term: term0}}}
		if got := holderSteps(recs, 0); !equalSteps(got, want0) {
			t.Errorf("(a) f/0 HolderChanged steps %+v, want %+v", got, want0)
		}

		// (b) The grant: records with an older term do not step.
		sleepUntil(t0.Add(2 * time.Second))
		st.Inject(tenure.Holding{Space: "f", Shard: 1, Holder: "x/1", Term: term1 - 1, Version: 60})
		st.Inject(tenure.Holding{Space: "f", Shard: 1, Term: term1 - 1, Version: 61})
		synctest.Wait()
		recs, _ = log.records()
		if got := holderSteps(recs, 1); len(got) != 0 {
			t.Errorf("(b) f/1 HolderChanged steps %+v, want none", got)
		}
		wantRunning(t, r.runs.Calls())
		// The table shows the records stepped, not the ones dropped.
		wantRows(t, n, rowWant{"f", 0, "owned", us, term0}, rowWant{"f", 1, "owned", us, term1})

		// (c) The Confirm hold-back. Renewals fail from T0+4s, so both shards degrade at T0+12s.
		sleepUntil(t0.Add(3 * time.Second))
		r.fs.Fail("KeepAlive", errBoom)
		sleepUntil(t0.Add(13 * time.Second))
		wantRows(t, n, rowWant{"f", 0, "degraded", us, term0}, rowWant{"f", 1, "degraded", us, term1})
		if tab := n.Table(); tab.SessionLive {
			t.Error("(c) Table().SessionLive = true at T0+13s, want false")
		}
		st.Inject(tenure.Holding{Space: "f", Shard: 0, Holder: us, Term: term0, Version: 70})
		r.fs.Fail("KeepAlive", nil)
		r.fs.Delay("Confirm", time.Second)
		sleepUntil(t0.Add(14 * time.Second))
		synctest.Wait() // the Confirm calls are out
		st.Inject(tenure.Holding{Space: "f", Shard: 0, Holder: us, Term: term0, Version: 71})
		sleepUntil(t0.Add(15 * time.Second))
		synctest.Wait()

		recs, _ = log.records()
		degrade := actionsAt[core.Degrade](recs)
		if got, want := readings(degrade), secs(12, 12); !slices.Equal(got, want) {
			t.Errorf("(c) Degrade at %v, want %v", got, want)
		}
		conf := r.fs.Calls("Confirm")
		if got, want := startsAt(conf, t0), secs(14, 14); !slices.Equal(got, want) {
			t.Errorf("(c) Confirm calls at %v, want %v", got, want)
		}
		restore0 := core.Restore{Space: "f", Shard: 0, Term: term0}
		got0 := holderSteps(recs, 0)
		if len(got0) != 2 || got0[1].Now != 14*time.Second || !hasAction(got0[1].Actions, restore0) {
			t.Errorf("(c) f/0 HolderChanged steps %+v, want the T0+1s one and one at T0+14s with %+v", got0, restore0)
		}
		restore1 := core.Restore{Space: "f", Shard: 1, Term: term1}
		got1 := holderSteps(recs, 1)
		if len(got1) != 1 || got1[0].Now != 15*time.Second || !hasAction(got1[0].Actions, restore1) {
			t.Errorf("(c) f/1 HolderChanged steps %+v, want one at T0+15s with %+v", got1, restore1)
		}
		if got, want := only(actionsAt[core.Restore](recs)), []core.Action{restore0, restore1}; !slices.Equal(got, want) {
			t.Errorf("(c) Restore actions %v, want %v", got, want)
		}
		h := r.fs.Calls("Holdings")
		if got, want := startsAt(h, t0), secs(15); !slices.Equal(got, want) {
			t.Errorf("(c) Holdings calls at %v, want %v", got, want)
		}
		for _, c := range conf {
			if len(h) > 0 && h[0].Seq < c.EndSeq {
				t.Errorf("(c) %v started before %v returned", h[0], c)
			}
		}
		r.fs.Delay("Confirm", 0)
		wantRows(t, n, rowWant{"f", 0, "owned", us, term0}, rowWant{"f", 1, "owned", us, term1})
		if tab := n.Table(); !tab.SessionLive {
			t.Error("(c) Table().SessionLive = false at T0+15s, want true")
		}

		// (d) The absent-record rule: a Holdings read that started before the record of f/1 was
		// stepped cannot show it gone; a read after can.
		sleepUntil(t0.Add(20 * time.Second))
		r.fs.Delay("Holdings", time.Second)
		r.fs.Answer("Holdings", func(ctx context.Context, args []any) ([]any, error) {
			res, err := r.fs.Forward(ctx, "Holdings", args)
			hs := slices.DeleteFunc(resultAt[[]tenure.Holding](res, 0), func(h tenure.Holding) bool { return h.Shard == 1 })
			return []any{hs}, err
		})
		st.CloseWatch()
		sleepUntil(t0.Add(22 * time.Second))
		synctest.Wait() // Holdings(f) is out
		st.Inject(tenure.Holding{Space: "f", Shard: 1, Holder: us, Term: term1, Version: 80})
		sleepUntil(t0.Add(23 * time.Second))
		synctest.Wait()
		recs, _ = log.records()
		want1 := []stepAt{{Now: 22 * time.Second, Input: core.HolderChanged{Space: "f", Shard: 1, Holder: us, Term: term1}}}
		if got := holderSteps(recs, 1)[1:]; !equalSteps(got, want1) {
			t.Errorf("(d) f/1 HolderChanged steps after T0+15s %+v, want %+v", got, want1)
		}
		wantRunning(t, r.runs.Calls())

		r.fs.Delay("Holdings", 0)
		sleepUntil(t0.Add(30 * time.Second))
		st.CloseWatch()
		sleepUntil(t0.Add(32 * time.Second))
		synctest.Wait()
		recs, _ = log.records()
		lost := core.StopRun{Space: "f", Shard: 1, Term: term1, Reason: core.StopLost}
		got1 = holderSteps(recs, 1)
		if len(got1) != 3 || got1[2].Now != 32*time.Second || got1[2].Input != (core.HolderChanged{Space: "f", Shard: 1}) ||
			!hasAction(got1[2].Actions, lost) {
			t.Errorf("(d) f/1 HolderChanged steps %+v, want a last one at T0+32s with no holder and %+v", got1, lost)
		}
		if got := r.runs.Of("f", 1); len(got) == 0 {
			t.Error("(d) no run of f/1")
		} else {
			wantStopped(t, got[0], t0.Add(32*time.Second), "lost")
		}
		if got := holderSteps(recs, 0); len(got) != 2 {
			t.Errorf("f/0 HolderChanged steps %+v, want the two of (a) and (c)", got)
		}
		wantRunning(t, r.runs.Of("f", 0))
		wantRows(t, n, rowWant{"f", 0, "owned", us, term0})
		// f/1's record had no holder, so n1 took it again at once, in a new term.
		if row := rowOf(t, n.Table(), "f", 1); row.State != "owned" || row.Holder != us || row.Term <= term1 ||
			!row.Since.Equal(t0.Add(32*time.Second)) || !row.Running || row.Stop != "" {
			t.Errorf("(d) row f/1 = %+v, want owned again since T0+32s by %s in a term above %d", row, us, term1)
		}
		closeNode(t, n)
		check()
	})
}

// equalSteps reports whether got and want hold the same readings and inputs.
func equalSteps(got, want []stepAt) bool {
	return slices.EqualFunc(got, want, func(a, b stepAt) bool { return a.Now == b.Now && a.Input == b.Input })
}

// TestATRUN37Degrade checks a fenced and an unfenced space through an outage: at the session
// deadline the fenced runs degrade and keep running while the unfenced ones expire; at the first
// renewal after the outage the fenced shards are confirmed, restored by the answers, and only then
// is every space resynced, f before u. The fenced runs are never canceled.
func TestATRUN37Degrade(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newHolderRig(t)
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.config(r.fs, "u", "f"))
		r.fs.Fail("KeepAlive", errBoom)
		synctest.Wait()
		acq := r.fs.Calls("Acquire")
		f0, f1 := termOf(t, acq, 0), termOf(t, acq, 1)

		sleepUntil(t0.Add(10 * time.Second))
		synctest.Wait()
		recs, _ := log.records()
		wantDegrade := []core.Action{
			core.Degrade{Space: "f", Shard: 0, Term: f0},
			core.Degrade{Space: "f", Shard: 1, Term: f1},
		}
		degrade := actionsAt[core.Degrade](recs)
		if got := only(degrade); !slices.Equal(got, wantDegrade) || !slices.Equal(readings(degrade), secs(10, 10)) {
			t.Errorf("Degrade %v at %v, want %v at T0+10s", got, readings(degrade), wantDegrade)
		}
		for id := range 2 {
			wantStopped(t, r.runs.Of("u", id)[0], t0.Add(10*time.Second), "expired")
		}
		wantRunning(t, append(r.runs.Of("f", 0), r.runs.Of("f", 1)...))

		sleepUntil(t0.Add(11 * time.Second))
		r.fs.Fail("KeepAlive", nil)
		sleepUntil(t0.Add(20 * time.Second))
		recs, _ = log.records()

		conf := r.fs.Calls("Confirm")
		if got, want := startsAt(conf, t0), secs(12, 12); !slices.Equal(got, want) {
			t.Fatalf("Confirm calls %v at %v, want two at T0+12s", conf, got)
		}
		wantRestore := []core.Action{
			core.Restore{Space: "f", Shard: 0, Term: f0},
			core.Restore{Space: "f", Shard: 1, Term: f1},
		}
		restore := actionsAt[core.Restore](recs)
		if got := only(restore); !sameActions(got, wantRestore) || !slices.Equal(readings(restore), secs(12, 12)) {
			t.Errorf("Restore %v at %v, want %v at T0+12s", got, readings(restore), wantRestore)
		}
		h := r.fs.Calls("Holdings")
		if got, want := callArgs(h), []string{"f", "u"}; !slices.Equal(got, want) {
			t.Fatalf("Holdings calls %v, want f then u", h)
		}
		if got, want := startsAt(h, t0), secs(12, 12); !slices.Equal(got, want) {
			t.Errorf("Holdings calls at %v, want %v", got, want)
		}
		for _, c := range conf {
			if h[0].Seq < c.EndSeq {
				t.Errorf("%v started before %v returned", h[0], c)
			}
		}
		if h[1].Seq < h[0].EndSeq {
			t.Errorf("Holdings(u) started before Holdings(f) returned: %v", h)
		}
		for id := range 2 {
			if got, want := runStarts(r.runs.Of("u", id), t0), secs(0, 12); !slices.Equal(got, want) {
				t.Errorf("u/%d runs start at %v, want %v", id, got, want)
			}
			fr := r.runs.Of("f", id)
			if len(fr) != 1 {
				t.Errorf("f/%d runs %v, want one", id, fr)
			}
			wantRunning(t, fr)
		}
		if got := actionsAt[core.StopRun](recs); len(got) != 2 {
			t.Errorf("StopRun actions %+v, want only the two expiries", got)
		}
		closeNode(t, n)
		for id := range 2 {
			if c := r.runs.Of("f", id)[0]; !c.Done || c.Cause == nil || c.Cause.Error() != stopText(c.Shard, "draining") {
				t.Errorf("%v at Close, want stopped for draining", c)
			}
		}
		check()
	})
}

// sameActions reports whether got and want hold the same actions, in any order.
func sameActions(got, want []core.Action) bool {
	if len(got) != len(want) {
		return false
	}
	for _, a := range want {
		if !slices.Contains(got, a) {
			return false
		}
	}
	return true
}

// TestATRUN38LostAfterOutage checks that a fenced shard taken during an outage stops at the first
// store answer that shows the loss: the Confirm answer of the first renewal after the outage. The
// run is stopped as lost, the shard is not released, and the other shard is restored.
func TestATRUN38LostAfterOutage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newHolderRig(t)
		o := newOutside(t, r.ms, core.DefaultTiming)
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.config(r.fs, "f"))
		r.fs.Fail("KeepAlive", errBoom)
		synctest.Wait()
		term0, term1 := termOf(t, r.fs.Calls("Acquire"), 0), termOf(t, r.fs.Calls("Acquire"), 1)

		// The record of f/0 is lost while the node is cut off, and the outside holder takes it.
		sleepUntil(t0.Add(11 * time.Second))
		r.ms.DeleteRecord("f", 0)
		taken, err := o.Acquire("f", 0)
		if err != nil {
			t.Fatalf("outside Acquire(f/0) = %v", err)
		}
		r.fs.Fail("KeepAlive", nil)
		recs, _ := log.records()
		if got := holderSteps(recs, 0); len(got) != 0 {
			t.Errorf("f/0 HolderChanged steps %+v during the outage, want none", got)
		}
		wantRunning(t, r.runs.Calls())

		sleepUntil(t0.Add(12 * time.Second))
		synctest.Wait()
		recs, _ = log.records()
		lost := core.StopRun{Space: "f", Shard: 0, Term: term0, Reason: core.StopLost}
		got0 := holderSteps(recs, 0)
		want0 := core.HolderChanged{Space: "f", Shard: 0, Holder: outsideHolder, Term: taken}
		if len(got0) == 0 || got0[0].Now != 12*time.Second || got0[0].Input != want0 || !hasAction(got0[0].Actions, lost) {
			t.Errorf("f/0 HolderChanged steps %+v, want the first at T0+12s, %+v with %+v", got0, want0, lost)
		}
		if c := onShard(r.fs.Calls("Confirm"), "f", 0); len(c) != 1 || !c[0].Start.Equal(t0.Add(12*time.Second)) {
			t.Errorf("Confirm(f/0) calls %v, want one at T0+12s", c)
		}
		wantStopped(t, r.runs.Of("f", 0)[0], t0.Add(12*time.Second), "lost")
		restore1 := core.Restore{Space: "f", Shard: 1, Term: term1}
		if got := only(actionsAt[core.Restore](recs)); !slices.Equal(got, []core.Action{restore1}) {
			t.Errorf("Restore actions %v, want %v", got, restore1)
		}
		wantRunning(t, r.runs.Of("f", 1))

		sleepUntil(t0.Add(19 * time.Second))
		synctest.Wait()
		if rel := onShard(r.fs.Calls("Release"), "f", 0); len(rel) != 0 {
			t.Errorf("Release(f/0) calls %v, want none: the shard is known gone", rel)
		}
		acq := onShard(r.fs.Calls("Acquire"), "f", 0)
		if got, want := startsAt(acq, t0), secs(0, 12, 14, 16, 18); !slices.Equal(got, want) {
			t.Errorf("Acquire(f/0) calls at %v, want %v", got, want)
		}
		for _, c := range acq[1:] {
			var held *tenure.HeldError
			if !errors.As(c.Err, &held) || held.Holder != outsideHolder {
				t.Errorf("%v, want held by %s", c, outsideHolder)
			}
		}
		if got := r.runs.Of("f", 0); len(got) != 1 {
			t.Errorf("f/0 runs %v, want only the one that was lost", got)
		}
		o.Stop()
		closeNode(t, n)
		check()
	})
}
