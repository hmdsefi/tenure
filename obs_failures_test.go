// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of the run, store and membership failure events: run.returned, store.failed and
// membership.failed. They use OBS §10's harness (obs_harness_test.go). The steps of the tests
// carrying an AT-OBS number were written from the issue's text.

// failKinds are the failure kinds these tests check.
var failKinds = []tenure.EventKind{tenure.EventRunReturned, tenure.EventStoreFailed, tenure.EventMembershipFailed}

// storeFailedEv returns the store.failed event of a failed call of method with err, about space,
// shard and slot ("" and -1 when unused).
func storeFailedEv(method, space string, shard, slot int, err error) tenure.Event {
	return tenure.Event{Kind: tenure.EventStoreFailed, Reason: method, Space: space, Shard: shard, Slot: slot, Err: err}
}

// membershipFailedEv returns the membership.failed event of a failed call of method with err.
func membershipFailedEv(method string, err error) tenure.Event {
	return tenure.Event{Kind: tenure.EventMembershipFailed, Reason: method, Shard: -1, Slot: -1, Err: err}
}

// wantEvents fails t unless got and want hold the same events but for Seq and Time, comparing
// each Err by its text, as an error the node makes is not the test's value.
func wantEvents(t *testing.T, what string, got, want []tenure.Event) {
	t.Helper()
	strip := func(es []tenure.Event) ([]tenure.Event, []string) {
		out, errs := bareAll(es), make([]string, len(es))
		for i := range out {
			if out[i].Err != nil {
				errs[i] = out[i].Err.Error()
			}
			out[i].Err = nil
		}
		return out, errs
	}
	g, gerrs := strip(got)
	w, werrs := strip(want)
	if !reflect.DeepEqual(g, w) || !reflect.DeepEqual(gerrs, werrs) {
		t.Errorf("%s =\n%+v\nwant\n%+v", what, got, want)
	}
}

// linesWith returns the log lines that contain all of subs, in order.
func linesWith(lines []string, subs ...string) []string {
	var out []string
	for _, l := range lines {
		ok := true
		for _, s := range subs {
			ok = ok && strings.Contains(l, s)
		}
		if ok {
			out = append(out, l)
		}
	}
	return out
}

// TestATOBS09RunReturned checks run.returned for a Run that returns boom at once: per shard,
// Count 1 to 5 with Duration 2 s, 4 s, 8 s, 15 s and 15 s, Err boom and the run's term, each at
// the return; the shard's table row has Restarts equal to the last Count; and WARN records with
// keys space, shard, term, restarts, restart_in and error after member and holder.
func TestATOBS09RunReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t, "a").Node("a")
		boom := errors.New("boom")
		a.Runs.Script(func(context.Context, *tenure.Shard, int) error { return boom })
		t0 := time.Now()
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		sleepUntil(t0.Add(30 * time.Second)) // the fifth return is at T0+29s, the sixth at T0+44s
		events := ofKinds(a.RecentEvents(), tenure.EventRunReturned)
		table := a.Table()

		backoffs := secs(2, 4, 8, 15, 15)
		returns := secs(0, 2, 6, 14, 29)
		for shard := range 2 {
			runs := a.Runs.Of(obsSpace, shard)
			if len(runs) != 5 {
				t.Fatalf("s/%d: %d Run calls, want 5", shard, len(runs))
			}
			term := runs[0].Shard.Term
			var want []tenure.Event
			var lines []string
			for i, d := range backoffs {
				e := shardEv(tenure.EventRunReturned, shard, term, "")
				e.Count, e.Duration, e.Err = i+1, d, boom
				want = append(want, e)
				lines = append(lines, fmt.Sprintf(`{"level":"WARN","msg":"tenure: run returned","member":"a",`+
					`"holder":"%s","space":"s","shard":%d,"term":%d,"restarts":%d,"restart_in":%d,"error":"boom"}`,
					obsHolderA, shard, term, i+1, d.Nanoseconds()))
			}
			got := ofShard(events, shard)
			wantEvents(t, fmt.Sprintf("s/%d run.returned events", shard), got, want)
			for i, e := range got {
				if i < len(returns) && e.Time != t0.Add(returns[i]).UTC() {
					t.Errorf("s/%d: run.returned %d at %v, want T0+%v", shard, i+1, e.Time, returns[i])
				}
			}
			for _, c := range runs {
				if c.Shard != runs[0].Shard {
					t.Errorf("%v, want the first call's Shard", c)
				}
			}
			if len(got) > 0 {
				if row := table.Spaces[0].Rows[shard]; row.Restarts != got[len(got)-1].Count {
					t.Errorf("s/%d: row Restarts %d, want the last run.returned Count %d", shard, row.Restarts, got[len(got)-1].Count)
				}
			}
			recs := linesWith(a.Log.Lines(), `"msg":"tenure: run returned"`, fmt.Sprintf(`"shard":%d,`, shard))
			if !reflect.DeepEqual(recs, lines) {
				t.Errorf("s/%d run.returned records =\n%s\nwant\n%s", shard, strings.Join(recs, "\n"), strings.Join(lines, "\n"))
			}
		}
		a.MustClose()
		wantDelivery(t, a)
		check()
	})
}

// TestATOBS13FailedCalls checks store.failed and membership.failed for calls the node repeats: a
// Release that fails once is repeated 2 s later, with one shard.releasing; a Watch channel that
// closes and a Watch that fails each emit membership.failed, and the next Watch goes out 2 s
// later; the channel Leave closes emits none.
func TestATOBS13FailedCalls(t *testing.T) {
	boom := errors.New("boom")

	t.Run("Release", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			h := newObsHarness(t)
			a := h.Start("a")
			synctest.Wait()
			a.Store.Fail("Release", boom, 1)
			b := h.Start("b")
			time.Sleep(time.Minute)

			rel := a.FS.Calls("Release")
			if len(rel) != 2 || rel[0].Err != boom || rel[1].Err != nil {
				t.Fatalf("a: Release calls %v, want one failed with boom and one that succeeds", rel)
			}
			moved := argAt[int](rel[0].Args, 2)
			term := argAt[core.Term](rel[0].Args, 3)
			if argAt[int](rel[1].Args, 2) != moved || argAt[core.Term](rel[1].Args, 3) != term {
				t.Errorf("a: Release calls %v, want both for one shard and term", rel)
			}
			if d := rel[1].Start.Sub(rel[0].Start); d != 2*time.Second {
				t.Errorf("a: Release repeated %v after the failed one, want 2s", d)
			}
			ae := a.RecentEvents()
			wantEvents(t, "a: failure events", ofKinds(ae, failKinds...),
				[]tenure.Event{storeFailedEv("Release", obsSpace, moved, -1, boom)})
			if n := count(ofShard(ae, moved), tenure.EventShardReleasing); n != 1 {
				t.Errorf("a: %d shard.releasing for s/%d, want 1", n, moved)
			}
			if got := ofKinds(ofShard(ae, moved), tenure.EventShardReleased); len(got) != 1 || got[0].Term != term {
				t.Errorf("a: shard.released for s/%d = %+v, want one in term %d", moved, got, term)
			}
			want := []string{fmt.Sprintf(`{"level":"WARN","msg":"tenure: store failed","member":"a","holder":"%s",`+
				`"op":"Release","space":"s","shard":%d,"error":"boom"}`, obsHolderA, moved)}
			if got := linesWith(a.Log.Lines(), `"msg":"tenure: store failed"`); !reflect.DeepEqual(got, want) {
				t.Errorf("a: store.failed records = %v, want %v", got, want)
			}

			b.MustClose()
			a.MustClose()
			wantDelivery(t, a)
			wantDelivery(t, b)
			check()
		})
	})

	t.Run("Watch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			t0 := time.Now()
			a := newObsHarness(t).Start("a")
			sleepUntil(t0.Add(10 * time.Second))
			a.M.Fail("Watch", boom)
			a.M.CloseWatch(0)
			sleepUntil(t0.Add(13 * time.Second))
			a.M.Fail("Watch", nil)
			sleepUntil(t0.Add(20 * time.Second))

			watches := a.M.Calls("Watch")
			if got, want := startsAt(watches, t0), secs(0, 12, 14); !reflect.DeepEqual(got, want) {
				t.Fatalf("Watch calls at %v, want %v", got, want)
			}
			if watches[1].Err != boom || watches[2].Err != nil {
				t.Errorf("Watch calls %v, want the second to fail with boom and the third to succeed", watches)
			}
			want := []tenure.Event{
				membershipFailedEv("Watch", errors.New("tenure: watch channel closed")),
				membershipFailedEv("Watch", boom),
			}
			got := ofKinds(a.RecentEvents(), failKinds...)
			wantEvents(t, "failure events", got, want)
			for i, at := range secs(10, 12) {
				if i < len(got) && got[i].Time != t0.Add(at).UTC() {
					t.Errorf("membership.failed %d at %v, want T0+%v", i+1, got[i].Time, at)
				}
			}
			wantLines := []string{
				`{"level":"WARN","msg":"tenure: membership failed","member":"a","holder":"` + obsHolderA +
					`","op":"Watch","error":"tenure: watch channel closed"}`,
				`{"level":"WARN","msg":"tenure: membership failed","member":"a","holder":"` + obsHolderA +
					`","op":"Watch","error":"boom"}`,
			}
			if lines := linesWith(a.Log.Lines(), `"msg":"tenure: membership failed"`); !reflect.DeepEqual(lines, wantLines) {
				t.Errorf("membership.failed records =\n%s\nwant\n%s", strings.Join(lines, "\n"), strings.Join(wantLines, "\n"))
			}

			// Leave closes the watch channel and then holds Close for a second, before node.closed:
			// a membership.failed for that channel would be emitted, and delivered, by then.
			a.M.Answer("Leave", func(ctx context.Context, _ []any) ([]any, error) {
				err := a.M.h.Leave(ctx)
				a.M.g.flush()
				time.Sleep(time.Second)
				return nil, err
			})
			a.MustClose()
			wantEvents(t, "failure events after Close", ofKinds(a.Events.Events(), failKinds...), want)
			if leaves := a.M.Calls("Leave"); len(leaves) != 1 || len(a.M.Calls("Watch")) != 3 {
				t.Errorf("Leave calls %v and Watch calls %v, want one Leave and no further Watch", leaves, a.M.Calls("Watch"))
			}
			wantDelivery(t, a)
			check()
		})
	})
}

// TestStoreFailedReasons checks store.failed for each store call that fails: its Reason is the
// method, Space, Shard and Slot those of the call when it has them. Failed Acquire and KeepAlive
// calls and a ClaimSlot that finds no free slot emit none.
func TestStoreFailedReasons(t *testing.T) {
	boom := errors.New("boom")
	// run makes member a of a new harness with opts and runs act on it in the bubble, which may
	// start it; then it starts a unless act did, closes it a minute later and returns its events.
	run := func(t *testing.T, act func(t *testing.T, a *obsNode), opts ...func(*tenure.Config)) []tenure.Event {
		t.Helper()
		var events []tenure.Event
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Node("a", opts...)
			act(t, a)
			if a.Node == nil {
				if err := a.Start(context.Background()); err != nil {
					t.Fatalf("Start(a) = %v, want nil", err)
				}
			}
			time.Sleep(time.Minute)
			a.MustClose()
			events = a.Events.Events()
			wantDelivery(t, a)
			check()
		})
		return events
	}
	slots := func(c *tenure.Config) { c.Slots = 1 }
	fenced := func(c *tenure.Config) { c.Spaces[0].Fenced = true }
	// failed returns the store.failed events of events.
	failed := func(events []tenure.Event) []tenure.Event { return ofKinds(events, tenure.EventStoreFailed) }
	// failedWith returns the number of calls of method that failed with err. Calls on two lanes
	// may take the injected error in another order than the log's, which is by start.
	failedWith := func(a *obsNode, method string, err error) int {
		n := 0
		for _, c := range a.FS.Calls(method) {
			if c.Err == err {
				n++
			}
		}
		return n
	}
	// failedShard returns the shard of the first failed call of method.
	failedShard := func(a *obsNode, method string) int {
		for _, c := range a.FS.Calls(method) {
			if c.Err != nil {
				return argAt[int](c.Args, 2)
			}
		}
		return -1
	}

	t.Run("Release", func(t *testing.T) {
		var a *obsNode
		events := run(t, func(t *testing.T, n *obsNode) { a = n; n.Store.Fail("Release", boom, 1) })
		want := []tenure.Event{storeFailedEv("Release", obsSpace, failedShard(a, "Release"), -1, boom)}
		wantEvents(t, "store.failed events", failed(events), want)
	})

	t.Run("Confirm", func(t *testing.T) {
		var a *obsNode
		events := run(t, func(t *testing.T, n *obsNode) {
			a = n
			t0 := time.Now()
			if err := n.Start(context.Background()); err != nil {
				t.Fatalf("Start(a) = %v, want nil", err)
			}
			n.Store.Fail("KeepAlive", boom, 0)
			sleepUntil(t0.Add(11 * time.Second)) // degraded at the deadline, T0+10s
			n.Store.Fail("Confirm", boom, 1)
			n.Store.Heal("KeepAlive")
		}, fenced)
		conf := a.FS.Calls("Confirm")
		if len(conf) < 3 || failedWith(a, "Confirm", boom) != 1 {
			t.Fatalf("Confirm calls %v, want one to fail and the confirms to go on", conf)
		}
		want := []tenure.Event{storeFailedEv("Confirm", obsSpace, failedShard(a, "Confirm"), -1, boom)}
		wantEvents(t, "store.failed events", failed(events), want)
	})

	t.Run("ReleaseSlot", func(t *testing.T) {
		events := run(t, func(t *testing.T, n *obsNode) { n.Store.Fail("ReleaseSlot", boom, 1) }, slots)
		wantEvents(t, "store.failed events", failed(events), []tenure.Event{storeFailedEv("ReleaseSlot", "", -1, 0, boom)})
	})

	t.Run("CloseSession", func(t *testing.T) {
		events := run(t, func(t *testing.T, n *obsNode) { n.Store.Fail("CloseSession", boom, 1) })
		wantEvents(t, "store.failed events", failed(events), []tenure.Event{storeFailedEv("CloseSession", "", -1, -1, boom)})
	})

	t.Run("Holdings", func(t *testing.T) {
		// The first store watch fails; the second, 2 s later, asks for a resync, whose first
		// Holdings call fails.
		var a *obsNode
		events := run(t, func(t *testing.T, n *obsNode) {
			a = n
			n.Store.Fail("WatchHolders", boom, 1)
			n.Store.Fail("Holdings", boom, 1)
		})
		if h := a.FS.Calls("Holdings"); len(h) < 2 || h[0].Err != boom || h[1].Err != nil {
			t.Errorf("Holdings calls %v, want a failed one and a repeat", h)
		}
		want := []tenure.Event{
			storeFailedEv("WatchHolders", "", -1, -1, boom),
			storeFailedEv("Holdings", obsSpace, -1, -1, boom),
		}
		wantEvents(t, "store.failed events", failed(events), want)
	})

	t.Run("WatchHoldersEnded", func(t *testing.T) {
		var a *obsNode
		events := run(t, func(t *testing.T, n *obsNode) {
			a = n
			n.Cfg.Store = n.FS.Injected()
			if err := n.Start(context.Background()); err != nil {
				t.Fatalf("Start(a) = %v, want nil", err)
			}
			time.Sleep(time.Second)
			n.FS.Injected().CloseWatch()
		})
		if w := a.FS.Calls("WatchHolders"); len(w) != 2 {
			t.Errorf("WatchHolders calls %v, want a second one after the channel closed", w)
		}
		want := []tenure.Event{storeFailedEv("WatchHolders", "", -1, -1, errors.New("tenure: store watch ended"))}
		wantEvents(t, "store.failed events", failed(events), want)
	})

	t.Run("Slots", func(t *testing.T) {
		events := run(t, func(t *testing.T, n *obsNode) {
			if err := n.Start(context.Background()); err != nil {
				t.Fatalf("Start(a) = %v, want nil", err)
			}
			n.Store.Fail("Slots", boom, 1) // the refresher's read at T0+2s
		})
		wantEvents(t, "store.failed events", failed(events), []tenure.Event{storeFailedEv("Slots", "", -1, -1, boom)})
	})

	t.Run("ClaimSlot", func(t *testing.T) {
		events := run(t, func(t *testing.T, n *obsNode) { n.Store.Fail("ClaimSlot", boom, 1) }, slots)
		wantEvents(t, "store.failed events", failed(events), []tenure.Event{storeFailedEv("ClaimSlot", "", -1, -1, boom)})
		if n := count(events, tenure.EventSlotClaimed); n > 1 {
			t.Errorf("%d slot.claimed events, want at most one", n)
		}
	})

	t.Run("NoSlot", func(t *testing.T) {
		// b finds a holding the only slot: its ClaimSlot calls return ErrNoSlot.
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			h := newObsHarness(t)
			a := h.Start("a", slots)
			synctest.Wait()
			b := h.Start("b", slots)
			time.Sleep(time.Minute)
			synctest.Wait() // a ClaimSlot that starts at this instant returns
			claims := b.FS.Calls("ClaimSlot")
			if len(claims) == 0 {
				t.Fatal("b made no ClaimSlot call")
			}
			for _, c := range claims {
				if !errors.Is(c.Err, tenure.ErrNoSlot) {
					t.Errorf("b: %v, want ErrNoSlot", c)
				}
			}
			if got := failed(b.RecentEvents()); len(got) != 0 {
				t.Errorf("b: store.failed events %+v, want none", got)
			}
			b.MustClose()
			a.MustClose()
			wantDelivery(t, a)
			wantDelivery(t, b)
			check()
		})
	})

	t.Run("Acquire", func(t *testing.T) {
		var a *obsNode
		events := run(t, func(t *testing.T, n *obsNode) { a = n; n.Store.Fail("Acquire", boom, 1) })
		if acq := a.FS.Calls("Acquire"); len(acq) < 3 || failedWith(a, "Acquire", boom) != 1 {
			t.Errorf("Acquire calls %v, want one failed and then the grants", acq)
		}
		if got := failed(events); len(got) != 0 {
			t.Errorf("store.failed events %+v, want none", got)
		}
		if n, calls := count(events, tenure.EventShardAcquiring), len(a.FS.Calls("Acquire")); n != calls {
			t.Errorf("%d shard.acquiring events for %d Acquire calls, want one per call", n, calls)
		}
	})

	t.Run("KeepAlive", func(t *testing.T) {
		var a *obsNode
		events := run(t, func(t *testing.T, n *obsNode) {
			a = n
			if err := n.Start(context.Background()); err != nil {
				t.Fatalf("Start(a) = %v, want nil", err)
			}
			n.Store.Fail("KeepAlive", boom, 2)
		})
		failedRenewals := 0
		for _, c := range a.FS.Calls("KeepAlive") {
			if c.Err == boom {
				failedRenewals++
			}
		}
		if failedRenewals != 2 {
			t.Errorf("%d KeepAlive calls failed with boom, want 2", failedRenewals)
		}
		if got := failed(events); len(got) != 0 {
			t.Errorf("store.failed events %+v, want none", got)
		}
	})

	t.Run("StartCleanup", func(t *testing.T) {
		// The loop's first step is held until Start's ctx has ended, so Start fails once the
		// session is open: its cleanup's CloseSession and Leave fail. Start's ctx is done, so it
		// waits for no delivery: Leave takes a second, in which store.failed is delivered, and
		// membership.failed, emitted right before the delivery stops, may be dropped.
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Node("a")
			a.Store.Fail("CloseSession", boom, 0)
			a.M.Fail("Leave", boom)
			a.M.Delay("Leave", time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var once sync.Once
			hook := tenure.WithStepHook(ctx, func(tenure.StepRecord) {
				once.Do(func() {
					<-ctx.Done()
					time.Sleep(time.Second)
				})
			})
			const errText = "tenure: start: context deadline exceeded"
			if err := a.Start(hook); err == nil || err.Error() != errText {
				t.Fatalf("Start() = %v, want %q", err, errText)
			}
			if cs := a.FS.Calls("CloseSession"); len(cs) != 1 || cs[0].Err != boom {
				t.Errorf("CloseSession calls %v, want one failed with boom", cs)
			}
			synctest.Wait() // an OnEvent call the delivery had committed to returns
			want := []tenure.Event{
				storeFailedEv("CloseSession", "", -1, -1, boom),
				membershipFailedEv("Leave", boom),
			}
			got := ofKinds(a.Events.Events(), failKinds...)
			if len(got) == 1 {
				want = want[:1]
			}
			wantEvents(t, "failure events delivered by Start", got, want)
			if n := count(a.Events.Events(), tenure.EventNodeClosed); n != 0 {
				t.Errorf("%d node.closed events, want none", n)
			}
			wantDelivery(t, a)
			check()
		})
	})

	t.Run("CutOffByStart", func(t *testing.T) {
		// The loop's second step, which queues the ClaimSlot, is held until Start's ctx ends at
		// T0+1s, and the ClaimSlot does not return before its ctx ends: Start's cleanup cancels
		// it. A call that the node itself cut off is not a failed store call, so it emits no
		// store.failed. Leave takes a second, in which such an event would be delivered.
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Node("a", slots)
			a.Store.Answer("ClaimSlot", func(ctx context.Context, args []any) ([]any, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
			a.M.Delay("Leave", time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			steps := 0
			hook := tenure.WithStepHook(ctx, func(tenure.StepRecord) {
				if steps++; steps == 2 {
					<-ctx.Done()
					time.Sleep(time.Second) // Start does not see the step done as ctx ends
				}
			})
			const errText = "tenure: start: context deadline exceeded"
			if err := a.Start(hook); err == nil || err.Error() != errText {
				t.Fatalf("Start() = %v, want %q", err, errText)
			}
			if cs := a.FS.Calls("ClaimSlot"); len(cs) != 1 || !errors.Is(cs[0].Err, context.Canceled) {
				t.Errorf("ClaimSlot calls %v, want one that Start's cleanup canceled", cs)
			}
			synctest.Wait()
			if got := ofKinds(a.Events.Events(), failKinds...); len(got) != 0 {
				t.Errorf("failure events %+v, want none", got)
			}
			wantDelivery(t, a)
			check()
		})
	})
}

// TestMembershipFailedReasons checks membership.failed for a SetDraining that fails, which the
// drain repeats, and for a Leave that fails in Close, before node.closed.
func TestMembershipFailedReasons(t *testing.T) {
	boom := errors.New("boom")

	t.Run("SetDraining", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Start("a")
			synctest.Wait()
			a.M.Fail("SetDraining", boom)
			t1 := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := a.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Drain() = %v, want an error wrapping %v", err, context.DeadlineExceeded)
			}
			a.M.Fail("SetDraining", nil) // the repeat at T1+2s succeeds
			a.MustClose()
			if sd := a.M.Calls("SetDraining"); len(sd) != 2 || sd[0].Err != boom || sd[1].Err != nil {
				t.Errorf("SetDraining calls %v, want a failed one and a repeat", sd)
			}
			got := ofKinds(a.Events.Events(), failKinds...)
			wantEvents(t, "failure events", got, []tenure.Event{membershipFailedEv("SetDraining", boom)})
			if len(got) == 1 && got[0].Time != t1.UTC() {
				t.Errorf("membership.failed at %v, want %v", got[0].Time, t1.UTC())
			}
			wantDelivery(t, a)
			check()
		})
	})

	t.Run("Leave", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Start("a")
			synctest.Wait()
			a.M.Fail("Leave", boom)
			a.MustClose()
			events := a.Events.Events()
			wantEvents(t, "failure events", ofKinds(events, failKinds...), []tenure.Event{membershipFailedEv("Leave", boom)})
			if i, j := indexOf(events, tenure.EventMembershipFailed, -1), indexOf(events, tenure.EventNodeClosed, -1); i < 0 || j != len(events)-1 || i > j {
				t.Errorf("membership.failed at %d, node.closed at %d of %d events: want node.closed last", i, j, len(events))
			}
			wantDelivery(t, a)
			check()
		})
	})
}
