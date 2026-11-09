// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
)

// The tests of the event log on running nodes: delivery at Close and at a failed Start, and the
// node lifecycle events. They use OBS §10's harness (obs_harness_test.go). The steps of the tests
// carrying an AT-OBS number were written from the issue's text.

// TestObsHarness checks the harness itself: the holders its Rand gives, and faulty's injected
// errors.
func TestObsHarness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		h := newObsHarness(t)
		a, b := h.Start("a"), h.Start("b")
		if a.Holder != obsHolderA || a.Node.Holder() != obsHolderA {
			t.Errorf("a's holder = %s (node %s), want %s", a.Holder, a.Node.Holder(), obsHolderA)
		}
		if b.Holder != obsHolderB || b.Node.Holder() != obsHolderB {
			t.Errorf("b's holder = %s (node %s), want %s", b.Holder, b.Node.Holder(), obsHolderB)
		}

		boom := errors.New("boom")
		ctx := context.Background()
		before := len(a.FS.Calls("Slots")) // Start's read
		a.Store.Fail("Slots", boom, 2)
		var errs []error
		for range 3 {
			_, _, err := a.Store.Slots(ctx)
			errs = append(errs, err)
		}
		if !reflect.DeepEqual(errs, []error{boom, boom, nil}) {
			t.Errorf("Slots errors with Fail(Slots, boom, 2) = %v, want [boom boom <nil>]", errs)
		}
		a.Store.Fail("Slots", boom, 0)
		if _, _, err := a.Store.Slots(ctx); err != boom {
			t.Errorf("Slots with Fail(Slots, boom, 0) = %v, want boom", err)
		}
		a.Store.Heal("Slots")
		if _, _, err := a.Store.Slots(ctx); err != nil {
			t.Errorf("Slots after Heal = %v, want nil", err)
		}
		if got := len(a.FS.Calls("Slots")) - before; got != 5 {
			t.Errorf("%d Slots calls recorded, want 5", got)
		}

		a.MustClose()
		b.MustClose()
		wantDelivery(t, a)
		wantDelivery(t, b)
		check()
	})
}

// TestATOBS15CloseAndDelivery checks how Close ends the delivery: node.closed is the last event
// OnEvent gets; and with OnEvent blocking from its third call, Close with a 5 s context returns
// after 5 s, no later OnEvent call starts, and RecentEvents still ends with node.closed.
func TestATOBS15CloseAndDelivery(t *testing.T) {
	t.Run("Delivered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Start("a")
			synctest.Wait()
			a.MustClose()

			events := a.Events.Events()
			want := tenure.Event{Kind: tenure.EventNodeClosed, Shard: -1, Slot: -1, Reason: "drained"}
			if len(events) == 0 || bare(events[len(events)-1]) != want {
				t.Fatalf("OnEvent got %v, want %+v last", events, want)
			}
			if recent := a.RecentEvents(); !reflect.DeepEqual(recent, events) {
				t.Errorf("RecentEvents() = %v, want what OnEvent got: %v", recent, events)
			}
			wantDelivery(t, a)
			check()
		})
	})

	t.Run("OnEventBlocks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Node("a")
			release := a.Events.BlockFrom(3)
			defer release()
			if err := a.Start(context.Background()); err != nil {
				t.Fatalf("Start(a) = %v, want nil", err)
			}
			synctest.Wait()

			t0 := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := a.Close(ctx); err != nil {
				t.Fatalf("Close() = %v, want nil", err)
			}
			if d := time.Since(t0); d != 5*time.Second {
				t.Errorf("Close returned after %v, want 5s", d)
			}
			if n := len(a.Events.Calls()); n != 3 {
				t.Errorf("%d OnEvent calls when Close returned, want 3", n)
			}
			recent := a.RecentEvents()
			want := tenure.Event{Kind: tenure.EventNodeClosed, Shard: -1, Slot: -1, Reason: "drained"}
			if len(recent) == 0 || bare(recent[len(recent)-1]) != want {
				t.Errorf("RecentEvents() = %v, want %+v last", recent, want)
			}

			release()
			time.Sleep(time.Minute)
			if calls := a.Events.Calls(); len(calls) != 3 || !calls[2].Done {
				t.Errorf("OnEvent calls after the release = %v, want the 3 that started before Close returned", calls)
			}
			if got := a.RecentEvents(); !reflect.DeepEqual(got, recent) {
				t.Errorf("RecentEvents() after the release = %v, want %v: nothing is emitted after Close", got, recent)
			}
			wantDelivery(t, a)
			check()
		})
	})
}

// TestATOBS16FailedStart checks a Start that fails: it returns the error once the events emitted
// so far are delivered, emits no node.closed and leaves no goroutine behind.
func TestATOBS16FailedStart(t *testing.T) {
	t.Run("JoinFails", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Node("a")
			errJoin := errors.New("join refused")
			a.M.Fail("Join", errJoin)
			err := a.Start(context.Background())
			if !errors.Is(err, errJoin) || !strings.HasPrefix(err.Error(), "tenure: join: ") {
				t.Fatalf("Start() = %v, want tenure: join: wrapping %v", err, errJoin)
			}
			if calls := a.Events.Calls(); len(calls) != 0 {
				t.Errorf("OnEvent calls = %v, want none", calls)
			}
			if lines := a.Log.Lines(); len(lines) != 0 {
				t.Errorf("log records = %v, want none", lines)
			}
			check()
		})
	})
}

// TestNodeLifecycleEvents checks node.draining, node.drained and node.closed: their fields, their
// log records, node.drained's Duration on the monotonic clock, and node.closed's reasons.
func TestNodeLifecycleEvents(t *testing.T) {
	draining := tenure.Event{Kind: tenure.EventNodeDraining, Shard: -1, Slot: -1}
	closed := func(reason string) tenure.Event {
		return tenure.Event{Kind: tenure.EventNodeClosed, Shard: -1, Slot: -1, Reason: reason}
	}
	// slowStop makes each Run return d after its ctx is done.
	slowStop := func(d time.Duration) runScript {
		return func(ctx context.Context, _ *tenure.Shard, _ int) error {
			<-ctx.Done()
			time.Sleep(d)
			return context.Cause(ctx)
		}
	}
	// startSlow starts member a with slowStop(d) as its Run. The script is set before Start: the
	// node starts the Runs in the background, and each call takes the script it starts with.
	startSlow := func(t *testing.T, d time.Duration) *obsNode {
		t.Helper()
		a := newObsHarness(t).Node("a")
		a.Runs.Script(slowStop(d))
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		return a
	}

	t.Run("Drained", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := startSlow(t, 3*time.Second)
			synctest.Wait()
			if n := len(a.Runs.Calls()); n != 2 {
				t.Fatalf("%d Run calls, want 2", n)
			}

			t0 := time.Now()
			a.MustClose()
			if d := time.Since(t0); d != 3*time.Second {
				t.Errorf("Close took %v, want 3s", d)
			}
			got := ofKinds(a.Events.Events(), nodeKinds...)
			want := []tenure.Event{
				draining,
				{Kind: tenure.EventNodeDrained, Shard: -1, Slot: -1, Duration: 3 * time.Second},
				closed("drained"),
			}
			if !reflect.DeepEqual(bareAll(got), want) {
				t.Fatalf("node events = %+v, want %+v", got, want)
			}
			if got[0].Time != t0.UTC() || got[1].Time != t0.Add(3*time.Second).UTC() || got[2].Time != got[1].Time {
				t.Errorf("node event times = %v, %v, %v; want %v, then 3s later twice", got[0].Time, got[1].Time, got[2].Time, t0.UTC())
			}

			wantLines := []string{
				`{"level":"INFO","msg":"tenure: node draining","member":"a","holder":"` + obsHolderA + `"}`,
				`{"level":"INFO","msg":"tenure: node drained","member":"a","holder":"` + obsHolderA + `","took":3000000000}`,
				`{"level":"INFO","msg":"tenure: node closed","member":"a","holder":"` + obsHolderA + `","reason":"drained"}`,
			}
			var lines []string
			for _, l := range a.Log.Lines() {
				if strings.Contains(l, `"msg":"tenure: node `) {
					lines = append(lines, l)
				}
			}
			if !reflect.DeepEqual(lines, wantLines) {
				t.Errorf("node records =\n%s\nwant\n%s", strings.Join(lines, "\n"), strings.Join(wantLines, "\n"))
			}
			wantDelivery(t, a)
			check()
		})
	})

	t.Run("DrainThenClose", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := startSlow(t, time.Second)
			synctest.Wait()
			if err := a.Drain(context.Background()); err != nil {
				t.Fatalf("Drain() = %v, want nil", err)
			}
			if err := a.Drain(context.Background()); err != nil {
				t.Fatalf("second Drain() = %v, want nil", err)
			}
			time.Sleep(time.Minute)
			a.MustClose()
			want := []tenure.Event{
				draining,
				{Kind: tenure.EventNodeDrained, Shard: -1, Slot: -1, Duration: time.Second},
				closed("drained"),
			}
			if got := ofKinds(a.Events.Events(), nodeKinds...); !reflect.DeepEqual(bareAll(got), want) {
				t.Errorf("node events = %+v, want %+v", got, want)
			}
			wantDelivery(t, a)
			check()
		})
	})

	t.Run("Deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := startSlow(t, 10*time.Second)
			synctest.Wait()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Close() = %v, want an error wrapping %v", err, context.DeadlineExceeded)
			}
			// A call the delivery goroutine had committed to when Close stopped it may still be
			// entering OnEvent: let it, at this instant, before counting.
			synctest.Wait()
			after := len(a.Events.Calls())
			got := ofKinds(a.RecentEvents(), nodeKinds...)
			if want := []tenure.Event{draining, closed("deadline")}; !reflect.DeepEqual(bareAll(got), want) {
				t.Errorf("node events = %+v, want %+v", got, want)
			}
			if recent := a.RecentEvents(); bare(recent[len(recent)-1]) != closed("deadline") {
				t.Errorf("last event = %+v, want %+v", recent[len(recent)-1], closed("deadline"))
			}

			time.Sleep(time.Minute) // every Run returns
			if n := len(a.Events.Calls()); n != after {
				t.Errorf("%d OnEvent calls a minute after Close, %d when it returned; want no new call", n, after)
			}
			wantDelivery(t, a)
			check()
		})
	})

	t.Run("DoneAtClose", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Start("a")
			synctest.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := a.Close(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Close() = %v, want an error wrapping %v", err, context.Canceled)
			}
			got := ofKinds(a.RecentEvents(), nodeKinds...)
			if want := []tenure.Event{closed("deadline")}; !reflect.DeepEqual(bareAll(got), want) {
				t.Errorf("node events = %+v, want %+v: no drain started", got, want)
			}
			synctest.Wait()
			wantDelivery(t, a)
			check()
		})
	})
}
