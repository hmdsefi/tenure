// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of event delivery under load and of the log record format on running nodes. They use
// OBS §10's harness (obs_harness_test.go). The steps of the tests carrying an AT-OBS or AT-RUN
// number were written from the issue's text.

// wantSeqs fails t unless events have the consecutive Seqs first, first+1, ….
func wantSeqs(t *testing.T, what string, events []tenure.Event, first uint64) {
	t.Helper()
	for i, e := range events {
		if want := first + uint64(i); e.Seq != want {
			t.Errorf("%s: event %d has Seq %d, want %d", what, i, e.Seq, want)
			return
		}
	}
}

// wantCadence fails t unless calls start every period from first on.
func wantCadence(t *testing.T, calls []call, first time.Time, period time.Duration) {
	t.Helper()
	for i, c := range calls {
		if want := first.Add(time.Duration(i) * period); !c.Start.Equal(want) {
			t.Errorf("KeepAlive call %d started at +%v, want +%v", i+1, c.Start.Sub(first), want.Sub(first))
		}
	}
}

// TestATOBS14DeliveryNeverBlocks checks that a slow OnEvent never slows the node: with OnEvent
// blocked 60 s in its first call, both shards' Run start, the session is renewed on its cadence
// and RecentEvents holds every event emitted; once the call returns, OnEvent gets every event in
// Seq order, one call at a time, each event's log record written before its call.
func TestATOBS14DeliveryNeverBlocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t, "a").Node("a")
		a.Events.Hook(func(i int, _ tenure.Event) {
			if i == 1 {
				time.Sleep(time.Minute)
			}
		})
		t0 := time.Now()
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		synctest.Wait()

		runs := a.Runs.Calls()
		if len(runs) != 2 {
			t.Fatalf("%d Run calls while OnEvent blocks, want 2", len(runs))
		}
		for _, r := range runs {
			if !r.Start.Equal(t0) {
				t.Errorf("Run of s/%d started at +%v, want at once", r.Shard.ID, r.Start.Sub(t0))
			}
		}
		calls := a.Events.Calls()
		if len(calls) != 1 || calls[0].Done || calls[0].Event.Seq != 1 {
			t.Fatalf("OnEvent calls = %+v, want the first, still running", calls)
		}
		recent := a.RecentEvents()
		wantSeqs(t, "RecentEvents()", recent, 1)
		for shard := range 2 {
			if indexOf(recent, tenure.EventShardAcquired, shard) < 0 {
				t.Errorf("RecentEvents() = %+v, want s/%d's shard.acquired", recent, shard)
			}
		}

		time.Sleep(59 * time.Second) // the first call still runs
		synctest.Wait()
		if calls := a.Events.Calls(); len(calls) != 1 || calls[0].Done {
			t.Fatalf("OnEvent calls after 59 s = %+v, want the first, still running", calls)
		}
		ka := a.FS.Calls("KeepAlive")
		if len(ka) != 30 {
			t.Errorf("%d KeepAlive calls in 59 s, want 30", len(ka))
		}
		wantCadence(t, ka, t0, core.DefaultTiming.RetryPeriod)
		if recent := a.RecentEvents(); len(recent) < 6 {
			t.Errorf("RecentEvents() = %+v, want at least the 6 events of the start", recent)
		} else {
			wantSeqs(t, "RecentEvents()", recent, 1)
		}

		time.Sleep(time.Second) // the first call returns
		synctest.Wait()
		calls = a.Events.Calls()
		if !calls[0].End.Equal(t0.Add(time.Minute)) {
			t.Errorf("first OnEvent call returned at +%v, want +1m", calls[0].End.Sub(t0))
		}
		if got, want := a.Events.Events(), a.RecentEvents(); !reflect.DeepEqual(got, want) {
			t.Errorf("OnEvent got %+v, want every event: %+v", got, want)
		}
		if len(calls) < 2 || !calls[1].Start.Equal(t0.Add(time.Minute)) {
			t.Errorf("OnEvent calls = %+v, want the second as the first returns", calls)
		}

		a.MustClose()
		if got, want := a.Events.Events(), a.RecentEvents(); !reflect.DeepEqual(got, want) {
			t.Errorf("OnEvent got %+v, want every event: %+v", got, want)
		}
		wantDelivery(t, a)
		check()
	})
}

// msgOf returns the log message of kind: "tenure: " and its name with "." and "_" as spaces.
func msgOf(k tenure.EventKind) string {
	return "tenure: " + strings.NewReplacer(".", " ", "_", " ").Replace(k.String())
}

// levelOf returns the log level of kind as the JSON handler writes it.
func levelOf(k tenure.EventKind) string {
	switch k {
	case tenure.EventShardAcquiring, tenure.EventShardAcquireFailed, tenure.EventShardReleasing:
		return "DEBUG"
	case tenure.EventSessionRenewFailed, tenure.EventSessionRenewedLate, tenure.EventShardDegraded,
		tenure.EventShardStuck, tenure.EventSlotCountMismatch, tenure.EventRunReturned,
		tenure.EventStoreFailed, tenure.EventMembershipFailed:
		return "WARN"
	case tenure.EventSessionExpired, tenure.EventSessionLost, tenure.EventShardExpired, tenure.EventShardLost:
		return "ERROR"
	}
	return "INFO"
}

// loggerRun starts member a of a new harness, opts changing its Config, lets it run 10 s and
// closes it.
func loggerRun(t *testing.T, opts ...func(*tenure.Config)) *obsNode {
	t.Helper()
	a := newObsHarness(t, "a").Start("a", opts...)
	time.Sleep(10 * time.Second)
	synctest.Wait()
	a.MustClose()
	return a
}

// TestATOBS17Logger checks the log records: s/0's shard.acquired record is exactly the level,
// message, member, holder, space, shard and term, the handler's time aside; shard.acquiring's
// record is at DEBUG; every event has one record, at its kind's level and with its kind's message,
// in Seq order; and a nil Logger changes none of the events and does not panic.
func TestATOBS17Logger(t *testing.T) {
	var logged []tenure.Event
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := loggerRun(t)
		events := a.Events.Events()
		logged = events
		lines := a.Log.Lines()
		if len(lines) != len(events) {
			t.Fatalf("%d log records for %d events, want as many:\n%s", len(lines), len(events), strings.Join(lines, "\n"))
		}

		i := indexOf(events, tenure.EventShardAcquired, 0)
		if i < 0 {
			t.Fatalf("events = %+v, want s/0's shard.acquired", events)
		}
		term := events[i].Term
		if term == 0 {
			t.Errorf("s/0's shard.acquired has term 0, want a granted term")
		}
		want := fmt.Sprintf(`{"level":"INFO","msg":"tenure: shard acquired","member":"a","holder":"%s","space":"s","shard":0,"term":%d}`, obsHolderA, term)
		if lines[i] != want {
			t.Errorf("shard.acquired record =\n%s\nwant\n%s", lines[i], want)
		}
		j := indexOf(events, tenure.EventShardAcquiring, 0)
		want = `{"level":"DEBUG","msg":"tenure: shard acquiring","member":"a","holder":"` + obsHolderA + `","space":"s","shard":0}`
		if j < 0 || lines[j] != want {
			t.Errorf("shard.acquiring record = %q, want\n%s", lines[max(j, 0)], want)
		}

		for k, e := range events {
			prefix := fmt.Sprintf(`{"level":"%s","msg":"%s","member":"a","holder":"%s"`, levelOf(e.Kind), msgOf(e.Kind), obsHolderA)
			if !strings.HasPrefix(lines[k], prefix) {
				t.Errorf("record %d = %s, want it to start %s for %+v", k+1, lines[k], prefix, e)
			}
		}
		wantDelivery(t, a)
		check()
	})

	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := loggerRun(t, func(c *tenure.Config) { c.Logger = nil })
		if lines := a.Log.Lines(); len(lines) != 0 {
			t.Errorf("records without a Logger = %v, want none", lines)
		}
		// The two shards' lanes may finish their calls of one instant in either order, so the
		// runs are compared as sets of events with their times, Seq aside. Whether the loop
		// steps the view that Close's Leave publishes before it stops depends on scheduling too,
		// so that view's view.changed (the one without members) is left out.
		if got, want := unordered(a.Events.Events()), unordered(logged); logged != nil && !reflect.DeepEqual(got, want) {
			t.Errorf("events without a Logger =\n%s\nwant those with one:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		wantSerial(t, a)
		check()
	})
}

// unordered returns each event without its Seq, formatted, in sorted order, leaving out any
// view.changed of a view without members.
func unordered(events []tenure.Event) []string {
	var out []string
	for _, e := range events {
		if e.Kind == tenure.EventViewChanged && e.Count == 0 {
			continue
		}
		e.Seq = 0
		out = append(out, fmt.Sprintf("%+v", e))
	}
	slices.Sort(out)
	return out
}

// wantSerial fails t unless the node's OnEvent got its events with Seq 1, 2, … in order, one
// call at a time: wantDelivery for a node without a Logger.
func wantSerial(t *testing.T, n *obsNode) {
	t.Helper()
	calls := n.Events.Calls()
	if m := n.Events.Most(); m > 1 {
		t.Errorf("%s: %d OnEvent calls ran at once, want 1", n.ID, m)
	}
	for i, c := range calls {
		if want := uint64(i + 1); c.Event.Seq != want {
			t.Errorf("%s: OnEvent call %d got Seq %d, want %d", n.ID, i+1, c.Event.Seq, want)
		}
		if i > 0 && (!calls[i-1].Done || calls[i-1].EndSeq > c.Seq) {
			t.Errorf("%s: OnEvent call %d started before call %d returned", n.ID, i+1, i)
		}
	}
}

// TestATOBS18RecentRing checks RecentEvents with 200 shards, whose start alone emits more than
// 256 events: it holds the newest 256 events, with consecutive Seqs ending at the newest, before
// and after Close; and changing the slice it returns changes nothing in the node.
func TestATOBS18RecentRing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t, "a").Start("a", func(c *tenure.Config) { c.Spaces[0].Shards = 200 })
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if n := len(a.Runs.Calls()); n != 200 {
			t.Fatalf("%d Run calls, want 200", n)
		}

		wantRing := func(when string) []tenure.Event {
			t.Helper()
			events, recent := a.Events.Events(), a.RecentEvents()
			if len(events) <= 256 {
				t.Fatalf("%s: %d events, want more than 256", when, len(events))
			}
			if len(recent) != 256 {
				t.Fatalf("%s: RecentEvents() holds %d events, want 256", when, len(recent))
			}
			newest := events[len(events)-1].Seq
			wantSeqs(t, when+": RecentEvents()", recent, newest-255)
			if want := events[len(events)-256:]; !reflect.DeepEqual(recent, want) {
				t.Errorf("%s: RecentEvents() = %+v, want the newest 256 events: %+v", when, recent, want)
			}
			return recent
		}

		recent := wantRing("running")
		kept := slices.Clone(recent)
		recent[0] = tenure.Event{Kind: tenure.EventNodeClosed, Seq: 1, Err: errors.New("changed")}
		recent[255].Space = "changed"
		_ = append(recent[:10], tenure.Event{})
		if got := a.RecentEvents(); !reflect.DeepEqual(got, kept) {
			t.Errorf("RecentEvents() after changing its result = %+v, want %+v", got, kept)
		}

		a.MustClose()
		recent = wantRing("closed")
		want := tenure.Event{Kind: tenure.EventNodeClosed, Shard: -1, Slot: -1, Reason: "drained"}
		if bare(recent[255]) != want {
			t.Errorf("newest event after Close = %+v, want %+v", recent[255], want)
		}
		wantDelivery(t, a)
		check()
	})
}

// TestATRUN91EventsNeverBlock checks that a node without a Logger whose OnEvent blocks keeps its
// session and its shards: with every OnEvent call taking 30 s, the node starts both shards' Run
// at once and calls KeepAlive every RetryPeriod for two minutes; a Close forced by its deadline
// returns at that deadline, and no OnEvent call starts after it has returned.
func TestATRUN91EventsNeverBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t, "a").Node("a", func(c *tenure.Config) { c.Logger = nil })
		a.Events.Hook(func(int, tenure.Event) { time.Sleep(30 * time.Second) })
		a.Runs.Script(stopAfter(10 * time.Second)) // set before Start: the node starts the Runs
		t0 := time.Now()
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		synctest.Wait()
		runs := a.Runs.Calls()
		if len(runs) != 2 {
			t.Fatalf("%d Run calls while OnEvent blocks, want 2", len(runs))
		}
		for _, r := range runs {
			if !r.Start.Equal(t0) {
				t.Errorf("Run of s/%d started at +%v, want at once", r.Shard.ID, r.Start.Sub(t0))
			}
		}

		time.Sleep(2*time.Minute - time.Second)
		synctest.Wait()
		ka := a.FS.Calls("KeepAlive")
		if len(ka) != 60 {
			t.Errorf("%d KeepAlive calls in 2 minutes, want 60", len(ka))
		}
		wantCadence(t, ka, t0, core.DefaultTiming.RetryPeriod)
		for _, c := range ka {
			if c.Err != nil {
				t.Errorf("%v, want nil", c)
			}
		}
		if n, emitted := len(a.Events.Calls()), len(a.RecentEvents()); n != 4 || emitted <= n {
			t.Errorf("%d OnEvent calls of %d events after 2 minutes, want 4 of more", n, emitted)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		closing := time.Now()
		if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close() = %v, want an error wrapping %v", err, context.DeadlineExceeded)
		}
		closed := time.Now()
		if d := closed.Sub(closing); d != 2*time.Second {
			t.Errorf("Close returned after %v, want 2s", d)
		}
		// A call the delivery goroutine had committed to when Close stopped it may still be
		// entering OnEvent: let it, at this instant, before counting.
		synctest.Wait()
		calls := a.Events.Calls()
		for i, c := range calls {
			if c.Start.After(closed) {
				t.Errorf("OnEvent call %d started at +%v, after Close returned at +%v", i+1, c.Start.Sub(t0), closed.Sub(t0))
			}
		}
		recent := a.RecentEvents()
		want := tenure.Event{Kind: tenure.EventNodeClosed, Shard: -1, Slot: -1, Reason: "deadline"}
		if bare(recent[len(recent)-1]) != want {
			t.Errorf("last event = %+v, want %+v", recent[len(recent)-1], want)
		}

		time.Sleep(10 * time.Minute) // every Run and the OnEvent call in progress return
		if n := len(a.Events.Calls()); n != len(calls) {
			t.Errorf("%d OnEvent calls 10 minutes after Close, %d when it returned; want no new call", n, len(calls))
		}
		if got := a.RecentEvents(); !reflect.DeepEqual(got, recent) {
			t.Errorf("RecentEvents() 10 minutes after Close = %+v, want %+v: nothing is emitted after Close", got, recent)
		}
		wantSerial(t, a)
		check()
	})
}
