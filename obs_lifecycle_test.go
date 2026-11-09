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

// The tests of the view, session and shard lifecycle events of running nodes, in the order of one
// step's events (OBS-110). They use OBS §10's harness (obs_harness_test.go). The steps of the tests
// carrying an AT-OBS number were written from the issue's text.

// shardKinds are the shard lifecycle kinds these tests check.
var shardKinds = []tenure.EventKind{
	tenure.EventShardAcquiring, tenure.EventShardAcquireFailed, tenure.EventShardAcquired,
	tenure.EventShardStopping, tenure.EventShardReleasing, tenure.EventShardReleased,
	tenure.EventShardHandoffCompleted,
}

// ofShard returns the events about shard of the harness's space, in order.
func ofShard(events []tenure.Event, shard int) []tenure.Event {
	var out []tenure.Event
	for _, e := range events {
		if e.Space == obsSpace && e.Shard == shard {
			out = append(out, e)
		}
	}
	return out
}

// indexOf returns the index of the first event of kind about shard (any shard for -1), or -1 when
// there is none.
func indexOf(events []tenure.Event, kind tenure.EventKind, shard int) int {
	return slices.IndexFunc(events, func(e tenure.Event) bool {
		return e.Kind == kind && (shard < 0 || e.Shard == shard)
	})
}

// count returns the number of events of kind.
func count(events []tenure.Event, kind tenure.EventKind) int {
	return len(ofKinds(events, kind))
}

// stopAfter makes each Run return d after its ctx is done, with ctx's cause.
func stopAfter(d time.Duration) runScript {
	return func(ctx context.Context, _ *tenure.Shard, _ int) error {
		<-ctx.Done()
		time.Sleep(d)
		return context.Cause(ctx)
	}
}

// shardEv returns the event of kind about shard of the harness's space, with term and reason.
func shardEv(kind tenure.EventKind, shard int, term core.Term, reason string) tenure.Event {
	return tenure.Event{Kind: kind, Space: obsSpace, Shard: shard, Term: term, Slot: -1, Reason: reason}
}

// lifecycle returns the events of one run of shard in term that is stopped for reason, from
// shard.acquiring to shard.handoff_completed with took.
func lifecycle(shard int, term core.Term, reason string, took time.Duration) []tenure.Event {
	handoff := shardEv(tenure.EventShardHandoffCompleted, shard, term, reason)
	handoff.Duration = took
	return []tenure.Event{
		shardEv(tenure.EventShardAcquiring, shard, 0, ""),
		shardEv(tenure.EventShardAcquired, shard, term, ""),
		shardEv(tenure.EventShardStopping, shard, term, reason),
		shardEv(tenure.EventShardReleasing, shard, term, ""),
		shardEv(tenure.EventShardReleased, shard, term, reason),
		handoff,
	}
}

// TestATOBS02SingleNodeLifecycle checks the events of one node that starts, acquires both shards
// and closes: Seq 1…N without gaps; session.opened, view.changed{Version: 1, Count: 1}, then
// shard.acquiring for s/0 and s/1; per shard exactly acquiring, acquired, stopping{draining},
// releasing, released{draining}, handoff_completed{draining}; node.draining before both stops,
// session.closed after both releases, then node.drained, and node.closed{drained} last; and no
// other kinds but further view.changed.
func TestATOBS02SingleNodeLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t, "a").Start("a")
		synctest.Wait()
		a.MustClose()

		events := a.Events.Events()
		wantDelivery(t, a) // Seq 1…N without gaps, in order
		if recent := a.RecentEvents(); !reflect.DeepEqual(recent, events) {
			t.Errorf("RecentEvents() = %v, want what OnEvent got: %v", recent, events)
		}

		start := []tenure.Event{
			{Kind: tenure.EventSessionOpened, Shard: -1, Slot: -1},
			{Kind: tenure.EventViewChanged, Shard: -1, Slot: -1, Version: 1, Count: 1},
			shardEv(tenure.EventShardAcquiring, 0, 0, ""),
			shardEv(tenure.EventShardAcquiring, 1, 0, ""),
		}
		if len(events) < len(start) || !reflect.DeepEqual(bareAll(events[:len(start)]), start) {
			t.Fatalf("first events = %+v, want %+v", events, start)
		}

		allowed := append([]tenure.EventKind{tenure.EventViewChanged, tenure.EventSessionOpened,
			tenure.EventSessionClosed}, nodeKinds...)
		allowed = append(allowed, shardKinds...)
		for _, e := range events {
			if !slices.Contains(allowed, e.Kind) || e.Kind == tenure.EventShardAcquireFailed {
				t.Errorf("event %+v: kind %v not expected", e, e.Kind)
			}
		}
		for _, k := range []tenure.EventKind{tenure.EventSessionOpened, tenure.EventSessionClosed,
			tenure.EventNodeDraining, tenure.EventNodeDrained, tenure.EventNodeClosed} {
			if n := count(events, k); n != 1 {
				t.Errorf("%d %v events, want 1", n, k)
			}
		}

		wantKinds := []tenure.EventKind{
			tenure.EventShardAcquiring, tenure.EventShardAcquired, tenure.EventShardStopping,
			tenure.EventShardReleasing, tenure.EventShardReleased, tenure.EventShardHandoffCompleted,
		}
		for shard := range 2 {
			got := ofShard(events, shard)
			var kinds []tenure.EventKind
			for _, e := range got {
				kinds = append(kinds, e.Kind)
			}
			if !slices.Equal(kinds, wantKinds) {
				t.Errorf("s/%d events %v, want %v", shard, kinds, wantKinds)
				continue
			}
			for _, e := range got[2:] {
				if e.Kind != tenure.EventShardReleasing && e.Reason != "draining" {
					t.Errorf("s/%d: %v has Reason %q, want draining", shard, e.Kind, e.Reason)
				}
			}
		}

		draining := indexOf(events, tenure.EventNodeDraining, -1)
		closed := indexOf(events, tenure.EventSessionClosed, -1)
		drained := indexOf(events, tenure.EventNodeDrained, -1)
		for shard := range 2 {
			if s := indexOf(events, tenure.EventShardStopping, shard); s < draining {
				t.Errorf("s/%d: shard.stopping at %d, node.draining at %d: want the drain first", shard, s, draining)
			}
			if r := indexOf(events, tenure.EventShardHandoffCompleted, shard); r > closed {
				t.Errorf("s/%d: release completed at %d, session.closed at %d: want the release first", shard, r, closed)
			}
		}
		if drained < closed {
			t.Errorf("node.drained at %d, session.closed at %d: want the session closed first", drained, closed)
		}
		last := tenure.Event{Kind: tenure.EventNodeClosed, Shard: -1, Slot: -1, Reason: "drained"}
		if bare(events[len(events)-1]) != last {
			t.Errorf("last event %+v, want %+v", events[len(events)-1], last)
		}
		check()
	})
}

// TestATOBS03EventFields checks the fields of one node's lifecycle events whose Run returns 3 s
// after its stop: the unused Space, Shard and Slot of view, session and node events; the terms of
// the shard events, which are those of the *Shard each Run got; and the handoff and drain
// durations, which equal the bubble time between the events they measure.
func TestATOBS03EventFields(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t, "a").Node("a")
		a.Runs.Script(stopAfter(3 * time.Second))
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		// The node runs a while first, so that a duration measured from Start would differ.
		time.Sleep(10 * time.Second)
		t0 := time.Now()
		a.MustClose()
		events := a.Events.Events()

		for _, e := range events {
			if slices.Contains(shardKinds, e.Kind) {
				if e.Space != obsSpace || e.Shard < 0 || e.Shard > 1 || e.Slot != -1 {
					t.Errorf("%+v: want Space %q, Shard 0 or 1, Slot -1", e, obsSpace)
				}
				continue
			}
			if e.Space != "" || e.Shard != -1 || e.Slot != -1 || e.Term != 0 || e.Holder != "" || e.Err != nil {
				t.Errorf("%+v: want Space \"\", Shard -1, Slot -1, no term, holder or error", e)
			}
		}
		for _, k := range []tenure.EventKind{tenure.EventSessionOpened, tenure.EventSessionClosed} {
			want := []tenure.Event{{Kind: k, Shard: -1, Slot: -1}}
			if got := bareAll(ofKinds(events, k)); !reflect.DeepEqual(got, want) {
				t.Errorf("%v events = %+v, want %+v", k, got, want)
			}
		}
		if v := ofKinds(events, tenure.EventViewChanged); len(v) == 0 || v[0].Version != 1 || v[0].Count != 1 {
			t.Errorf("view.changed events = %+v, want Version 1 and Count 1 first", v)
		}

		for shard := range 2 {
			runs := a.Runs.Of(obsSpace, shard)
			if len(runs) != 1 {
				t.Fatalf("s/%d: %d Run calls, want 1", shard, len(runs))
			}
			term := runs[0].Shard.Term
			got := ofShard(events, shard)
			if want := lifecycle(shard, term, "draining", 3*time.Second); !reflect.DeepEqual(bareAll(got), want) {
				t.Errorf("s/%d events =\n%+v\nwant\n%+v", shard, bareAll(got), want)
				continue
			}
			stopping, released, handoff := got[2], got[4], got[5]
			if stopping.Time != t0.UTC() {
				t.Errorf("s/%d: shard.stopping at %v, want %v", shard, stopping.Time, t0.UTC())
			}
			if d := released.Time.Sub(stopping.Time); handoff.Duration != d {
				t.Errorf("s/%d: handoff took %v, want %v between shard.stopping and shard.released", shard, handoff.Duration, d)
			}
		}

		draining := ofKinds(events, tenure.EventNodeDraining)
		drained := ofKinds(events, tenure.EventNodeDrained)
		if len(draining) != 1 || len(drained) != 1 {
			t.Fatalf("node.draining %v, node.drained %v: want one each", draining, drained)
		}
		if d := drained[0].Time.Sub(draining[0].Time); drained[0].Duration != d || d != 3*time.Second {
			t.Errorf("node.drained Duration %v, %v after node.draining: want both 3s", drained[0].Duration, d)
		}
		wantDelivery(t, a)
		check()
	})
}

// TestATOBS04Handoff checks a handoff between two nodes: a owns both shards, and once b starts
// every shard that moves to b goes through stopping{moved}, releasing, released{moved} and
// handoff_completed{moved} on a, its Run returning 1 s after its stop; b tries to acquire it,
// fails while a holds it, and acquires it in a greater term. Both see view.changed{Count: 2}.
func TestATOBS04Handoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		h := newObsHarness(t)
		a := h.Node("a")
		a.Runs.Script(stopAfter(time.Second))
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		synctest.Wait()
		termA := make(map[int]core.Term)
		for shard := range 2 {
			runs := a.Runs.Of(obsSpace, shard)
			if len(runs) != 1 {
				t.Fatalf("a: s/%d has %d Run calls, want 1", shard, len(runs))
			}
			termA[shard] = runs[0].Shard.Term
		}

		b := h.Start("b")
		time.Sleep(time.Minute)
		ae, be := a.RecentEvents(), b.RecentEvents()

		for _, n := range []struct {
			id     string
			events []tenure.Event
		}{{"a", ae}, {"b", be}} {
			if !slices.ContainsFunc(ofKinds(n.events, tenure.EventViewChanged), func(e tenure.Event) bool { return e.Count == 2 }) {
				t.Errorf("%s: view.changed events %+v, want one with Count 2", n.id, ofKinds(n.events, tenure.EventViewChanged))
			}
		}

		var moved []int
		for _, e := range ofKinds(ae, tenure.EventShardStopping) {
			if e.Reason == "moved" {
				moved = append(moved, e.Shard)
			}
		}
		if len(moved) == 0 {
			t.Fatalf("a: no shard.stopping{moved} in %+v", ae)
		}
		for shard := range 2 {
			if !slices.Contains(moved, shard) && len(ofShard(be, shard)) > 0 {
				t.Errorf("b: events %+v for s/%d, which stayed with a", ofShard(be, shard), shard)
			}
		}
		count2 := slices.IndexFunc(ae, func(e tenure.Event) bool {
			return e.Kind == tenure.EventViewChanged && e.Count == 2
		})
		for _, shard := range moved {
			got := ofShard(ae, shard)
			if want := lifecycle(shard, termA[shard], "moved", time.Second); !reflect.DeepEqual(bareAll(got), want) {
				t.Errorf("a: s/%d events =\n%+v\nwant\n%+v", shard, bareAll(got), want)
			}
			if s := indexOf(ae, tenure.EventShardStopping, shard); count2 < 0 || s < count2 {
				t.Errorf("a: s/%d stopped at %d, view.changed{Count: 2} at %d: want the view first", shard, s, count2)
			}

			// b's attempts: one shard.acquire_failed per Acquire a's record refused, then the grant.
			var want []tenure.Event
			for _, c := range b.FS.Calls("Acquire") {
				if c.Args[2] != shard {
					continue
				}
				want = append(want, shardEv(tenure.EventShardAcquiring, shard, 0, ""))
				var held *tenure.HeldError
				if errors.As(c.Err, &held) {
					f := shardEv(tenure.EventShardAcquireFailed, shard, 0, "")
					f.Holder, f.Duration = held.Holder, held.RetryAfter
					want = append(want, f)
					if held.Holder != obsHolderA {
						t.Errorf("b: Acquire of s/%d held by %s, want %s", shard, held.Holder, obsHolderA)
					}
				}
			}
			if count(want, tenure.EventShardAcquireFailed) == 0 {
				t.Errorf("b: no Acquire of s/%d failed while a held it", shard)
			}
			runs := b.Runs.Of(obsSpace, shard)
			if len(runs) != 1 {
				t.Fatalf("b: s/%d has %d Run calls, want 1", shard, len(runs))
			}
			termB := runs[0].Shard.Term
			if termB <= termA[shard] {
				t.Errorf("b: s/%d term %d, want above a's %d", shard, termB, termA[shard])
			}
			want = append(want, shardEv(tenure.EventShardAcquired, shard, termB, ""))
			if got := bareAll(ofShard(be, shard)); !reflect.DeepEqual(got, want) {
				t.Errorf("b: s/%d events =\n%+v\nwant\n%+v", shard, got, want)
			}
		}

		b.MustClose()
		a.MustClose()
		wantDelivery(t, a)
		wantDelivery(t, b)
		check()
	})
}

// TestAcquireCallFailed checks shard.acquire_failed for an Acquire call that fails with an error
// that is not a *HeldError, as from a store that is unreachable: the first of a's Acquire calls
// fails with "unreachable" at T0. Its shard gets shard.acquiring, then shard.acquire_failed{Holder:
// "", Duration: 2s, Err: unreachable} at T0, then shard.acquiring and shard.acquired at T0+2s,
// when the Acquire repeated after RetryPeriod is granted; the other shard is acquired at once.
// The event's log record is at DEBUG, with keys space, shard, holder, retry_after and error after
// member and holder.
func TestAcquireCallFailed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		unreachable := errors.New("unreachable")
		t0 := time.Now()
		a := newObsHarness(t, "a").Node("a")
		a.Store.Fail("Acquire", unreachable, 1)
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		time.Sleep(10 * time.Second)
		events := a.RecentEvents()

		// The log is in start order; the two calls at T0 run on two lanes, and either may take the
		// injected error.
		acq := a.FS.Calls("Acquire")
		if len(acq) != 3 {
			t.Fatalf("Acquire calls %v, want 3", acq)
		}
		if got, want := startsAt(acq, t0), secs(0, 0, 2); !reflect.DeepEqual(got, want) {
			t.Errorf("Acquire calls at %v, want %v", got, want)
		}
		i := slices.IndexFunc(acq, func(c call) bool { return c.Err != nil })
		if i < 0 || i > 1 || acq[i].Err != unreachable || acq[1-i].Err != nil || acq[2].Err != nil {
			t.Fatalf("Acquire calls %v, want one at T0 to fail with unreachable and the others granted", acq)
		}
		failed := argAt[int](acq[i].Args, 2)
		other := 1 - failed
		if argAt[int](acq[1-i].Args, 2) != other || argAt[int](acq[2].Args, 2) != failed {
			t.Errorf("Acquire calls %v, want one per shard at T0 and the failed one repeated", acq)
		}

		lifecycleKinds := []tenure.EventKind{tenure.EventShardAcquiring, tenure.EventShardAcquireFailed,
			tenure.EventShardAcquired}
		term := func(shard int) core.Term {
			runs := a.Runs.Of(obsSpace, shard)
			if len(runs) != 1 {
				t.Fatalf("s/%d has %d Run calls, want 1", shard, len(runs))
			}
			return runs[0].Shard.Term
		}
		fail := shardEv(tenure.EventShardAcquireFailed, failed, 0, "")
		fail.Duration, fail.Err = 2*time.Second, unreachable
		want := []tenure.Event{
			shardEv(tenure.EventShardAcquiring, failed, 0, ""),
			fail,
			shardEv(tenure.EventShardAcquiring, failed, 0, ""),
			shardEv(tenure.EventShardAcquired, failed, term(failed), ""),
		}
		got := ofKinds(ofShard(events, failed), lifecycleKinds...)
		if !reflect.DeepEqual(bareAll(got), want) {
			t.Fatalf("s/%d events =\n%+v\nwant\n%+v", failed, bareAll(got), want)
		}
		for i, at := range []time.Duration{0, 0, 2 * time.Second, 2 * time.Second} {
			if got[i].Time != t0.Add(at).UTC() {
				t.Errorf("s/%d: %v at %v, want T0+%v", failed, got[i].Kind, got[i].Time, at)
			}
		}
		wantOther := []tenure.Event{
			shardEv(tenure.EventShardAcquiring, other, 0, ""),
			shardEv(tenure.EventShardAcquired, other, term(other), ""),
		}
		if got := bareAll(ofKinds(ofShard(events, other), lifecycleKinds...)); !reflect.DeepEqual(got, wantOther) {
			t.Errorf("s/%d events =\n%+v\nwant\n%+v", other, got, wantOther)
		}

		wantLines := []string{fmt.Sprintf(`{"level":"DEBUG","msg":"tenure: shard acquire failed","member":"a",`+
			`"holder":"%s","space":"s","shard":%d,"holder":"","retry_after":2000000000,"error":"unreachable"}`,
			obsHolderA, failed)}
		if lines := linesWith(a.Log.Lines(), `"msg":"tenure: shard acquire failed"`); !reflect.DeepEqual(lines, wantLines) {
			t.Errorf("shard.acquire_failed records =\n%s\nwant\n%s", strings.Join(lines, "\n"), strings.Join(wantLines, "\n"))
		}

		a.MustClose()
		wantDelivery(t, a)
		check()
	})
}

// TestReleasedWithoutRun checks a grant that arrives after its shard moved away: a's Acquire
// calls are granted by the store but answered only once b has started and the view has moved one
// shard to b. a releases that shard without a run: shard.acquiring, shard.releasing and
// shard.released{Reason: ""}, with no acquired, stopping or handoff event. a's node has a store
// watch that reports nothing, so the grant reaches it only as the Acquire's answer.
func TestReleasedWithoutRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		h := newObsHarness(t)
		a := h.Node("a")
		a.Cfg.Store = a.FS.Injected()
		gate := make(chan struct{})
		a.FS.Answer("Acquire", func(ctx context.Context, args []any) ([]any, error) {
			res, err := a.FS.Forward(ctx, "Acquire", args)
			<-gate
			return res, err
		})
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		synctest.Wait()
		b := h.Start("b")
		synctest.Wait()

		a.FS.Answer("Acquire", nil)
		close(gate)
		time.Sleep(time.Minute)
		ae := a.RecentEvents()

		acq := ofKinds(b.RecentEvents(), tenure.EventShardAcquiring)
		if len(acq) == 0 {
			t.Fatal("b: no shard.acquiring")
		}
		moved := acq[0].Shard
		var term core.Term
		for _, c := range a.FS.Calls("Acquire") {
			if c.Args[2] == moved {
				term = resultAt[core.Term](c.Result, 0)
			}
		}
		want := []tenure.Event{
			shardEv(tenure.EventShardAcquiring, moved, 0, ""),
			shardEv(tenure.EventShardReleasing, moved, term, ""),
			shardEv(tenure.EventShardReleased, moved, term, ""),
		}
		if got := bareAll(ofShard(ae, moved)); term == 0 || !reflect.DeepEqual(got, want) {
			t.Errorf("a: s/%d events =\n%+v\nwant\n%+v", moved, got, want)
		}
		if n := count(ae, tenure.EventShardHandoffCompleted); n != 0 {
			t.Errorf("a: %d shard.handoff_completed events, want none", n)
		}
		if runs := a.Runs.Of(obsSpace, moved); len(runs) != 0 {
			t.Errorf("a: Run calls %v for s/%d, want none", runs, moved)
		}
		if runs := b.Runs.Of(obsSpace, moved); len(runs) != 1 {
			t.Errorf("b: %d Run calls for s/%d, want 1", len(runs), moved)
		}

		b.MustClose()
		a.MustClose()
		wantDelivery(t, a)
		wantDelivery(t, b)
		check()
	})
}

// TestStoppingClosed checks the shard events of a Close whose ctx is done when it is called: each
// shard the node owns stops with shard.stopping{Reason: "closed"} in its run's term, and nothing
// about it follows, since the node no longer steps; node.closed{deadline} is last.
func TestStoppingClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t, "a").Start("a")
		synctest.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := a.Close(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Close() = %v, want an error wrapping %v", err, context.Canceled)
		}
		synctest.Wait()
		events := a.RecentEvents()
		for shard := range 2 {
			term := a.Runs.Of(obsSpace, shard)[0].Shard.Term
			got := bareAll(ofShard(events, shard))
			want := []tenure.Event{
				shardEv(tenure.EventShardAcquiring, shard, 0, ""),
				shardEv(tenure.EventShardAcquired, shard, term, ""),
				shardEv(tenure.EventShardStopping, shard, term, "closed"),
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("s/%d events =\n%+v\nwant\n%+v", shard, got, want)
			}
		}
		last := tenure.Event{Kind: tenure.EventNodeClosed, Shard: -1, Slot: -1, Reason: "deadline"}
		if bare(events[len(events)-1]) != last {
			t.Errorf("last event %+v, want %+v", events[len(events)-1], last)
		}
		wantDelivery(t, a)
		check()
	})
}

// TestViewChangedWithoutStore checks view.changed on a node without a store, which publishes each
// merged view as its watcher takes it: one event per published view, with its Version and the
// number of its members, and no event of any other kind but the node's own.
func TestViewChangedWithoutStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		h := newObsHarness(t, "a")
		noStore := func(c *tenure.Config) { c.Store, c.Spaces = nil, nil }
		a := h.Start("a", noStore)
		synctest.Wait()
		b := h.Start("b", noStore)
		synctest.Wait()
		events := a.RecentEvents()

		want := []tenure.Event{
			{Kind: tenure.EventViewChanged, Shard: -1, Slot: -1, Version: 1, Count: 1},
			{Kind: tenure.EventViewChanged, Shard: -1, Slot: -1, Version: 2, Count: 2},
		}
		if got := bareAll(events); !reflect.DeepEqual(got, want) {
			t.Errorf("a: events =\n%+v\nwant\n%+v", got, want)
		}
		b.MustClose()
		a.MustClose()
		for _, e := range a.RecentEvents() {
			if e.Kind != tenure.EventViewChanged && !slices.Contains(nodeKinds, e.Kind) {
				t.Errorf("a: event %+v, want only view.changed and node events", e)
			}
		}
		wantDelivery(t, a)
		wantDelivery(t, b)
		check()
	})
}
