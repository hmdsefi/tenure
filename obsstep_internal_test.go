// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestStepCollectorInputs checks the step collector on single steps and short runs of steps: an
// input the machine ignores emits nothing, but a stepped view still emits view.changed, and a
// failed renewal the machine accepts is not ignored; shard.acquire_failed carries the step's error
// unless it is a *HeldError; the input's events come before the actions' events; shard.releasing
// comes once per releasing term; and shard.released takes the stop reason only of the run that
// held its term.
func TestStepCollectorInputs(t *testing.T) {
	status := func(now time.Duration, shards ...core.ShardStatus) core.Status {
		return core.Status{Now: now, Session: core.SessionStateLive, Deadline: time.Minute, Slot: -1,
			ViewVersion: 3, Spaces: []core.SpaceStatus{{Name: "s", Shards: shards}}}
	}
	idle := core.ShardStatus{State: core.ShardStateIdle}
	acquiring := core.ShardStatus{State: core.ShardStateAcquiring}
	waiting := core.ShardStatus{State: core.ShardStateIdle, RetryAt: 4 * time.Second}
	owned := func(term core.Term) core.ShardStatus {
		return core.ShardStatus{State: core.ShardStateOwned, Term: term}
	}
	stopping := func(term core.Term) core.ShardStatus {
		return core.ShardStatus{State: core.ShardStateStopping, Term: term, Reason: core.StopMoved}
	}
	releasing := func(term core.Term) core.ShardStatus {
		return core.ShardStatus{State: core.ShardStateReleasing, Term: term}
	}

	failed := core.AcquireFailed{Now: 2 * time.Second, Space: "s", Shard: 0, Holder: "b/2", RetryAfter: 3 * time.Second}
	boom := errors.New("boom")
	wake := []core.Action{core.Wake{At: 4 * time.Second}}
	ev := func(kind EventKind, term core.Term, reason string) Event {
		return Event{Kind: kind, Space: "s", Shard: 0, Slot: -1, Term: term, Reason: reason}
	}
	acquireFailed := func(err error) Event {
		e := ev(EventShardAcquireFailed, 0, "")
		e.Holder, e.Duration, e.Err = "b/2", 3*time.Second, err
		return e
	}
	handoff := func(term core.Term, took time.Duration) Event {
		e := ev(EventShardHandoffCompleted, term, "moved")
		e.Duration = took
		return e
	}
	released := core.Released{Now: 5 * time.Second, Space: "s", Shard: 0, Term: 7}
	stop7 := core.StopRun{Space: "s", Shard: 0, Term: 7, Reason: core.StopMoved}
	release := func(term core.Term) core.Action { return core.Release{Space: "s", Shard: 0, Term: term} }
	acquire := core.Acquire{Space: "s", Shard: 0}

	for _, tc := range []struct {
		name string
		recs []stepRecord
		want []Event
	}{
		{
			// The answer changes nothing but the clock: not even an acquiring shard's
			// shard.acquire_failed comes out.
			name: "IgnoredAnswer",
			recs: []stepRecord{{Now: 2 * time.Second, Input: failed, Before: status(time.Second, acquiring),
				After: status(2*time.Second, acquiring), Actions: wake, Err: boom}},
		},
		{
			// The step changes the shard, but it was not acquiring: the answer is not its.
			name: "AnswerNotAcquiring",
			recs: []stepRecord{{Now: 2 * time.Second, Input: failed, Before: status(time.Second, idle),
				After: status(2*time.Second, waiting), Actions: wake, Err: boom}},
		},
		{
			name: "IgnoredView",
			recs: []stepRecord{{Now: 2 * time.Second, Before: status(time.Second, idle), After: status(2*time.Second, idle),
				Input: core.ViewChanged{View: core.View{Version: 2, Members: []core.Member{{ID: "a"}, {ID: "b"}}}}}},
			want: []Event{{Kind: EventViewChanged, Shard: -1, Slot: -1, Version: 2, Count: 2}},
		},
		{
			name: "HeldError",
			recs: []stepRecord{{Now: 2 * time.Second, Input: failed, Before: status(time.Second, acquiring),
				After: status(2*time.Second, waiting), Actions: wake, Err: &HeldError{Holder: "b/2", RetryAfter: 3 * time.Second}}},
			want: []Event{acquireFailed(nil)},
		},
		{
			name: "OtherError",
			recs: []stepRecord{{Now: 2 * time.Second, Input: failed, Before: status(time.Second, acquiring),
				After: status(2*time.Second, waiting), Actions: wake, Err: boom}},
			want: []Event{acquireFailed(boom)},
		},
		{
			// A shard released and acquired again in one step: shard.released and its handoff,
			// which are the input's, come before the Acquire's shard.acquiring.
			name: "InputBeforeActions",
			recs: []stepRecord{
				{Now: time.Second, Input: core.Tick{Now: time.Second}, Before: status(0, owned(7)),
					After: status(time.Second, stopping(7)), Actions: []core.Action{stop7}},
				{Now: 5 * time.Second, Input: released, Before: status(4*time.Second, releasing(7)),
					After: status(5*time.Second, acquiring), Actions: []core.Action{acquire}},
			},
			want: []Event{
				ev(EventShardStopping, 7, "moved"),
				ev(EventShardReleased, 7, "moved"),
				handoff(7, 4*time.Second),
				ev(EventShardAcquiring, 0, ""),
			},
		},
		{
			// A Release repeated for the same term emits nothing; one for a new term emits
			// shard.releasing again.
			name: "ReleasingOncePerTerm",
			recs: []stepRecord{
				{Now: time.Second, Input: core.Tick{Now: time.Second}, Before: status(0, idle),
					After: status(time.Second, releasing(7)), Actions: []core.Action{release(7)}},
				{Now: 3 * time.Second, Input: core.Tick{Now: 3 * time.Second}, Before: status(time.Second, releasing(7)),
					After: status(3*time.Second, releasing(7)), Actions: []core.Action{release(7)}},
				{Now: 4 * time.Second, Input: core.Tick{Now: 4 * time.Second}, Before: status(3*time.Second, releasing(7)),
					After: status(4*time.Second, releasing(9)), Actions: []core.Action{release(9)}},
			},
			want: []Event{
				ev(EventShardReleasing, 7, ""),
				ev(EventShardReleased, 7, ""),
				ev(EventShardReleasing, 9, ""),
			},
		},
		{
			// Term 7's run was stopped as moved; term 9, granted later, is released without a
			// run, so its shard.released has no reason and no handoff follows.
			name: "ReleasedWithoutRunAfterRun",
			recs: []stepRecord{
				{Now: time.Second, Input: core.Tick{Now: time.Second}, Before: status(0, owned(7)),
					After: status(time.Second, stopping(7)), Actions: []core.Action{stop7}},
				{Now: 2 * time.Second, Input: core.Released{Space: "s", Shard: 0, Term: 7},
					Before: status(time.Second, releasing(7)), After: status(2*time.Second, idle)},
				{Now: 3 * time.Second, Input: core.Acquired{Space: "s", Shard: 0, Term: 9},
					Before: status(2*time.Second, acquiring), After: status(3*time.Second, releasing(9)),
					Actions: []core.Action{release(9)}},
				{Now: 4 * time.Second, Input: core.Released{Space: "s", Shard: 0, Term: 9},
					Before: status(3*time.Second, releasing(9)), After: status(4*time.Second, idle)},
			},
			want: []Event{
				ev(EventShardStopping, 7, "moved"),
				ev(EventShardReleased, 7, "moved"),
				handoff(7, time.Second),
				ev(EventShardReleasing, 9, ""),
				ev(EventShardReleased, 9, ""),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []Event
			c := newStepCollector([]Space{{Name: "s", Shards: 1}}, func(e Event) uint64 {
				got = append(got, e)
				return uint64(len(got))
			})
			for _, rec := range tc.recs {
				c.collect(rec)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("events =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}
}

// TestStepCollectorIgnored checks which steps count as ignored: a step that changes nothing but
// the clock and returns no action but a Wake, except a failed renewal the machine accepts, which
// may change nothing that Status shows.
func TestStepCollectorIgnored(t *testing.T) {
	late := core.Status{Now: 2 * time.Second, Session: core.SessionStateLate, Deadline: time.Second,
		LastFailure: core.CauseUnreachable, Slot: -1}
	later := late
	later.Now = 4 * time.Second
	closing := late
	closing.Session = core.SessionStateClosing
	closingLater := closing
	closingLater.Now = 4 * time.Second
	unreachable := core.SessionFailed{Now: 4 * time.Second, Cause: core.CauseUnreachable}

	for _, tc := range []struct {
		name string
		rec  stepRecord
		want bool
	}{
		{"Tick", stepRecord{Input: core.Tick{Now: 4 * time.Second}, Before: late, After: later,
			Actions: []core.Action{core.Wake{At: 6 * time.Second}}}, true},
		{"Action", stepRecord{Input: core.Tick{Now: 4 * time.Second}, Before: late, After: later,
			Actions: []core.Action{core.KeepAlive{}}}, false},
		{"StatusChanged", stepRecord{Input: core.Tick{Now: 4 * time.Second}, Before: late, After: closingLater}, false},
		{"RepeatedFailure", stepRecord{Input: unreachable, Before: late, After: later}, false},
		{"FailureWhileClosing", stepRecord{Input: unreachable, Before: closing, After: closingLater}, true},
		{"FailureWithoutCause", stepRecord{Input: core.SessionFailed{Now: 4 * time.Second}, Before: late, After: later}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ignored(tc.rec); got != tc.want {
				t.Errorf("ignored(%+v) = %v, want %v", tc.rec, got, tc.want)
			}
		})
	}
}
