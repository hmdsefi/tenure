// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestStepCollectorInputs checks the step collector on single steps and short runs of steps: an
// input the machine ignores emits nothing, but a stepped view still emits view.changed, and a
// failed renewal the machine accepts is not ignored; shard.acquire_failed carries the step's error
// unless it is a *HeldError; an acquiring shard's Acquired with no grant emits shard.acquire_failed,
// with no holder, the machine's wait and the error, only when it carries the failed Acquire call's
// error, not for an answer with no grant nor for the one the loop owes a lost machine's acquiring
// shard; the input's events come before the actions' events; shard.releasing
// comes once per releasing term; and shard.released takes the stop reason only of the run that
// held its term.
func TestStepCollectorInputs(t *testing.T) {
	status := func(now time.Duration, shards ...core.ShardStatus) core.Status {
		return core.Status{Now: now, Session: core.SessionStateLive, Deadline: time.Minute, Slot: -1,
			ViewVersion: 3, Spaces: []core.SpaceStatus{{Name: "s", Shards: shards}}}
	}
	lost := func(now time.Duration, shards ...core.ShardStatus) core.Status {
		st := status(now, shards...)
		st.Session, st.LastFailure = core.SessionStateLost, core.CauseLost
		return st
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
	noGrant := core.Acquired{Now: 2 * time.Second, Space: "s", Shard: 0}
	callFailed := func(err error, retry time.Duration) Event {
		e := ev(EventShardAcquireFailed, 0, "")
		e.Duration, e.Err = retry, err
		return e
	}

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
			// RUN-150 maps a failed Acquire call to an Acquired with no grant, stepped with the
			// call's error: no holder is known, and the machine waits RetryPeriod (2 s here).
			name: "AcquireCallFailed",
			recs: []stepRecord{{Now: 2 * time.Second, Input: noGrant, Before: status(time.Second, acquiring),
				After: status(2*time.Second, waiting), Actions: wake, Err: boom}},
			want: []Event{callFailed(boom, 2*time.Second)},
		},
		{
			// A draining machine makes no further Acquire, so it waits for none.
			name: "AcquireCallFailedDraining",
			recs: []stepRecord{{Now: 2 * time.Second, Input: noGrant, Before: status(time.Second, acquiring),
				After: status(2*time.Second, idle), Err: boom}},
			want: []Event{callFailed(boom, 0)},
		},
		{
			// An Acquire that fails with ErrSessionLost answers with no grant, then loses the
			// session: shard.acquire_failed with the error, then session.lost.
			name: "AcquireCallSessionLost",
			recs: []stepRecord{
				{Now: 2 * time.Second, Input: noGrant, Before: status(time.Second, acquiring),
					After: status(2*time.Second, waiting), Actions: wake, Err: ErrSessionLost},
				{Now: 2 * time.Second, Input: core.SessionFailed{Now: 2 * time.Second, Cause: core.CauseLost},
					Before: status(2*time.Second, waiting), After: lost(2*time.Second, idle), Err: ErrSessionLost},
			},
			want: []Event{
				callFailed(ErrSessionLost, 2*time.Second),
				{Kind: EventSessionLost, Shard: -1, Slot: -1, Err: ErrSessionLost},
			},
		},
		{
			// A call that answers with no grant and no error did not fail.
			name: "NoGrantWithoutError",
			recs: []stepRecord{{Now: 2 * time.Second, Input: noGrant, Before: status(time.Second, acquiring),
				After: status(2*time.Second, waiting), Actions: wake}},
		},
		{
			// The Acquired with no grant that the loop steps, with no error, for an acquiring
			// shard of a lost machine, whose call's answer it discarded.
			name: "OwedAfterLoss",
			recs: []stepRecord{{Now: 2 * time.Second, Input: noGrant, Before: lost(2*time.Second, acquiring),
				After: lost(2*time.Second, idle)}},
		},
		{
			// The shard was not acquiring: the answer is not its.
			name: "AcquireCallFailedNotAcquiring",
			recs: []stepRecord{{Now: 2 * time.Second, Input: noGrant, Before: status(time.Second, idle),
				After: status(2*time.Second, waiting), Actions: wake, Err: boom}},
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
			// shard.expired and shard.lost take the place of shard.stopping, with no reason;
			// the shard.released of a lost run takes its reason, and no handoff follows.
			name: "ExpiredAndLost",
			recs: []stepRecord{
				{Now: time.Second, Input: core.Tick{Now: time.Second}, Before: status(0, owned(7)),
					After:   status(time.Second, stopping(7)),
					Actions: []core.Action{core.StopRun{Space: "s", Shard: 0, Term: 7, Reason: core.StopExpired}}},
				{Now: 2 * time.Second, Input: core.Tick{Now: 2 * time.Second}, Before: status(time.Second, owned(8)),
					After:   status(2*time.Second, stopping(8)),
					Actions: []core.Action{core.StopRun{Space: "s", Shard: 0, Term: 8, Reason: core.StopLost}}},
				{Now: 3 * time.Second, Input: core.Released{Space: "s", Shard: 0, Term: 8},
					Before: status(2*time.Second, releasing(8)), After: status(3*time.Second, idle)},
			},
			want: []Event{
				ev(EventShardExpired, 7, ""),
				ev(EventShardLost, 8, ""),
				ev(EventShardReleased, 8, "lost"),
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

// TestStepCollectorOutage checks the outage, loss and stuck events on short runs of steps: one
// session.renew_failed per failed KeepAlive the machine accepts, none for a loss another call
// reports, and session.lost once; session.expired once per deadline value, and none once the
// session is lost; session.renewed_late and session.recovered only for a renewal that makes a
// late session live, counting the failures since the last accepted renewal, in this generation;
// shard.expired and shard.lost in place of shard.stopping, and shard.released with their reason
// and no handoff, also when the run ends without a release; shard.degraded and shard.recovered;
// and one shard.stuck per run, after the events of the step's actions.
func TestStepCollectorOutage(t *testing.T) {
	const deadline = 10 * time.Second
	live, late, lost := core.SessionStateLive, core.SessionStateLate, core.SessionStateLost
	none, unr := core.CauseNone, core.CauseUnreachable
	status := func(now time.Duration, st core.SessionState, d time.Duration, fail core.Cause, sh core.ShardStatus) core.Status {
		return core.Status{Now: now, Session: st, Deadline: d, LastFailure: fail, Slot: -1,
			Spaces: []core.SpaceStatus{{Name: "s", Shards: []core.ShardStatus{sh}}}}
	}
	shard := func(state core.ShardState, reason core.StopReason, stuck bool) core.ShardStatus {
		return core.ShardStatus{State: state, Term: 7, Reason: reason, Stuck: stuck}
	}
	idle := core.ShardStatus{State: core.ShardStateIdle}
	owned := shard(core.ShardStateOwned, core.StopNone, false)
	boom := errors.New("boom")
	wall := time.Unix(1, 0)
	unreachable := func(now time.Duration) core.SessionFailed {
		return core.SessionFailed{Now: now, Cause: core.CauseUnreachable}
	}
	renewed := func(now time.Duration) core.SessionRenewed { return core.SessionRenewed{Now: now, SentAt: now} }
	tick := func(now time.Duration) core.Tick { return core.Tick{Now: now} }
	keepAlive := []core.Action{core.KeepAlive{}}
	sess := func(kind EventKind) Event { return Event{Kind: kind, Shard: -1, Slot: -1} }
	failed := func(err error) Event {
		e := sess(EventSessionRenewFailed)
		e.Err = err
		return e
	}
	sessionLost := sess(EventSessionLost)
	sessionLost.Err = ErrSessionLost
	renewedLate := func(d time.Duration, failures int) []Event {
		e, r := sess(EventSessionRenewedLate), sess(EventSessionRecovered)
		e.Duration, r.Count = d, failures
		return []Event{e, r}
	}
	ev := func(kind EventKind, reason string) Event {
		return Event{Kind: kind, Space: "s", Shard: 0, Term: 7, Slot: -1, Reason: reason}
	}
	stopRun := func(reason core.StopReason) []core.Action {
		return []core.Action{core.StopRun{Space: "s", Shard: 0, Term: 7, Reason: reason}}
	}
	ranStopped := core.RunStopped{Now: 11 * time.Second, Space: "s", Shard: 0, Term: 7}

	for _, tc := range []struct {
		name string
		recs []stepRecord
		want []Event
	}{
		{
			name: "RenewFailedAndLost",
			recs: []stepRecord{
				{Now: 2 * time.Second, Input: unreachable(2 * time.Second), SentWall: wall, Err: boom,
					Before: status(0, live, deadline, none, idle), After: status(2*time.Second, live, deadline, unr, idle)},
				{Now: 4 * time.Second, Input: unreachable(4 * time.Second), SentWall: wall, Err: boom,
					Before: status(2*time.Second, live, deadline, unr, idle), After: status(4*time.Second, live, deadline, unr, idle)},
				// A Release reports the loss: no renewal failed.
				{Now: 5 * time.Second, Input: core.SessionFailed{Now: 5 * time.Second, Cause: core.CauseLost}, Err: ErrSessionLost,
					Before: status(4*time.Second, live, deadline, unr, idle), After: status(5*time.Second, lost, deadline, core.CauseLost, idle)},
				// The session has ended: the machine does not take a later failure.
				{Now: 6 * time.Second, Input: core.SessionFailed{Now: 6 * time.Second, Cause: core.CauseLost}, SentWall: wall,
					Err: ErrSessionLost, Before: status(5*time.Second, lost, deadline, core.CauseLost, idle),
					After: status(6*time.Second, lost, deadline, core.CauseLost, idle)},
			},
			want: []Event{failed(boom), failed(boom), sessionLost},
		},
		{
			name: "ExpiredOncePerDeadline",
			recs: []stepRecord{
				{Now: deadline, Input: tick(deadline), Actions: keepAlive,
					Before: status(9*time.Second, live, deadline, none, idle), After: status(deadline, late, deadline, none, idle)},
				{Now: 12 * time.Second, Input: tick(12 * time.Second), Actions: keepAlive,
					Before: status(deadline, late, deadline, none, idle), After: status(12*time.Second, late, deadline, none, idle)},
				// An old renewal raises the deadline to one that has passed already.
				{Now: 13 * time.Second, Input: core.SessionRenewed{Now: 13 * time.Second, SentAt: 2 * time.Second},
					Before: status(12*time.Second, late, deadline, none, idle), After: status(13*time.Second, late, 12*time.Second, none, idle)},
				{Now: 14 * time.Second, Input: tick(14 * time.Second), Actions: keepAlive,
					Before: status(13*time.Second, late, 12*time.Second, none, idle), After: status(14*time.Second, late, 12*time.Second, none, idle)},
			},
			want: []Event{sess(EventSessionExpired), sess(EventSessionExpired)},
		},
		{
			name: "RenewedLate",
			recs: []stepRecord{
				{Gen: 1, Now: 2 * time.Second, Input: unreachable(2 * time.Second), SentWall: wall, Err: boom,
					Before: status(0, live, deadline, none, idle), After: status(2*time.Second, live, deadline, unr, idle)},
				// Renewed before the deadline: no event, and the count starts again.
				{Gen: 1, Now: 4 * time.Second, Input: renewed(4 * time.Second),
					Before: status(2*time.Second, live, deadline, unr, idle), After: status(4*time.Second, live, 14*time.Second, none, idle)},
				{Gen: 1, Now: 6 * time.Second, Input: unreachable(6 * time.Second), SentWall: wall, Err: boom,
					Before: status(4*time.Second, live, 14*time.Second, none, idle), After: status(6*time.Second, live, 14*time.Second, unr, idle)},
				{Gen: 1, Now: 16 * time.Second, Input: renewed(16 * time.Second),
					Before: status(15*time.Second, late, 14*time.Second, unr, idle), After: status(16*time.Second, live, 26*time.Second, none, idle)},
				// A failure of generation 1 that no renewal followed does not count in generation 2.
				{Gen: 1, Now: 28 * time.Second, Input: unreachable(28 * time.Second), SentWall: wall, Err: boom,
					Before: status(27*time.Second, late, 26*time.Second, none, idle), After: status(28*time.Second, late, 26*time.Second, unr, idle)},
				{Gen: 2, Now: 40 * time.Second, Input: renewed(40 * time.Second),
					Before: status(39*time.Second, late, 30*time.Second, none, idle), After: status(40*time.Second, live, 50*time.Second, none, idle)},
			},
			want: slices.Concat([]Event{failed(boom), failed(boom), sess(EventSessionExpired)}, renewedLate(2*time.Second, 1),
				[]Event{sess(EventSessionExpired), failed(boom), sess(EventSessionExpired)}, renewedLate(10*time.Second, 0)),
		},
		{
			name: "ExpiredRunReleased",
			recs: []stepRecord{
				{Now: deadline, Input: tick(deadline), Actions: stopRun(core.StopExpired),
					Before: status(9*time.Second, live, deadline, none, owned),
					After:  status(deadline, late, deadline, none, shard(core.ShardStateExpired, core.StopExpired, false))},
				{Now: 11 * time.Second, Input: ranStopped, Actions: []core.Action{core.Release{Space: "s", Shard: 0, Term: 7}},
					Before: status(deadline, late, deadline, none, shard(core.ShardStateExpired, core.StopExpired, false)),
					After:  status(11*time.Second, late, deadline, none, shard(core.ShardStateReleasing, core.StopNone, false))},
				{Now: 12 * time.Second, Input: core.Released{Now: 12 * time.Second, Space: "s", Shard: 0, Term: 7},
					Before: status(11*time.Second, late, deadline, none, shard(core.ShardStateReleasing, core.StopNone, false)),
					After:  status(12*time.Second, late, deadline, none, idle)},
			},
			want: []Event{sess(EventSessionExpired), ev(EventShardExpired, ""), ev(EventShardReleasing, ""),
				ev(EventShardReleased, "expired")},
		},
		{
			// The record names another holder: the run ends without a release.
			name: "LostRunWithoutRelease",
			recs: []stepRecord{
				{Now: 2 * time.Second, Input: core.HolderChanged{Now: 2 * time.Second, Space: "s", Shard: 0, Holder: "b/2", Term: 8},
					Actions: stopRun(core.StopLost), Before: status(time.Second, live, deadline, none, owned),
					After: status(2*time.Second, live, deadline, none, shard(core.ShardStateLost, core.StopLost, false))},
				{Now: 11 * time.Second, Input: ranStopped,
					Before: status(2*time.Second, live, 20*time.Second, none, shard(core.ShardStateLost, core.StopLost, false)),
					After:  status(11*time.Second, live, 20*time.Second, none, idle)},
			},
			want: []Event{ev(EventShardLost, ""), ev(EventShardReleased, "lost")},
		},
		{
			name: "DegradedAndRecovered",
			recs: []stepRecord{
				{Now: deadline, Input: tick(deadline), Actions: []core.Action{core.Degrade{Space: "s", Shard: 0, Term: 7}},
					Before: status(9*time.Second, live, deadline, none, owned),
					After:  status(deadline, late, deadline, none, shard(core.ShardStateDegraded, core.StopNone, false))},
				{Now: 13 * time.Second, Input: core.HolderChanged{Now: 13 * time.Second, Space: "s", Shard: 0, Holder: "a/1", Term: 7},
					Actions: []core.Action{core.Restore{Space: "s", Shard: 0, Term: 7}},
					Before:  status(12*time.Second, live, 22*time.Second, none, shard(core.ShardStateDegraded, core.StopNone, false)),
					After:   status(13*time.Second, live, 22*time.Second, none, owned)},
			},
			want: []Event{sess(EventSessionExpired), ev(EventShardDegraded, ""), ev(EventShardRecovered, "")},
		},
		{
			name: "StuckOncePerRun",
			recs: []stepRecord{
				{Now: 11 * time.Second, Input: tick(11 * time.Second),
					Before: status(9*time.Second, live, 20*time.Second, none, shard(core.ShardStateStopping, core.StopDraining, false)),
					After:  status(11*time.Second, live, 20*time.Second, none, shard(core.ShardStateStopping, core.StopDraining, true))},
				{Now: 12 * time.Second, Input: tick(12 * time.Second), Actions: keepAlive,
					Before: status(11*time.Second, live, 20*time.Second, none, shard(core.ShardStateStopping, core.StopDraining, true)),
					After:  status(12*time.Second, live, 20*time.Second, none, shard(core.ShardStateStopping, core.StopDraining, true))},
			},
			want: []Event{ev(EventShardStuck, "draining")},
		},
		{
			// A step that marks one run stuck and acquires another shard: shard.stuck comes after
			// the actions' events.
			name: "StuckAfterActions",
			recs: []stepRecord{
				{Now: 11 * time.Second, Input: tick(11 * time.Second),
					Actions: []core.Action{core.Acquire{Space: "s", Shard: 1}},
					Before: core.Status{Now: 9 * time.Second, Session: live, Deadline: 20 * time.Second, Slot: -1,
						Spaces: []core.SpaceStatus{{Name: "s", Shards: []core.ShardStatus{
							shard(core.ShardStateStopping, core.StopMoved, false), idle}}}},
					After: core.Status{Now: 11 * time.Second, Session: live, Deadline: 20 * time.Second, Slot: -1,
						Spaces: []core.SpaceStatus{{Name: "s", Shards: []core.ShardStatus{
							shard(core.ShardStateStopping, core.StopMoved, true), {State: core.ShardStateAcquiring}}}}}},
			},
			want: []Event{{Kind: EventShardAcquiring, Space: "s", Shard: 1, Slot: -1}, ev(EventShardStuck, "moved")},
		},
		{
			// A loss reported by the KeepAlive, then by a Release, in a step that is not ignored
			// since a run turns stuck, past the deadline: session.lost comes once, and the
			// deadline of a lost session emits no session.expired.
			name: "LostOnceNoExpiry",
			recs: []stepRecord{
				{Now: 2 * time.Second, Input: core.SessionFailed{Now: 2 * time.Second, Cause: core.CauseLost}, SentWall: wall,
					Err: ErrSessionLost, Actions: stopRun(core.StopLost), Before: status(time.Second, live, deadline, none, owned),
					After: status(2*time.Second, lost, deadline, core.CauseLost, shard(core.ShardStateLost, core.StopLost, false))},
				{Now: 12 * time.Second, Input: core.SessionFailed{Now: 12 * time.Second, Cause: core.CauseLost}, Err: ErrSessionLost,
					Before: status(2*time.Second, lost, deadline, core.CauseLost, shard(core.ShardStateLost, core.StopLost, false)),
					After:  status(12*time.Second, lost, deadline, core.CauseLost, shard(core.ShardStateLost, core.StopLost, true))},
			},
			want: []Event{failed(ErrSessionLost), sessionLost, ev(EventShardLost, ""), ev(EventShardStuck, "lost")},
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

// TestStepCollectorSlots checks slot.claimed and slot.released from the held slot before and after
// a step, with Reason "lost" for a hold that no SlotReleased ended, as at a lost session, and else
// "retired", or "draining" once the drain has started; and that noSlot emits slot.standby once
// per standby period, which a claimed slot ends.
func TestStepCollectorSlots(t *testing.T) {
	var got []Event
	c := newStepCollector(nil, func(e Event) uint64 {
		got = append(got, e)
		return uint64(len(got))
	})
	held := func(slot int) core.Status {
		return core.Status{Session: core.SessionStateLive, Deadline: time.Minute, Slot: slot}
	}
	lost := held(-1)
	lost.Session = core.SessionStateLost
	slotEv := func(kind EventKind, slot int, reason string) Event {
		return Event{Kind: kind, Shard: -1, Slot: slot, Reason: reason}
	}
	standby := slotEv(EventSlotStandby, -1, "")

	c.noSlot()
	c.noSlot()
	c.collect(stepRecord{Input: core.SlotClaimed{Slot: 1}, Before: held(-1), After: held(1)})
	c.collect(stepRecord{Input: core.SessionFailed{Cause: core.CauseLost}, Before: held(1), After: lost})
	c.noSlot()
	c.collect(stepRecord{Input: core.SlotClaimed{Slot: 0}, Before: held(-1), After: held(0)})
	c.collect(stepRecord{Input: core.SlotReleased{Slot: 1}, Before: held(0), After: held(0)})
	c.collect(stepRecord{Input: core.SlotReleased{Slot: 0}, Before: held(0), After: held(-1)})
	c.noSlot()
	c.collect(stepRecord{Input: core.SlotClaimed{Slot: 0}, Before: held(-1), After: held(0)})
	c.draining.Store(true)
	c.collect(stepRecord{Input: core.SlotReleased{Slot: 0}, Before: held(0), After: held(-1)})

	want := []Event{
		standby,
		slotEv(EventSlotClaimed, 1, ""),
		{Kind: EventSessionLost, Shard: -1, Slot: -1}, // the loss comes before the slot events
		slotEv(EventSlotReleased, 1, "lost"),
		standby,
		slotEv(EventSlotClaimed, 0, ""),
		slotEv(EventSlotReleased, 0, "retired"),
		standby,
		slotEv(EventSlotClaimed, 0, ""),
		slotEv(EventSlotReleased, 0, "draining"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("events =\n%+v\nwant\n%+v", got, want)
	}
}
