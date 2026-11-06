// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases

import (
	"slices"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// handoffCases returns the handover and stale-input sequences, and what repeats until answered.
func handoffCases() []Case {
	return []Case{
		{
			// A grant the machine can no longer run: the view takes the shard away while its Acquire
			// is out, so the answer is released with no run, and the store's answer ends it.
			ID:     "AT-MCH-13",
			Config: C(S("s", 1)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vm(1, "m1"), Ms(2000), core.Acquire{Space: "s", Shard: 0}),
				ViewAt(Ms(300), Vm(2, "m2"), Ms(2000)),
				{
					Input: core.Acquired{Now: Ms(400), Space: "s", Term: 9},
					Want:  want(Ms(2000), core.Release{Space: "s", Term: 9}),
				},
				{Input: core.Released{Now: Ms(500), Space: "s", Term: 9}, Want: want(Ms(2000))},
			},
		},
		{
			// The view takes away the one shard this member runs. The run is told to stop, and
			// the shard the member never had stays idle.
			ID:     "AT-MCH-14",
			Config: C(S("s", 2)),
			Steps:  movedSteps(),
		},
		{
			// The move goes on to its end: the run reports that it stopped, the shard releases
			// its term, the store answers, and no timer of the shard stays armed.
			ID:     "AT-MCH-15",
			Config: C(S("s", 2)),
			Steps: append(movedSteps(),
				Step{
					Input: core.RunStopped{Now: Ms(1500), Space: "s", Shard: 0, Term: 7},
					Want:  want(Ms(2000), core.Release{Space: "s", Shard: 0, Term: 7}),
				},
				Step{Input: core.Released{Now: Ms(1600), Space: "s", Shard: 0, Term: 7}, Want: want(Ms(2000))},
				Step{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(4000), core.KeepAlive{})},
			),
		},
		{
			// Another holder is on the record of an owned shard. The run is told to stop, its
			// RunStopped releases nothing, and the shard, still wanted, asks again and is told who
			// has the record.
			ID:     "AT-MCH-16",
			Config: C(S("s", 1)),
			Steps: append(Boot(7, S("s", 1)),
				Step{
					Input: core.HolderChanged{Now: Ms(1000), Space: "s", Holder: "h2", Term: 8},
					Want:  want(Ms(2000), core.StopRun{Space: "s", Term: 7, Reason: core.StopLost}),
				},
				Step{
					Input: core.RunStopped{Now: Ms(1500), Space: "s", Term: 7},
					Want:  want(Ms(2000), core.Acquire{Space: "s"}),
				},
				Step{
					Input: core.AcquireFailed{Now: Ms(1600), Space: "s", Holder: "h2", RetryAfter: Ms(3000)},
					Want:  want(Ms(2000)),
				},
			),
		},
		{
			// Two failed answers: a RetryAfter above LeaseDuration at @3.3 waits only LeaseDuration,
			// until @18.3, and one below RetryPeriod at @3.4 waits RetryPeriod, until @5.4. Each
			// shard asks again at exactly its own reading.
			ID:     "AT-MCH-17",
			Config: C(S("s", 2)),
			Steps: slices.Concat(
				[]Step{
					{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
					Renew(Ms(100), Ms(0), Ms(2000)),
					ViewAt(Ms(200), Vm(1, "m1"), Ms(2000), core.Acquire{Space: "s", Shard: 0}, core.Acquire{Space: "s", Shard: 1}),
					{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(4000), core.KeepAlive{})},
					Renew(Ms(2100), Ms(2000), Ms(4000)),
					{
						Input: core.AcquireFailed{Now: Ms(3300), Space: "s", Shard: 0, Holder: "h2", RetryAfter: Ms(60000)},
						Want:  want(Ms(4000)),
					},
					{
						Input: core.AcquireFailed{Now: Ms(3400), Space: "s", Shard: 1, Holder: "h3", RetryAfter: Ms(100)},
						Want:  want(Ms(4000)),
					},
					{Input: core.Tick{Now: Ms(4000)}, Want: want(Ms(5400), core.KeepAlive{})},
					Renew(Ms(4100), Ms(4000), Ms(5400)),
					{Input: core.Tick{Now: Ms(5400)}, Want: want(Ms(6000), core.Acquire{Space: "s", Shard: 1})},
					Grant(Ms(5500), "s", 1, 9, Ms(14000), Ms(6000)),
				},
				beats(Ms(6000), Ms(18000), Ms(18300)),
				[]Step{{Input: core.Tick{Now: Ms(18300)}, Want: want(Ms(20000), core.Acquire{Space: "s", Shard: 0})}},
			),
		},
		{
			// An AcquireFailed that names no holder and asks for no wait retries after RetryPeriod,
			// and the retry is granted.
			ID:     "AT-MCH-19 (a)",
			Config: C(S("s", 1)),
			Steps: retrySteps(Step{
				Input: core.AcquireFailed{Now: Ms(200), Space: "s"},
				Want:  want(Ms(2000)),
			}),
		},
		{
			// An Acquired with term 0 at @0.2 retries at @2.2, and the retry is granted.
			ID:     "AT-MCH-19 (b)",
			Config: C(S("s", 1)),
			Steps: retrySteps(Step{
				Input: core.Acquired{Now: Ms(200), Space: "s"},
				Want:  want(Ms(2000)),
			}),
		},
		{
			// Only the run's own term moves the shard, and only in the state that waits for it: a
			// RunStopped for an owned shard, other terms, a repeated RunStopped and a repeated
			// Released all do nothing.
			ID:     "AT-MCH-20",
			Config: C(S("s", 1)),
			Steps: append(Boot(7, S("s", 1)),
				Step{Input: core.RunStopped{Now: Ms(500), Space: "s", Term: 7}, Want: want(Ms(2000))},
				Step{Input: core.Released{Now: Ms(600), Space: "s", Term: 7}, Want: want(Ms(2000))},
				ViewAt(Ms(1000), Vm(2, "m2"), Ms(2000), core.StopRun{Space: "s", Term: 7, Reason: core.StopMoved}),
				Step{Input: core.RunStopped{Now: Ms(1100), Space: "s", Term: 6}, Want: want(Ms(2000))},
				Step{Input: core.RunStopped{Now: Ms(1200), Space: "s", Term: 8}, Want: want(Ms(2000))},
				Step{Input: core.Released{Now: Ms(1300), Space: "s", Term: 7}, Want: want(Ms(2000))},
				Step{
					Input: core.RunStopped{Now: Ms(1500), Space: "s", Term: 7},
					Want:  want(Ms(2000), core.Release{Space: "s", Term: 7}),
				},
				Step{Input: core.RunStopped{Now: Ms(1600), Space: "s", Term: 7}, Want: want(Ms(2000))},
				Step{Input: core.Released{Now: Ms(1700), Space: "s", Term: 6}, Want: want(Ms(2000))},
				Step{Input: core.Released{Now: Ms(1800), Space: "s", Term: 8}, Want: want(Ms(2000))},
				Step{Input: core.Released{Now: Ms(1900), Space: "s", Term: 7}, Want: want(Ms(2000))},
				Step{Input: core.Released{Now: Ms(1950), Space: "s", Term: 7}, Want: want(Ms(2000))},
			),
		},
		{
			// While a shard releases term 50, its record turns out to name this member in term 60.
			// That term goes out at once, the answer to the old term does nothing, and the answer
			// to the new one ends it.
			ID:     "AT-MCH-22",
			Config: C(S("s", 1)),
			Steps: append(Boot(50, S("s", 1)),
				ViewAt(Ms(1000), Vm(2, "m2"), Ms(2000), core.StopRun{Space: "s", Term: 50, Reason: core.StopMoved}),
				Step{
					Input: core.RunStopped{Now: Ms(1500), Space: "s", Term: 50},
					Want:  want(Ms(2000), core.Release{Space: "s", Term: 50}),
				},
				Step{
					Input: core.HolderChanged{Now: Ms(1600), Space: "s", Holder: "h1", Term: 60},
					Want:  want(Ms(2000), core.Release{Space: "s", Term: 60}),
				},
				Step{Input: core.Released{Now: Ms(1700), Space: "s", Term: 50}, Want: want(Ms(2000))},
				Step{Input: core.Released{Now: Ms(1800), Space: "s", Term: 60}, Want: want(Ms(2000))},
			),
		},
		{
			// Release repeats every RetryPeriod until it is answered, also once the session is
			// late, and the answer ends the repeats.
			ID:     "AT-MCH-23 (a)",
			Config: C(S("s", 1)),
			Steps: append(Boot(7, S("s", 1)),
				ViewAt(Ms(1000), Vm(2, "m2"), Ms(2000), core.StopRun{Space: "s", Term: 7, Reason: core.StopMoved}),
				Step{
					Input: core.RunStopped{Now: Ms(1500), Space: "s", Term: 7},
					Want:  want(Ms(2000), core.Release{Space: "s", Term: 7}),
				},
				Step{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(3500), core.KeepAlive{})},
				Step{Input: core.Tick{Now: Ms(3500)}, Want: want(Ms(4000), core.Release{Space: "s", Term: 7})},
				Step{Input: core.Tick{Now: Ms(4000)}, Want: want(Ms(5500), core.KeepAlive{})},
				Step{Input: core.Tick{Now: Ms(5500)}, Want: want(Ms(6000), core.Release{Space: "s", Term: 7})},
				Step{Input: core.Tick{Now: Ms(6000)}, Want: want(Ms(7500), core.KeepAlive{})},
				Step{Input: core.Tick{Now: Ms(7500)}, Want: want(Ms(8000), core.Release{Space: "s", Term: 7})},
				Step{Input: core.Tick{Now: Ms(8000)}, Want: want(Ms(9500), core.KeepAlive{})},
				Step{Input: core.Tick{Now: Ms(9500)}, Want: want(Ms(10000), core.Release{Space: "s", Term: 7})},
				Step{Input: core.Tick{Now: Ms(10000)}, Want: want(Ms(11500), core.KeepAlive{})},
				Step{Input: core.Tick{Now: Ms(11500)}, Want: want(Ms(12000), core.Release{Space: "s", Term: 7})},
				Step{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(13500), core.KeepAlive{})},
				Step{Input: core.Released{Now: Ms(12500), Space: "s", Term: 7}, Want: want(Ms(14000))},
				Step{Input: core.Tick{Now: Ms(13500)}, Want: want(Ms(14000))},
			),
		},
		{
			// KeepAlive repeats every RetryPeriod through an outage, also once the session is
			// late, until a renewal is accepted and brings the session back to live.
			ID:     "AT-MCH-23 (b)",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				[]Step{
					{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
					Renew(Ms(100), Ms(0), Ms(2000)),
				},
				Outage(Ms(2000), Ms(11000), Ms(10000)),
				[]Step{
					Renew(Ms(10500), Ms(10000), Ms(12000)),
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
				},
			),
		},
		{
			// An answer that no Acquire is waiting for does nothing: not before the view, not a
			// second answer for an owned shard in the same term or a later one, and not for a shard
			// the machine does not have. The highest started term is per shard, so another shard
			// can start in the same term.
			ID:     "AT-MCH-24",
			Config: C(S("s", 2)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				{Input: core.Acquired{Now: Ms(150), Space: "s", Shard: 0, Term: 9}, Want: want(Ms(2000))},
				ViewAt(Ms(200), Vm(1, "m1"), Ms(2000), core.Acquire{Space: "s", Shard: 0}, core.Acquire{Space: "s", Shard: 1}),
				Grant(Ms(300), "s", 0, 7, Ms(10000), Ms(2000)),
				{Input: core.Acquired{Now: Ms(350), Space: "s", Shard: 0, Term: 7}, Want: want(Ms(2000))},
				{Input: core.Acquired{Now: Ms(400), Space: "s", Shard: 0, Term: 8}, Want: want(Ms(2000))},
				{Input: core.Acquired{Now: Ms(450), Space: "other", Shard: 0, Term: 8}, Want: want(Ms(2000))},
				{Input: core.Acquired{Now: Ms(460), Space: "s", Shard: 2, Term: 8}, Want: want(Ms(2000))},
				{Input: core.Acquired{Now: Ms(470), Space: "s", Shard: -1, Term: 8}, Want: want(Ms(2000))},
				Grant(Ms(500), "s", 1, 7, Ms(10000), Ms(2000)),
			},
		},
		{
			// A run told to stop at @1 goes on running. At @11 it has outlived RenewDeadline and is
			// stuck, on a live session, and the shard still waits: its Release comes only with the
			// RunStopped at @11.5.
			ID:     "AT-MCH-29",
			Config: C(S("s", 1)),
			Steps: append(Boot(7, S("s", 1)),
				ViewAt(Ms(1000), Vm(2, "m2"), Ms(2000), core.StopRun{Space: "s", Term: 7, Reason: core.StopMoved}),
				Step{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(4000), core.KeepAlive{})},
				Renew(Ms(2100), Ms(2000), Ms(4000)),
				Step{Input: core.Tick{Now: Ms(4000)}, Want: want(Ms(6000), core.KeepAlive{})},
				Renew(Ms(4100), Ms(4000), Ms(6000)),
				Step{Input: core.Tick{Now: Ms(6000)}, Want: want(Ms(8000), core.KeepAlive{})},
				Renew(Ms(6100), Ms(6000), Ms(8000)),
				Step{Input: core.Tick{Now: Ms(8000)}, Want: want(Ms(10000), core.KeepAlive{})},
				Renew(Ms(8100), Ms(8000), Ms(10000)),
				Step{Input: core.Tick{Now: Ms(10000)}, Want: want(Ms(11000), core.KeepAlive{})},
				Renew(Ms(10100), Ms(10000), Ms(11000)),
				Step{Input: core.Tick{Now: Ms(10999)}, Want: want(Ms(11000))},
				Step{Input: core.Tick{Now: Ms(11000)}, Want: want(Ms(12000))},
				Step{
					Input: core.RunStopped{Now: Ms(11500), Space: "s", Term: 7},
					Want:  want(Ms(12000), core.Release{Space: "s", Term: 7}),
				},
			),
		},
		{
			// The reply to a grant was lost. The record says this member holds shard 0 in term 100,
			// so the shard adopts the grant, ending the wait AcquireFailed left it with. Later the
			// record of shard 1, which this member does not want, names it too, and that leftover
			// is released with no run.
			ID:     "AT-MCH-51",
			Config: C(S("s", 2)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vm(1, "m1", "m2"), Ms(2000), core.Acquire{Space: "s", Shard: 0}),
				{
					Input: core.AcquireFailed{Now: Ms(300), Space: "s", Shard: 0, Holder: "h2", RetryAfter: Ms(1000)},
					Want:  want(Ms(2000)),
				},
				{
					Input: core.HolderChanged{Now: Ms(400), Space: "s", Shard: 0, Holder: "h1", Term: 100},
					Want:  want(Ms(2000), core.StartRun{Space: "s", Shard: 0, Term: 100, Deadline: Ms(10000)}),
				},
				{
					Input: core.HolderChanged{Now: Ms(500), Space: "s", Shard: 1, Holder: "h1", Term: 90},
					Want:  want(Ms(2000), core.Release{Space: "s", Shard: 1, Term: 90}),
				},
				{Input: core.Released{Now: Ms(600), Space: "s", Shard: 1, Term: 90}, Want: want(Ms(2000))},
			},
		},
	}
}

// retrySteps are the steps of AT-MCH-19: answer tells the Acquire of the one shard at @0.2 that there
// is no grant, so the shard asks again at @2.2, and the second answer is a grant of term 7.
func retrySteps(answer Step) []Step {
	return []Step{
		{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
		Renew(Ms(100), Ms(0), Ms(2000)),
		ViewAt(Ms(200), Vm(1, "m1"), Ms(2000), core.Acquire{Space: "s"}),
		answer,
		{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(2200), core.KeepAlive{})},
		Renew(Ms(2100), Ms(2000), Ms(2200)),
		{Input: core.Tick{Now: Ms(2200)}, Want: want(Ms(4000), core.Acquire{Space: "s"})},
		Grant(Ms(2300), "s", 0, 7, Ms(12000), Ms(4000)),
	}
}

// beats are the KeepAlive and its accepted renewal, 100 ms later, every RetryPeriod from from through
// to, on a machine with nothing else due. last is the armed Wake after to's renewal.
func beats(from, to, last time.Duration) []Step {
	var steps []Step
	for now := from; now <= to; now += core.DefaultTiming.RetryPeriod {
		wake := now + core.DefaultTiming.RetryPeriod
		if now == to {
			wake = last
		}
		steps = append(steps,
			Step{Input: core.Tick{Now: now}, Want: want(wake, core.KeepAlive{})},
			Renew(now+Ms(100), now, wake),
		)
	}
	return steps
}

// movedSteps are the steps of AT-MCH-14, on two shards: the first view gives this member shard 0 and
// another member shard 1, the grant starts the run of shard 0 in term 7, and the second view leaves
// this member with nothing.
func movedSteps() []Step {
	return []Step{
		{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
		Renew(Ms(100), Ms(0), Ms(2000)),
		ViewAt(Ms(200), Vm(1, "m1", "m2"), Ms(2000), core.Acquire{Space: "s", Shard: 0}),
		Grant(Ms(300), "s", 0, 7, Ms(10000), Ms(2000)),
		ViewAt(Ms(1000), Vm(2, "m2", "m3"), Ms(2000), core.StopRun{Space: "s", Shard: 0, Term: 7, Reason: core.StopMoved}),
	}
}
