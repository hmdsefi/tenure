// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases

import (
	"slices"

	"github.com/hmdsefi/tenure/core"
)

// handoffCases returns the handover and stale-input sequences, and what repeats until answered.
func handoffCases() []Case {
	return []Case{
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
	}
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
