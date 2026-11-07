// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases

import (
	"slices"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// drainingCases returns the sequences of draining and closing the session. Unless a case says
// otherwise, it has one shard, s/0, whose run has term 7.
func drainingCases() []Case {
	closeSession := core.CloseSession{}
	draining := core.StopRun{Space: "s", Term: 7, Reason: core.StopDraining}
	release := core.Release{Space: "s", Term: 7}
	return []Case{
		{
			// A degraded run is drained while the session stays late. Its StopRun says draining, its
			// Release repeats while the session is late, and the session closes, late, once the Release
			// is answered.
			ID:     "AT-MCH-12 (a)",
			Config: CF(S("s", 1)),
			Steps: slices.Concat(
				BootF(7, S("s", 1)),
				[]Step{
					{Input: core.Drain{Now: Ms(10500)}, Want: want(Ms(12000), draining)},
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
					{Input: core.RunStopped{Now: Ms(12500), Space: "s", Term: 7}, Want: want(Ms(14000), release)},
					{Input: core.Tick{Now: Ms(14000)}, Want: want(Ms(14500), core.KeepAlive{})},
					{Input: core.Tick{Now: Ms(14500)}, Want: want(Ms(16000), release)},
					{Input: core.Released{Now: Ms(15000), Space: "s", Term: 7}, Want: want(Ms(17000), closeSession)},
					{Input: core.SessionClosed{Now: Ms(15500)}},
				},
			),
		},
		{
			// A degraded run is drained while the session is live again and its Confirm is out. No
			// Confirm goes after the StopRun, and the answer to the one that went restores nothing.
			ID:     "AT-MCH-12 (b)",
			Config: CF(S("s", 1)),
			Steps: slices.Concat(
				BootF(7, S("s", 1)),
				[]Step{
					Renew(Ms(10500), Ms(10400), Ms(12000), core.Confirm{Space: "s", Term: 7}),
					{Input: core.Drain{Now: Ms(11000)}, Want: want(Ms(12000), draining)},
					recordOfS0(Ms(11500), "h1", 7, Ms(12000)),
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
					Renew(Ms(12100), Ms(12000), Ms(14000)),
					{Input: core.RunStopped{Now: Ms(12500), Space: "s", Term: 7}, Want: want(Ms(14000), release)},
					{Input: core.Released{Now: Ms(12600), Space: "s", Term: 7}, Want: want(Ms(14600), closeSession)},
					{Input: core.SessionClosed{Now: Ms(13000)}},
				},
			),
		},
		{
			// Inputs that arrive while the session is closing, and after it has closed, have no effect:
			// a report of the run that has returned, answers nobody waits for, records, a view, a
			// renewal, a lost session and a second Drain. While closing each returns only the Wake of
			// the next CloseSession, and once closed each returns nil.
			ID:     "AT-MCH-21",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				Boot(7, S("s", 1)),
				[]Step{
					{Input: core.Drain{Now: Ms(1000)}, Want: want(Ms(2000), draining)},
					{Input: core.RunStopped{Now: Ms(1200), Space: "s", Term: 7}, Want: want(Ms(2000), release)},
					{Input: core.Released{Now: Ms(1400), Space: "s", Term: 7}, Want: want(Ms(3400), closeSession)},
				},
				staleSteps(Ms(1500), Ms(3400)),
				[]Step{{Input: core.SessionClosed{Now: Ms(2500)}}},
				staleSteps(Ms(3400), 0),
				[]Step{
					{Input: core.SessionClosed{Now: Ms(5000)}},
					{Input: core.Tick{Now: Ms(5000)}},
				},
			),
		},
		{
			// A drained run returns and is released, and the session closes. The CloseSession repeats
			// after RetryPeriod with no KeepAlive in between, until SessionClosed ends the session.
			// Then the machine is done and its clock stays at @3.5.
			ID:     "AT-MCH-25",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				Boot(7, S("s", 1)),
				[]Step{
					{Input: core.Drain{Now: Ms(1000)}, Want: want(Ms(2000), draining)},
					{Input: core.RunStopped{Now: Ms(1200), Space: "s", Term: 7}, Want: want(Ms(2000), release)},
					{Input: core.Released{Now: Ms(1400), Space: "s", Term: 7}, Want: want(Ms(3400), closeSession)},
					{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(3400))},
					{Input: core.Tick{Now: Ms(3400)}, Want: want(Ms(5400), closeSession)},
					{Input: core.SessionClosed{Now: Ms(3500)}},
					{Input: core.Tick{Now: Ms(5400)}},
				},
			),
		},
		{
			// A drained run does not return. It is stuck at @11 on a live session, and the session
			// does not close: closing would make the record takeable while the run goes on. The
			// session closes only once the run has returned and its Release is answered.
			ID:     "AT-MCH-27",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				Boot(7, S("s", 1)),
				[]Step{{Input: core.Drain{Now: Ms(1000)}, Want: want(Ms(2000), draining)}},
				beats(Ms(2000), Ms(10000), Ms(11000)),
				[]Step{
					{Input: core.Tick{Now: Ms(11000)}, Want: want(Ms(12000))},
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
					Renew(Ms(12100), Ms(12000), Ms(14000)),
					{Input: core.RunStopped{Now: Ms(13000), Space: "s", Term: 7}, Want: want(Ms(14000), release)},
					{Input: core.Released{Now: Ms(13200), Space: "s", Term: 7}, Want: want(Ms(15200), closeSession)},
					{Input: core.SessionClosed{Now: Ms(13500)}},
				},
			),
		},
		{
			// A member drains while its session is late. Its run was told at the deadline to stop
			// as expired, so the Drain stops nothing more. The Release repeats while the session is
			// late, and the session closes, late, once it is answered.
			ID:     "AT-MCH-28 (a)",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				Boot(7, S("s", 1)),
				[]Step{
					{
						Input: core.Tick{Now: Ms(10000)},
						Want:  want(Ms(12000), core.KeepAlive{}, core.StopRun{Space: "s", Term: 7, Reason: core.StopExpired}),
					},
					{Input: core.Drain{Now: Ms(10500)}, Want: want(Ms(12000))},
					{Input: core.RunStopped{Now: Ms(11000), Space: "s", Term: 7}, Want: want(Ms(12000), release)},
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(13000), core.KeepAlive{})},
					{Input: core.Tick{Now: Ms(13000)}, Want: want(Ms(14000), release)},
					{Input: core.Released{Now: Ms(13500), Space: "s", Term: 7}, Want: want(Ms(15500), closeSession)},
					{Input: core.Tick{Now: Ms(14000)}, Want: want(Ms(15500))},
					{Input: core.SessionClosed{Now: Ms(14500)}},
				},
			),
		},
		{
			// A draining member loses its session. The lost path wins: the run, already told to stop,
			// gets no second StopRun, its RunStopped releases nothing, and the session is never
			// closed. The machine is done once the run has returned.
			ID:     "AT-MCH-28 (c)",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				Boot(7, S("s", 1)),
				[]Step{
					{Input: core.Drain{Now: Ms(1000)}, Want: want(Ms(2000), draining)},
					Fail(Ms(1500), core.CauseLost, Ms(11000)),
					{Input: core.RunStopped{Now: Ms(2000), Space: "s", Term: 7}},
					{Input: core.Tick{Now: Ms(11000)}},
					{Input: core.Drain{Now: Ms(12000)}},
					{Input: core.SessionClosed{Now: Ms(12000)}},
				},
			),
		},
		{
			// A member drains three runs in two spaces, given as b then a. The runs return and their
			// Releases are answered in another order than the shards', and the session closes only
			// after the last answer. The test runs the same steps with the spaces given as a then b.
			ID:     "AT-MCH-39",
			Config: C(S("b", 2), S("a", 1)),
			Steps: slices.Concat(
				Boot(1, S("b", 2), S("a", 1)),
				[]Step{
					{
						Input: core.Drain{Now: Ms(1000)},
						Want: want(Ms(2000),
							core.StopRun{Space: "a", Term: 1, Reason: core.StopDraining},
							core.StopRun{Space: "b", Term: 2, Reason: core.StopDraining},
							core.StopRun{Space: "b", Shard: 1, Term: 3, Reason: core.StopDraining},
						),
					},
					{
						Input: core.RunStopped{Now: Ms(1100), Space: "b", Shard: 1, Term: 3},
						Want:  want(Ms(2000), core.Release{Space: "b", Shard: 1, Term: 3}),
					},
					{
						Input: core.RunStopped{Now: Ms(1200), Space: "a", Term: 1},
						Want:  want(Ms(2000), core.Release{Space: "a", Term: 1}),
					},
					{Input: core.Released{Now: Ms(1300), Space: "a", Term: 1}, Want: want(Ms(2000))},
					{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(3100), core.KeepAlive{})},
					{
						Input: core.RunStopped{Now: Ms(2500), Space: "b", Term: 2},
						Want:  want(Ms(3100), core.Release{Space: "b", Term: 2}),
					},
					{
						Input: core.Tick{Now: Ms(3100)},
						Want:  want(Ms(4000), core.Release{Space: "b", Shard: 1, Term: 3}),
					},
					{Input: core.Released{Now: Ms(3200), Space: "b", Shard: 1, Term: 3}, Want: want(Ms(4000))},
					{Input: core.Released{Now: Ms(3300), Space: "b", Term: 2}, Want: want(Ms(5300), closeSession)},
					{Input: core.SessionClosed{Now: Ms(3400)}},
				},
			),
		},
	}
}

// staleSteps are the inputs of AT-MCH-21 that must have no effect on a machine of shard s/0 whose
// session is closing or closed, at readings 100 ms apart from from, before the next CloseSession is
// due. Each returns Wake{wake}, or nil for a wake of 0.
func staleSteps(from, wake time.Duration) []Step {
	inputs := []func(now time.Duration) core.Input{
		func(now time.Duration) core.Input { return core.RunStopped{Now: now, Space: "s", Term: 7} },
		func(now time.Duration) core.Input { return core.Released{Now: now, Space: "s", Term: 7} },
		func(now time.Duration) core.Input { return core.Acquired{Now: now, Space: "s", Term: 8} },
		func(now time.Duration) core.Input {
			return core.AcquireFailed{Now: now, Space: "s", Holder: "h2", RetryAfter: Ms(1000)}
		},
		func(now time.Duration) core.Input {
			return core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 9}
		},
		func(now time.Duration) core.Input { return core.HolderChanged{Now: now, Space: "s"} },
		func(now time.Duration) core.Input {
			return core.ViewChanged{Now: now, View: Vm(2, "m1"), Wall: Wall(now)}
		},
		func(now time.Duration) core.Input { return core.SessionRenewed{Now: now, SentAt: now} },
		func(now time.Duration) core.Input { return core.SessionFailed{Now: now, Cause: core.CauseLost} },
		func(now time.Duration) core.Input { return core.Drain{Now: now} },
		func(now time.Duration) core.Input { return core.SlotClaimed{Now: now, Slot: 0} },
	}
	steps := make([]Step, len(inputs))
	for i, in := range inputs {
		steps[i] = Step{Input: in(from + time.Duration(i)*Ms(100)), Want: want(wake)}
	}
	return steps
}
