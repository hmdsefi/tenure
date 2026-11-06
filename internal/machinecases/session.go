// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases

import (
	"slices"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// sessionCases returns the session sequences: renewals, deadlines, expiry and loss.
func sessionCases() []Case {
	return []Case{
		{
			// A deadline under a Timing of its own, in two spaces: every timer follows that Timing.
			// Both runs stop at exactly the deadline, one returns and repeats its Release while the
			// session stays late, the other goes stuck, and a renewal lets the first acquire again.
			ID:     "AT-MCH-04",
			Config: expiryConfig(),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(5000), core.KeepAlive{})},
				{
					Input: core.SessionRenewed{Now: Ms(100), SentAt: Ms(0)},
					Want:  want(Ms(5000), core.ExtendDeadline{Deadline: Ms(20000)}),
				},
				ViewAt(Ms(200), Vm(1, "m1"), Ms(5000), core.Acquire{Space: "a"}, core.Acquire{Space: "b"}),
				Grant(Ms(300), "a", 0, 7, Ms(20000), Ms(5000)),
				Grant(Ms(400), "b", 0, 8, Ms(20000), Ms(5000)),
				{Input: core.Tick{Now: Ms(5000)}, Want: want(Ms(10000), core.KeepAlive{})},
				Fail(Ms(5100), core.CauseUnreachable, Ms(10000)),
				{Input: core.Tick{Now: Ms(10000)}, Want: want(Ms(15000), core.KeepAlive{})},
				{Input: core.Tick{Now: Ms(15000)}, Want: want(Ms(20000), core.KeepAlive{})},
				{Input: core.Tick{Now: Ms(19999)}, Want: want(Ms(20000))},
				{
					Input: core.Tick{Now: Ms(20000)},
					Want: want(Ms(25000),
						core.KeepAlive{},
						core.StopRun{Space: "a", Term: 7, Reason: core.StopExpired},
						core.StopRun{Space: "b", Term: 8, Reason: core.StopExpired},
					),
				},
				{
					Input: core.RunStopped{Now: Ms(21000), Space: "a", Term: 7},
					Want:  want(Ms(25000), core.Release{Space: "a", Term: 7}),
				},
				{Input: core.Tick{Now: Ms(25000)}, Want: want(Ms(26000), core.KeepAlive{})},
				{Input: core.Tick{Now: Ms(26000)}, Want: want(Ms(30000), core.Release{Space: "a", Term: 7})},
				{Input: core.Released{Now: Ms(26500), Space: "a", Term: 7}, Want: want(Ms(30000))},
				{Input: core.Tick{Now: Ms(30000)}, Want: want(Ms(35000), core.KeepAlive{})},
				{Input: core.Tick{Now: Ms(35000)}, Want: want(Ms(40000), core.KeepAlive{})},
				{Input: core.Tick{Now: Ms(40000)}, Want: want(Ms(45000), core.KeepAlive{})},
				{
					Input: core.RunStopped{Now: Ms(41000), Space: "b", Term: 8},
					Want:  want(Ms(45000), core.Release{Space: "b", Term: 8}),
				},
				{
					Input: core.SessionRenewed{Now: Ms(41500), SentAt: Ms(41000)},
					Want:  want(Ms(45000), core.ExtendDeadline{Deadline: Ms(61000)}, core.Acquire{Space: "a"}),
				},
				{
					Input: core.Released{Now: Ms(42000), Space: "b", Term: 8},
					Want:  want(Ms(45000), core.Acquire{Space: "b"}),
				},
				Grant(Ms(43000), "a", 0, 9, Ms(61000), Ms(45000)),
			},
		},
		{
			// The deadline counts from SentAt and only moves later: a late answer to an older
			// renewal moves nothing, and neither does a renewal from the future.
			ID:     "AT-MCH-05",
			Config: C(S("s", 1)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(4000), core.KeepAlive{})},
				Renew(Ms(2100), Ms(2000), Ms(4000)),
				{Input: core.SessionRenewed{Now: Ms(2200), SentAt: Ms(0)}, Want: want(Ms(4000))},
				{Input: core.SessionRenewed{Now: Ms(2300), SentAt: Ms(5000)}, Want: want(Ms(4000))},
			},
		},
		{
			// An outage reaches the deadline with two runs going. Both stop there. One returns and
			// is released, and its shard stays idle because the session is late; the other is still
			// going at @11.
			ID:     "AT-MCH-06",
			Config: C(S("s", 2)),
			Steps: slices.Concat(
				Boot(7, S("s", 2)),
				Outage(Ms(2000), Ms(10000), Ms(10000)),
				[]Step{
					{
						Input: core.Tick{Now: Ms(10000)},
						Want: want(Ms(12000),
							core.KeepAlive{},
							core.StopRun{Space: "s", Term: 7, Reason: core.StopExpired},
							core.StopRun{Space: "s", Shard: 1, Term: 8, Reason: core.StopExpired},
						),
					},
					Fail(Ms(10000), core.CauseUnreachable, Ms(12000)),
					{
						Input: core.RunStopped{Now: Ms(10100), Space: "s", Term: 7},
						Want:  want(Ms(12000), core.Release{Space: "s", Term: 7}),
					},
					{Input: core.Released{Now: Ms(10200), Space: "s", Term: 7}, Want: want(Ms(12000))},
					{Input: core.Tick{Now: Ms(11000)}, Want: want(Ms(12000))},
				},
			),
		},
		{
			// A lost session with a run going, a run already told to stop and an Acquire out. The
			// machine sends the one StopRun still owed, then only waits: for the Acquire's answer
			// and for both runs. It is done once all three are over, having released nothing.
			ID:     "AT-MCH-44",
			Config: C(S("s", 3)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vm(1, "m1"), Ms(2000),
					core.Acquire{Space: "s"},
					core.Acquire{Space: "s", Shard: 1},
					core.Acquire{Space: "s", Shard: 2},
				),
				Grant(Ms(300), "s", 0, 7, Ms(10000), Ms(2000)),
				Grant(Ms(300), "s", 1, 8, Ms(10000), Ms(2000)),
				{
					Input: core.HolderChanged{Now: Ms(400), Space: "s", Shard: 1, Holder: "h2", Term: 9},
					Want:  want(Ms(2000), core.StopRun{Space: "s", Shard: 1, Term: 8, Reason: core.StopLost}),
				},
				Fail(Ms(1000), core.CauseLost, Ms(10400), core.StopRun{Space: "s", Term: 7, Reason: core.StopLost}),
				{Input: core.Acquired{Now: Ms(1100), Space: "s", Shard: 2, Term: 9}, Want: want(Ms(10400))},
				{Input: core.RunStopped{Now: Ms(1200), Space: "s", Shard: 1, Term: 8}, Want: want(Ms(11000))},
				{Input: core.RunStopped{Now: Ms(1300), Space: "s", Term: 7}},
				{Input: core.Tick{Now: Ms(60000)}},
			},
		},
	}
}

// expiryConfig is AT-MCH-04's config: two spaces and a Timing of their own.
func expiryConfig() core.MachineConfig {
	cfg := C(S("b", 1), S("a", 1))
	cfg.Timing = core.Timing{
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 20 * time.Second,
		RetryPeriod:   5 * time.Second,
		SlotGrace:     time.Minute,
	}
	return cfg
}
