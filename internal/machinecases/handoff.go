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
	}
}
