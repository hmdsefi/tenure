// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases

import (
	"slices"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// fencedCases returns the sequences of fenced spaces: degraded runs and their confirmation. Each has one
// shard, s/0, whose run has term 7 and goes degraded at @10.
func fencedCases() []Case {
	confirm := core.Confirm{Space: "s", Term: 7}
	lost := core.StopRun{Space: "s", Term: 7, Reason: core.StopLost}
	return []Case{
		{
			// A degraded run is confirmed. The record already names it at @11, but with the session late
			// and no Confirm out, that says nothing. Only the answer to the Confirm that the renewal
			// brings makes the shard owned again, and the run never stops.
			ID:     "AT-MCH-08",
			Config: CF(S("s", 1)),
			Steps: slices.Concat(
				BootF(7, S("s", 1)),
				[]Step{
					recordOfS0(Ms(11000), "h1", 7, Ms(12000)),
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
					Renew(Ms(12100), Ms(12000), Ms(14000), confirm),
					recordOfS0(Ms(12300), "h1", 7, Ms(14000), core.Restore{Space: "s", Term: 7}),
				},
			),
		},
		{
			// Another holder takes a degraded shard. Its run stops, and its RunStopped has nothing to
			// release: the record is not this member's.
			ID:     "AT-MCH-09",
			Config: CF(S("s", 1)),
			Steps: slices.Concat(
				BootF(7, S("s", 1)),
				[]Step{
					recordOfS0(Ms(10500), "h2", 8, Ms(12000), lost),
					{Input: core.RunStopped{Now: Ms(11000), Space: "s", Term: 7}, Want: want(Ms(12000))},
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
				},
			),
		},
		{
			// A degraded shard's session is lost. Its run stops and nothing more goes out, and when the
			// run has returned the machine is done and stays quiet.
			ID:     "AT-MCH-10",
			Config: CF(S("s", 1)),
			Steps: slices.Concat(
				BootF(7, S("s", 1)),
				[]Step{
					Fail(Ms(11000), core.CauseLost, Ms(21000), lost),
					{Input: core.RunStopped{Now: Ms(11500), Space: "s", Term: 7}},
					{Input: core.Tick{Now: Ms(60000)}},
				},
			),
		},
		{
			// A degraded shard's Confirm goes every RetryPeriod while the session is live and no answer
			// comes. No more go out once the deadline has passed again, and the shard stays degraded.
			ID:     "AT-MCH-11",
			Config: CF(S("s", 1)),
			Steps: slices.Concat(
				BootF(7, S("s", 1)),
				[]Step{
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
					Renew(Ms(12100), Ms(12000), Ms(14000), confirm),
					{Input: core.Tick{Now: Ms(14000)}, Want: want(Ms(14100), core.KeepAlive{})},
					{Input: core.Tick{Now: Ms(14100)}, Want: want(Ms(16000), confirm)},
					{Input: core.Tick{Now: Ms(16000)}, Want: want(Ms(16100), core.KeepAlive{})},
					{Input: core.Tick{Now: Ms(16100)}, Want: want(Ms(18000), confirm)},
					{Input: core.Tick{Now: Ms(18000)}, Want: want(Ms(18100), core.KeepAlive{})},
					{Input: core.Tick{Now: Ms(18100)}, Want: want(Ms(20000), confirm)},
					{Input: core.Tick{Now: Ms(20000)}, Want: want(Ms(20100), core.KeepAlive{})},
					{Input: core.Tick{Now: Ms(20100)}, Want: want(Ms(22000), confirm)},
					{Input: core.Tick{Now: Ms(22000)}, Want: want(Ms(24000), core.KeepAlive{})},
					{Input: core.Tick{Now: Ms(22100)}, Want: want(Ms(24000))},
				},
			),
		},
	}
}

// recordOfS0 is a HolderChanged for shard s/0 at now, with a result of actions and then Wake{wake}.
func recordOfS0(now time.Duration, holder string, term core.Term, wake time.Duration, actions ...core.Action) Step {
	return Step{
		Input: core.HolderChanged{Now: now, Space: "s", Holder: holder, Term: term},
		Want:  want(wake, actions...),
	}
}
