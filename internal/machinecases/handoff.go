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
	}
}
