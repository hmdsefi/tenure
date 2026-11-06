// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases

import "github.com/hmdsefi/tenure/core"

// sessionCases returns the session sequences: renewals, deadlines, expiry and loss.
func sessionCases() []Case {
	return []Case{
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
	}
}
