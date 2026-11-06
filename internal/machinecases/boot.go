// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases

import "github.com/hmdsefi/tenure/core"

// bootCases returns the boot and view sequences.
func bootCases() []Case {
	return []Case{
		{
			// A member boots through its first grant: the session opens, the view gives it the
			// shard, and the grant starts the run.
			ID:     "AT-MCH-01",
			Config: C(S("s", 1)),
			Steps:  Boot(7, S("s", 1)),
		},
		{
			// A view with an older version, and a view that is not valid, change nothing. A view
			// with the applied version applies.
			ID:     "AT-MCH-02",
			Config: C(S("s", 2)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vm(5, "m2"), Ms(2000)),
				ViewAt(Ms(300), Vm(4, "m1"), Ms(2000)),
				ViewAt(Ms(400), core.View{Version: 9, Members: []core.Member{{ID: "m1", Slot: -1}, {ID: "m1", Slot: -1}}}, Ms(2000)),
				ViewAt(Ms(500), Vm(5, "m1"), Ms(2000), core.Acquire{Space: "s", Shard: 0}, core.Acquire{Space: "s", Shard: 1}),
			},
		},
		{
			// A wanted shard waits while the session is opening, and goes in the step that makes
			// the session live.
			ID:     "AT-MCH-03 (a)",
			Config: C(S("s", 1)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				ViewAt(Ms(100), Vm(1, "m1"), Ms(2000)),
				{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(4000), core.KeepAlive{})},
				Renew(Ms(2100), Ms(2000), Ms(4000), core.Acquire{Space: "s", Shard: 0}),
			},
		},
		{
			// A wanted shard waits while the session is late, and goes in the step that brings
			// the session back to live.
			ID:     "AT-MCH-03 (b)",
			Config: C(S("s", 1)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				{Input: core.Tick{Now: Ms(10000)}, Want: want(Ms(12000), core.KeepAlive{})},
				ViewAt(Ms(10100), Vm(1, "m1"), Ms(12000)),
				{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
				Renew(Ms(12100), Ms(12000), Ms(14000), core.Acquire{Space: "s", Shard: 0}),
			},
		},
		{
			// Two members share two spaces: the member acquires only the shards the assignment
			// gives it, by space name and then by shard.
			ID:     "AT-MCH-07",
			Config: C(S("b", 2), S("a", 3)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vm(1, "m1", "m2"), Ms(2000), core.Acquire{Space: "a", Shard: 2}, core.Acquire{Space: "b", Shard: 1}),
				Grant(Ms(300), "b", 1, 4, Ms(10000), Ms(2000)),
			},
		},
		{
			// Each applied view makes a new assignment. The session is still opening, so nothing is
			// acquired and only the KeepAlive's timer is armed.
			ID:     "AT-MCH-41",
			Config: C(S("s", 4)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				ViewAt(Ms(500), Vm(1, "m1"), Ms(2000)),
				ViewAt(Ms(1000), Vm(2, "m1", "m2"), Ms(2000)),
			},
		},
		{
			// The panic this case checks is not a step: its test calls Step(nil) after this list.
			ID:     "AT-MCH-42",
			Config: C(S("s", 2)),
			Steps:  Boot(7, S("s", 2)),
		},
		{
			// A machine with something in every Status field that has a value so far: two spaces,
			// one fenced, with shards owned, acquiring and not wanted, and a failed renewal.
			ID: "AT-MCH-43",
			Config: C(
				S("orders", 4),
				core.SpaceSpec{Name: "audit", Shards: 2, Fenced: true},
			),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vm(7, "m1", "m2"), Ms(2000),
					core.Acquire{Space: "audit", Shard: 0},
					core.Acquire{Space: "orders", Shard: 0},
					core.Acquire{Space: "orders", Shard: 1},
				),
				Grant(Ms(300), "orders", 1, 12, Ms(10000), Ms(2000)),
				Grant(Ms(400), "audit", 0, 5, Ms(10000), Ms(2000)),
				Fail(Ms(500), core.CauseUnreachable, Ms(2000)),
			},
		},
	}
}
