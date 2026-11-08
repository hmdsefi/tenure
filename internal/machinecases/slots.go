// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases

import (
	"slices"

	"github.com/hmdsefi/tenure/core"
)

// slotCases returns the sequences of claiming, holding and releasing slots. Unless a case says
// otherwise, it has one shard, s/0, whose run has term 7. With two slots, slot-0 gives this member
// shard s/1 and slot-1 gives it s/0; with three, slot-0 gives it s/1.
func slotCases() []Case {
	closeSession := core.CloseSession{}
	draining := core.StopRun{Space: "s", Term: 7, Reason: core.StopDraining}
	release := core.Release{Space: "s", Term: 7}
	releaseSlot0 := core.ReleaseSlot{Slot: 0}
	return []Case{
		{
			// A draining member releases its slot after its shard, and closes its session after the
			// slot: the run returns, its Release is answered, the ReleaseSlot goes, and the
			// SlotReleased lets the session close.
			ID:     "AT-MCH-26",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				SlotBoot(7, S("s", 1)),
				[]Step{
					{Input: core.Drain{Now: Ms(1000)}, Want: want(Ms(2000), draining)},
					{Input: core.RunStopped{Now: Ms(1200), Space: "s", Term: 7}, Want: want(Ms(2000), release)},
					{Input: core.Released{Now: Ms(1400), Space: "s", Term: 7}, Want: want(Ms(2000), releaseSlot0)},
					{Input: core.SlotReleased{Now: Ms(1600), Slot: 0}, Want: want(Ms(3600), closeSession)},
					{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(3600))},
					{Input: core.SessionClosed{Now: Ms(2500)}},
				},
			),
		},
		{
			// A draining member whose run does not return releases its slot once the run is stuck,
			// because the run still holds its shard record. The session waits: it closes only once
			// the run has returned and its Release is answered.
			ID:     "AT-MCH-30",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				SlotBoot(7, S("s", 1)),
				[]Step{{Input: core.Drain{Now: Ms(1000)}, Want: want(Ms(2000), draining)}},
				beats(Ms(2000), Ms(10000), Ms(11000)),
				[]Step{
					{Input: core.Tick{Now: Ms(11000)}, Want: want(Ms(12000), releaseSlot0)},
					{Input: core.SlotReleased{Now: Ms(11200), Slot: 0}, Want: want(Ms(12000))},
					{Input: core.Tick{Now: Ms(12000)}, Want: want(Ms(14000), core.KeepAlive{})},
					Renew(Ms(12100), Ms(12000), Ms(14000)),
					{Input: core.RunStopped{Now: Ms(13000), Space: "s", Term: 7}, Want: want(Ms(14000), release)},
					{Input: core.Released{Now: Ms(13200), Space: "s", Term: 7}, Want: want(Ms(15200), closeSession)},
					{Input: core.SessionClosed{Now: Ms(13500)}},
				},
			),
		},
		{
			// A standby in a full group claims on the retry cadence. The view that shows the slot
			// vacant does not bring the claim forward; the answer to the claim at @2.2 gives the
			// member slot 0 at @3.2, and the slot's shard is acquired at once.
			ID:     "AT-MCH-31",
			Config: C(S("s", 1)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vs(1, []string{"h2"}, "m1", "m2"), Ms(2000), core.ClaimSlot{}),
				{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(2200), core.KeepAlive{})},
				Renew(Ms(2100), Ms(2000), Ms(2200)),
				{Input: core.Tick{Now: Ms(2200)}, Want: want(Ms(4000), core.ClaimSlot{})},
				ViewAt(Ms(3000), Vs(2, []string{""}, "m1"), Ms(4000)),
				{Input: core.SlotClaimed{Now: Ms(3200), Slot: 0}, Want: want(Ms(4000), core.Acquire{Space: "s"})},
				Grant(Ms(3300), "s", 0, 7, Ms(12000), Ms(4000)),
			},
		},
		{
			// A standby in a full group holds no slot throughout. Its claims go every RetryPeriod and
			// none succeeds. A claim answer with no slot is ignored; one for a slot the view does not
			// count is given back, again every RetryPeriod until answered; and the answer for a slot
			// it is not giving back changes nothing.
			ID:     "AT-MCH-32 (a)",
			Config: C(S("s", 1)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vs(1, []string{"h2"}, "m1", "m2"), Ms(2000), core.ClaimSlot{}),
				{Input: core.SlotClaimed{Now: Ms(1000), Slot: -1}, Want: want(Ms(2000))},
				{Input: core.SlotClaimed{Now: Ms(1500), Slot: 1}, Want: want(Ms(2000), core.ReleaseSlot{Slot: 1})},
				{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(2200), core.KeepAlive{})},
				{Input: core.Tick{Now: Ms(2200)}, Want: want(Ms(3500), core.ClaimSlot{})},
				{Input: core.Tick{Now: Ms(3500)}, Want: want(Ms(4000), core.ReleaseSlot{Slot: 1})},
				{Input: core.SlotReleased{Now: Ms(3600), Slot: 1}, Want: want(Ms(4000))},
				{Input: core.SlotReleased{Now: Ms(3700), Slot: 0}, Want: want(Ms(4000))},
				{Input: core.Tick{Now: Ms(4000)}, Want: want(Ms(4200), core.KeepAlive{})},
				{Input: core.Tick{Now: Ms(4200)}, Want: want(Ms(6000), core.ClaimSlot{})},
			},
		},
		{
			// A member that holds slot 0 of two gets a second claim answer, for slot 1. It holds at
			// most one slot, so it gives slot 1 back, again every RetryPeriod until answered, and keeps
			// slot 0 and its shard s/1. A second answer for slot 0 changes nothing.
			ID:     "AT-MCH-32 (b)",
			Config: C(S("s", 2)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vs(1, []string{"", ""}, "m1"), Ms(2000), core.ClaimSlot{}),
				{Input: core.SlotClaimed{Now: Ms(300), Slot: 0}, Want: want(Ms(2000), core.Acquire{Space: "s", Shard: 1})},
				Grant(Ms(400), "s", 1, 7, Ms(10000), Ms(2000)),
				{Input: core.SlotClaimed{Now: Ms(1000), Slot: 1}, Want: want(Ms(2000), core.ReleaseSlot{Slot: 1})},
				{Input: core.SlotClaimed{Now: Ms(1100), Slot: 0}, Want: want(Ms(2000))},
				{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(3000), core.KeepAlive{})},
				Renew(Ms(2100), Ms(2000), Ms(3000)),
				{Input: core.Tick{Now: Ms(3000)}, Want: want(Ms(4000), core.ReleaseSlot{Slot: 1})},
				{Input: core.SlotReleased{Now: Ms(3500), Slot: 1}, Want: want(Ms(4000))},
			},
		},
		{
			// A view gives the member's slot to another holder. The member no longer holds it and
			// claims again, and the slot's run stops as lost. The shard record still names this
			// member, so the run's RunStopped releases it, and nothing before.
			ID:     "AT-MCH-35",
			Config: C(S("s", 1)),
			Steps: slices.Concat(
				SlotBoot(7, S("s", 1)),
				[]Step{
					ViewAt(Ms(1300), Vs(2, []string{"h2"}, "m1", "m2"), Ms(2000),
						core.StopRun{Space: "s", Term: 7, Reason: core.StopLost}, core.ClaimSlot{}),
					{Input: core.RunStopped{Now: Ms(1400), Space: "s", Term: 7}, Want: want(Ms(2000), release)},
					{Input: core.Released{Now: Ms(1500), Space: "s", Term: 7}, Want: want(Ms(2000))},
					{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(3300), core.KeepAlive{})},
				},
			),
		},
		{
			// The group's slot count goes down from two to one while the member holds slot 1. The slot
			// is retired: its run stops as moved, and once the shard is released the slot is too. Then
			// the member is a standby and claims again at once.
			ID:     "AT-MCH-36",
			Config: C(S("s", 2)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vs(1, []string{"h2", ""}, "m1", "m2"), Ms(2000), core.ClaimSlot{}),
				{Input: core.SlotClaimed{Now: Ms(300), Slot: 1}, Want: want(Ms(2000), core.Acquire{Space: "s"})},
				Grant(Ms(400), "s", 0, 7, Ms(10000), Ms(2000)),
				ViewAt(Ms(500), Vs(2, []string{"h2"}, "m1", "m2"), Ms(2000), core.StopRun{Space: "s", Term: 7, Reason: core.StopMoved}),
				{Input: core.RunStopped{Now: Ms(600), Space: "s", Term: 7}, Want: want(Ms(2000), release)},
				{Input: core.Released{Now: Ms(700), Space: "s", Term: 7}, Want: want(Ms(2000), core.ReleaseSlot{Slot: 1})},
				{Input: core.SlotReleased{Now: Ms(1000), Slot: 1}, Want: want(Ms(2000), core.ClaimSlot{})},
			},
		},
		{
			// A standby claims, and then a view lists two of three slots for it. It adopts the lowest,
			// slot 0, at @0.3, acquires slot 0's shard s/1, and gives slot 2 back, again every
			// RetryPeriod until answered. The answer to its claim, for slot 0, changes nothing.
			ID:     "AT-MCH-38",
			Config: C(S("s", 3)),
			Steps: []Step{
				{Input: core.Tick{Now: Ms(0)}, Want: want(Ms(2000), core.KeepAlive{})},
				Renew(Ms(100), Ms(0), Ms(2000)),
				ViewAt(Ms(200), Vs(1, []string{"", "", ""}, "m1"), Ms(2000), core.ClaimSlot{}),
				ViewAt(Ms(300), Vs(2, []string{"h1", "", "h1"}, "m1"), Ms(2000),
					core.Acquire{Space: "s", Shard: 1}, core.ReleaseSlot{Slot: 2}),
				{Input: core.SlotClaimed{Now: Ms(400), Slot: 0}, Want: want(Ms(2000))},
				Grant(Ms(500), "s", 1, 7, Ms(10000), Ms(2000)),
				{Input: core.Tick{Now: Ms(2000)}, Want: want(Ms(2300), core.KeepAlive{})},
				{Input: core.Tick{Now: Ms(2300)}, Want: want(Ms(4000), core.ReleaseSlot{Slot: 2})},
				{Input: core.SlotReleased{Now: Ms(2500), Slot: 2}, Want: want(Ms(4000))},
			},
		},
	}
}
