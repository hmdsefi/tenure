// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"github.com/hmdsefi/faultline/kernel"
)

// The labels of the events that change the slot count: the global one at SetSlotsAt, and the
// post on the member that sends the request.
const (
	labelSetSlots     = "tenure/set_slots"
	labelSetSlotsPost = "tenure/set_slots/post"
)

// planSetSlots plans the change of the slot count of a scenario with SetSlotsAt (SIM-360): at
// SetSlotsAt a global event posts on m1 a setSlots(SetSlotsTo) request on its slot lane, the
// lane that serializes claimSlot and releaseSlot too. The request goes out again every
// RetryPeriod until the store has answered it: when it times out on the member's clock and, for
// a request that a crash of m1 took with it, from a global event every RetryPeriod, which sends
// nothing while m1 is down or has no running driver. The store keeps the records of the slots
// at or above the new count; the members drop them from the placement once their holders'
// sessions end or the slots stay vacant for SlotGrace. R is at least SetSlotsAt (see
// recoveryStart).
//
// A scenario without a SetSlotsAt plans nothing. The state of the change lives in the closures,
// so each run of the world function has its own.
func (w *World) planSetSlots() {
	if w.sc.SetSlotsAt <= 0 {
		return
	}
	m := w.members[0]
	count := w.sc.SetSlotsTo
	retry := w.sc.Timing.RetryPeriod
	var (
		done    bool    // the store answered
		pending *call   // the request queued or in flight, nil when none is
		sender  *driver // the driver that made pending
	)
	// send submits the request on the slot lane of m's present driver, when it has one that
	// runs and m's node is up. A timeout of the request sends it again.
	var send func()
	send = func() {
		d := w.drivers.current(m)
		if done || d == nil || d.stopped || m.Node.State() != kernel.NodeUp {
			return
		}
		c := &call{key: laneKey{kind: laneSlot}}
		c.onReply = func(*call, reply) { done = true }
		c.onTimeout = func(*call) {
			pending = nil
			send()
		}
		d.md.lastID++
		c.req = withID(setSlotsReq{Count: count}, reqHead{ID: d.md.lastID, Holder: d.holder})
		if d.lanes.submit(c) {
			pending, sender = c, d
		}
	}
	// watch makes sure a request is out: after a crash of m1 the request of the driver that
	// died is gone, and the next driver sends it.
	var watch func()
	watch = func() {
		if done || m.phase != Active {
			return
		}
		if pending == nil || sender.stopped || sender != w.drivers.current(m) {
			pending = nil
			m.Node.Post(labelSetSlotsPost, send)
		}
		w.sim.At(w.sim.Now().Add(retry), labelSetSlots, watch)
	}
	w.sim.At(kernel.Time(w.sc.SetSlotsAt), labelSetSlots, func() {
		m.Node.Post(labelSetSlotsPost, send)
		w.sim.At(w.sim.Now().Add(retry), labelSetSlots, watch)
	})
}

// Placement is what the machine of one member says about the placement of the shards.
type Placement struct {
	// Member is the member's name, such as "m3".
	Member string
	// Holder is the holder string of the member's present incarnation.
	Holder string
	// Slot is the slot the member's machine holds, or -1.
	Slot int
	// Assignment returns the placement key of each shard of the space, as the machine's
	// Assignment does: "slot-2", or "" for a shard with no assignee.
	Assignment func(space string) []string
}

// Placements returns the placement of each member that is active and up and whose present
// incarnation has a running driver, in NodeID order: the members whose machines the final
// checks judge. Call it from a final check or from an event of the run.
func (w *World) Placements() []Placement {
	var out []Placement
	for _, lm := range w.liveMembers() {
		out = append(out, Placement{Member: lm.name, Holder: lm.holder, Slot: lm.slot, Assignment: lm.assign})
	}
	return out
}
