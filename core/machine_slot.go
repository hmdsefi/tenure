// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"slices"
	"strconv"
	"time"
)

// slotBook is the slot bookkeeping that Status does not report. Status.Slot is the held slot: the only
// slot data the machine trusts about itself. A view says which slots name this member only through
// SlotHolders compared with the session holder, never through a Member's Slot (MCH-630).
type slotBook struct {
	// claimAt is the reading of the next ClaimSlot, and 0 sends it in the first step that may claim. It
	// means something only while the machine may claim: see mayClaim.
	claimAt time.Duration

	// retired says an applied view has retired the held slot (MCH-650): the view has no slots, or not
	// as many as the held slot needs. It is for good: the slot is released once its shards are, even if
	// a later view counts it again, and a slot that is retired gives this member no shard.
	retired bool

	// releasing says a ReleaseSlot of the held slot has gone out and no SlotReleased has answered it.
	releasing bool

	// releaseAt is the reading of the next ReleaseSlot of the held slot. It means something only while
	// releasing.
	releaseAt time.Duration

	// giveBacks holds the slots other than the held one that this member may hold and is giving back
	// (MCH-680), by slot ascending, each with the reading of its next ReleaseSlot.
	giveBacks []giveBack

	// lost says that this step's view gave the held slot to another holder, so phase D stops the runs
	// the slot gave this member as lost (MCH-470 item 3). Phase E clears it.
	lost bool
}

// giveBack is a slot this member is giving back, with the reading of its next ReleaseSlot.
type giveBack struct {
	slot int
	at   time.Duration
}

// applySlotInput applies a slot input (MCH-620, MCH-670). Other inputs have no effect here. A lost
// session holds no slot and gives none back (MCH-250): its records are takeable, so the machine forgets
// its slots and takes no slot input any more.
func (m *Machine) applySlotInput(in Input) {
	if m.sessionLost() {
		m.forgetSlots()
		return
	}
	switch v := in.(type) {
	case SlotClaimed:
		m.applySlotClaimed(v.Slot)
	case SlotReleased:
		m.applySlotReleased(v.Slot)
	}
}

// applySlotClaimed applies the report that this member claimed slot s (MCH-620). A standby that is not
// draining holds it. A member that already holds another slot, or drains, gives it back, since it holds at
// most one and a draining member holds none it can keep. So does a member whose applied view has no such
// slot: the slot is retired, or the group has no slots. A report of the held slot changes nothing, and
// one of a slot being given back leaves it given back: the ReleaseSlot that is out may have come before or
// after the claim, so only its answer settles the slot. A negative slot names none and is ignored.
func (m *Machine) applySlotClaimed(s int) {
	switch {
	case s < 0 || s == m.status.Slot || m.givingBack(s):
	case m.status.Slot >= 0 || m.status.Draining || (m.view.applied && s >= m.view.view.Slots):
		m.addGiveBack(s)
	default:
		m.holdSlot(s)
	}
}

// applySlotReleased applies the report that slot s is released (MCH-670): it ends the release of the held
// slot, or the give-back of s. A report of the held slot while no ReleaseSlot of it is out answers nothing
// this member asked, so it is ignored, as is a report of any other slot.
func (m *Machine) applySlotReleased(s int) {
	if s == m.status.Slot && s >= 0 {
		if m.slot.releasing {
			m.leaveSlot()
		}
		return
	}
	if i, ok := m.giveBackIndex(s); ok {
		m.slot.giveBacks = slices.Delete(m.slot.giveBacks, i, i+1)
	}
}

// applyViewSlots applies the slot rules of MCH-630 to the view just applied. Let L be the slots whose
// SlotHolders entry is this member's holder:
//
//  1. a standby that is not draining adopts the lowest of L and gives back the rest;
//  2. a member holding a slot in L keeps it and gives back the rest of L;
//  3. a member whose held slot names another holder has lost it, and its runs stop as lost;
//  4. a member whose held slot is vacant in the view keeps it, because views lag;
//  5. a member whose held slot is not in the view, because the view has fewer slots or none, retires it.
//
// A slot already being given back is neither adopted nor given back again: only its answer ends that. A
// draining member gives back every slot of L but the one it holds. After the session has ended no view
// changes the slots.
func (m *Machine) applyViewSlots() {
	if m.sessionEnded() {
		return
	}
	v := &m.view.view
	if h := m.status.Slot; h >= 0 {
		switch {
		case h >= v.Slots:
			m.slot.retired = true
		case v.SlotHolders[h] != "" && v.SlotHolders[h] != m.cfg.Holder:
			m.loseSlot()
		}
	}
	for s, holder := range v.SlotHolders {
		switch {
		case holder != m.cfg.Holder || s == m.status.Slot || m.givingBack(s):
		case m.status.Slot < 0 && !m.status.Draining:
			m.holdSlot(s)
		default:
			m.addGiveBack(s)
		}
	}
}

// holdSlot makes s the held slot, which recomputes the assignments.
func (m *Machine) holdSlot(s int) {
	m.status.Slot = s
	m.slot.retired, m.slot.releasing = false, false
	m.view.dirty = true
}

// leaveSlot makes the member a standby, which recomputes the assignments. The next ClaimSlot may go at
// once.
func (m *Machine) leaveSlot() {
	m.status.Slot = -1
	m.slot.retired, m.slot.releasing = false, false
	m.slot.claimAt = 0
	m.view.dirty = true
}

// loseSlot applies a view that gives the held slot to another holder (MCH-630 item 3): the member no
// longer holds it, sends no ReleaseSlot for it, and phase D stops the slot's runs as lost (MCH-470 item
// 3). Their shard records still name this member, so each is released once its run has returned.
func (m *Machine) loseSlot() {
	m.leaveSlot()
	m.slot.lost = true
}

// forgetSlots drops every slot after a lost session (MCH-250): the member holds none and gives none back,
// and no slot action goes out again. It changes nothing once done, so a later step does not recompute.
func (m *Machine) forgetSlots() {
	if m.status.Slot >= 0 {
		m.leaveSlot()
	}
	m.slot.giveBacks = m.slot.giveBacks[:0]
}

// givingBack reports that slot s is being given back.
func (m *Machine) givingBack(s int) bool {
	_, ok := m.giveBackIndex(s)
	return ok
}

// giveBackIndex returns the index of slot s in giveBacks, and whether it is there.
func (m *Machine) giveBackIndex(s int) (int, bool) {
	return slices.BinarySearchFunc(m.slot.giveBacks, s, func(g giveBack, s int) int { return g.slot - s })
}

// addGiveBack starts giving slot s back. Its first ReleaseSlot goes in this step.
func (m *Machine) addGiveBack(s int) {
	if i, ok := m.giveBackIndex(s); !ok {
		m.slot.giveBacks = slices.Insert(m.slot.giveBacks, i, giveBack{slot: s, at: m.status.Now})
	}
}

// stepSlots is phase E. It returns, in this order, the ReleaseSlot of the held slot once it is due, a
// ReleaseSlot of each slot being given back once due, by slot, and the ClaimSlot once due. A session
// that has ended has no slot action; nor does a member about to close, whose CloseSession frees the
// slots it is giving back (MCH-710), so it drops them instead. The clock is at or below maxReading, so
// each next reading fits in a Duration.
func (m *Machine) stepSlots() {
	m.slot.lost = false
	if m.sessionEnded() {
		return
	}
	if m.readyToClose() {
		m.slot.giveBacks = m.slot.giveBacks[:0]
		return
	}
	m.releaseHeldSlot()
	for i := range m.slot.giveBacks {
		g := &m.slot.giveBacks[i]
		if m.status.Now >= g.at {
			g.at = m.status.Now + m.cfg.Timing.RetryPeriod
			m.emit(groupSlot, ReleaseSlot{Slot: g.slot})
		}
	}
	if m.mayClaim() && m.status.Now >= m.slot.claimAt {
		m.slot.claimAt = m.status.Now + m.cfg.Timing.RetryPeriod
		m.emit(groupSlot, ClaimSlot{})
	}
}

// releaseHeldSlot returns the ReleaseSlot of a retired slot, or of the slot of a draining member, once
// its shards are released (MCH-650, MCH-660, MCH-720), and again every RetryPeriod, counted from the last
// one, until a SlotReleased answers it. A shard is released when it is idle, or when its run is stuck:
// that run still holds its shard record, and whoever claims the slot next cannot acquire the shard while
// it does, so the slot need not wait for it. A shard whose run is stopping and not stuck, or whose Release
// or Acquire is out, holds the slot back, repeats included: a stuck run that returns makes its shard
// release, and the repeat waits for that Release to be answered.
func (m *Machine) releaseHeldSlot() {
	h := m.status.Slot
	switch {
	case h < 0 || !m.slot.retired && !m.status.Draining || !m.shardsReleased():
		return
	case m.slot.releasing:
		if m.status.Now < m.slot.releaseAt {
			return
		}
	default:
		m.slot.releasing = true
	}
	m.slot.releaseAt = m.status.Now + m.cfg.Timing.RetryPeriod
	m.emit(groupSlot, ReleaseSlot{Slot: h})
}

// shardsReleased reports that every shard is idle or has a stuck run.
func (m *Machine) shardsReleased() bool {
	for i := range m.status.Spaces {
		for j := range m.status.Spaces[i].Shards {
			sh := &m.status.Spaces[i].Shards[j]
			if sh.State != ShardStateIdle && (!runToldToStop(sh) || !sh.Stuck) {
				return false
			}
		}
	}
	return true
}

// mayClaim reports that claimAt is all that stands between the member and a ClaimSlot (MCH-610): it
// holds no slot, the applied view has slots, the session is live and the member does not drain. The
// claim does not wait for the view to show a free slot: the store knows better, and a claim in a full
// group is routine.
func (m *Machine) mayClaim() bool {
	return m.status.Slot < 0 && m.view.view.Slots > 0 && m.live && !m.status.Draining
}

// slotKey returns the placement key of the held slot, or "" when the member holds none that gives it
// shards: a slot it has retired, or one the applied view does not count. The applied view has slots.
func (m *Machine) slotKey() string {
	h := m.status.Slot
	if h < 0 || h >= m.view.view.Slots || m.slot.retired {
		return ""
	}
	return "slot-" + strconv.Itoa(h)
}

// applySlotLoss is the part of D1 that a lost slot applies to every shard (MCH-470 item 3): an owned or
// degraded run gets its StopRun and the shard goes lost. The record still names this member, so the run's
// RunStopped releases it, as for any run told to stop.
func (m *Machine) applySlotLoss(ref shardRef) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateOwned && sh.State != ShardStateDegraded {
		return
	}
	m.shards[ref.space][ref.shard].recordHeld = true
	m.loseRun(ref, sh.Holder)
}

// armSlotTimers arms the next ReleaseSlot of the held slot (MCH-800 item 10), while its shards are
// released, of each slot being given back (item 13), and the next ClaimSlot (item 9). Each is after the
// clock: stepSlots has just returned what was due. A step that makes the shards released again runs
// stepSlots first, so a repeat that waited for them goes in that step. A session that has ended arms none: a lost one holds no slot, and a closing one holds none
// and gives none back.
func (m *Machine) armSlotTimers(w *wakeTimer) {
	if m.sessionEnded() {
		return
	}
	if m.slot.releasing && m.shardsReleased() {
		w.arm(m.slot.releaseAt)
	}
	for _, g := range m.slot.giveBacks {
		w.arm(g.at)
	}
	if m.mayClaim() {
		w.arm(m.slot.claimAt)
	}
}
