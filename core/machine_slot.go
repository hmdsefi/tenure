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

	// lost says that this step's view gave the held slot to another holder, or that the views showed it
	// vacant for too long, so phase D stops the runs the slot gave this member as lost (MCH-470 item 3).
	// Phase E clears it.
	lost bool

	// vacancy holds, by slot, how long this member has seen each slot of the applied view vacant
	// (MCH-310). It has an entry per slot of the latest view applied before the session ended.
	vacancy []vacancy

	// heldAt is the reading at which the member took the held slot. It means something only while it
	// holds one.
	heldAt time.Duration

	// shownVacant says that the latest applied view shows the held slot vacant, and vacantSince is the
	// reading from which the views have shown it vacant while the member held it (MCH-640). vacantSince
	// means something only while shownVacant.
	shownVacant bool
	vacantSince time.Duration
}

// vacancy is one slot's vacancy streak (MCH-310): whether the member has seen the slot vacant since the
// reading since, with no view showing it held and no claim of it in between, and whether that has lasted
// SlotGrace, which drops the slot out of the assignment. since and expired mean something only during a
// streak.
type vacancy struct {
	streak  bool
	expired bool
	since   time.Duration
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
//  4. a member whose held slot is vacant in the view keeps it, because views lag, until the views have
//     shown it vacant for LeaseDuration (see loseVacantSlot);
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

// holdSlot makes s the held slot, which recomputes the assignments. The held slot has no vacancy streak,
// so this ends any streak of s: a claim the store answered says the slot is held (MCH-310).
func (m *Machine) holdSlot(s int) {
	m.status.Slot = s
	m.slot.retired, m.slot.releasing = false, false
	m.slot.heldAt, m.slot.shownVacant = m.status.Now, false
	m.endVacancy(s)
	m.view.dirty = true
}

// leaveSlot makes the member a standby, which recomputes the assignments. The next ClaimSlot may go at
// once. A slot the latest view shows vacant starts a vacancy streak now, since the member no longer
// holds it (MCH-310).
func (m *Machine) leaveSlot() {
	h := m.status.Slot
	m.status.Slot = -1
	m.slot.retired, m.slot.releasing = false, false
	m.slot.claimAt = 0
	m.slot.shownVacant = false
	m.startVacancy(h)
	m.view.dirty = true
}

// loseSlot applies a view that gives the held slot to another holder (MCH-630 item 3), or views that have
// shown it vacant for LeaseDuration (MCH-640): the member no longer holds it, sends no ReleaseSlot for it,
// and phase D stops the slot's runs as lost (MCH-470 item 3). Their shard records still name this member,
// so each is released once its run has returned.
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
// released, of each slot being given back (item 13), the next ClaimSlot (item 9), and the vacancy timers
// (items 7 and 8, see armVacancyTimers). Each is after the clock: stepSlots and phase B have just applied
// what was due. A step that makes the shards released again runs stepSlots first, so a repeat that waited
// for them goes in that step. A session that has ended arms none: a lost one holds no slot, a closing one
// holds none and gives none back, and no slot expires after the session has ended.
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
	m.armVacancyTimers(w)
}

// trackVacancy applies the view just applied to the vacancy streaks (MCH-310), before the slot rules
// read it. A slot the view shows vacant starts a streak at the view's reading unless it has one; a slot
// the view shows held ends its streak, and so does the held slot, which never has one. The slots the
// view does not count lose theirs. After the session has ended nothing changes, since no slot expires
// then.
func (m *Machine) trackVacancy() {
	if m.sessionEnded() {
		return
	}
	v := &m.view.view
	if len(m.slot.vacancy) > v.Slots {
		for s := v.Slots; s < len(m.slot.vacancy); s++ {
			m.endVacancy(s)
		}
		m.slot.vacancy = m.slot.vacancy[:v.Slots]
	}
	for len(m.slot.vacancy) < v.Slots {
		m.slot.vacancy = append(m.slot.vacancy, vacancy{})
	}
	for s, holder := range v.SlotHolders {
		switch {
		case holder != "" || s == m.status.Slot:
			m.endVacancy(s)
		case !m.slot.vacancy[s].streak:
			m.slot.vacancy[s] = vacancy{streak: true, since: m.view.appliedAt}
		}
	}
}

// startVacancy starts a vacancy streak of slot s now, when the member stops holding it and the latest
// view shows it vacant (MCH-310). A slot with a streak keeps it. After the session has ended nothing
// starts.
func (m *Machine) startVacancy(s int) {
	if s < 0 || s >= len(m.slot.vacancy) || s >= m.view.view.Slots || m.sessionEnded() || m.view.view.SlotHolders[s] != "" {
		return
	}
	if !m.slot.vacancy[s].streak {
		m.slot.vacancy[s] = vacancy{streak: true, since: m.status.Now}
	}
}

// endVacancy ends the vacancy streak of slot s, if it has one. Ending a mark recomputes the assignments,
// which count the slot again.
func (m *Machine) endVacancy(s int) {
	if s < 0 || s >= len(m.slot.vacancy) {
		return
	}
	if m.slot.vacancy[s].expired {
		m.view.dirty = true
	}
	m.slot.vacancy[s] = vacancy{}
}

// trackVacantDisplay is the part of phase A that times how long the views show the held slot vacant while
// the member holds it (MCH-640). The timer starts when the latest view shows the held slot vacant and the
// timer is not running, at that view's reading, or at the reading the member took the slot if that came
// later: a view from before the claim says nothing about the claim. A view that keeps showing the slot
// vacant keeps the start. Anything else stops the timer: a view that lists the slot for this member, gives
// it to another holder or does not count it, and a member that holds no slot. A change of the held slot
// stops it too, and it starts again here if the latest view shows the new one vacant. After the session
// has ended it stays stopped.
func (m *Machine) trackVacantDisplay() {
	h := m.status.Slot
	v := &m.view.view
	switch {
	case m.sessionEnded() || h < 0 || h >= v.Slots || v.SlotHolders[h] != "":
		m.slot.shownVacant = false
	case !m.slot.shownVacant:
		m.slot.shownVacant = true
		m.slot.vacantSince = max(m.view.appliedAt, m.slot.heldAt)
	}
}

// expireVacancies is phase B. It marks every slot whose streak has lasted SlotGrace as expired, which
// recomputes the assignments without it (MCH-310), and then gives up a held slot the views have shown
// vacant for LeaseDuration (MCH-640, see loseVacantSlot). After the session has ended nothing expires.
// The clock is at or below maxReading and every start is at or below the clock, so each sum fits in a
// Duration.
func (m *Machine) expireVacancies() {
	if m.sessionEnded() {
		return
	}
	for s := range m.slot.vacancy {
		vac := &m.slot.vacancy[s]
		if vac.streak && !vac.expired && m.status.Now >= vac.since+m.cfg.Timing.SlotGrace {
			vac.expired = true
			m.view.dirty = true
		}
	}
	m.loseVacantSlot()
}

// loseVacantSlot gives up the held slot once the views have shown it vacant for LeaseDuration and the
// session is live (MCH-640). The slot's record may have lapsed in the store, and the other members drop
// a slot they have seen vacant for SlotGrace, which is at least LeaseDuration, and take its shards. So the
// member loses the slot as if another holder had it: it sends no ReleaseSlot, and its runs stop as lost.
// While the session is not live the loss waits, and the first live step loses the slot. Losing the slot
// starts its vacancy streak.
func (m *Machine) loseVacantSlot() {
	if m.slot.shownVacant && m.live && m.status.Now >= m.slot.vacantSince+m.cfg.Timing.LeaseDuration {
		m.loseSlot()
	}
}

// armVacancyTimers arms the expiry of each streak that has not expired (MCH-800 item 7) and, while the
// session is live, the loss of a held slot the views show vacant (item 8). Each is after the clock: phase
// B has just applied what was due. While the session is not live no loss is armed: only a renewal makes
// it live again, and the step of that renewal loses the slot if it is due.
func (m *Machine) armVacancyTimers(w *wakeTimer) {
	for _, vac := range m.slot.vacancy {
		if vac.streak && !vac.expired {
			w.arm(vac.since + m.cfg.Timing.SlotGrace)
		}
	}
	if m.slot.shownVacant && m.live {
		w.arm(m.slot.vacantSince + m.cfg.Timing.LeaseDuration)
	}
}

// expiredSlots returns the expiry marks of the applied view's slots as PlacementKeys takes them: nil for a
// view without slots, and otherwise one entry per slot, true for a slot whose streak has lasted SlotGrace.
func (m *Machine) expiredSlots() []bool {
	n := m.view.view.Slots
	if n == 0 {
		return nil
	}
	expired := make([]bool, n)
	for s := range min(n, len(m.slot.vacancy)) {
		expired[s] = m.slot.vacancy[s].expired
	}
	return expired
}
