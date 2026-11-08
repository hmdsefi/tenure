// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import "time"

// sessionTimers is the session's bookkeeping that Status does not report.
type sessionTimers struct {
	// renewed reports that a renewal has been accepted, which ends the opening state.
	renewed bool

	// keepAliveSet reports that keepAliveAt is set. A reading of 0 is a valid keepAliveAt.
	keepAliveSet bool

	// keepAliveAt is L: the reading of the last returned KeepAlive. When the first accepted
	// renewal comes before any KeepAlive, it is that renewal's SentAt: the driver opens the
	// session before its first Step, and that open counts as the first send.
	keepAliveAt time.Duration

	// closeAt is the reading of the next CloseSession: X + RetryPeriod, where X is the reading of the
	// last one. It means something only while the session is closing.
	closeAt time.Duration
}

// applySessionInput applies the effect of a session input. Other inputs have none here.
func (m *Machine) applySessionInput(in Input) {
	switch v := in.(type) {
	case SessionRenewed:
		m.applyRenewal(v.SentAt)
	case SessionFailed:
		m.applyFailure(v.Cause)
	case SessionClosed:
		m.applySessionClosed()
	case Drain:
		m.applyDrain()
	}
}

// closingInput returns the input a step applies while the session is closing (MCH-140): a Tick or a
// SessionClosed as it is, and in place of any other a Tick at the clock's reading, which has no effect
// of its own. The session is about to end and every shard is idle, so nothing else may act any more:
// not a renewal, a failure, a view, a record or an answer, however late it arrives.
func (m *Machine) closingInput(in Input) Input {
	switch in.(type) {
	case Tick, SessionClosed:
		return in
	}
	return Tick{Now: m.status.Now}
}

// applySessionClosed applies the report that the session is closed. Only a closing session takes it,
// so a report the machine did not ask for does nothing, and once closed every later Step returns nil.
func (m *Machine) applySessionClosed() {
	if m.status.Session == SessionStateClosing {
		m.status.Session = SessionStateClosed
	}
}

// applyDrain starts draining for good (MCH-700): from this step on nothing is wanted, so D3 stops every
// run with StopRun{draining}, a grant takes the Release without a run, no shard acquires again, and phase F
// closes the session once every shard is idle. No shard waits to ask again, so RetryAt is cleared. A
// second Drain changes nothing, and a Drain after the session has ended is ignored: a lost session ends
// on its own path and never closes.
func (m *Machine) applyDrain() {
	if m.status.Draining || m.sessionEnded() {
		return
	}
	m.status.Draining = true
	for i := range m.status.Spaces {
		shards := m.status.Spaces[i].Shards
		for j := range shards {
			shards[j].Wanted = false
			shards[j].RetryAt = 0
		}
	}
}

// closeIfDrained is phase F (MCH-710, MCH-730): a draining member whose shards are all idle, and which
// holds no slot, closes its session, and the CloseSession goes again every RetryPeriod, counted from the
// last one, until the session is closed. Closing frees every slot the member is still giving back. Closing makes every record of this member's takeable, so it must wait for every run
// to return, a stuck one included: a shard is idle only once its run has returned, and its Acquire and
// Release answered. A lost session never closes: its records are takeable already. The session closes
// from opening and late too, since the handover needs nothing more from it. The clock is at or below
// maxReading, so the next reading fits in a Duration.
func (m *Machine) closeIfDrained() {
	switch {
	case m.status.Session == SessionStateClosing:
		if m.status.Now < m.session.closeAt {
			return
		}
	case !m.readyToClose():
		return
	default:
		m.status.Session = SessionStateClosing
		m.dropSessionOutput()
	}
	m.session.closeAt = m.status.Now + m.cfg.Timing.RetryPeriod
	m.emit(groupClose, CloseSession{})
}

// dropSessionOutput drops the ExtendDeadline of the step that starts closing, which returns only
// CloseSession and Wake, as every step while closing does. Only a renewal step holds one, and a renewal
// step starts closing only when the session it makes live gives up a held slot the views have shown
// vacant too long (MCH-640). The session is about to end and no run is left, so nothing needs the later
// deadline. The KeepAlive comes after phase F, so the step has none to drop.
func (m *Machine) dropSessionOutput() {
	clear(m.out[groupSession])
	m.out[groupSession] = m.out[groupSession][:0]
}

// readyToClose reports that phase F starts closing the session in this step: the member drains, the
// session has not ended, every shard is idle and the member holds no slot, a slot it is releasing
// included.
func (m *Machine) readyToClose() bool {
	return m.status.Draining && !m.sessionEnded() && m.status.Slot < 0 && m.everyShardIdle()
}

// applyRenewal applies a renewal sent at sentAt. The deadline counts from the send time and
// never moves earlier. A renewal sent after the clock's reading, or one that arrives after the
// session has ended, is ignored. An older renewal moves nothing but still clears the failure.
func (m *Machine) applyRenewal(sentAt time.Duration) {
	if sentAt > m.status.Now || m.sessionEnded() {
		return
	}
	m.session.renewed = true
	m.status.LastFailure = CauseNone
	if !m.session.keepAliveSet {
		m.session.keepAliveSet = true
		m.session.keepAliveAt = sentAt
	}
	if d := sentAt + m.cfg.Timing.RenewDeadline; d > m.status.Deadline {
		m.status.Deadline = d
		m.emit(groupSession, ExtendDeadline{Deadline: d})
	}
}

// applyFailure applies a failed renewal. An unreachable store only records the cause: the next
// KeepAlive comes on the normal cadence. A lost session is final. CauseNone and values without
// a name are ignored, and so is any failure after the session has ended.
func (m *Machine) applyFailure(cause Cause) {
	if m.sessionEnded() {
		return
	}
	switch cause {
	case CauseUnreachable:
		m.status.LastFailure = cause
	case CauseLost:
		m.status.LastFailure = cause
		m.status.Session = SessionStateLost
	}
}

// sessionEnded reports that the session can no longer renew.
func (m *Machine) sessionEnded() bool {
	switch m.status.Session {
	case SessionStateLost, SessionStateClosing, SessionStateClosed:
		return true
	}
	return false
}

// sessionLost reports that the session is lost, rather than closing or closed.
func (m *Machine) sessionLost() bool {
	return m.status.Session == SessionStateLost
}

// refreshSession sets the session's state from the clock: opening until the first accepted
// renewal, then live before the deadline and late from the deadline on. A driver that wakes at
// the deadline therefore finds it already past. A clock at maxReading cannot move on to a deadline
// past it, so the session is late there too rather than live for good.
func (m *Machine) refreshSession() {
	switch {
	case m.sessionEnded() || !m.session.renewed:
	case m.status.Now < m.status.Deadline && m.status.Now < maxReading:
		m.status.Session = SessionStateLive
	default:
		m.status.Session = SessionStateLate
	}
}

// emitKeepAlive returns a KeepAlive when one is due: none has been returned, or RetryPeriod has
// passed since the last one. A failed renewal does not change the cadence.
func (m *Machine) emitKeepAlive() {
	if m.sessionEnded() {
		return
	}
	if m.session.keepAliveSet && m.status.Now < m.session.keepAliveAt+m.cfg.Timing.RetryPeriod {
		return
	}
	m.session.keepAliveSet = true
	m.session.keepAliveAt = m.status.Now
	m.emit(groupSession, KeepAlive{})
}

// armSessionTimers arms the next KeepAlive and, while the session is live, its deadline, or, while
// the session is closing, only the next CloseSession (MCH-800 item 11). Each is after the clock:
// emitKeepAlive and closeIfDrained have just returned whatever was due, and live means the deadline has
// not passed.
func (m *Machine) armSessionTimers(w *wakeTimer) {
	if m.status.Session == SessionStateClosing {
		w.arm(m.session.closeAt)
		return
	}
	if m.sessionEnded() {
		return
	}
	w.arm(m.session.keepAliveAt + m.cfg.Timing.RetryPeriod)
	if m.live {
		w.arm(m.status.Deadline)
	}
}
