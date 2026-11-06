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
}

// applySessionInput applies the effect of a session input. Other inputs have none here.
func (m *Machine) applySessionInput(in Input) {
	switch v := in.(type) {
	case SessionRenewed:
		m.applyRenewal(v.SentAt)
	case SessionFailed:
		m.applyFailure(v.Cause)
	}
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

// armSessionTimers arms the next KeepAlive and, while the session is live, its deadline. Both
// are after the clock: emitKeepAlive has just returned any KeepAlive that was due, and live
// means the deadline has not passed.
func (m *Machine) armSessionTimers(w *wakeTimer) {
	if m.sessionEnded() {
		return
	}
	w.arm(m.session.keepAliveAt + m.cfg.Timing.RetryPeriod)
	if m.live {
		w.arm(m.status.Deadline)
	}
}
