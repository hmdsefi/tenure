// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"fmt"
	"time"
)

// Input is one event the driver feeds to a Machine through Step. Every input carries Now, the
// driver's monotonic reading when it made the input.
//
// Write input values with keyed fields: later versions add fields.
type Input interface{ isInput() }

// Tick reports that time has passed and nothing else has happened.
type Tick struct {
	// Now is the driver's monotonic reading.
	Now time.Duration
}

// ViewChanged delivers a new membership snapshot. The machine ignores a snapshot that is not valid
// or whose version is older than the last one it applied. A snapshot of the same version applies.
type ViewChanged struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// View is the new snapshot.
	View View

	// Wall is the wall-clock time at Now.
	Wall time.Time
}

// SessionRenewed reports a successful session renewal.
type SessionRenewed struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// SentAt is the driver's monotonic reading when it sent the renewal.
	SentAt time.Duration
}

// SessionFailed reports a failed session renewal.
type SessionFailed struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// Cause says how the renewal failed.
	Cause Cause
}

// SessionClosed reports that the session has been closed.
type SessionClosed struct {
	// Now is the driver's monotonic reading.
	Now time.Duration
}

// Acquired answers an Acquire action.
type Acquired struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term the store granted; 0 means no grant.
	Term Term
}

// AcquireFailed answers an Acquire action when another holder has the shard.
type AcquireFailed struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// Space and Shard name the shard.
	Space string
	Shard int

	// Holder is the holder the shard's record names.
	Holder string

	// RetryAfter is how long the store asks the member to wait before it tries again. The machine
	// waits at least RetryPeriod and at most LeaseDuration, whatever this says.
	RetryAfter time.Duration
}

// Released answers a Release action.
type Released struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term that was released.
	Term Term
}

// RunStopped reports that the run of a shard has ended.
type RunStopped struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term of the run that ended.
	Term Term
}

// HolderChanged reports what a shard's record says now.
type HolderChanged struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// Space and Shard name the shard.
	Space string
	Shard int

	// Holder is the holder the record names; "" means the record has no holder.
	Holder string

	// Term is the term of the record.
	Term Term
}

// SlotClaimed reports that the member now holds a slot.
type SlotClaimed struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// Slot is the slot the member claimed.
	Slot int
}

// SlotReleased reports that the member no longer holds a slot.
type SlotReleased struct {
	// Now is the driver's monotonic reading.
	Now time.Duration

	// Slot is the slot the member released.
	Slot int
}

// Drain asks the member to hand its work over before it leaves.
type Drain struct {
	// Now is the driver's monotonic reading.
	Now time.Duration
}

func (Tick) isInput()           {}
func (ViewChanged) isInput()    {}
func (SessionRenewed) isInput() {}
func (SessionFailed) isInput()  {}
func (SessionClosed) isInput()  {}
func (Acquired) isInput()       {}
func (AcquireFailed) isInput()  {}
func (Released) isInput()       {}
func (RunStopped) isInput()     {}
func (HolderChanged) isInput()  {}
func (SlotClaimed) isInput()    {}
func (SlotReleased) isInput()   {}
func (Drain) isInput()          {}

// Action is one thing a Machine asks its driver to do.
type Action interface{ isAction() }

// KeepAlive asks the driver to send a session renewal.
type KeepAlive struct{}

// ExtendDeadline reports that the session deadline has moved later.
type ExtendDeadline struct {
	// Deadline is the new session deadline, as a monotonic reading.
	Deadline time.Duration
}

// Acquire asks the driver to try to acquire a shard.
type Acquire struct {
	// Space and Shard name the shard.
	Space string
	Shard int
}

// Release asks the driver to release a shard's record at a term.
type Release struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term to release.
	Term Term
}

// StartRun asks the driver to start the run of a shard.
type StartRun struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term of the run.
	Term Term

	// Deadline is the session deadline when the run starts, as a monotonic reading.
	Deadline time.Duration
}

// StopRun asks the driver to stop the run of a shard.
type StopRun struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term of the run.
	Term Term

	// Reason says why the run must stop.
	Reason StopReason
}

// Confirm asks the driver to read a shard's record back, so that a degraded run can be
// restored. The answer is a HolderChanged input.
//
// A Confirm carries no ID. Once one is out, and for as long as the session stays live, any record
// that names this holder in the run's term restores the run, and the machine cannot tell the
// answer from a reading taken earlier, before another holder took the shard. So while Status shows
// the shard degraded, the driver must step such a record only when its read started after the
// shard's latest Confirm, and hold back any other.
type Confirm struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term of the run.
	Term Term
}

// Degrade tells the driver that a run in a fenced space has lost its live session. The run
// keeps going.
type Degrade struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term of the run.
	Term Term
}

// Restore tells the driver that a degraded run is owned again.
type Restore struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term of the run.
	Term Term
}

// ClaimSlot asks the driver to try to claim a slot.
type ClaimSlot struct{}

// ReleaseSlot asks the driver to release a slot.
type ReleaseSlot struct {
	// Slot is the slot to release.
	Slot int
}

// CloseSession asks the driver to close the session.
type CloseSession struct{}

// Wake asks the driver to call Step again once its monotonic clock reaches a reading.
type Wake struct {
	// At is the reading, the one NextWake returns.
	At time.Duration
}

func (KeepAlive) isAction()      {}
func (ExtendDeadline) isAction() {}
func (Acquire) isAction()        {}
func (Release) isAction()        {}
func (StartRun) isAction()       {}
func (StopRun) isAction()        {}
func (Confirm) isAction()        {}
func (Degrade) isAction()        {}
func (Restore) isAction()        {}
func (ClaimSlot) isAction()      {}
func (ReleaseSlot) isAction()    {}
func (CloseSession) isAction()   {}
func (Wake) isAction()           {}

// Cause says how a session renewal failed.
type Cause int

const (
	// CauseNone means no failure.
	CauseNone Cause = iota

	// CauseUnreachable means the store could not be reached. The session may still exist.
	CauseUnreachable

	// CauseLost means the session is gone for good.
	CauseLost
)

// String returns the name of c, or text such as "Cause(7)" for a value without a name.
func (c Cause) String() string {
	switch c {
	case CauseNone:
		return "none"
	case CauseUnreachable:
		return "unreachable"
	case CauseLost:
		return "lost"
	}
	return fmt.Sprintf("Cause(%d)", int(c))
}

// StopReason says why a run was stopped.
type StopReason int

const (
	// StopNone means the run was not stopped.
	StopNone StopReason = iota

	// StopMoved means the assignment no longer gives the shard to this member.
	StopMoved

	// StopDraining means the member is handing its work over before it leaves.
	StopDraining

	// StopExpired means the session deadline passed in a space without fencing.
	StopExpired

	// StopLost means another holder has the shard, or the session is gone for good.
	StopLost
)

// String returns the name of r, or text such as "StopReason(7)" for a value without a name.
func (r StopReason) String() string {
	switch r {
	case StopNone:
		return "none"
	case StopMoved:
		return "moved"
	case StopDraining:
		return "draining"
	case StopExpired:
		return "expired"
	case StopLost:
		return "lost"
	}
	return fmt.Sprintf("StopReason(%d)", int(r))
}

// ShardState is where a shard stands on one member.
type ShardState int

const (
	// ShardStateIdle means the member has neither a run of the shard nor a request for it.
	ShardStateIdle ShardState = iota

	// ShardStateAcquiring means an Acquire action waits for its answer.
	ShardStateAcquiring

	// ShardStateOwned means the shard's run is going under a live session.
	ShardStateOwned

	// ShardStateDegraded means the run of a fenced space keeps going, not yet confirmed
	// after the session stopped being live.
	ShardStateDegraded

	// ShardStateStopping means the run was told to stop because the shard is no longer
	// wanted.
	ShardStateStopping

	// ShardStateExpired means the run was told to stop because the session deadline passed.
	ShardStateExpired

	// ShardStateLost means the run was told to stop because the member no longer holds the
	// shard.
	ShardStateLost

	// ShardStateReleasing means the shard has no run and a Release action waits for its answer.
	ShardStateReleasing
)

// String returns the name of s, or text such as "ShardState(9)" for a value without a name.
func (s ShardState) String() string {
	switch s {
	case ShardStateIdle:
		return "idle"
	case ShardStateAcquiring:
		return "acquiring"
	case ShardStateOwned:
		return "owned"
	case ShardStateDegraded:
		return "degraded"
	case ShardStateStopping:
		return "stopping"
	case ShardStateExpired:
		return "expired"
	case ShardStateLost:
		return "lost"
	case ShardStateReleasing:
		return "releasing"
	}
	return fmt.Sprintf("ShardState(%d)", int(s))
}

// SessionState is where the member's session stands.
type SessionState int

const (
	// SessionStateOpening means no renewal has been accepted yet.
	SessionStateOpening SessionState = iota

	// SessionStateLive means the session deadline has not passed.
	SessionStateLive

	// SessionStateLate means the session deadline has passed. A renewal can make the session
	// live again.
	SessionStateLate

	// SessionStateLost means the session is gone for good.
	SessionStateLost

	// SessionStateClosing means the member is closing the session.
	SessionStateClosing

	// SessionStateClosed means the session is closed.
	SessionStateClosed
)

// String returns the name of s, or text such as "SessionState(9)" for a value without a name.
func (s SessionState) String() string {
	switch s {
	case SessionStateOpening:
		return "opening"
	case SessionStateLive:
		return "live"
	case SessionStateLate:
		return "late"
	case SessionStateLost:
		return "lost"
	case SessionStateClosing:
		return "closing"
	case SessionStateClosed:
		return "closed"
	}
	return fmt.Sprintf("SessionState(%d)", int(s))
}
