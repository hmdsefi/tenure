// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"errors"
	"math"
	"slices"
	"time"
)

// maxReading is the highest reading the clock takes. A timer's reading is a reading at or below the
// clock plus one Timing field, and maxTimingField bounds those, so every timer's reading fits in a
// Duration and an armed timer is after the clock. A driver's readings stay far below this: only a
// driver that breaks its contract reaches it.
const maxReading = time.Duration(math.MaxInt64) - maxTimingField

// Machine is the ownership state machine of one member. It reads no clock, starts nothing and
// performs no I/O: the driver feeds it inputs through Step and carries out the actions Step
// returns.
//
// A Machine is not safe for concurrent use.
type Machine struct {
	// cfg is the validated config: Spaces is a private copy sorted by name, and Timing is
	// never the zero value.
	cfg MachineConfig

	// status holds everything Status reports. status.Spaces is parallel to cfg.Spaces.
	status Status

	// view is the applied view.
	view viewState

	// assignments holds the placement key per shard. It is parallel to cfg.Spaces.
	assignments [][]string

	// shards holds the shards' bookkeeping that Status does not report. It is parallel to
	// cfg.Spaces, and each entry to that space's shards.
	shards [][]shardBook

	// session holds the session's bookkeeping that Status does not report.
	session sessionTimers

	// slot holds the slot bookkeeping that Status does not report.
	slot slotBook

	// live is the step's one evaluation of the session, made in phase A. Every later phase of
	// the step reads it, so they all agree.
	live bool

	// out holds the current step's actions per output group. Between steps its slices are
	// empty; they keep their capacity.
	out [groupCount][]Action

	// wake is the earliest armed timer as of the end of the last step.
	wake wakeTimer
}

// outputGroup is one group of Step's output order. Within a group, actions are in space name
// order, then shard order, then the order the phases emitted them.
type outputGroup int

const (
	// groupSession holds ExtendDeadline, then KeepAlive.
	groupSession outputGroup = iota

	// groupRunChange holds StopRun, Degrade and Restore.
	groupRunChange

	// groupRunStart holds StartRun.
	groupRunStart

	// groupRecord holds Release, Confirm and Acquire.
	groupRecord

	// groupSlot holds ReleaseSlot and ClaimSlot.
	groupSlot

	// groupClose holds CloseSession.
	groupClose

	// groupReserved stays empty until a later phase of the project adds its action.
	groupReserved //nolint:unused // A later phase of the project emits to it.

	// groupCount is the number of groups. The Wake comes after them all: phase H appends it.
	groupCount
)

// wakeTimer is the earliest of the timers armed so far.
type wakeTimer struct {
	at    time.Duration
	armed bool
}

// arm makes at the timer's reading if no timer is armed or at is earlier.
func (w *wakeTimer) arm(at time.Duration) {
	if !w.armed || at < w.at {
		w.at, w.armed = at, true
	}
}

// NewMachine returns a Machine for cfg, with its spaces sorted by name, its session opening, no
// slot, every shard idle and no timer armed. It panics with the error of cfg.Validate when cfg
// is not valid. It copies cfg.Spaces and does not modify it.
func NewMachine(cfg MachineConfig) *Machine {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	cfg.Timing = cfg.timing()
	cfg.Spaces = slices.Clone(cfg.Spaces)
	slices.SortStableFunc(cfg.Spaces, func(a, b SpaceSpec) int { return cmp.Compare(a.Name, b.Name) })

	m := &Machine{
		cfg: cfg,
		status: Status{
			Session: SessionStateOpening,
			Slot:    -1,
			Spaces:  make([]SpaceStatus, len(cfg.Spaces)),
		},
		assignments: make([][]string, len(cfg.Spaces)),
		shards:      make([][]shardBook, len(cfg.Spaces)),
	}
	for i, s := range cfg.Spaces {
		m.status.Spaces[i] = SpaceStatus{Name: s.Name, Fenced: s.Fenced, Shards: make([]ShardStatus, s.Shards)}
		m.assignments[i] = make([]string, s.Shards)
		m.shards[i] = make([]shardBook, s.Shards)
	}
	return m
}

// Step applies one input and returns the actions the driver must carry out, or nil when there
// are none. It panics when in is nil.
//
// The machine's clock moves to the input's Now and never back, so an input with an older Now
// runs at the machine's own reading. The clock stops 24 hours short of the largest Duration, where
// the session is never live, so that every timer's reading fits. A value of any other type that
// satisfies Input, such as a pointer to an input, leaves the clock where it is. An input that does
// not apply, such as a renewal sent after the clock's reading or an answer to a request the machine
// did not make, has no effect of its own, but the rest of the step still runs, so whatever is due
// still happens.
//
// Actions come in a fixed order: ExtendDeadline and KeepAlive; then StopRun, Degrade and
// Restore; StartRun; Release, Confirm and Acquire; ReleaseSlot and ClaimSlot; CloseSession; and
// Wake last. When a timer is armed the result ends with a Wake for the reading NextWake returns,
// which is after the machine's clock, and contains no other Wake.
//
// The step that accepts a lost session returns a StopRun for each run not yet told to stop. Later
// steps return no action but a Wake, until every shard is idle and Status reports Done.
//
// After a Drain the machine wants no shard, so it stops every run and hands each shard over. Once
// every shard is idle, every run having returned, a stuck one included, it returns CloseSession, and
// again every RetryPeriod until a SessionClosed. While the session is closing, every input but a Tick
// and a SessionClosed is ignored, and a step returns nothing but CloseSession and Wake. A lost session
// is never closed.
//
// Once Status reports Done, Step returns nil. Once the session is closed, the clock stops too.
func (m *Machine) Step(in Input) []Action {
	if in == nil {
		panic(errors.New("core: Step: nil Input"))
	}
	if m.status.Session == SessionStateClosed {
		return nil
	}

	// Phase A: the clock, the session and view inputs, the step's one evaluation of live, and what a
	// session not live does to the Confirms.
	m.status.Now = min(max(m.status.Now, inputNow(in)), maxReading)
	if m.status.Session == SessionStateClosing {
		in = m.closingInput(in)
	}
	m.applySessionInput(in)
	m.applyViewInput(in)
	m.applySlotInput(in)
	m.refreshSession()
	m.live = m.status.Session == SessionStateLive
	if !m.live {
		m.forgetConfirms()
	}

	// Phase B (slot marks) goes here.

	// Phase C: the assignments, if a view has changed them.
	m.recomputeAssignments()

	// Phase D: the shards.
	m.stepShards(in)

	// Phase E: the slots. Phase F: closing. The KeepAlive comes after them because closing the session
	// ends the cadence, so the step that starts closing returns none.
	m.stepSlots()
	m.closeIfDrained()
	m.emitKeepAlive()
	m.status.Done = m.done()

	// Phase G: the output order. Phase H: the wake.
	result := m.assemble()
	m.armTimers()
	if m.wake.armed {
		result = append(result, Wake{At: m.wake.at})
	}
	return result
}

// inputNow returns the reading in carries, or 0, which never moves the clock, for a type that
// is not one of the machine's inputs.
func inputNow(in Input) time.Duration {
	switch v := in.(type) {
	case Tick:
		return v.Now
	case ViewChanged:
		return v.Now
	case SessionRenewed:
		return v.Now
	case SessionFailed:
		return v.Now
	case SessionClosed:
		return v.Now
	case Acquired:
		return v.Now
	case AcquireFailed:
		return v.Now
	case Released:
		return v.Now
	case RunStopped:
		return v.Now
	case HolderChanged:
		return v.Now
	case SlotClaimed:
		return v.Now
	case SlotReleased:
		return v.Now
	case Drain:
		return v.Now
	}
	return 0
}

// emit adds a to group g of the current step's output.
func (m *Machine) emit(g outputGroup, a Action) {
	m.out[g] = append(m.out[g], a)
}

// assemble empties the group buffers into a new slice, in group order, with room for the Wake.
// It returns nil when they are empty.
func (m *Machine) assemble() []Action {
	n := 0
	for g := range m.out {
		n += len(m.out[g])
	}
	if n == 0 {
		return nil
	}
	result := make([]Action, 0, n+1)
	for g := range m.out {
		result = append(result, m.out[g]...)
		clear(m.out[g])
		m.out[g] = m.out[g][:0]
	}
	return result
}

// armTimers sets the earliest armed timer. Every timer it arms is after the clock.
func (m *Machine) armTimers() {
	m.wake = wakeTimer{}
	m.armSessionTimers(&m.wake)
	m.armShardTimers(&m.wake)
	m.armSlotTimers(&m.wake)
}

// NextWake returns the reading of the earliest armed timer and true, or 0 and false when no
// timer is armed. A new Machine has no timer armed.
func (m *Machine) NextWake() (time.Duration, bool) {
	return m.wake.at, m.wake.armed
}
