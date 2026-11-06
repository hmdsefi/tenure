// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"slices"
	"time"
)

// Status is a snapshot of a Machine.
type Status struct {
	// Now is the machine's monotonic reading.
	Now time.Duration

	// Session is where the session stands.
	Session SessionState

	// Deadline is the session deadline, or 0 until the first accepted renewal.
	Deadline time.Duration

	// LastFailure is the cause of the last renewal failure, or CauseNone when no renewal has
	// failed since the last accepted one.
	LastFailure Cause

	// Done reports that the machine has nothing left to do.
	Done bool

	// Draining reports that the member is handing its work over before it leaves.
	Draining bool

	// Slot is the slot the member holds, or -1 for none.
	Slot int

	// ViewVersion is the version of the last applied view.
	ViewVersion uint64

	// Spaces holds the spaces sorted by name.
	Spaces []SpaceStatus
}

// SpaceStatus is the state of one space in a Status.
type SpaceStatus struct {
	// Name and Fenced are the values of the space's SpaceSpec.
	Name   string
	Fenced bool

	// Shards holds one entry per shard, indexed by shard number.
	Shards []ShardStatus
}

// ShardStatus is the state of one shard in a SpaceStatus.
type ShardStatus struct {
	// State is where the shard stands.
	State ShardState

	// Since is the machine's reading at the last change of State.
	Since time.Duration

	// Term is the term of the shard's run, or the term being released.
	Term Term

	// Reason says why the run was stopped, or is StopNone when it was not.
	Reason StopReason

	// Holder is the last known holder of the shard's record.
	Holder string

	// Wanted reports that the assignment gives the shard to this member.
	Wanted bool

	// Stuck reports that the run has outlived RenewDeadline since its StopRun action.
	Stuck bool

	// RetryAt is the earliest reading for the next Acquire action, or 0 for none.
	RetryAt time.Duration
}

// Status returns a snapshot of the machine. The result shares no memory with the machine.
func (m *Machine) Status() Status {
	st := m.status
	st.Spaces = make([]SpaceStatus, len(m.status.Spaces))
	for i, s := range m.status.Spaces {
		s.Shards = slices.Clone(s.Shards)
		st.Spaces[i] = s
	}
	return st
}

// Assignment returns the placement key of each shard of the named space, indexed by shard
// number, or nil when the machine has no such space. The key is "" for a shard with no key, so
// every key is "" before the first view. Callers must not modify the result. A new assignment
// is stored in a new slice, so an earlier result never changes.
func (m *Machine) Assignment(space string) []string {
	i, ok := slices.BinarySearchFunc(m.cfg.Spaces, space, func(s SpaceSpec, name string) int {
		return cmp.Compare(s.Name, name)
	})
	if !ok {
		return nil
	}
	return m.assignments[i]
}
