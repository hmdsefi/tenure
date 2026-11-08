// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import "time"

// viewState is the applied view and what recomputing the assignments needs from it.
type viewState struct {
	// view is the applied view. Status reports its version.
	view View

	// applied reports that a view has been applied.
	applied bool

	// wall is W: the view's wall-clock time, without a monotonic reading.
	wall time.Time

	// appliedAt is A: the machine's reading when the view was applied.
	appliedAt time.Duration

	// dirty reports that the assignments no longer match the applied view. It is the only
	// thing that makes a step recompute, so a plain tick never does.
	dirty bool
}

// applyViewInput applies a view that arrives as in. Other inputs have no effect here.
func (m *Machine) applyViewInput(in Input) {
	if v, ok := in.(ViewChanged); ok {
		m.applyView(v)
	}
}

// applyView applies the view of in unless its version is older than the applied one or it is not
// valid. A view of the same version applies: the driver may correct a snapshot without renumbering
// it.
func (m *Machine) applyView(in ViewChanged) {
	if in.View.Version < m.status.ViewVersion || in.View.Validate() != nil {
		return
	}
	m.status.ViewVersion = in.View.Version
	m.view = viewState{view: in.View, applied: true, wall: in.Wall.Round(0), appliedAt: m.status.Now, dirty: true}
	m.trackVacancy()
	m.applyViewSlots()
}

// recomputeAssignments is phase C: it stores a new assignment for every space, and the Wanted flag
// of every shard, when the applied view, the held slot or a slot's expiry mark has changed since the last
// time (MCH-320). A vacant slot marked expired is no placement key. The assignment follows the view while
// the member drains, but nothing is wanted then.
func (m *Machine) recomputeAssignments() {
	if !m.view.dirty {
		return
	}
	m.view.dirty = false

	keys, _ := PlacementKeys(m.view.view, m.view.wall.Add(m.status.Now-m.view.appliedAt), m.expiredSlots())

	own := m.placementKey()
	for i, s := range m.cfg.Spaces {
		assignment := make([]string, s.Shards)
		shards := m.status.Spaces[i].Shards
		for shard, k := range Balanced(s.Name, s.Shards, keys) {
			if k >= 0 {
				assignment[shard] = keys[k]
			}
			shards[shard].Wanted = !m.status.Draining && own != "" && assignment[shard] == own
		}
		m.assignments[i] = assignment
	}
}

// placementKey returns the key under which the applied view gives this member shards, or "" when
// it gives the member none (MCH-330). With slots that is the key of the held slot, so a standby, or a
// member whose slot is retired, wants nothing. Without slots it is the member's ID, unless the member
// still holds a slot: a group that has dropped its slots retires that slot, and the member wants nothing
// until it has released it.
func (m *Machine) placementKey() string {
	if m.view.view.Slots > 0 {
		return m.slotKey()
	}
	if m.status.Slot >= 0 {
		return ""
	}
	return m.cfg.Member
}
