// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import "time"

// viewState is the applied view and what recomputing the assignments needs from it.
type viewState struct {
	// view is the applied view. Status reports its version.
	view View

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
	m.view = viewState{view: in.View, wall: in.Wall.Round(0), appliedAt: m.status.Now, dirty: true}
}

// recomputeAssignments is phase C: it stores a new assignment for every space, and the Wanted flag
// of every shard, when the applied view has changed since the last time. The assignment follows the
// view while the member drains, but nothing is wanted then.
func (m *Machine) recomputeAssignments() {
	if !m.view.dirty {
		return
	}
	m.view.dirty = false

	// expired stays all false until the machine tracks how long slots have been vacant.
	var expired []bool
	if m.view.view.Slots > 0 {
		expired = make([]bool, m.view.view.Slots)
	}
	keys, _ := PlacementKeys(m.view.view, m.view.wall.Add(m.status.Now-m.view.appliedAt), expired)

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
// it gives the member none. Without slots that is the member's ID. With slots it would be the key
// of the slot the member holds, and the machine cannot hold a slot yet, so nothing is wanted in a
// view with slots.
func (m *Machine) placementKey() string {
	if m.view.view.Slots > 0 {
		return ""
	}
	return m.cfg.Member
}
