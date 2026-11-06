// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import "time"

// MachineTiming returns the timing m keeps, for tests in package core_test.
func MachineTiming(m *Machine) Timing { return m.cfg.Timing }

// ForceDraining sets m's draining flag directly, for tests of what the flag changes that do not
// depend on how it was set.
func ForceDraining(m *Machine) { m.status.Draining = true }

// ForceSessionClosed closes m's session directly, for tests of a closed machine that do not depend
// on how it closed.
func ForceSessionClosed(m *Machine) { m.status.Session = SessionStateClosed }

// ViewWall returns the wall-clock time m keeps for its applied view, for tests in package core_test.
func ViewWall(m *Machine) time.Time { return m.view.wall }

// MarkViewDirty makes m's next step recompute its assignments, for tests of a recompute later than
// the step that applied the view. Nothing else does that yet: a change of slot data will.
func MarkViewDirty(m *Machine) { m.view.dirty = true }
