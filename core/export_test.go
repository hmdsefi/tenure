// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

// MachineTiming returns the timing m keeps, for tests in package core_test.
func MachineTiming(m *Machine) Timing { return m.cfg.Timing }

// ForceDraining sets m's draining flag directly, for tests of what the flag changes that do not
// depend on how it was set.
func ForceDraining(m *Machine) { m.status.Draining = true }
