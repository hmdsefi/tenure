// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

// MachineTiming returns the timing m keeps, for tests in package core_test.
func MachineTiming(m *Machine) Timing { return m.cfg.Timing }

// ForceSessionClosed closes m's session directly, for tests of a closed machine that do not depend
// on how it closed.
func ForceSessionClosed(m *Machine) { m.status.Session = SessionStateClosed }
