// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !tenure_mutants

package world

import "testing"

// TestMutantsOff checks that a build without the tag tenure_mutants plants no bug, whatever a
// scenario's Mutation names, and that a machine then gets the scenario's own timing.
func TestMutantsOff(t *testing.T) {
	all := []Mutation{MutationNone, GrantHeld, SkipRaise, LateDeadline, ReadConfirm}
	for _, m := range all {
		for _, bug := range all {
			if m.plants(bug) {
				t.Errorf("Mutation %v plants %v in a build without tenure_mutants", m, bug)
			}
		}
	}
	sc := Scenario{Mutation: LateDeadline}.withDefaults()
	if got := machineTiming(sc); got != sc.Timing {
		t.Errorf("machineTiming with LateDeadline = %+v, want the scenario's %+v", got, sc.Timing)
	}
}
