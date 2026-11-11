// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build tenure_mutants

package world

import (
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestMutantsOn checks the selection of a build with the tag tenure_mutants: a world plants
// the bug its scenario's Mutation names, or the one SetMutationForTest names until the
// function it returned restores the previous one, and MutationNone plants nothing. It also
// checks LateDeadline's machine timing: twice the RenewDeadline, a LeaseDuration longer by the
// scenario's RenewDeadline, and a valid timing.
func TestMutantsOn(t *testing.T) {
	if !SkipRaise.plants(SkipRaise) || SkipRaise.plants(GrantHeld) || MutationNone.plants(MutationNone) {
		t.Error("the scenario's Mutation does not plant exactly its own bug")
	}
	restore := SetMutationForTest(GrantHeld)
	if !MutationNone.plants(GrantHeld) || !SkipRaise.plants(GrantHeld) || MutationNone.plants(SkipRaise) {
		t.Error("SetMutationForTest(GrantHeld) does not plant GrantHeld alone in a scenario without one")
	}
	restore()
	if MutationNone.plants(GrantHeld) {
		t.Error("GrantHeld is still planted after the restore")
	}

	sc := Scenario{Mutation: LateDeadline}.withDefaults()
	got := machineTiming(sc)
	want := core.Timing{LeaseDuration: 25 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 2 * time.Second, SlotGrace: 60 * time.Second}
	if got != want {
		t.Errorf("machineTiming with LateDeadline = %+v, want %+v", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("LateDeadline's machine timing is not valid: %v", err)
	}
}
