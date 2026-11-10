// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
)

// TestSetupErrors checks every setup row with its exact message, the order of the rows, and
// scenarios without an error.
func TestSetupErrors(t *testing.T) {
	minute := at(time.Minute)
	book := func(spaces ...core.SpaceSpec) []core.SpaceSpec { return spaces }
	slow := core.Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 9 * time.Second, SlotGrace: time.Minute}
	jump := fault.Rule{Kind: fault.KindClockJump, Every: time.Second, Magnitude: int64(time.Millisecond)}
	tests := []struct {
		name string
		sc   Scenario
		end  kernel.Time // 0: one minute
		want string      // "" for none
	}{
		// One row each, in the order they are checked.
		{"takeover", Scenario{Takeover: 7}, 0, "unknown Takeover 7"},
		{"members above", Scenario{Members: 65}, 0, "Members 65 is outside [1, 64]"},
		{"members below", Scenario{Members: -1}, 0, "Members -1 is outside [1, 64]"},
		{"no spaces", Scenario{Spaces: []core.SpaceSpec{}}, 0, "no spaces"},
		{"empty name", Scenario{Spaces: book(core.SpaceSpec{Shards: 1})}, 0, "Spaces[0]: empty Name"},
		{"no shards", Scenario{Spaces: book(core.SpaceSpec{Name: "book"})}, 0, `space "book": Shards 0 is below 1`},
		{"duplicate space", Scenario{Spaces: book(core.SpaceSpec{Name: "book", Shards: 1}, core.SpaceSpec{Name: "book", Shards: 2})},
			0, `duplicate space "book"`},
		{"timing", Scenario{Timing: slow}, 0, "Timing: RenewDeadline 10s must be greater than 1.2 times RetryPeriod 9s"},
		{"slots below", Scenario{Slots: -1}, 0, "Slots -1 is outside [0, 10000]"},
		{"slots above", Scenario{Slots: 10_001}, 0, "Slots 10001 is outside [0, 10000]"},
		{"rho below", Scenario{Rho: -1}, 0, "Rho -1 is outside [0, 500000]"},
		{"rho above", Scenario{Rho: 500_001}, 0, "Rho 500001 is outside [0, 500000]"},
		{"delta", Scenario{Delta: -1}, 0, "Delta -1ns is negative"},
		{"wall skew", Scenario{WallSkew: -1}, 0, "WallSkew -1ns is negative"},
		{"wall steps", Scenario{WallSteps: -1}, 0, "WallSteps -1ns is negative"},
		{"stop delay", Scenario{StopDelay: -1}, 0, "StopDelay -1ns is negative"},
		{"end after", Scenario{EndAfter: -1}, 0, "EndAfter -1ns is negative"},
		{"set slots at", Scenario{SetSlotsAt: -1}, 0, "SetSlotsAt -1ns is negative"},
		{"set slots to below", Scenario{SetSlotsTo: -1}, 0, "SetSlotsTo -1 is outside [0, 10000]"},
		{"set slots to above", Scenario{SetSlotsTo: 10_001}, 0, "SetSlotsTo 10001 is outside [0, 10000]"},
		{"set slots without slots", Scenario{SetSlotsAt: time.Second, SetSlotsTo: 2}, 0, "SetSlotsAt without Slots"},
		{"rollout without slots", Scenario{Rollout: rollout}, 0, "Rollout without Slots"},
		{"rollout gap", Scenario{Slots: 2, Rollout: &Rollout{Surge: 1}}, 0, "Rollout.Gap 0s is not positive"},
		{"rollout surge", Scenario{Slots: 2, Rollout: &Rollout{Gap: 1, Surge: -1}}, 0, "Rollout.Surge -1ns is not positive"},
		{"rollout start", Scenario{Slots: 2, Rollout: &Rollout{Start: -1, Gap: 1, Surge: 1}}, 0, "Rollout.Start -1ns is negative"},
		{"outage for", Scenario{Outage: &Outage{At: time.Second}}, 0, "Outage.For 0s is not positive"},
		{"outage at", Scenario{Outage: &Outage{At: -1, For: time.Second}}, 0, "Outage.At -1ns is negative"},
		{"mutation", Scenario{Mutation: 9}, 0, "unknown Mutation 9"},
		{"clock jump", Scenario{Faults: random(jump)}, 0,
			"Faults: clock-jump rules are not supported: a jump would move the monotonic clock; use WallSteps"},
		{"quiet negative", Scenario{Faults: &fault.Random{Quiet: -1}}, 0, "Faults.Quiet -1ns is negative"},
		{"quiet as long as the run", Scenario{Faults: &fault.Random{Quiet: time.Minute}}, 0,
			"quiet 60.000000000s is not shorter than the run 60.000000000s"},
		{"default quiet as long as the run", Scenario{WallSteps: time.Second}, at(20 * time.Second),
			"quiet 22.000000000s is not shorter than the run 20.000000000s"},
		{"recovery + bound, faults", Scenario{Faults: &fault.Random{Quiet: 10 * time.Second}}, 0,
			"recovery 50.000000000s + bound 21.045000000s is after the end 60.000000000s"},
		{"recovery + bound, no faults", Scenario{}, at(21 * time.Second),
			"recovery 0.000000000s + bound 21.045000000s is after the end 21.000000000s"},

		// The order of the rows: the earlier one wins.
		{"takeover before members", Scenario{Takeover: 2, Members: 99}, 0, "unknown Takeover 2"},
		{"members before spaces", Scenario{Members: 99, Spaces: []core.SpaceSpec{}}, 0, "Members 99 is outside [1, 64]"},
		{"spaces before timing", Scenario{Spaces: book(core.SpaceSpec{Name: "a"}), Timing: slow}, 0, `space "a": Shards 0 is below 1`},
		{"timing before slots", Scenario{Timing: slow, Slots: -1}, 0, "Timing: RenewDeadline 10s must be greater than 1.2 times RetryPeriod 9s"},
		{"slots before rho", Scenario{Slots: -1, Rho: -1}, 0, "Slots -1 is outside [0, 10000]"},
		{"rho before delta", Scenario{Rho: -1, Delta: -1}, 0, "Rho -1 is outside [0, 500000]"},
		{"durations in order", Scenario{WallSkew: -2, StopDelay: -1, SetSlotsAt: -1}, 0, "WallSkew -2ns is negative"},
		{"durations before set slots to", Scenario{SetSlotsAt: -1, SetSlotsTo: -1}, 0, "SetSlotsAt -1ns is negative"},
		{"set slots to before without slots", Scenario{SetSlotsAt: 1, SetSlotsTo: -1}, 0, "SetSlotsTo -1 is outside [0, 10000]"},
		{"without slots before the rollout", Scenario{Rollout: &Rollout{}}, 0, "Rollout without Slots"},
		{"rollout before outage", Scenario{Slots: 1, Rollout: &Rollout{}, Outage: &Outage{}}, 0, "Rollout.Gap 0s is not positive"},
		{"outage before mutation", Scenario{Outage: &Outage{}, Mutation: 9}, 0, "Outage.For 0s is not positive"},
		{"mutation before faults", Scenario{Mutation: 5, Faults: random(jump)}, 0, "unknown Mutation 5"},
		{"clock jump before quiet", Scenario{Faults: &fault.Random{Quiet: -1, Rules: []fault.Rule{jump}}}, 0,
			"Faults: clock-jump rules are not supported: a jump would move the monotonic clock; use WallSteps"},
		{"fields before the end", Scenario{Mutation: 9}, at(time.Second), "unknown Mutation 9"},

		// No error.
		{"defaults", Scenario{}, 0, ""},
		{"defaults at the bound", Scenario{}, at(21_045_000_000), ""},
		{"faults", Scenario{Faults: crashes(0)}, 0, ""},
		{"everything", Scenario{
			Takeover: Observer, Members: 64, Slots: 10_000, Rho: 100_000, WallSkew: time.Second, WallSteps: time.Second,
			StopDelay: time.Second, EndAfter: time.Minute, SetSlotsAt: time.Second, SetSlotsTo: 3, Rollout: rollout,
			Outage: &Outage{At: time.Second, For: time.Second}, Mutation: ReadConfirm, Faults: crashes(0),
		}, at(10 * time.Minute), ""},
		{"zero slots to set", Scenario{Slots: 3, SetSlotsAt: time.Second}, at(2 * time.Minute), ""},
		{"rollout at the start", Scenario{Slots: 1, Rollout: &Rollout{Gap: 1, Surge: 1}}, at(2 * time.Minute), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			end := tt.end
			if end == 0 {
				end = minute
			}
			err := SetupError(tt.sc, end)
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("SetupError = %v, want nil", err)
			case tt.want != "" && (err == nil || err.Error() != setupPrefix+tt.want):
				t.Errorf("SetupError = %v, want %q", err, setupPrefix+tt.want)
			}
		})
	}
}
