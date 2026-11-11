// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
)

// random returns a fault plan with the rules.
func random(rules ...fault.Rule) *fault.Random {
	return &fault.Random{Rules: rules}
}

// drift returns a clock-drift rule with the magnitude, in ppm.
func drift(magnitude int64) fault.Rule {
	return fault.Rule{Kind: fault.KindClockDrift, Every: 5 * time.Second, Magnitude: magnitude}
}

// pause returns a pause rule for the target.
func pause(target string) fault.Rule {
	return fault.Rule{Kind: fault.KindPause, Every: 5 * time.Second, MaxFor: time.Second, Target: target}
}

// TestATSIM27ComputeBound checks the bound: the scenario with every default and drifts up to
// 100 000 ppm under both rules, then each term of the sum, the drift and the rounding.
func TestATSIM27ComputeBound(t *testing.T) {
	// A timing in nanoseconds, which no real scenario has, shows the rounding: the sum is
	// 4 + 3·1 + 9·1 = 16 ns under the server rule.
	tiny := Scenario{
		Timing: core.Timing{LeaseDuration: 4, RenewDeadline: 2, RetryPeriod: 1, SlotGrace: 4},
		Delta:  1,
	}
	withRho := func(sc Scenario, rho int32) Scenario {
		sc.Rho = rho
		return sc
	}
	day := core.Timing{
		LeaseDuration: 24 * time.Hour, RenewDeadline: 12 * time.Hour,
		RetryPeriod: 24 * time.Hour / 3, SlotGrace: 24 * time.Hour,
	}
	tests := []struct {
		name string
		sc   Scenario
		want time.Duration
	}{
		// (15 s + 3·2 s + 9·5 ms) / 0.9 and (15 s + 3·2 s + 18·5 ms) / 0.9, rounded up.
		{"defaults and 100000 ppm, server", Scenario{Rho: 100_000}, 23_383_333_334},
		{"defaults and 100000 ppm, observer", Scenario{Takeover: Observer, Rho: 100_000}, 23_433_333_334},

		{"no drift, server", Scenario{}, 21_045_000_000},
		{"no drift, observer", Scenario{Takeover: Observer}, 21_090_000_000},
		{"a negative Rho counts as no drift", Scenario{Rho: -100_000}, 21_045_000_000},
		{"StopDelay", Scenario{StopDelay: 3 * time.Second}, 24_045_000_000},
		// 24.045 s / 0.9 = 26.716666666.6… s
		{"StopDelay and drift", Scenario{Rho: 100_000, StopDelay: 3 * time.Second}, 26_716_666_667},
		{"slots add SlotGrace", Scenario{Slots: 3}, 81_045_000_000},
		// 81.045 s / 0.9 = 90.05 s exactly: nothing to round up.
		{"slots and drift", Scenario{Slots: 1, Rho: 100_000}, 90_050_000_000},
		{"slots and drift, observer", Scenario{Takeover: Observer, Slots: 5, Rho: 100_000}, 90_100_000_000},
		{"Delta, server", Scenario{Delta: 20 * time.Millisecond}, 21_180_000_000},
		{"Delta, observer", Scenario{Takeover: Observer, Delta: 20 * time.Millisecond}, 21_360_000_000},
		{
			"another timing",
			Scenario{Timing: core.Timing{
				LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second,
				RetryPeriod: 4 * time.Second, SlotGrace: 2 * time.Minute,
			}, Slots: 2},
			162_045_000_000,
		},
		{"members, spaces and wall clocks do not count",
			Scenario{
				Members: 9, Spaces: []core.SpaceSpec{{Name: "ledger", Shards: 64, Fenced: true}},
				WallSkew: time.Hour, WallSteps: time.Minute, EndAfter: time.Minute,
				Rollout: &Rollout{Start: time.Second, Gap: time.Minute, Surge: time.Second},
				Outage:  &Outage{At: time.Second, For: time.Minute},
			},
			21_045_000_000,
		},

		// 21.045 s / 0.8
		{"a clock-drift rule above Rho", Scenario{Rho: 100_000, Faults: random(drift(200_000))}, 26_306_250_000},
		{"a negative clock-drift rule above Rho", Scenario{Rho: 100_000, Faults: random(drift(-200_000))}, 26_306_250_000},
		{"the largest of several clock-drift rules", Scenario{Faults: random(drift(50_000), drift(200_000), drift(100_000))}, 26_306_250_000},
		{"a clock-drift rule below Rho", Scenario{Rho: 100_000, Faults: random(drift(50_000))}, 23_383_333_334},
		{"a clock-drift rule and no Rho", Scenario{Faults: random(drift(100_000))}, 23_383_333_334},
		{
			"the Magnitude of another kind of rule",
			Scenario{Rho: 100_000, Faults: random(
				fault.Rule{Kind: fault.KindSyncFail, Every: time.Second, Magnitude: 400_000},
				pause(""),
			)},
			23_383_333_334,
		},
		{"faults without rules", Scenario{Rho: 100_000, Faults: random()}, 23_383_333_334},

		{"16 ns and no drift", tiny, 16},
		{"16 ns at 1 ppm rounds up", withRho(tiny, 1), 17},
		{"16 ns at 500000 ppm doubles", withRho(tiny, 500_000), 32},
		{"16 ns at 999999 ppm", withRho(tiny, 999_999), 16_000_000},
		{"25 ns at 1 ppm, observer", Scenario{Takeover: Observer, Timing: tiny.Timing, Delta: 1, Rho: 1}, 26},

		// (24 h + 3·8 h + 9·5 ms) · 10⁶ needs more than 64 bits.
		{"the largest timing", Scenario{Timing: day}, 172_800_045_000_000},
		{"the largest timing at 500000 ppm", Scenario{Timing: day, Rho: 500_000}, 345_600_090_000_000},
		{"the largest timing with slots at 999999 ppm", Scenario{Timing: day, Slots: 1, Rho: 999_999}, math.MaxInt64},
		{"clocks that may stand still", Scenario{Rho: 1_000_000}, math.MaxInt64},
		{"clocks that may run backwards", Scenario{Faults: random(drift(math.MinInt64))}, math.MaxInt64},

		// Durations no world accepts still give the formula's value, rounded up, or its
		// nearest Duration: (16 ns − 26 ns) · 10⁶ / 999 999 is −10.00001 ns.
		{"a negative sum rounds up", Scenario{Timing: tiny.Timing, Delta: 1, StopDelay: -26, Rho: 1}, -10},
		{"a sum below every Duration", Scenario{StopDelay: math.MinInt64, Rho: 500_000}, math.MinInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ComputeBound(tt.sc); got != tt.want {
				t.Errorf("ComputeBound = %d ns, want %d ns", got, tt.want)
			}
		})
	}
}

// TestAssertsG2Rule checks when a scenario asserts g2: its largest drift is strictly below the
// bound of its timing, no fault rule pauses a member, and the store ends no session out of
// band (EndAfter).
func TestAssertsG2Rule(t *testing.T) {
	// DefaultTiming tolerates (15 s − 10 s) / (15 s + 10 s) = 200 000 ppm, exclusive.
	if got := core.DefaultTiming.DriftBound(); got != 0.2 {
		t.Fatalf("DefaultTiming.DriftBound() = %v, want 0.2", got)
	}
	crash := fault.Rule{Kind: fault.KindCrash, Every: 5 * time.Second, MaxFor: time.Second}
	partition := fault.Rule{Kind: fault.KindPartition, Every: 5 * time.Second, MaxFor: 2 * time.Second}
	// client-go's smallest tolerance, 5%: 21 s and 19 s.
	narrow := core.Timing{
		LeaseDuration: 21 * time.Second, RenewDeadline: 19 * time.Second,
		RetryPeriod: 2 * time.Second, SlotGrace: time.Minute,
	}
	// The largest fields: (24 h − 12 h) / (24 h + 12 h) is a third, and both products of the
	// comparison are beyond 64 bits.
	day := core.Timing{
		LeaseDuration: 24 * time.Hour, RenewDeadline: 12 * time.Hour,
		RetryPeriod: time.Hour, SlotGrace: 24 * time.Hour,
	}
	// The widest tolerance a timing can have: LeaseDuration at its largest and RenewDeadline
	// at its smallest.
	wide := core.Timing{LeaseDuration: 24 * time.Hour, RenewDeadline: 2, RetryPeriod: 1, SlotGrace: 24 * time.Hour}
	for name, timing := range map[string]core.Timing{"narrow": narrow, "day": day, "wide": wide} {
		if err := timing.Validate(); err != nil {
			t.Fatalf("the %s timing is not legal: %v", name, err)
		}
	}

	tests := []struct {
		name string
		sc   Scenario
		want bool
	}{
		{"no drift and no faults", Scenario{}, true},
		{"both rules alike", Scenario{Takeover: Observer}, true},
		{"drift below the bound", Scenario{Rho: 100_000}, true},
		{"drift 1 ppm below the bound", Scenario{Rho: 199_999}, true},
		{"drift at the bound", Scenario{Rho: 200_000}, false},
		{"drift 1 ppm above the bound", Scenario{Rho: 200_001}, false},
		{"drift far above the bound", Scenario{Rho: 500_000}, false},
		{"a negative Rho counts as no drift", Scenario{Rho: -300_000}, true},

		{"a clock-drift rule below the bound", Scenario{Faults: random(drift(100_000))}, true},
		{"a clock-drift rule 1 ppm below the bound", Scenario{Rho: 100_000, Faults: random(drift(199_999))}, true},
		{"a clock-drift rule at the bound", Scenario{Rho: 100_000, Faults: random(drift(200_000))}, false},
		{"a clock-drift rule above the bound", Scenario{Faults: random(drift(300_000))}, false},
		{"a negative clock-drift rule at the bound", Scenario{Faults: random(drift(-200_000))}, false},
		{"a clock-drift rule above the bound among others", Scenario{Faults: random(crash, drift(100_000), drift(300_000))}, false},
		{"a clock-drift rule for the store counts too", Scenario{Faults: random(fault.Rule{
			Kind: fault.KindClockDrift, Every: time.Second, Magnitude: 300_000, Target: "store",
		})}, false},
		{"a clock-drift rule below a Rho above the bound", Scenario{Rho: 250_000, Faults: random(drift(100_000))}, false},

		{"a pause of any member", Scenario{Faults: random(pause(""))}, false},
		{"a pause of the members", Scenario{Faults: random(pause("members"))}, false},
		{"a pause of the store", Scenario{Faults: random(pause("store"))}, true},
		{"a pause of the fence", Scenario{Faults: random(pause("fence"))}, true},
		{"pauses of the store and of the members", Scenario{Faults: random(pause("store"), crash, pause("members"))}, false},
		{"a pause of the store and drift above the bound", Scenario{Rho: 300_000, Faults: random(pause("store"))}, false},

		{"sessions that end out of band", Scenario{EndAfter: 40 * time.Second}, false},
		{"sessions that end 1 ns after they open", Scenario{EndAfter: 1}, false},
		{"a zero EndAfter ends no session", Scenario{EndAfter: 0}, true},
		{"sessions that end, under the observer rule", Scenario{Takeover: Observer, EndAfter: time.Minute}, false},
		{"sessions that end and crashes", Scenario{EndAfter: time.Minute, Faults: random(crash)}, false},

		{"faults without rules", Scenario{Faults: random()}, true},
		{"crashes", Scenario{Faults: random(crash)}, true},
		{"crashes of the store and its isolation", Scenario{Faults: random(
			fault.Rule{Kind: fault.KindCrash, Every: 10 * time.Second, MaxFor: 2 * time.Second, Target: "store"},
			fault.Rule{Kind: fault.KindIsolate, Every: 10 * time.Second, MaxFor: 2 * time.Second, Target: "store"},
		)}, true},
		{"partitions", Scenario{Faults: random(partition), Delta: 500 * time.Millisecond}, true},
		{"wall-clock skew and steps", Scenario{WallSkew: time.Hour, WallSteps: time.Minute}, true},
		{"slots, a rollout and an outage", Scenario{
			Slots:   5,
			Rollout: &Rollout{Start: time.Second, Gap: time.Minute, Surge: time.Second},
			Outage:  &Outage{At: time.Second, For: time.Minute},
		}, true},

		{"a 5% timing, 1 ppm below its bound", Scenario{Timing: narrow, Rho: 49_999}, true},
		{"a 5% timing at its bound", Scenario{Timing: narrow, Rho: 50_000}, false},
		{"the largest timing below its bound", Scenario{Timing: day, Rho: 333_333}, true},
		{"the largest timing above its bound", Scenario{Timing: day, Rho: 333_334}, false},
		{"the widest timing at 999999 ppm", Scenario{Timing: wide, Rho: 999_999}, true},
		{"the widest timing at 1000000 ppm", Scenario{Timing: wide, Rho: 1_000_000}, false},
		{"the widest timing and the largest drift", Scenario{Timing: wide, Rho: math.MaxInt32}, false},
		{"the largest timing and a drift rule beyond int32", Scenario{Timing: day, Faults: random(drift(math.MaxInt64))}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sc.AssertsG2(); got != tt.want {
				t.Errorf("AssertsG2() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMaxDrift checks the largest drift of a scenario: Rho or the largest clock-drift rule.
func TestMaxDrift(t *testing.T) {
	tests := []struct {
		name string
		sc   Scenario
		want int32
	}{
		{"nothing", Scenario{}, 0},
		{"Rho", Scenario{Rho: 100_000}, 100_000},
		{"a negative Rho", Scenario{Rho: -1}, 0},
		{"faults without rules", Scenario{Rho: 7, Faults: random()}, 7},
		{"a rule above Rho", Scenario{Rho: 100_000, Faults: random(drift(200_000))}, 200_000},
		{"a rule below Rho", Scenario{Rho: 100_000, Faults: random(drift(50_000))}, 100_000},
		{"a negative rule", Scenario{Faults: random(drift(-300_000), drift(200_000))}, 300_000},
		{"rules of other kinds", Scenario{Rho: 5, Faults: random(pause(""), fault.Rule{Kind: fault.KindClockJump, Magnitude: 1 << 40})}, 5},
		{"the largest int32", Scenario{Faults: random(drift(math.MaxInt32))}, math.MaxInt32},
		{"beyond int32", Scenario{Faults: random(drift(math.MaxInt32 + 1))}, math.MaxInt32},
		{"below -int32", Scenario{Faults: random(drift(math.MinInt32))}, math.MaxInt32},
		{"the smallest int64", Scenario{Faults: random(drift(math.MinInt64), drift(1))}, math.MaxInt32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sc.MaxDrift(); got != tt.want {
				t.Errorf("MaxDrift() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestScenarioDefaults checks the defaults: which zero fields have one, and that set fields,
// the caller's value and a second application are left alone.
func TestScenarioDefaults(t *testing.T) {
	got := Scenario{}.withDefaults()
	want := Scenario{
		Members: 5,
		Spaces:  []core.SpaceSpec{{Name: "book", Shards: 16}},
		Timing:  core.DefaultTiming,
		Delta:   5 * time.Millisecond,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Scenario{}.withDefaults() = %+v, want %+v", got, want)
	}
	if again := got.withDefaults(); !reflect.DeepEqual(again, want) {
		t.Errorf("withDefaults applied twice = %+v, want %+v", again, want)
	}

	// Every field set: nothing changes, and the pointers are the caller's.
	full := Scenario{
		Takeover:    Observer,
		Members:     3,
		Spaces:      []core.SpaceSpec{{Name: "ledger", Shards: 4, Fenced: true}},
		Timing:      core.Timing{LeaseDuration: 21 * time.Second, RenewDeadline: 19 * time.Second, RetryPeriod: time.Second, SlotGrace: time.Minute},
		Slots:       3,
		Rho:         100_000,
		Delta:       time.Millisecond,
		WallSkew:    time.Second,
		WallSteps:   time.Second,
		StopDelay:   3 * time.Second,
		EndAfter:    time.Minute,
		Faults:      random(pause("store")),
		Rollout:     &Rollout{Start: time.Second, Gap: time.Minute, Surge: time.Second},
		Outage:      &Outage{At: time.Second, For: time.Minute},
		SetSlotsAt:  time.Minute,
		SetSlotsTo:  2,
		NoLiveness:  true,
		IgnoreCheck: true,
		Mutation:    ReadConfirm,
	}
	if got := full.withDefaults(); !reflect.DeepEqual(got, full) || got.Faults != full.Faults {
		t.Errorf("withDefaults changed a scenario with every field set: %+v, want %+v", got, full)
	}

	// Values that are out of range, and lists and timings that are set in part, have no default.
	odd := Scenario{
		Members: -1,
		Spaces:  []core.SpaceSpec{},
		Timing:  core.Timing{RetryPeriod: time.Second},
		Delta:   -1,
	}
	got = odd.withDefaults()
	if !reflect.DeepEqual(got, odd) || got.Spaces == nil {
		t.Errorf("withDefaults changed %+v into %+v", odd, got)
	}
}

// TestTakeoverAndMutationStrings checks the names of the rules and of the planted bugs.
func TestTakeoverAndMutationStrings(t *testing.T) {
	takeovers := map[Takeover]string{
		Server: "server", Observer: "observer", 2: "Takeover(2)", 7: "Takeover(7)", 255: "Takeover(255)",
	}
	for v, want := range takeovers {
		if got := v.String(); got != want {
			t.Errorf("Takeover(%d).String() = %q, want %q", uint8(v), got, want)
		}
	}
	mutations := map[Mutation]string{
		MutationNone: "MutationNone", GrantHeld: "GrantHeld", SkipRaise: "SkipRaise",
		LateDeadline: "LateDeadline", ReadConfirm: "ReadConfirm", 5: "Mutation(5)", 9: "Mutation(9)",
	}
	for v, want := range mutations {
		if got := v.String(); got != want {
			t.Errorf("Mutation(%d).String() = %q, want %q", uint8(v), got, want)
		}
	}
	if Server != 0 || MutationNone != 0 {
		t.Errorf("Server = %d and MutationNone = %d, want the zero values", Server, MutationNone)
	}
}
