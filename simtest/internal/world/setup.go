// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
)

// The largest values a Scenario may ask for.
const (
	maxMembers = 64
	maxSlots   = 10_000
	maxRho     = 500_000 // the kernel's clocks run at half speed at least
)

// setupPrefix starts the text of every setup error.
const setupPrefix = "tenure: setup: "

// clockJumpText is the setup error of a scenario with a clock-jump rule (SIM-120), and the
// failure of a run in which a node's clock jumped all the same, as a replayed fault schedule
// can make it.
const clockJumpText = setupPrefix +
	"Faults: clock-jump rules are not supported: a jump would move the monotonic clock; use WallSteps"

// setup is what Build derives from a scenario and the end of its run before it adds a node.
type setup struct {
	sc    Scenario      // the scenario, with its defaults
	bound time.Duration // the scenario's bound
	// quiet is the length of the quiet window at the end of the run, in which no fault starts
	// and no wall clock steps. It is 0 when nothing disturbs the run.
	quiet time.Duration
	// recovery is the time from which the world is left alone, so that every shard must have
	// an owner again once the bound has passed after it.
	recovery kernel.Time
}

// SetupError returns the first reason why a run of sc that ends at the time end cannot be
// built or cannot be checked, and nil when there is none. The text of every error starts with
// "tenure: setup: ". The fields are checked with their defaults, in this order:
//
//   - Takeover: "unknown Takeover 7";
//   - Members: "Members 65 is outside [1, 64]";
//   - Spaces: "no spaces" for an empty list, then what core.MachineConfig says about the
//     list: "Spaces[0]: empty Name", `space "book": Shards 0 is below 1` or
//     `duplicate space "book"`;
//   - Timing: "Timing: " and what core.Timing says about it;
//   - Slots: "Slots -1 is outside [0, 10000]";
//   - Rho: "Rho -1 is outside [0, 500000]";
//   - Delta, WallSkew, WallSteps, StopDelay, EndAfter and SetSlotsAt, in this order:
//     "Delta -1ns is negative";
//   - SetSlotsTo: "SetSlotsTo -1 is outside [0, 10000]";
//   - a slot count to set, or a rollout, in a world without slots: "SetSlotsAt without Slots",
//     "Rollout without Slots";
//   - Rollout: "Rollout.Gap 0s is not positive", the same for Surge, then
//     "Rollout.Start -1ns is negative";
//   - Outage: "Outage.For 0s is not positive", then "Outage.At -1ns is negative";
//   - Mutation: "unknown Mutation 9";
//   - a clock-jump rule in Faults: "Faults: clock-jump rules are not supported: a jump would
//     move the monotonic clock; use WallSteps";
//   - the quiet window of Faults: "Faults.Quiet -1ns is negative";
//   - a quiet window as long as the run: "quiet 60.000000000s is not shorter than the run
//     60.000000000s";
//   - a run too short to show that every shard finds an owner again:
//     "recovery 45.000000000s + bound 21.045000000s is after the end 60.000000000s".
//
// The last two are about the end of the run. The quiet window is the time at the end of the
// run in which no fault starts and no wall clock steps: the Quiet of Faults or, when that is 0,
// the larger of a quarter of the run and the scenario's bound rounded up to a whole second. A
// scenario without Faults and without WallSteps has none. The recovery time is where that
// window starts, or the start of the run when there is no window, and the bound is that of
// ComputeBound: a run must last until the bound has passed after the recovery time.
func SetupError(sc Scenario, end kernel.Time) error {
	_, err := newSetup(sc, end)
	return err
}

// newSetup returns what Build needs of a scenario whose run ends at end, or the scenario's
// first setup error.
func newSetup(sc Scenario, end kernel.Time) (setup, error) {
	sc = sc.withDefaults()
	if text := sc.problem(); text != "" {
		return setup{}, errors.New(setupPrefix + text)
	}
	st := setup{sc: sc, bound: ComputeBound(sc)}
	if sc.disturbed() {
		st.quiet = quietTime(sc, st.bound, end)
		if kernel.Time(st.quiet) >= end {
			return setup{}, fmt.Errorf("%squiet %s is not shorter than the run %s",
				setupPrefix, kernel.Time(st.quiet), end)
		}
	}
	st.recovery = recoveryStart(sc, st.quiet, end)
	// R + B > End, compared as B > End − R: R is in [0, End], and B may be the largest
	// Duration.
	if st.bound > end.Sub(st.recovery) {
		return setup{}, fmt.Errorf("%srecovery %s + bound %s is after the end %s",
			setupPrefix, st.recovery, kernel.Time(st.bound), end)
	}
	return st, nil
}

// disturbed reports whether something disturbs a run of the scenario until a quiet window at
// its end: random faults, or wall clocks that step.
func (sc Scenario) disturbed() bool {
	return sc.Faults != nil || sc.WallSteps > 0
}

// quietTime returns the length of the quiet window of a disturbed scenario whose bound is bound
// and whose run ends at end: the Quiet of its Faults or, when it has no Faults or their Quiet
// is 0, the larger of a quarter of the run and the bound rounded up to a whole second. So the
// default window always leaves the bound to recover in.
func quietTime(sc Scenario, bound time.Duration, end kernel.Time) time.Duration {
	if sc.Faults != nil && sc.Faults.Quiet != 0 {
		return sc.Faults.Quiet
	}
	return max(time.Duration(end)/4, wholeSeconds(bound))
}

// wholeSeconds rounds a positive duration up to a whole second. A duration too close to the
// largest one stays as it is.
func wholeSeconds(d time.Duration) time.Duration {
	if d > math.MaxInt64-time.Second {
		return d
	}
	return (d + time.Second - 1) / time.Second * time.Second
}

// recoveryStart returns R, the time from which the world of sc is left alone: the start of the
// quiet window, of length quiet, of a disturbed run that ends at end, and the start of the run
// otherwise. A part of the world that disturbs the run at a time of its own, as an outage, a
// change of the slot count or a rollout does, raises R here: EndAfter raises it to the latest
// end of a first session (sessionEndTime), so that the setup check, the
// tenure.bound record and the final checks agree on it.
func recoveryStart(sc Scenario, quiet time.Duration, end kernel.Time) kernel.Time {
	r := kernel.Time(0)
	if sc.disturbed() {
		r = end.Add(-quiet)
	}
	if sc.SetSlotsAt > 0 {
		r = max(r, kernel.Time(sc.SetSlotsAt)) // the slot count changes at SetSlotsAt (1.8-14)
	}
	if sc.EndAfter > 0 {
		r = max(r, sessionEndTime(sc))
	}
	return max(r, outageHeal(sc)) // the store's outage heals then (outage.go)
}

// problem returns the first thing that is wrong with the fields of sc, which has its defaults,
// or "" when they are all usable.
func (sc Scenario) problem() string {
	switch {
	case sc.Takeover > Observer:
		return fmt.Sprintf("unknown Takeover %d", sc.Takeover)
	case sc.Members < 1 || sc.Members > maxMembers:
		return fmt.Sprintf("Members %d is outside [1, %d]", sc.Members, maxMembers)
	case len(sc.Spaces) == 0:
		return "no spaces"
	}
	// core checks the spaces and then the timing, as it does for the machine of every member.
	// It checks the holder and the member first, so they are given valid values.
	cfg := core.MachineConfig{Holder: "m1/0", Member: "m1", Spaces: sc.Spaces, Timing: sc.Timing}
	if err := cfg.Validate(); err != nil {
		if errors.Is(err, core.ErrInvalidTiming) {
			return "Timing: " + strings.TrimPrefix(err.Error(), core.ErrInvalidTiming.Error()+": ")
		}
		return strings.TrimPrefix(err.Error(), core.ErrInvalidMachineConfig.Error()+": ")
	}
	switch {
	case sc.Slots < 0 || sc.Slots > maxSlots:
		return fmt.Sprintf("Slots %d is outside [0, %d]", sc.Slots, maxSlots)
	case sc.Rho < 0 || sc.Rho > maxRho:
		return fmt.Sprintf("Rho %d is outside [0, %d]", sc.Rho, maxRho)
	}
	durations := [...]struct {
		name string
		d    time.Duration
	}{
		{"Delta", sc.Delta},
		{"WallSkew", sc.WallSkew},
		{"WallSteps", sc.WallSteps},
		{"StopDelay", sc.StopDelay},
		{"EndAfter", sc.EndAfter},
		{"SetSlotsAt", sc.SetSlotsAt},
	}
	for _, f := range durations {
		if f.d < 0 {
			return fmt.Sprintf("%s %v is negative", f.name, f.d)
		}
	}
	switch {
	case sc.SetSlotsTo < 0 || sc.SetSlotsTo > maxSlots:
		return fmt.Sprintf("SetSlotsTo %d is outside [0, %d]", sc.SetSlotsTo, maxSlots)
	case sc.SetSlotsAt > 0 && sc.Slots == 0:
		return "SetSlotsAt without Slots"
	case sc.Rollout != nil && sc.Slots == 0:
		return "Rollout without Slots"
	}
	if r := sc.Rollout; r != nil {
		switch {
		case r.Gap <= 0:
			return fmt.Sprintf("Rollout.Gap %v is not positive", r.Gap)
		case r.Surge <= 0:
			return fmt.Sprintf("Rollout.Surge %v is not positive", r.Surge)
		case r.Start < 0:
			return fmt.Sprintf("Rollout.Start %v is negative", r.Start)
		}
	}
	if o := sc.Outage; o != nil {
		switch {
		case o.For <= 0:
			return fmt.Sprintf("Outage.For %v is not positive", o.For)
		case o.At < 0:
			return fmt.Sprintf("Outage.At %v is negative", o.At)
		}
	}
	if sc.Mutation > ReadConfirm {
		return fmt.Sprintf("unknown Mutation %d", sc.Mutation)
	}
	if sc.Faults != nil {
		for _, rule := range sc.Faults.Rules {
			if rule.Kind == fault.KindClockJump {
				return strings.TrimPrefix(clockJumpText, setupPrefix)
			}
		}
		if sc.Faults.Quiet < 0 {
			return fmt.Sprintf("Faults.Quiet %v is negative", sc.Faults.Quiet)
		}
	}
	return ""
}
