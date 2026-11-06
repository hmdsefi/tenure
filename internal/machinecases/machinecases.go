// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package machinecases holds the scripted step lists of core.Machine: its table tests and the
// machine digest both read them, so a list exists only here. Readings are integer milliseconds,
// written Ms(4550) for @4.55.
//
// Cases use core.DefaultTiming unless a case's Config says otherwise, and the macros assume it.
package machinecases

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// Step is one input and the exact actions Step must return for it.
type Step struct {
	// Input is the input to the machine.
	Input core.Input

	// Want is the exact action list, or nil for a nil result.
	Want []core.Action
}

// Case is one scripted sequence, fed to a new machine.
type Case struct {
	// ID names the case: "AT-MCH-05", or "AT-MCH-23 (b)" for a variant.
	ID string

	// Config configures the machine.
	Config core.MachineConfig

	// Steps lists the steps in order, with macros expanded. Step numbers count from 1.
	Steps []Step
}

// AT returns the case's bare AT ID: its ID without any variant, the name the digest uses.
func (c Case) AT() string {
	at, _, _ := strings.Cut(c.ID, " ")
	return at
}

// sections lists the section files in order. Each returns its cases by AT number, and the
// variants of one AT number stay together in one section.
var sections = []func() []Case{
	bootCases,
	sessionCases,
	handoffCases,
	fencedCases,
	drainingCases,
	slotCases,
}

// Cases returns every case in section order. The result shares no memory with another call's.
func Cases() []Case {
	var all []Case
	for _, section := range sections {
		all = append(all, section()...)
	}
	return all
}

// Get returns the case with the given ID, including its variant if it has one. It panics when
// there is no such case.
func Get(id string) Case {
	for _, c := range Cases() {
		if c.ID == id {
			return c
		}
	}
	panic(fmt.Errorf("machinecases: no case %q", id))
}

// Ms returns ms milliseconds as a reading.
func Ms(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

// S returns the spec of an unfenced space.
func S(name string, shards int) core.SpaceSpec {
	return core.SpaceSpec{Name: name, Shards: shards}
}

// C returns the config of member m1, with session holder h1 and DefaultTiming, for spaces in
// the given order.
func C(spaces ...core.SpaceSpec) core.MachineConfig {
	return core.MachineConfig{Holder: "h1", Member: "m1", Spaces: spaces}
}

// CF returns C(spaces...) with every space fenced.
func CF(spaces ...core.SpaceSpec) core.MachineConfig {
	fenced := slices.Clone(spaces)
	for i := range fenced {
		fenced[i].Fenced = true
	}
	return C(fenced...)
}

// Vm returns a view without slots of the given member IDs, sorted.
func Vm(version uint64, ids ...string) core.View {
	return core.View{Version: version, Members: members(ids)}
}

// Vs returns a view with one slot per entry of holders, each the holder string of the slot's
// holder or "" for a vacant slot, and the given member IDs, sorted, each with Slot -1.
func Vs(version uint64, holders []string, ids ...string) core.View {
	return core.View{
		Version:     version,
		Members:     members(ids),
		Slots:       len(holders),
		SlotHolders: slices.Clone(holders),
	}
}

// members returns one member per ID, sorted by ID, each with Slot -1.
func members(ids []string) []core.Member {
	sorted := slices.Sorted(slices.Values(ids))
	list := make([]core.Member, len(sorted))
	for i, id := range sorted {
		list[i] = core.Member{ID: id, Slot: -1}
	}
	return list
}

// Renew is an accepted SessionRenewed at now for a renewal sent at sentAt, which moves the
// deadline to sentAt + RenewDeadline. Want is ExtendDeadline, then extra, then Wake{wake}; a
// wake of 0 means no timer is armed.
func Renew(now, sentAt, wake time.Duration, extra ...core.Action) Step {
	move := core.ExtendDeadline{Deadline: sentAt + core.DefaultTiming.RenewDeadline}
	return Step{
		Input: core.SessionRenewed{Now: now, SentAt: sentAt},
		Want:  want(wake, append([]core.Action{move}, extra...)...),
	}
}

// Fail is a SessionFailed at now with the given cause. Want is extra, then Wake{wake}; a wake
// of 0 means no timer is armed, and with no extra the result is nil.
func Fail(now time.Duration, cause core.Cause, wake time.Duration, extra ...core.Action) Step {
	return Step{
		Input: core.SessionFailed{Now: now, Cause: cause},
		Want:  want(wake, extra...),
	}
}

// Outage is a store outage on a machine with no shard work, driven as a driver follows Wake: at
// from, and then every RetryPeriod before to, a Tick returns the due KeepAlive and that renewal
// fails as unreachable. A deadline between two of those readings gets a Tick of its own.
// deadline is the session deadline, which the outage does not move. A KeepAlive must be due at
// from.
func Outage(from, to, deadline time.Duration) []Step {
	retry := core.DefaultTiming.RetryPeriod
	var steps []Step
	for now := from; now < to; now += retry {
		wake := now + retry
		between := now < deadline && deadline < wake
		if between {
			wake = deadline
		}
		steps = append(steps,
			Step{Input: core.Tick{Now: now}, Want: want(wake, core.KeepAlive{})},
			Fail(now, core.CauseUnreachable, wake),
		)
		if between && deadline < to {
			steps = append(steps, Step{Input: core.Tick{Now: deadline}, Want: want(now + retry)})
		}
	}
	return steps
}

// want returns actions followed by Wake{wake}; a wake of 0 adds none. With nothing to return it
// returns nil.
func want(wake time.Duration, actions ...core.Action) []core.Action {
	if wake == 0 {
		return actions
	}
	return append(slices.Clip(actions), core.Wake{At: wake})
}
