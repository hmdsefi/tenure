// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"errors"
	"slices"
	"time"
)

// Machine is the ownership state machine of one member. It reads no clock, starts nothing and
// performs no I/O: the driver feeds it inputs through Step and carries out the actions Step
// returns.
//
// A Machine is not safe for concurrent use.
type Machine struct {
	// cfg is the validated config: Spaces is a private copy sorted by name, and Timing is
	// never the zero value.
	cfg MachineConfig

	// status holds everything Status reports. status.Spaces is parallel to cfg.Spaces.
	status Status

	// assignments holds the placement key per shard. It is parallel to cfg.Spaces.
	assignments [][]string
}

// NewMachine returns a Machine for cfg, with its spaces sorted by name, its session opening, no
// slot, every shard idle and no timer armed. It panics with the error of cfg.Validate when cfg
// is not valid. It copies cfg.Spaces and does not modify it.
func NewMachine(cfg MachineConfig) *Machine {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	cfg.Timing = cfg.timing()
	cfg.Spaces = slices.Clone(cfg.Spaces)
	slices.SortStableFunc(cfg.Spaces, func(a, b SpaceSpec) int { return cmp.Compare(a.Name, b.Name) })

	m := &Machine{
		cfg: cfg,
		status: Status{
			Session: SessionStateOpening,
			Slot:    -1,
			Spaces:  make([]SpaceStatus, len(cfg.Spaces)),
		},
		assignments: make([][]string, len(cfg.Spaces)),
	}
	for i, s := range cfg.Spaces {
		m.status.Spaces[i] = SpaceStatus{Name: s.Name, Fenced: s.Fenced, Shards: make([]ShardStatus, s.Shards)}
		m.assignments[i] = make([]string, s.Shards)
	}
	return m
}

// Step applies one input and returns the actions the driver must carry out, or nil when there
// are none. It panics when in is nil. For now, every other input has no effect and Step returns
// nil.
func (m *Machine) Step(in Input) []Action {
	if in == nil {
		panic(errors.New("core: Step: nil Input"))
	}
	return nil
}

// NextWake returns the reading of the earliest armed timer and true, or 0 and false when no
// timer is armed. A new Machine has no timer armed.
func (m *Machine) NextWake() (time.Duration, bool) {
	return 0, false
}
