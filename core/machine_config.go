// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidMachineConfig is wrapped by the errors MachineConfig.Validate returns, except a
// timing error, which wraps ErrInvalidTiming.
var ErrInvalidMachineConfig = errors.New("core: invalid machine config")

// SpaceSpec describes one space of shards.
//
// Write SpaceSpec values with keyed fields: later versions add fields.
type SpaceSpec struct {
	// Name identifies the space. It is non-empty and distinct within a MachineConfig.
	Name string

	// Shards is the number of shards in the space, at least 1.
	Shards int

	// Fenced reports that the space's runs keep going, degraded, when the session is not
	// live, instead of being told to stop.
	Fenced bool
}

// MachineConfig configures a Machine.
//
// Write MachineConfig values with keyed fields: later versions add fields.
type MachineConfig struct {
	// Holder is the session holder string, the name a shard's record uses for this member's
	// session. It is non-empty.
	Holder string

	// Member is this member's ID, which is also its placement key in a view without slots.
	// It is non-empty and contains neither '/' nor a zero byte.
	Member string

	// Spaces lists the spaces the member takes part in. It may be empty: the machine then runs
	// only the session and the slot.
	Spaces []SpaceSpec

	// Timing holds the session timings. The zero value means DefaultTiming.
	Timing Timing
}

// Validate reports whether c can configure a Machine: Holder is non-empty, Member is non-empty
// and contains neither '/' nor a zero byte, every space has a non-empty, distinct Name and at
// least one shard, and Timing is valid. It checks in that order and returns the first failure,
// which wraps ErrInvalidMachineConfig. A failure of Timing is the error of Timing.Validate,
// unchanged, so it wraps ErrInvalidTiming instead. A config with no spaces is valid.
func (c MachineConfig) Validate() error {
	switch {
	case c.Holder == "":
		return fmt.Errorf("%w: empty Holder", ErrInvalidMachineConfig)
	case c.Member == "":
		return fmt.Errorf("%w: empty Member", ErrInvalidMachineConfig)
	case strings.IndexByte(c.Member, '/') >= 0:
		return fmt.Errorf("%w: Member %q contains '/'", ErrInvalidMachineConfig, c.Member)
	case strings.IndexByte(c.Member, 0) >= 0:
		return fmt.Errorf("%w: Member %q contains a zero byte", ErrInvalidMachineConfig, c.Member)
	}
	seen := make(map[string]bool, len(c.Spaces))
	for i, s := range c.Spaces {
		switch {
		case s.Name == "":
			return fmt.Errorf("%w: Spaces[%d]: empty Name", ErrInvalidMachineConfig, i)
		case s.Shards < 1:
			return fmt.Errorf("%w: space %q: Shards %d is below 1", ErrInvalidMachineConfig, s.Name, s.Shards)
		case seen[s.Name]:
			return fmt.Errorf("%w: duplicate space %q", ErrInvalidMachineConfig, s.Name)
		}
		seen[s.Name] = true
	}
	return c.timing().Validate()
}

// timing returns c.Timing, or DefaultTiming when c.Timing is the zero value.
func (c MachineConfig) timing() Timing {
	if c.Timing == (Timing{}) {
		return DefaultTiming
	}
	return c.Timing
}
