// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// Member is a member of a group, as core.Member describes it. The alias lets a program that uses
// tenure name members without importing core.
type Member = core.Member

// DefaultSlotGrace is the SlotGrace that Start uses when Config.Timing sets other fields but
// leaves SlotGrace zero. It equals core.DefaultTiming.SlotGrace.
const DefaultSlotGrace = 60 * time.Second

// Config configures a node for Start.
//
// Write Config values with keyed fields: later versions add fields.
type Config struct {
	// Self is this member. Its ID must be valid as core.Member.Validate says, and no key of its
	// Meta may start with "tenure.", a prefix tenure reserves. Start ignores Slot and Draining:
	// the member always joins with Slot -1 and Draining false.
	Self Member

	// Membership tells the node which members its group has. It is required.
	Membership Membership

	// Store keeps the shard records, the session and, with slots, the slot records. It may be
	// nil only when Spaces is empty and Slots is 0: the node then only joins the group and
	// follows its views.
	Store Store

	// Slots is the slot count this member expects the group to have, or 0 for no slots. A count
	// above 0 needs a Store that is a SlotStore. Start writes the count when the store holds
	// none; when the store holds another count, the stored count wins.
	Slots int

	// Spaces lists the spaces whose shards this member takes part in.
	Spaces []Space

	// Timing holds the session timings. The zero value means core.DefaultTiming; otherwise a
	// zero SlotGrace means DefaultSlotGrace, so a LeaseDuration above DefaultSlotGrace needs
	// SlotGrace set.
	Timing core.Timing

	// Rand is the source of the 16 random bytes that name each session of this member. nil
	// means crypto/rand.Reader.
	Rand io.Reader

	// OnEvent, when not nil, is called with each event of the node, one at a time and in order.
	OnEvent func(Event)
}

// Space is a set of shards that the members of a group divide among themselves.
//
// Write Space values with keyed fields: later versions add fields.
type Space struct {
	// Name identifies the space in the store and in routing. It is non-empty, contains neither
	// "/" nor a zero byte, differs from the Name of every other space of the Config, and is none
	// of the names tenure reserves: "generation", "member", "slot", "slots" and "request".
	Name string

	// Shards is the number of shards in the space, at least 1.
	Shards int

	// Fenced reports that a downstream fence checks the term of every write to the space's
	// state. A fenced shard's Run keeps going, degraded, while the session is not live; an
	// unfenced shard stops at the session deadline.
	Fenced bool

	// Run serves one shard while this member owns it. The node calls it with the shard's
	// Shard and cancels ctx when the ownership ends. When Run returns while the member still
	// owns the shard, the node calls it again after a backoff. It is required.
	Run func(ctx context.Context, s *Shard) error

	// ShardOf maps a key to its shard for Node.Route and Node.Local. It must return a shard
	// from 0 to Shards minus 1, the same one for the same key on every member, and be safe for
	// concurrent use; Route and Local fail for a key it maps outside. nil means
	// core.KeyShard(key, Shards).
	ShardOf func(key string) int
}

// NameChecker is implemented by a Store or a Membership that cannot hold every name tenure
// accepts. Once the rest of the Config is valid, Start calls CheckNames before any other method
// of the store or the membership, and fails with an error that wraps the one CheckNames returns.
type NameChecker interface {
	// CheckNames returns an error when the backend cannot hold one of the space names, given in
	// Config.Spaces order and nil when there are none, or the member ID. A backend that keeps
	// no spaces ignores spaces.
	CheckNames(spaces []string, member string) error
}

// TimingChecker is implemented by a Store or a Membership that cannot work with every valid
// Timing. Start calls CheckTiming after CheckNames and before any other method of the store or
// the membership, and fails with an error that wraps the one CheckTiming returns.
type TimingChecker interface {
	// CheckTiming returns an error when the backend cannot work with t, the timing the node
	// will use, its defaults applied.
	CheckTiming(t core.Timing) error
}

// reservedMetaPrefix starts the Meta keys that tenure keeps for itself (SPL-040).
const reservedMetaPrefix = "tenure."

// reservedSpaceNames are the space names that tenure keeps for its own records (SPL-030).
var reservedSpaceNames = []string{"generation", "member", "slot", "slots", "request"}

// errConfig starts the text of every error checkConfig returns.
const errConfig = "tenure: invalid config: "

// checkConfig applies RUN-040's defaults to cfg and then runs RUN-010's checks in order. It
// returns the configuration the node uses, or the first failure as "tenure: invalid config:
// <reason>", wrapping a core error or a checker's error. Spaces and Self.Meta of the result are
// copies, so a caller that changes its own afterwards does not change the node's.
//
// Checks 1 to 12 call nothing of the store or the membership; the checkers of checks 13 and 14
// run only when those pass, and nothing else of either is called.
func checkConfig(cfg Config) (Config, error) {
	cfg = withDefaults(cfg)
	if err := checkFields(cfg); err != nil {
		return Config{}, err
	}
	if err := checkBackends(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// withDefaults returns cfg with RUN-040's defaults applied and its mutable parts copied.
func withDefaults(cfg Config) Config {
	switch {
	case cfg.Timing == (core.Timing{}):
		cfg.Timing = core.DefaultTiming
	case cfg.Timing.SlotGrace == 0:
		cfg.Timing.SlotGrace = DefaultSlotGrace
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.Reader
	}
	cfg.Self.Slot = -1
	cfg.Self.Draining = false
	cfg.Self.Meta = maps.Clone(cfg.Self.Meta)
	cfg.Spaces = slices.Clone(cfg.Spaces)
	return cfg
}

// checkFields runs RUN-010's checks 1 to 12, which look only at cfg's values.
func checkFields(cfg Config) error {
	if err := cfg.Self.Validate(); err != nil {
		return invalid("Self: %w", err)
	}
	if key, ok := reservedMetaKey(cfg.Self.Meta); ok {
		return invalid("Self.Meta: key %q is reserved", key)
	}
	if cfg.Membership == nil {
		return invalid("Membership: nil")
	}
	if err := cfg.Timing.Validate(); err != nil {
		return invalid("Timing: %w", err)
	}
	if cfg.Slots < 0 {
		return invalid("Slots: %d is below 0", cfg.Slots)
	}
	for i, sp := range cfg.Spaces {
		switch {
		case sp.Name == "" || strings.ContainsAny(sp.Name, "/\x00"):
			return invalid("Spaces[%d].Name: invalid name %q", i, sp.Name)
		case slices.Contains(reservedSpaceNames, sp.Name):
			return invalid("Spaces[%d].Name: reserved name %q", i, sp.Name)
		case slices.ContainsFunc(cfg.Spaces[:i], func(e Space) bool { return e.Name == sp.Name }):
			return invalid("Spaces[%d].Name: duplicate name %q", i, sp.Name)
		case sp.Shards < 1:
			return invalid("Spaces[%d].Shards: %d is below 1", i, sp.Shards)
		case sp.Run == nil:
			return invalid("Spaces[%d].Run: nil", i)
		}
	}
	if cfg.Store == nil {
		if len(cfg.Spaces) > 0 || cfg.Slots > 0 {
			return invalid("Store: nil")
		}
		return nil
	}
	if _, ok := cfg.Store.(SlotStore); !ok && cfg.Slots > 0 {
		return invalid("Store: %T is not a SlotStore", cfg.Store)
	}
	return nil
}

// checkBackends runs RUN-010's checks 13 and 14: CheckNames on the store and then on the
// membership, then CheckTiming on both in the same order, each where implemented.
func checkBackends(cfg Config) error {
	var names []string
	if len(cfg.Spaces) > 0 {
		names = make([]string, len(cfg.Spaces))
		for i, sp := range cfg.Spaces {
			names[i] = sp.Name
		}
	}
	if c, ok := cfg.Store.(NameChecker); ok {
		if err := c.CheckNames(slices.Clone(names), cfg.Self.ID); err != nil {
			return invalid("Store: %w", err)
		}
	}
	if c, ok := cfg.Membership.(NameChecker); ok {
		if err := c.CheckNames(slices.Clone(names), cfg.Self.ID); err != nil {
			return invalid("Membership: %w", err)
		}
	}
	if c, ok := cfg.Store.(TimingChecker); ok {
		if err := c.CheckTiming(cfg.Timing); err != nil {
			return invalid("Store: %w", err)
		}
	}
	if c, ok := cfg.Membership.(TimingChecker); ok {
		if err := c.CheckTiming(cfg.Timing); err != nil {
			return invalid("Membership: %w", err)
		}
	}
	return nil
}

// reservedMetaKey returns the lowest key of meta that starts with "tenure.", if any.
func reservedMetaKey(meta map[string]string) (string, bool) {
	var low string
	found := false
	for k := range meta {
		if strings.HasPrefix(k, reservedMetaPrefix) && (!found || k < low) {
			low, found = k, true
		}
	}
	return low, found
}

// invalid returns the error "tenure: invalid config: " followed by the formatted reason.
func invalid(format string, args ...any) error {
	return fmt.Errorf(errConfig+format, args...)
}
