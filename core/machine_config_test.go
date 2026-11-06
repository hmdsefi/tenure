// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// validConfig returns a config that passes Validate.
func validConfig() core.MachineConfig {
	return core.MachineConfig{
		Holder: "h1",
		Member: "m1",
		Spaces: []core.SpaceSpec{{Name: "orders", Shards: 2}},
	}
}

// panicError returns the error f panics with. It fails the test when f does not panic, or
// panics with something else.
func panicError(t *testing.T, f func()) (err error) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("did not panic")
		}
		e, ok := r.(error)
		if !ok {
			t.Fatalf("panic value = %#v, want an error", r)
		}
		err = e
	}()
	f()
	return nil
}

func TestATMCH40MachineConfigValidate(t *testing.T) {
	const prefix = "core: invalid machine config: "
	with := func(f func(*core.MachineConfig)) core.MachineConfig {
		c := validConfig()
		f(&c)
		return c
	}
	withSpaces := func(spaces ...core.SpaceSpec) core.MachineConfig {
		return with(func(c *core.MachineConfig) { c.Spaces = spaces })
	}
	tests := []struct {
		name string
		cfg  core.MachineConfig
		want string
	}{
		{"zero config", core.MachineConfig{}, "empty Holder"},
		{"empty Holder", with(func(c *core.MachineConfig) { c.Holder = "" }), "empty Holder"},
		{"empty Member", with(func(c *core.MachineConfig) { c.Member = "" }), "empty Member"},
		{"Member with slash", with(func(c *core.MachineConfig) { c.Member = "a/b" }), `Member "a/b" contains '/'`},
		{"Member with zero byte", with(func(c *core.MachineConfig) { c.Member = "a\x00b" }), `Member "a\x00b" contains a zero byte`},
		{"nil Spaces", with(func(c *core.MachineConfig) { c.Spaces = nil }), "no spaces"},
		{"empty Spaces", withSpaces(), "no spaces"},
		{"empty Name", withSpaces(core.SpaceSpec{Name: "a", Shards: 1}, core.SpaceSpec{Shards: 1}), "Spaces[1]: empty Name"},
		{"zero Shards", withSpaces(core.SpaceSpec{Name: "a"}), `space "a": Shards 0 is below 1`},
		{"negative Shards", withSpaces(core.SpaceSpec{Name: "a", Shards: -3}), `space "a": Shards -3 is below 1`},
		{"duplicate space", withSpaces(core.SpaceSpec{Name: "a", Shards: 1}, core.SpaceSpec{Name: "b", Shards: 1}, core.SpaceSpec{Name: "a", Shards: 2}), `duplicate space "a"`},

		// Check order: the first failing check wins.
		{"Holder before Member", core.MachineConfig{Member: "a/b"}, "empty Holder"},
		{"slash before zero byte", with(func(c *core.MachineConfig) { c.Member = "a\x00/b" }), `Member "a\x00/b" contains '/'`},
		{"Member before Spaces", core.MachineConfig{Holder: "h1", Member: "a/b"}, `Member "a/b" contains '/'`},
		{"Spaces before Timing", core.MachineConfig{Holder: "h1", Member: "m1", Timing: core.Timing{RetryPeriod: 1}}, "no spaces"},
		{"Name before Shards", withSpaces(core.SpaceSpec{}), "Spaces[0]: empty Name"},
		{"Shards before duplicate", withSpaces(core.SpaceSpec{Name: "a", Shards: 1}, core.SpaceSpec{Name: "a"}), `space "a": Shards 0 is below 1`},
		{"spaces in the given order", withSpaces(core.SpaceSpec{Name: "b"}, core.SpaceSpec{Name: "a"}), `space "b": Shards 0 is below 1`},
		{"Spaces before invalid Timing", core.MachineConfig{Holder: "h1", Member: "m1", Spaces: []core.SpaceSpec{{Name: "a"}}, Timing: core.Timing{RetryPeriod: 1}}, `space "a": Shards 0 is below 1`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := prefix + tc.want
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want %q", want)
			}
			if err.Error() != want {
				t.Fatalf("Validate() = %q, want %q", err, want)
			}
			if !errors.Is(err, core.ErrInvalidMachineConfig) {
				t.Fatalf("errors.Is(%q, ErrInvalidMachineConfig) = false", err)
			}
			if errors.Is(err, core.ErrInvalidTiming) {
				t.Fatalf("errors.Is(%q, ErrInvalidTiming) = true", err)
			}

			got := panicError(t, func() { core.NewMachine(tc.cfg) })
			if got.Error() != want {
				t.Fatalf("NewMachine panicked with %q, want %q", got, want)
			}
			if !errors.Is(got, core.ErrInvalidMachineConfig) {
				t.Fatalf("errors.Is(%q, ErrInvalidMachineConfig) = false", got)
			}
		})
	}
}

func TestATMCH40ValidConfigs(t *testing.T) {
	tests := []struct {
		name string
		cfg  core.MachineConfig
	}{
		{"one space", validConfig()},
		{"Holder with slash", core.MachineConfig{Holder: "n1/0001", Member: "m1", Spaces: []core.SpaceSpec{{Name: "a", Shards: 1}}}},
		{"explicit Timing", core.MachineConfig{Holder: "h1", Member: "m1", Spaces: []core.SpaceSpec{{Name: "a", Shards: 1}}, Timing: core.Timing{
			LeaseDuration: 30 * time.Second,
			RenewDeadline: 20 * time.Second,
			RetryPeriod:   4 * time.Second,
			SlotGrace:     30 * time.Second,
		}}},
		{"unsorted spaces", core.MachineConfig{Holder: "h1", Member: "m1", Spaces: []core.SpaceSpec{{Name: "b", Shards: 1}, {Name: "a", Shards: 3, Fenced: true}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if core.NewMachine(tc.cfg) == nil {
				t.Fatal("NewMachine returned nil")
			}
		})
	}
}

// TestATMCH40ZeroTimingMeansDefault compares the timing a machine keeps with the values of
// DefaultTiming written out. Nothing exported shows that timing yet, so it reads it through
// MachineTiming.
func TestATMCH40ZeroTimingMeansDefault(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with zero Timing = %v, want nil", err)
	}
	want := core.Timing{
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		SlotGrace:     60 * time.Second,
	}
	if got := core.MachineTiming(core.NewMachine(cfg)); got != want {
		t.Fatalf("timing kept for a zero Timing = %+v, want %+v", got, want)
	}

	cfg.Timing = core.Timing{
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 20 * time.Second,
		RetryPeriod:   4 * time.Second,
		SlotGrace:     30 * time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with explicit Timing = %v, want nil", err)
	}
	if got := core.MachineTiming(core.NewMachine(cfg)); got != cfg.Timing {
		t.Fatalf("timing kept for an explicit Timing = %+v, want %+v", got, cfg.Timing)
	}
}

func TestATMCH40TimingErrorUnchanged(t *testing.T) {
	tests := []struct {
		name   string
		timing core.Timing
	}{
		// Not the zero value, so DefaultTiming does not replace it.
		{"partly set", core.Timing{RetryPeriod: time.Second}},
		{"only RetryPeriod unset", core.Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, SlotGrace: 60 * time.Second}},
		{"retry too long", core.Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 10 * time.Second, SlotGrace: 60 * time.Second}},
		{"grace too short", core.Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second, SlotGrace: time.Second}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.timing.Validate()
			if want == nil {
				t.Fatal("timing is valid")
			}
			cfg := validConfig()
			cfg.Timing = tc.timing

			err := cfg.Validate()
			if err == nil || err.Error() != want.Error() {
				t.Fatalf("Validate() = %v, want %q", err, want)
			}
			if !errors.Is(err, core.ErrInvalidTiming) {
				t.Fatalf("errors.Is(%q, ErrInvalidTiming) = false", err)
			}
			if errors.Is(err, core.ErrInvalidMachineConfig) {
				t.Fatalf("errors.Is(%q, ErrInvalidMachineConfig) = true", err)
			}

			got := panicError(t, func() { core.NewMachine(cfg) })
			if got.Error() != want.Error() {
				t.Fatalf("NewMachine panicked with %q, want %q", got, want)
			}
			if !errors.Is(got, core.ErrInvalidTiming) {
				t.Fatalf("errors.Is(%q, ErrInvalidTiming) = false", got)
			}
		})
	}
}
