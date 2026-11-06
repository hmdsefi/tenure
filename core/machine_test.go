// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

func TestNewMachineInitialState(t *testing.T) {
	input := []core.SpaceSpec{
		{Name: "orders", Shards: 2},
		{Name: "Zed", Shards: 1},
		{Name: "aa", Shards: 3, Fenced: true},
		{Name: "a", Shards: 1},
	}
	given := slices.Clone(input)
	m := core.NewMachine(core.MachineConfig{Holder: "h1", Member: "m1", Spaces: input})

	idle := func(n int) []core.ShardStatus { return make([]core.ShardStatus, n) }
	// Names sort by bytes. Validate rejects equal names, so the stability of the sort has no
	// visible effect here.
	want := core.Status{
		Session: core.SessionStateOpening,
		Slot:    -1,
		Spaces: []core.SpaceStatus{
			{Name: "Zed", Shards: idle(1)},
			{Name: "a", Shards: idle(1)},
			{Name: "aa", Fenced: true, Shards: idle(3)},
			{Name: "orders", Shards: idle(2)},
		},
	}

	t.Run("status", func(t *testing.T) {
		if got := m.Status(); !reflect.DeepEqual(got, want) {
			t.Fatalf("Status() = %+v, want %+v", got, want)
		}
		for _, s := range m.Status().Spaces {
			for i, sh := range s.Shards {
				if sh.State != core.ShardStateIdle {
					t.Errorf("space %q shard %d state = %v, want idle", s.Name, i, sh.State)
				}
			}
		}
	})

	t.Run("next wake", func(t *testing.T) {
		if at, ok := m.NextWake(); at != 0 || ok {
			t.Errorf("NextWake() = (%v, %v), want (0, false)", at, ok)
		}
	})

	t.Run("assignment", func(t *testing.T) {
		tests := []struct {
			space string
			want  []string
		}{
			{"Zed", []string{""}},
			{"a", []string{""}},
			{"aa", []string{"", "", ""}},
			{"orders", []string{"", ""}},
			{"unknown", nil},
			{"", nil},
		}
		for _, tc := range tests {
			got := m.Assignment(tc.space)
			if (got == nil) != (tc.want == nil) || !slices.Equal(got, tc.want) {
				t.Errorf("Assignment(%q) = %#v, want %#v", tc.space, got, tc.want)
			}
		}
	})

	t.Run("status is a deep copy", func(t *testing.T) {
		got := m.Status()
		got.Now = time.Hour
		got.Slot = 7
		got.Spaces[0].Name = "changed"
		got.Spaces[0].Shards[0] = core.ShardStatus{State: core.ShardStateOwned, Term: 9, Holder: "h9"}
		got.Spaces[3].Shards[1].Stuck = true
		if after := m.Status(); !reflect.DeepEqual(after, want) {
			t.Fatalf("Status() after changing an earlier result = %+v, want %+v", after, want)
		}
	})

	t.Run("input not modified", func(t *testing.T) {
		if !slices.Equal(input, given) {
			t.Fatalf("NewMachine modified Spaces: %+v, want %+v", input, given)
		}
	})

	// This subtest changes input, so it stays last.
	t.Run("input not aliased", func(t *testing.T) {
		input[0] = core.SpaceSpec{Name: "changed", Shards: 9}
		input[3].Fenced = true
		if got := m.Status(); !reflect.DeepEqual(got, want) {
			t.Fatalf("Status() after changing the input = %+v, want %+v", got, want)
		}
		if got := m.Assignment("changed"); got != nil {
			t.Errorf("Assignment(changed) = %q, want nil", got)
		}
		if got := m.Assignment("orders"); !slices.Equal(got, []string{"", ""}) {
			t.Errorf("Assignment(orders) = %q, want two empty keys", got)
		}
	})
}

func TestStepNilPanics(t *testing.T) {
	m := core.NewMachine(validConfig())
	want := m.Status()

	const text = "core: Step: nil Input"
	if err := panicError(t, func() { m.Step(nil) }); err.Error() != text {
		t.Fatalf("Step(nil) panicked with %q, want %q", err, text)
	}
	if got := m.Status(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Status() after Step(nil) = %+v, want %+v", got, want)
	}

	t.Run("other inputs have no effect", func(t *testing.T) {
		const now = 5 * time.Second
		inputs := []core.Input{
			core.Tick{Now: now},
			core.ViewChanged{Now: now, View: core.View{Version: 3, Members: []core.Member{{ID: "m1", Slot: -1}}}, Wall: time.Unix(100, 0)},
			core.SessionRenewed{Now: now, SentAt: now},
			core.SessionFailed{Now: now, Cause: core.CauseLost},
			core.SessionClosed{Now: now},
			core.Acquired{Now: now, Space: "orders", Shard: 0, Term: 4},
			core.AcquireFailed{Now: now, Space: "orders", Shard: 0, Holder: "h2", RetryAfter: time.Second},
			core.Released{Now: now, Space: "orders", Shard: 0, Term: 4},
			core.RunStopped{Now: now, Space: "orders", Shard: 0, Term: 4},
			core.HolderChanged{Now: now, Space: "orders", Shard: 0, Holder: "h1", Term: 4},
			core.SlotClaimed{Now: now, Slot: 1},
			core.SlotReleased{Now: now, Slot: 1},
			core.Drain{Now: now},
			// Pointers to input types also satisfy Input. Only a nil Input panics.
			&core.Tick{Now: now},
			(*core.Tick)(nil),
		}
		for _, in := range inputs {
			if got := m.Step(in); got != nil {
				t.Errorf("Step(%T) = %#v, want nil", in, got)
			}
			if got := m.Status(); !reflect.DeepEqual(got, want) {
				t.Errorf("Status() after Step(%T) = %+v, want %+v", in, got, want)
			}
			if at, ok := m.NextWake(); at != 0 || ok {
				t.Errorf("NextWake() after Step(%T) = (%v, %v), want (0, false)", in, at, ok)
			}
			if got := m.Assignment("orders"); !slices.Equal(got, []string{"", ""}) {
				t.Errorf("Assignment(orders) after Step(%T) = %q", in, got)
			}
		}
	})
}
