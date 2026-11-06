// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package machinecases_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
	mc "github.com/hmdsefi/tenure/internal/machinecases"
)

func TestMs(t *testing.T) {
	if got, want := mc.Ms(4550), 4550*time.Millisecond; got != want {
		t.Errorf("Ms(4550) = %v, want %v", got, want)
	}
}

func TestConfigs(t *testing.T) {
	spaces := []core.SpaceSpec{mc.S("b", 2), mc.S("a", 1)}

	c := mc.C(spaces...)
	if err := c.Validate(); err != nil {
		t.Errorf("C: %v", err)
	}
	if c.Holder != "h1" || c.Member != "m1" || c.Timing != (core.Timing{}) {
		t.Errorf("C = %+v, want holder h1, member m1 and the zero Timing", c)
	}
	if !slices.Equal(c.Spaces, spaces) {
		t.Errorf("C spaces = %+v, want %+v in the given order", c.Spaces, spaces)
	}

	f := mc.CF(spaces...)
	if err := f.Validate(); err != nil {
		t.Errorf("CF: %v", err)
	}
	want := []core.SpaceSpec{{Name: "b", Shards: 2, Fenced: true}, {Name: "a", Shards: 1, Fenced: true}}
	if !slices.Equal(f.Spaces, want) {
		t.Errorf("CF spaces = %+v, want %+v", f.Spaces, want)
	}
	if spaces[0].Fenced {
		t.Error("CF modified its argument")
	}
}

func TestViews(t *testing.T) {
	v := mc.Vm(3, "m2", "m1")
	want := core.View{Version: 3, Members: []core.Member{{ID: "m1", Slot: -1}, {ID: "m2", Slot: -1}}}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("Vm = %+v, want %+v", v, want)
	}
	if err := v.Validate(); err != nil {
		t.Errorf("Vm: %v", err)
	}

	holders := []string{"h1", ""}
	s := mc.Vs(4, holders, "m1")
	want = core.View{
		Version:     4,
		Members:     []core.Member{{ID: "m1", Slot: -1}},
		Slots:       2,
		SlotHolders: []string{"h1", ""},
	}
	if !reflect.DeepEqual(s, want) {
		t.Errorf("Vs = %+v, want %+v", s, want)
	}
	if err := s.Validate(); err != nil {
		t.Errorf("Vs: %v", err)
	}
	holders[0] = "changed"
	if s.SlotHolders[0] != "h1" {
		t.Error("Vs shares SlotHolders with its argument")
	}
}

func TestRenew(t *testing.T) {
	got := mc.Renew(mc.Ms(2100), mc.Ms(2000), mc.Ms(4000))
	want := mc.Step{
		Input: core.SessionRenewed{Now: mc.Ms(2100), SentAt: mc.Ms(2000)},
		Want:  []core.Action{core.ExtendDeadline{Deadline: mc.Ms(12000)}, core.Wake{At: mc.Ms(4000)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Renew = %+v, want %+v", got, want)
	}

	got = mc.Renew(mc.Ms(2100), mc.Ms(2000), 0, core.KeepAlive{})
	wantActions := []core.Action{core.ExtendDeadline{Deadline: mc.Ms(12000)}, core.KeepAlive{}}
	if !reflect.DeepEqual(got.Want, wantActions) {
		t.Errorf("Renew with an extra action and no wake = %+v, want %+v", got.Want, wantActions)
	}
}

func TestFail(t *testing.T) {
	got := mc.Fail(mc.Ms(1), core.CauseLost, 0)
	want := mc.Step{Input: core.SessionFailed{Now: mc.Ms(1), Cause: core.CauseLost}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Fail with nothing to return = %#v, want a nil Want", got)
	}

	got = mc.Fail(mc.Ms(1), core.CauseUnreachable, mc.Ms(2), core.KeepAlive{})
	wantActions := []core.Action{core.KeepAlive{}, core.Wake{At: mc.Ms(2)}}
	if !reflect.DeepEqual(got.Want, wantActions) {
		t.Errorf("Fail with an extra action = %+v, want %+v", got.Want, wantActions)
	}
}

func TestWall(t *testing.T) {
	if got, want := mc.Wall(mc.Ms(1500)).Sub(mc.Wall(0)), mc.Ms(1500); got != want {
		t.Errorf("Wall(1500ms) - Wall(0) = %v, want %v", got, want)
	}
}

func TestViewAt(t *testing.T) {
	v := mc.Vm(1, "m1")
	got := mc.ViewAt(mc.Ms(1), v, 0)
	want := mc.Step{Input: core.ViewChanged{Now: mc.Ms(1), View: v, Wall: mc.Wall(mc.Ms(1))}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ViewAt with nothing to return = %#v, want a nil Want", got)
	}

	got = mc.ViewAt(mc.Ms(1), v, mc.Ms(2), core.Acquire{Space: "s"})
	wantActions := []core.Action{core.Acquire{Space: "s"}, core.Wake{At: mc.Ms(2)}}
	if !reflect.DeepEqual(got.Want, wantActions) {
		t.Errorf("ViewAt with an extra action = %+v, want %+v", got.Want, wantActions)
	}
}

func TestGrant(t *testing.T) {
	got := mc.Grant(mc.Ms(1), "s", 2, 7, mc.Ms(10), mc.Ms(3))
	want := mc.Step{
		Input: core.Acquired{Now: mc.Ms(1), Space: "s", Shard: 2, Term: 7},
		Want: []core.Action{
			core.StartRun{Space: "s", Shard: 2, Term: 7, Deadline: mc.Ms(10)},
			core.Wake{At: mc.Ms(3)},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Grant = %+v, want %+v", got, want)
	}
}

func TestBoot(t *testing.T) {
	spaces := []core.SpaceSpec{mc.S("b", 1), mc.S("a", 2)}
	got := mc.Boot(7, spaces...)
	wake := core.Wake{At: mc.Ms(2000)}
	want := []mc.Step{
		{Input: core.Tick{}, Want: []core.Action{core.KeepAlive{}, wake}},
		mc.Renew(mc.Ms(100), 0, mc.Ms(2000)),
		mc.ViewAt(mc.Ms(200), mc.Vm(1, "m1"), mc.Ms(2000),
			core.Acquire{Space: "a", Shard: 0}, core.Acquire{Space: "a", Shard: 1}, core.Acquire{Space: "b", Shard: 0}),
		mc.Grant(mc.Ms(300), "a", 0, 7, mc.Ms(10000), mc.Ms(2000)),
		mc.Grant(mc.Ms(300), "a", 1, 8, mc.Ms(10000), mc.Ms(2000)),
		mc.Grant(mc.Ms(300), "b", 0, 9, mc.Ms(10000), mc.Ms(2000)),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Boot = %+v, want %+v", got, want)
	}
	if spaces[0].Name != "b" {
		t.Error("Boot sorted its argument")
	}

	// The same steps hold for a fenced space: nothing in them depends on Fenced.
	m := core.NewMachine(mc.CF(spaces...))
	for i, s := range got {
		if result := m.Step(s.Input); !reflect.DeepEqual(result, s.Want) {
			t.Errorf("step %d on a fenced machine = %+v, want %+v", i+1, result, s.Want)
		}
	}
}

func TestOutage(t *testing.T) {
	keepAlive := func(now, wake int64) mc.Step {
		return mc.Step{
			Input: core.Tick{Now: mc.Ms(now)},
			Want:  []core.Action{core.KeepAlive{}, core.Wake{At: mc.Ms(wake)}},
		}
	}
	failed := func(now, wake int64) mc.Step {
		return mc.Fail(mc.Ms(now), core.CauseUnreachable, mc.Ms(wake))
	}

	tests := []struct {
		name               string
		from, to, deadline int64
		want               []mc.Step
	}{
		{
			name: "deadline on the cadence",
			from: 8000, to: 11000, deadline: 10000,
			want: []mc.Step{keepAlive(8000, 10000), failed(8000, 10000), keepAlive(10000, 12000), failed(10000, 12000)},
		},
		{
			name: "deadline between two readings",
			from: 4000, to: 7000, deadline: 5000,
			want: []mc.Step{
				keepAlive(4000, 5000), failed(4000, 5000),
				{Input: core.Tick{Now: mc.Ms(5000)}, Want: []core.Action{core.Wake{At: mc.Ms(6000)}}},
				keepAlive(6000, 8000), failed(6000, 8000),
			},
		},
		{
			name: "deadline already past",
			from: 12000, to: 13000, deadline: 10000,
			want: []mc.Step{keepAlive(12000, 14000), failed(12000, 14000)},
		},
		{
			name: "deadline after the outage",
			from: 2000, to: 3000, deadline: 3500,
			want: []mc.Step{keepAlive(2000, 3500), failed(2000, 3500)},
		},
		{name: "empty", from: 2000, to: 2000, deadline: 10000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mc.Outage(mc.Ms(tc.from), mc.Ms(tc.to), mc.Ms(tc.deadline))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Outage = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCases(t *testing.T) {
	cases := mc.Cases()
	if len(cases) == 0 {
		t.Fatal("Cases() is empty")
	}
	seen := map[string]bool{}
	for _, c := range cases {
		if seen[c.ID] {
			t.Errorf("duplicate case %q", c.ID)
		}
		seen[c.ID] = true
		if !strings.HasPrefix(c.ID, "AT-MCH-") {
			t.Errorf("case ID %q does not start with AT-MCH-", c.ID)
		}
		if got := mc.Get(c.ID); !reflect.DeepEqual(got, c) {
			t.Errorf("Get(%q) = %+v, want %+v", c.ID, got, c)
		}
	}

	t.Run("the first step of the first case is KeepAlive", func(t *testing.T) {
		c := cases[0]
		line := fmt.Sprintf("%s 1 %T%+v", c.AT(), c.Steps[0].Want[0], c.Steps[0].Want[0])
		if want := "AT-MCH-01 1 core.KeepAlive{}"; line != want {
			t.Errorf("first digest line = %q, want %q", line, want)
		}
	})

	t.Run("results share no memory", func(t *testing.T) {
		first := mc.Cases()
		first[0].ID = "changed"
		first[0].Steps[0].Want = nil
		if got := mc.Cases(); !reflect.DeepEqual(got, cases) {
			t.Errorf("Cases() after changing an earlier result = %+v, want %+v", got, cases)
		}
	})
}

func TestGetUnknownPanics(t *testing.T) {
	// A bare AT ID does not name a case that has variants.
	for _, id := range []string{"AT-MCH-00", "", "AT-MCH-23"} {
		t.Run(id, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("Get(%q) did not panic", id)
				}
				err, ok := r.(error)
				if !ok || !strings.Contains(err.Error(), "machinecases: no case") {
					t.Errorf("Get(%q) panicked with %v", id, r)
				}
			}()
			mc.Get(id)
		})
	}
}

func TestCaseAT(t *testing.T) {
	tests := []struct{ id, want string }{
		{"AT-MCH-05", "AT-MCH-05"},
		{"AT-MCH-23 (b)", "AT-MCH-23"},
	}
	for _, tc := range tests {
		if got := (mc.Case{ID: tc.id}).AT(); got != tc.want {
			t.Errorf("Case{ID: %q}.AT() = %q, want %q", tc.id, got, tc.want)
		}
	}
}
