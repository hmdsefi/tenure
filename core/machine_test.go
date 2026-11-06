// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
	mc "github.com/hmdsefi/tenure/internal/machinecases"
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

	t.Run("no other input panics", func(t *testing.T) {
		inputs := append(allInputs(5*time.Second), &core.Tick{Now: 5 * time.Second}, (*core.Tick)(nil))
		for _, in := range inputs {
			core.NewMachine(validConfig()).Step(in)
		}
	})
}

// stepChecks holds a case's checks of Status, NextWake and Assignment, by step number, counting
// from 1. They run after that step.
type stepChecks map[int]func(t *testing.T, m *core.Machine)

// runCase runs the named case of internal/machinecases on a new machine.
func runCase(t *testing.T, id string, checks stepChecks) {
	t.Helper()
	runSteps(t, mc.Get(id), checks)
}

// runSteps runs c on a new machine and returns the machine. It asserts c's assumptions, compares
// every result with reflect.DeepEqual, so a nil Want means a nil result, and checks the wake rules
// after every step.
func runSteps(t *testing.T, c mc.Case, checks stepChecks) *core.Machine {
	t.Helper()
	assumeCase(t, c)
	m := core.NewMachine(c.Config)
	ran := 0
	for i, s := range c.Steps {
		n := i + 1
		got := m.Step(s.Input)
		if !reflect.DeepEqual(got, s.Want) {
			t.Fatalf("%s step %d: Step(%#v) = %#v, want %#v", c.ID, n, s.Input, got, s.Want)
		}
		at, armed := m.NextWake()
		st := m.Status()
		for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
			t.Errorf("%s step %d: %s", c.ID, n, v)
		}
		if check, ok := checks[n]; ok {
			ran++
			check(t, m)
		}
	}
	if ran != len(checks) {
		t.Fatalf("%s: ran %d of %d step checks: a check names a step the case does not have", c.ID, ran, len(checks))
	}
	return m
}

// assumeCase fails the test unless c can be run as written: a valid config, at least one step, an
// input on every step, a Want that is nil or has an action, and a new machine that starts from
// the initial state, so a case's readings mean the same on every run.
func assumeCase(t *testing.T, c mc.Case) {
	t.Helper()
	if err := c.Config.Validate(); err != nil {
		t.Fatalf("%s: config: %v", c.ID, err)
	}
	if len(c.Steps) == 0 {
		t.Fatalf("%s: no steps", c.ID)
	}
	for i, s := range c.Steps {
		if s.Input == nil {
			t.Fatalf("%s step %d: nil Input", c.ID, i+1)
		}
		if s.Want != nil && len(s.Want) == 0 {
			t.Fatalf("%s step %d: Want is empty but not nil, and Step never returns that", c.ID, i+1)
		}
	}
	st := core.NewMachine(c.Config).Status()
	if st.Now != 0 || st.Session != core.SessionStateOpening || st.Deadline != 0 || st.Done {
		t.Fatalf("%s: a new machine has Status %+v", c.ID, st)
	}
}

// wakeViolations returns how a step's result breaks the wake rules: nil rather than empty, a last
// action of Wake{NextWake} when a timer is armed and no other Wake, an armed timer strictly after
// the clock and no timer on a done machine, and no Wake and a reading of 0 when none is armed.
func wakeViolations(got []core.Action, now time.Duration, done bool, at time.Duration, armed bool) []string {
	var found []string
	if got != nil && len(got) == 0 {
		found = append(found, "result is empty but not nil")
	}
	wakes := 0
	for _, a := range got {
		if _, ok := a.(core.Wake); ok {
			wakes++
		}
	}
	if !armed {
		if wakes != 0 {
			found = append(found, fmt.Sprintf("no timer is armed, but %v has a Wake", got))
		}
		if at != 0 {
			found = append(found, fmt.Sprintf("no timer is armed, but NextWake returned %v", at))
		}
		return found
	}
	if len(got) == 0 || got[len(got)-1] != core.Action(core.Wake{At: at}) {
		found = append(found, fmt.Sprintf("last action of %v is not Wake{%v}", got, at))
	}
	if wakes > 1 {
		found = append(found, fmt.Sprintf("%v has %d Wakes", got, wakes))
	}
	if at <= now {
		found = append(found, fmt.Sprintf("armed timer %v is not after the clock %v", at, now))
	}
	if done {
		found = append(found, fmt.Sprintf("the machine is done, but timer %v is armed", at))
	}
	return found
}

func TestWakeViolations(t *testing.T) {
	wake := core.Wake{At: 2 * time.Second}
	tests := []struct {
		name  string
		got   []core.Action
		now   time.Duration
		done  bool
		at    time.Duration
		armed bool
		want  int
	}{
		{name: "armed", got: []core.Action{core.KeepAlive{}, wake}, at: 2 * time.Second, armed: true},
		{name: "not armed", got: nil},
		{name: "not armed with an action", got: []core.Action{core.KeepAlive{}}},
		{name: "empty result", got: []core.Action{}, want: 1},
		{name: "armed without a result", got: nil, at: 2 * time.Second, armed: true, want: 1},
		{name: "armed without a Wake", got: []core.Action{core.KeepAlive{}}, at: 2 * time.Second, armed: true, want: 1},
		{name: "Wake not last", got: []core.Action{wake, core.KeepAlive{}}, at: 2 * time.Second, armed: true, want: 1},
		{name: "Wake for another reading", got: []core.Action{wake}, at: 3 * time.Second, armed: true, want: 1},
		{name: "two Wakes", got: []core.Action{wake, wake}, at: 2 * time.Second, armed: true, want: 1},
		{name: "timer at the clock", got: []core.Action{wake}, now: 2 * time.Second, at: 2 * time.Second, armed: true, want: 1},
		{name: "timer before the clock", got: []core.Action{wake}, now: 3 * time.Second, at: 2 * time.Second, armed: true, want: 1},
		{name: "Wake with no timer", got: []core.Action{wake}, want: 1},
		{name: "reading with no timer", got: nil, at: 2 * time.Second, want: 1},
		{name: "done with no timer", got: nil, done: true},
		{name: "done with a timer", got: []core.Action{wake}, done: true, at: 2 * time.Second, armed: true, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := wakeViolations(tc.got, tc.now, tc.done, tc.at, tc.armed); len(got) != tc.want {
				t.Errorf("wakeViolations = %q, want %d", got, tc.want)
			}
		})
	}
}

// scenario returns a case that is not in the shared lists, on one space of one shard.
func scenario(steps ...mc.Step) mc.Case {
	return mc.Case{ID: "scenario", Config: mc.C(mc.S("s", 1)), Steps: steps}
}

// tick is a Tick at ms milliseconds with the given result.
func tick(ms int64, want ...core.Action) mc.Step {
	return mc.Step{Input: core.Tick{Now: mc.Ms(ms)}, Want: want}
}

// keepAlive is a Tick at ms milliseconds that returns a KeepAlive, then a Wake at wake.
func keepAlive(ms, wake int64) mc.Step {
	return tick(ms, core.KeepAlive{}, wakeAt(wake))
}

// wakeAt is the Wake at ms milliseconds.
func wakeAt(ms int64) core.Action {
	return core.Wake{At: mc.Ms(ms)}
}

// extended is the ExtendDeadline to ms milliseconds.
func extended(ms int64) core.Action {
	return core.ExtendDeadline{Deadline: mc.Ms(ms)}
}

// renewed is a SessionRenewed at now, sent at sentAt, in milliseconds, with the given result.
func renewed(now, sentAt int64, want ...core.Action) mc.Step {
	return mc.Step{Input: core.SessionRenewed{Now: mc.Ms(now), SentAt: mc.Ms(sentAt)}, Want: want}
}

// failed is a SessionFailed at now in milliseconds with the given result.
func failed(now int64, cause core.Cause, want ...core.Action) mc.Step {
	return mc.Step{Input: core.SessionFailed{Now: mc.Ms(now), Cause: cause}, Want: want}
}

// sessionIs checks the session fields of Status: Now, Session, Deadline and LastFailure, with
// readings in milliseconds, and that Done matches a lost session.
func sessionIs(now int64, state core.SessionState, deadline int64, failure core.Cause) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		st := m.Status()
		if st.Now != mc.Ms(now) {
			t.Errorf("Now = %v, want %v", st.Now, mc.Ms(now))
		}
		if st.Session != state {
			t.Errorf("Session = %v, want %v", st.Session, state)
		}
		if st.Deadline != mc.Ms(deadline) {
			t.Errorf("Deadline = %v, want %v", st.Deadline, mc.Ms(deadline))
		}
		if st.LastFailure != failure {
			t.Errorf("LastFailure = %v, want %v", st.LastFailure, failure)
		}
		if want := state == core.SessionStateLost; st.Done != want {
			t.Errorf("Done = %v, want %v", st.Done, want)
		}
	}
}

// allInputs returns one input of each type at the reading now.
func allInputs(now time.Duration) []core.Input {
	return []core.Input{
		core.Tick{Now: now},
		core.ViewChanged{Now: now, View: core.View{Version: 3, Members: []core.Member{{ID: "m1", Slot: -1}}}, Wall: time.Unix(100, 0)},
		core.SessionRenewed{Now: now, SentAt: now},
		core.SessionFailed{Now: now, Cause: core.CauseUnreachable},
		core.SessionClosed{Now: now},
		core.Acquired{Now: now, Space: "orders", Shard: 0, Term: 4},
		core.AcquireFailed{Now: now, Space: "orders", Shard: 0, Holder: "h2", RetryAfter: time.Second},
		core.Released{Now: now, Space: "orders", Shard: 0, Term: 4},
		core.RunStopped{Now: now, Space: "orders", Shard: 0, Term: 4},
		core.HolderChanged{Now: now, Space: "orders", Shard: 0, Holder: "h1", Term: 4},
		core.SlotClaimed{Now: now, Slot: 1},
		core.SlotReleased{Now: now, Slot: 1},
		core.Drain{Now: now},
	}
}

func TestATMCH05RenewalDeadline(t *testing.T) {
	const (
		opening = core.SessionStateOpening
		live    = core.SessionStateLive
		none    = core.CauseNone
	)
	runCase(t, "AT-MCH-05", stepChecks{
		1: sessionIs(0, opening, 0, none),
		2: sessionIs(100, live, 10000, none),
		3: sessionIs(2000, live, 10000, none),
		4: sessionIs(2100, live, 12000, none),
		5: sessionIs(2200, live, 12000, none),
		6: sessionIs(2300, live, 12000, none),
	})
	if got := len(mc.Get("AT-MCH-05").Steps); got != 6 {
		t.Errorf("AT-MCH-05 has %d slices, want 6", got)
	}
}

func TestATMCH23RepeatsUntilAnswered(t *testing.T) {
	t.Run("b", func(t *testing.T) {
		const (
			live        = core.SessionStateLive
			late        = core.SessionStateLate
			unreachable = core.CauseUnreachable
		)
		runCase(t, "AT-MCH-23 (b)", stepChecks{
			2:  sessionIs(100, live, 10000, core.CauseNone),
			3:  sessionIs(2000, live, 10000, core.CauseNone),
			4:  sessionIs(2000, live, 10000, unreachable),
			10: sessionIs(8000, live, 10000, unreachable),
			11: sessionIs(10000, late, 10000, unreachable),
			12: sessionIs(10000, late, 10000, unreachable),
			13: sessionIs(10500, live, 20000, core.CauseNone),
			14: sessionIs(12000, live, 20000, core.CauseNone),
		})
	})
}

// TestMachineCases runs every shared case with no extra checks, so the wake rules hold after every
// step of every case, whichever test owns it.
func TestMachineCases(t *testing.T) {
	for _, c := range mc.Cases() {
		t.Run(c.ID, func(t *testing.T) { runSteps(t, c, nil) })
	}
}

func TestSessionDeadlineRules(t *testing.T) {
	const (
		opening = core.SessionStateOpening
		live    = core.SessionStateLive
		late    = core.SessionStateLate
	)
	tests := []struct {
		name  string
		steps []mc.Step
		check func(*testing.T, *core.Machine)
	}{
		{
			name:  "counts from SentAt, not Now",
			steps: []mc.Step{keepAlive(0, 2000), renewed(1500, 1000, extended(11000), wakeAt(2000))},
			check: sessionIs(1500, live, 11000, core.CauseNone),
		},
		{
			name:  "SentAt equal to the clock is accepted",
			steps: []mc.Step{keepAlive(0, 2000), renewed(1000, 1000, extended(11000), wakeAt(2000))},
			check: sessionIs(1000, live, 11000, core.CauseNone),
		},
		{
			name:  "SentAt after the clock is ignored",
			steps: []mc.Step{keepAlive(0, 2000), renewed(1000, 1001, wakeAt(2000))},
			check: sessionIs(1000, opening, 0, core.CauseNone),
		},
		{
			name:  "SentAt is judged by the clock, not by an older Now",
			steps: []mc.Step{keepAlive(0, 2000), tick(1000, wakeAt(2000)), renewed(500, 1000, extended(11000), wakeAt(2000))},
			check: sessionIs(1000, live, 11000, core.CauseNone),
		},
		{
			name:  "SentAt is judged after the input moves the clock",
			steps: []mc.Step{keepAlive(0, 2000), renewed(1500, 1500, extended(11500), wakeAt(2000))},
			check: sessionIs(1500, live, 11500, core.CauseNone),
		},
		{
			name: "older SentAt moves nothing",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(1000, 1000, extended(11000), wakeAt(2000)),
				renewed(1100, 500, wakeAt(2000)),
			},
			check: sessionIs(1100, live, 11000, core.CauseNone),
		},
		{
			name: "equal SentAt moves nothing",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(1000, 1000, extended(11000), wakeAt(2000)),
				renewed(1100, 1000, wakeAt(2000)),
			},
			check: sessionIs(1100, live, 11000, core.CauseNone),
		},
		{
			name: "older SentAt still clears the failure",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(1000, 1000, extended(11000), wakeAt(2000)),
				failed(1100, core.CauseUnreachable, wakeAt(2000)),
				renewed(1200, 500, wakeAt(2000)),
			},
			check: sessionIs(1200, live, 11000, core.CauseNone),
		},
		{
			name: "ignored SentAt keeps the failure",
			steps: []mc.Step{
				keepAlive(0, 2000),
				failed(1100, core.CauseUnreachable, wakeAt(2000)),
				renewed(1200, 1300, wakeAt(2000)),
			},
			check: sessionIs(1200, opening, 0, core.CauseUnreachable),
		},
		{
			name:  "a first renewal that is already past its deadline is accepted, and late",
			steps: []mc.Step{keepAlive(0, 2000), renewed(11000, 0, extended(10000), core.KeepAlive{}, wakeAt(13000))},
			check: sessionIs(11000, late, 10000, core.CauseNone),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := runSteps(t, scenario(tc.steps...), nil)
			tc.check(t, m)
		})
	}
}

func TestSessionStates(t *testing.T) {
	const (
		opening = core.SessionStateOpening
		live    = core.SessionStateLive
		late    = core.SessionStateLate
		lost    = core.SessionStateLost
	)
	tests := []struct {
		name   string
		steps  []mc.Step
		states []core.SessionState
	}{
		{
			name:   "opening stays opening without a renewal, whatever the clock",
			steps:  []mc.Step{keepAlive(0, 2000), keepAlive(20000, 22000)},
			states: []core.SessionState{opening, opening},
		},
		{
			name:   "opening stays opening through an unreachable store",
			steps:  []mc.Step{keepAlive(0, 2000), failed(100, core.CauseUnreachable, wakeAt(2000))},
			states: []core.SessionState{opening, opening},
		},
		{
			name: "live until the deadline, late at it",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(9999, 10000),
				tick(10000, wakeAt(11999)),
			},
			states: []core.SessionState{opening, live, live, late},
		},
		{
			name: "a renewal brings late back to live",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(10000, 12000),
				renewed(10100, 10000, extended(20000), wakeAt(12000)),
			},
			states: []core.SessionState{opening, live, late, live},
		},
		{
			name: "a renewal whose deadline has also passed leaves it late",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(11500, 13500),
				renewed(11600, 1000, extended(11000), wakeAt(13500)),
			},
			states: []core.SessionState{opening, live, late, late},
		},
		{
			name: "a renewal whose deadline is the clock's reading leaves it late",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(11000, 13000),
				renewed(11000, 1000, extended(11000), wakeAt(13000)),
			},
			states: []core.SessionState{opening, live, late, late},
		},
		{
			name: "an older renewal leaves it late",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(10000, 12000),
				renewed(10100, 0, wakeAt(12000)),
			},
			states: []core.SessionState{opening, live, late, late},
		},
		{
			name:   "opening to lost",
			steps:  []mc.Step{keepAlive(0, 2000), failed(100, core.CauseLost)},
			states: []core.SessionState{opening, lost},
		},
		{
			name: "live to lost",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				failed(200, core.CauseLost),
			},
			states: []core.SessionState{opening, live, lost},
		},
		{
			name: "late to lost",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(10000, 12000),
				failed(10100, core.CauseLost),
			},
			states: []core.SessionState{opening, live, late, lost},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checks := stepChecks{}
			for i, state := range tc.states {
				checks[i+1] = func(t *testing.T, m *core.Machine) {
					t.Helper()
					if got := m.Status().Session; got != state {
						t.Errorf("step %d: Session = %v, want %v", i+1, got, state)
					}
				}
			}
			runSteps(t, scenario(tc.steps...), checks)
		})
	}
}

func TestSessionFailureCauses(t *testing.T) {
	const (
		live = core.SessionStateLive
		lost = core.SessionStateLost
	)
	running := []mc.Step{
		keepAlive(0, 2000),
		renewed(100, 0, extended(10000), wakeAt(2000)),
	}
	tests := []struct {
		name  string
		steps []mc.Step
		check func(*testing.T, *core.Machine)
	}{
		{
			name:  "unreachable records the cause and nothing else",
			steps: []mc.Step{failed(200, core.CauseUnreachable, wakeAt(2000))},
			check: sessionIs(200, live, 10000, core.CauseUnreachable),
		},
		{
			name:  "none is ignored",
			steps: []mc.Step{failed(200, core.CauseNone, wakeAt(2000))},
			check: sessionIs(200, live, 10000, core.CauseNone),
		},
		{
			name: "none does not clear a recorded cause",
			steps: []mc.Step{
				failed(200, core.CauseUnreachable, wakeAt(2000)),
				failed(300, core.CauseNone, wakeAt(2000)),
			},
			check: sessionIs(300, live, 10000, core.CauseUnreachable),
		},
		{
			name:  "a value above the range is ignored",
			steps: []mc.Step{failed(200, core.Cause(7), wakeAt(2000))},
			check: sessionIs(200, live, 10000, core.CauseNone),
		},
		{
			name:  "a value below the range is ignored",
			steps: []mc.Step{failed(200, core.Cause(-1), wakeAt(2000))},
			check: sessionIs(200, live, 10000, core.CauseNone),
		},
		{
			name:  "an ignored cause still runs the step",
			steps: []mc.Step{failed(2000, core.Cause(7), core.KeepAlive{}, wakeAt(4000))},
			check: sessionIs(2000, live, 10000, core.CauseNone),
		},
		{
			name:  "unreachable does not change the cadence",
			steps: []mc.Step{failed(2000, core.CauseUnreachable, core.KeepAlive{}, wakeAt(4000))},
			check: sessionIs(2000, live, 10000, core.CauseUnreachable),
		},
		{
			name: "a renewal clears unreachable",
			steps: []mc.Step{
				failed(200, core.CauseUnreachable, wakeAt(2000)),
				renewed(300, 300, extended(10300), wakeAt(2000)),
			},
			check: sessionIs(300, live, 10300, core.CauseNone),
		},
		{
			name:  "lost is final and leaves the deadline",
			steps: []mc.Step{failed(200, core.CauseLost)},
			check: sessionIs(200, lost, 10000, core.CauseLost),
		},
		{
			name:  "lost at a due KeepAlive sends none",
			steps: []mc.Step{failed(2000, core.CauseLost)},
			check: sessionIs(2000, lost, 10000, core.CauseLost),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := runSteps(t, scenario(slices.Concat(running, tc.steps)...), nil)
			tc.check(t, m)
		})
	}
}

func TestSessionLostIsFinal(t *testing.T) {
	starts := []struct {
		name     string
		steps    []mc.Step
		deadline int64
	}{
		{name: "from opening", steps: []mc.Step{keepAlive(0, 2000)}},
		{
			name:     "from live",
			steps:    []mc.Step{keepAlive(0, 2000), renewed(100, 0, extended(10000), wakeAt(2000))},
			deadline: 10000,
		},
		{
			name: "from late",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(10000, 12000),
			},
			deadline: 10000,
		},
	}
	for _, start := range starts {
		t.Run(start.name, func(t *testing.T) {
			const lostAt = 11000
			steps := append(slices.Clone(start.steps), failed(lostAt, core.CauseLost))

			// Every later input returns nil, at a reading past every old timer, and the clock
			// keeps moving.
			later := lostAt + core.DefaultTiming.LeaseDuration.Milliseconds()
			inputs := append(allInputs(mc.Ms(later)),
				core.SessionRenewed{Now: mc.Ms(later), SentAt: mc.Ms(later)},
				core.SessionFailed{Now: mc.Ms(later), Cause: core.CauseLost},
				core.SessionFailed{Now: mc.Ms(later), Cause: core.CauseNone},
				&core.Tick{Now: mc.Ms(later)},
			)
			checks := stepChecks{
				len(steps): sessionIs(lostAt, core.SessionStateLost, start.deadline, core.CauseLost),
			}
			// Checked after every one of them: a later failure must not replace the cause, and
			// the last input must not be what puts it back.
			for _, in := range inputs {
				steps = append(steps, mc.Step{Input: in})
				checks[len(steps)] = sessionIs(later, core.SessionStateLost, start.deadline, core.CauseLost)
			}
			m := runSteps(t, scenario(steps...), checks)
			if at, ok := m.NextWake(); at != 0 || ok {
				t.Errorf("NextWake() = (%v, %v), want (0, false)", at, ok)
			}
			if got := m.Step(core.Tick{Now: mc.Ms(later + 1)}); got != nil {
				t.Errorf("Step after the loss = %#v, want nil", got)
			}
		})
	}
}

func TestSessionKeepAliveCadence(t *testing.T) {
	tests := []struct {
		name  string
		steps []mc.Step
	}{
		{name: "the first step returns one", steps: []mc.Step{keepAlive(0, 2000)}},
		{
			name: "not before RetryPeriod, a reading of 0 counts as set",
			steps: []mc.Step{
				keepAlive(0, 2000),
				tick(1, wakeAt(2000)),
				tick(1999, wakeAt(2000)),
				keepAlive(2000, 4000),
			},
		},
		{
			name: "one renewal before any Tick does not send a second KeepAlive",
			steps: []mc.Step{
				renewed(100, 0, extended(10000), wakeAt(2000)),
				tick(1000, wakeAt(2000)),
				keepAlive(2000, 4000),
			},
		},
		{
			name:  "one renewal before any Tick sends a KeepAlive if RetryPeriod has passed since SentAt",
			steps: []mc.Step{renewed(2500, 0, extended(10000), core.KeepAlive{}, wakeAt(4500))},
		},
		{
			name: "an ignored renewal before any Tick does not count as a send",
			steps: []mc.Step{
				renewed(100, 500, core.KeepAlive{}, wakeAt(2100)),
				renewed(200, 100, extended(10100), wakeAt(2100)),
			},
		},
		{
			name:  "a failure before any Tick does not count as a send",
			steps: []mc.Step{failed(100, core.CauseUnreachable, core.KeepAlive{}, wakeAt(2100))},
		},
		{
			name: "counts from the reading of the last KeepAlive",
			steps: []mc.Step{
				keepAlive(0, 2000),
				keepAlive(5000, 7000),
				tick(6000, wakeAt(7000)),
				keepAlive(7000, 9000),
			},
		},
		{
			name: "a later renewal does not move the cadence",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(1000, 1000, extended(11000), wakeAt(2000)),
				keepAlive(2000, 4000),
			},
		},
		{
			name: "ExtendDeadline comes before KeepAlive",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(2000, 1000, extended(11000), core.KeepAlive{}, wakeAt(4000)),
			},
		},
		{
			name: "the wake is the deadline when that comes first, and only while it is ahead",
			steps: []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(9000, 10000),
				tick(10000, wakeAt(11000)),
				keepAlive(11000, 13000),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runSteps(t, scenario(tc.steps...), nil) })
	}

	t.Run("an outage with the deadline between two readings", func(t *testing.T) {
		runSteps(t, scenario(slices.Concat(
			[]mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				keepAlive(7000, 9000),
			},
			mc.Outage(mc.Ms(9000), mc.Ms(12000), mc.Ms(10000)),
		)...), nil)
	})
}

func TestStepClockNeverDecreases(t *testing.T) {
	runSteps(t, scenario(
		keepAlive(5000, 7000),
		tick(3000, wakeAt(7000)),
		tick(5000, wakeAt(7000)),
		tick(6000, wakeAt(7000)),
		tick(0, wakeAt(7000)),
	), stepChecks{
		1: sessionIs(5000, core.SessionStateOpening, 0, core.CauseNone),
		2: sessionIs(5000, core.SessionStateOpening, 0, core.CauseNone),
		3: sessionIs(5000, core.SessionStateOpening, 0, core.CauseNone),
		4: sessionIs(6000, core.SessionStateOpening, 0, core.CauseNone),
		5: sessionIs(6000, core.SessionStateOpening, 0, core.CauseNone),
	})
}

func TestStepClockOlderInput(t *testing.T) {
	t.Run("a renewal runs at the machine's reading", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(5000, 7000),
			renewed(1000, 4000, extended(14000), wakeAt(7000)),
		), stepChecks{2: sessionIs(5000, core.SessionStateLive, 14000, core.CauseNone)})
	})
	t.Run("a late session does not go back to live", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			keepAlive(10000, 12000),
			tick(9000, wakeAt(12000)),
		), stepChecks{4: sessionIs(10000, core.SessionStateLate, 10000, core.CauseNone)})
	})
	t.Run("a failure runs at the machine's reading", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(5000, 7000),
			failed(1000, core.CauseUnreachable, wakeAt(7000)),
		), stepChecks{2: sessionIs(5000, core.SessionStateOpening, 0, core.CauseUnreachable)})
	})
}

// TestStepClockClosedMachine checks that the closed check comes before the clock: a closed machine
// returns nil for every input and its reading stays where it was. Nothing closes a machine yet, so
// the test closes it directly.
func TestStepClockClosedMachine(t *testing.T) {
	const closedAt = time.Second
	m := core.NewMachine(mc.C(mc.S("s", 1)))
	m.Step(core.Tick{Now: closedAt})
	core.ForceSessionClosed(m)
	for _, in := range allInputs(5 * time.Second) {
		if got := m.Step(in); got != nil {
			t.Errorf("Step(%T) on a closed machine = %#v, want nil", in, got)
		}
		if got := m.Status().Now; got != closedAt {
			t.Errorf("Now after Step(%T) on a closed machine = %v, want %v", in, got, closedAt)
		}
	}
	if err := panicError(t, func() { m.Step(nil) }); err.Error() != "core: Step: nil Input" {
		t.Errorf("Step(nil) on a closed machine panicked with %q", err)
	}
}

// maxReading is the highest reading the clock takes: 24 hours short of the largest Duration.
const maxReading = time.Duration(math.MaxInt64) - 24*time.Hour

// TestStepClockExtremeReadings pins what the machine does with readings outside the driver's range,
// and at the top of it. The wake rules that runSteps checks are the point of most of these: a timer
// must fit in a Duration and be after the clock.
func TestStepClockExtremeReadings(t *testing.T) {
	const (
		retry    = 2 * time.Second
		deadline = 10 * time.Second
		top      = time.Duration(1<<62 - 1)
	)
	ka := func(now, wake time.Duration) mc.Step {
		return mc.Step{Input: core.Tick{Now: now}, Want: []core.Action{core.KeepAlive{}, core.Wake{At: wake}}}
	}
	tests := []struct {
		name    string
		steps   []mc.Step
		now     time.Duration
		session core.SessionState
		dl      time.Duration
	}{
		{
			name:    "a negative first reading leaves the clock at 0",
			steps:   []mc.Step{ka(-time.Second, retry)},
			session: core.SessionStateOpening,
		},
		{
			name:    "the lowest reading leaves the clock at 0",
			steps:   []mc.Step{ka(math.MinInt64, retry)},
			session: core.SessionStateOpening,
		},
		{
			name: "a negative reading later leaves the clock where it is",
			steps: []mc.Step{
				ka(time.Second, time.Second+retry),
				{Input: core.Tick{Now: -time.Second}, Want: []core.Action{core.Wake{At: time.Second + retry}}},
			},
			now:     time.Second,
			session: core.SessionStateOpening,
		},
		{
			name: "a negative SentAt that still reaches past the clock counts",
			steps: []mc.Step{
				ka(0, retry),
				{
					Input: core.SessionRenewed{SentAt: -time.Second},
					Want:  []core.Action{core.ExtendDeadline{Deadline: 9 * time.Second}, core.Wake{At: retry}},
				},
			},
			session: core.SessionStateLive,
			dl:      9 * time.Second,
		},
		{
			name: "a SentAt too old to reach 0 moves nothing, and the session is late",
			steps: []mc.Step{
				ka(0, retry),
				{Input: core.SessionRenewed{SentAt: math.MinInt64}, Want: []core.Action{core.Wake{At: retry}}},
			},
			session: core.SessionStateLate,
		},
		{
			name: "the top of the driver's range is an ordinary reading",
			steps: []mc.Step{
				ka(top, top+retry),
				{
					Input: core.SessionRenewed{Now: top, SentAt: top},
					Want:  []core.Action{core.ExtendDeadline{Deadline: top + deadline}, core.Wake{At: top + retry}},
				},
			},
			now:     top,
			session: core.SessionStateLive,
			dl:      top + deadline,
		},
		{
			name:    "the highest reading is where the clock stops",
			steps:   []mc.Step{ka(maxReading, maxReading+retry)},
			now:     maxReading,
			session: core.SessionStateOpening,
		},
		{
			name:    "a reading above it stops there",
			steps:   []mc.Step{ka(maxReading+1, maxReading+retry)},
			now:     maxReading,
			session: core.SessionStateOpening,
		},
		{
			name:    "a reading whose timer would overflow stops there",
			steps:   []mc.Step{ka(math.MaxInt64-retry+1, maxReading+retry)},
			now:     maxReading,
			session: core.SessionStateOpening,
		},
		{
			name:    "the largest Duration stops there",
			steps:   []mc.Step{ka(math.MaxInt64, maxReading+retry)},
			now:     maxReading,
			session: core.SessionStateOpening,
		},
		{
			name: "a SentAt above the stopped clock is ignored",
			steps: []mc.Step{
				ka(math.MaxInt64, maxReading+retry),
				{
					Input: core.SessionRenewed{Now: math.MaxInt64, SentAt: math.MaxInt64},
					Want:  []core.Action{core.Wake{At: maxReading + retry}},
				},
			},
			now:     maxReading,
			session: core.SessionStateOpening,
		},
		{
			name: "a renewal at the stopped clock moves the deadline but cannot make it live",
			steps: []mc.Step{
				ka(math.MaxInt64, maxReading+retry),
				{
					Input: core.SessionRenewed{Now: math.MaxInt64, SentAt: maxReading},
					Want:  []core.Action{core.ExtendDeadline{Deadline: maxReading + deadline}, core.Wake{At: maxReading + retry}},
				},
			},
			now:     maxReading,
			session: core.SessionStateLate,
			dl:      maxReading + deadline,
		},
		{
			name: "a live session goes late when the clock stops short of its deadline",
			steps: []mc.Step{
				ka(maxReading-1, maxReading-1+retry),
				{
					Input: core.SessionRenewed{Now: maxReading - 1, SentAt: maxReading - 1},
					Want: []core.Action{
						core.ExtendDeadline{Deadline: maxReading - 1 + deadline},
						core.Wake{At: maxReading - 1 + retry},
					},
				},
				{Input: core.Tick{Now: math.MaxInt64}, Want: []core.Action{core.Wake{At: maxReading - 1 + retry}}},
			},
			now:     maxReading,
			session: core.SessionStateLate,
			dl:      maxReading - 1 + deadline,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := runSteps(t, scenario(tc.steps...), nil).Status()
			if st.Now != tc.now || st.Session != tc.session || st.Deadline != tc.dl {
				t.Errorf("Now, Session, Deadline = %v, %v, %v, want %v, %v, %v",
					st.Now, st.Session, st.Deadline, tc.now, tc.session, tc.dl)
			}
		})
	}

	t.Run("the largest Timing fits", func(t *testing.T) {
		cfg := mc.C(mc.S("s", 1))
		cfg.Timing = core.Timing{LeaseDuration: 24 * time.Hour, RenewDeadline: 21 * time.Hour, RetryPeriod: 17 * time.Hour, SlotGrace: 24 * time.Hour}
		m := core.NewMachine(cfg)
		m.Step(core.Tick{Now: math.MaxInt64})
		got := m.Step(core.SessionRenewed{Now: math.MaxInt64, SentAt: maxReading})
		want := []core.Action{
			core.ExtendDeadline{Deadline: maxReading + 21*time.Hour},
			core.Wake{At: maxReading + 17*time.Hour},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("renewal at the stopped clock = %#v, want %#v", got, want)
		}
	})
}

// TestStepClockOddReadings runs short sequences over every combination of odd readings: no Step may
// panic, the clock never goes back, the wake rules hold, and a live session is before its deadline.
func TestStepClockOddReadings(t *testing.T) {
	readings := []time.Duration{
		math.MinInt64, -time.Hour, -1, 0, 1, 2 * time.Second, 10 * time.Second,
		1<<62 - 1, 1 << 62, maxReading - 1, maxReading, maxReading + 1, math.MaxInt64 - 1, math.MaxInt64,
	}
	for _, first := range readings {
		for _, second := range readings {
			for _, sentAt := range readings {
				m := core.NewMachine(mc.C(mc.S("s", 1)))
				clock := time.Duration(0)
				for _, in := range []core.Input{
					core.Tick{Now: first},
					core.SessionRenewed{Now: second, SentAt: sentAt},
					core.SessionFailed{Now: first, Cause: core.CauseUnreachable},
					core.Tick{Now: second},
					core.SessionRenewed{Now: first, SentAt: sentAt},
				} {
					got := m.Step(in)
					at, armed := m.NextWake()
					st := m.Status()
					for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
						t.Errorf("readings %d, %d, %d: Step(%#v): %s", first, second, sentAt, in, v)
					}
					if st.Now < clock {
						t.Errorf("readings %d, %d, %d: Step(%#v) moved the clock from %v to %v", first, second, sentAt, in, clock, st.Now)
					}
					clock = st.Now
					if st.Session == core.SessionStateLive && st.Now >= st.Deadline {
						t.Errorf("readings %d, %d, %d: Step(%#v): live at %v with deadline %v", first, second, sentAt, in, st.Now, st.Deadline)
					}
				}
			}
		}
	}
}

func TestStepClockEveryInput(t *testing.T) {
	const now = 5 * time.Second
	for _, in := range allInputs(now) {
		m := core.NewMachine(validConfig())
		m.Step(in)
		if got := m.Status().Now; got != now {
			t.Errorf("Now after Step(%T) = %v, want %v", in, got, now)
		}
	}
}

func TestStepClockOtherInputTypes(t *testing.T) {
	const later = 5 * time.Second
	for _, in := range []core.Input{&core.Tick{Now: later}, (*core.Tick)(nil), &core.SessionRenewed{Now: later, SentAt: later}} {
		runSteps(t, scenario(
			tick(1000, core.KeepAlive{}, wakeAt(3000)),
			mc.Step{Input: in, Want: []core.Action{wakeAt(3000)}},
		), stepChecks{2: sessionIs(1000, core.SessionStateOpening, 0, core.CauseNone)})
	}
}

// TestStepClockIgnoredInputRunsTheStep checks that an ignored input skips only its own effect: the
// clock moves and whatever is due still happens.
func TestStepClockIgnoredInputRunsTheStep(t *testing.T) {
	const now = 2 * time.Second
	inputs := []core.Input{
		core.SessionRenewed{Now: now, SentAt: now + 1},
		core.SessionFailed{Now: now, Cause: core.CauseNone},
		core.Acquired{Now: now, Space: "unknown", Shard: 0, Term: 4},
		core.AcquireFailed{Now: now, Space: "unknown", Shard: 0, Holder: "h2"},
		core.Released{Now: now, Space: "unknown", Shard: 0, Term: 4},
		core.RunStopped{Now: now, Space: "unknown", Shard: 0, Term: 4},
		core.HolderChanged{Now: now, Space: "unknown", Shard: 0, Holder: "h2", Term: 4},
	}
	for _, in := range inputs {
		t.Run(fmt.Sprintf("%T", in), func(t *testing.T) {
			runSteps(t, scenario(
				keepAlive(0, 2000),
				mc.Step{Input: in, Want: []core.Action{core.KeepAlive{}, wakeAt(4000)}},
			), stepChecks{2: sessionIs(2000, core.SessionStateOpening, 0, core.CauseNone)})
		})
	}
}

func TestStepResultSharesNoMemory(t *testing.T) {
	m := core.NewMachine(mc.C(mc.S("s", 1)))
	first := m.Step(core.Tick{})
	want := slices.Clone(first)
	m.Step(core.SessionRenewed{Now: mc.Ms(2000), SentAt: mc.Ms(1000)})
	if !reflect.DeepEqual(first, want) {
		t.Errorf("an earlier result changed to %#v, want %#v", first, want)
	}

	second := m.Step(core.Tick{Now: mc.Ms(4000)})
	second[0] = core.Wake{}
	third := m.Step(core.Tick{Now: mc.Ms(6000)})
	wantThird := []core.Action{core.KeepAlive{}, wakeAt(8000)}
	if !reflect.DeepEqual(third, wantThird) {
		t.Errorf("Step after changing an earlier result = %#v, want %#v", third, wantThird)
	}
}
