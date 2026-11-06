// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
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

// scenarioIn returns a case that is not in the shared lists, on the given config.
func scenarioIn(cfg core.MachineConfig, steps ...mc.Step) mc.Case {
	return mc.Case{ID: "scenario", Config: cfg, Steps: steps}
}

// viewed is a ViewChanged at ms milliseconds delivering v with the given result.
func viewed(ms int64, v core.View, want ...core.Action) mc.Step {
	return mc.Step{Input: core.ViewChanged{Now: mc.Ms(ms), View: v, Wall: mc.Wall(mc.Ms(ms))}, Want: want}
}

// acquired is an Acquired for shard s/0 at ms milliseconds with the given result.
func acquired(ms int64, term core.Term, want ...core.Action) mc.Step {
	return mc.Step{Input: core.Acquired{Now: mc.Ms(ms), Space: "s", Term: term}, Want: want}
}

// acquireS0 is the Acquire of shard s/0.
func acquireS0() core.Action { return core.Acquire{Space: "s"} }

// startedS0 is the StartRun of shard s/0 with a deadline of ms milliseconds.
func startedS0(term core.Term, ms int64) core.Action {
	return core.StartRun{Space: "s", Term: term, Deadline: mc.Ms(ms)}
}

// runStopped is a RunStopped for shard s/0 at ms milliseconds with the given result.
func runStopped(ms int64, term core.Term, want ...core.Action) mc.Step {
	return mc.Step{Input: core.RunStopped{Now: mc.Ms(ms), Space: "s", Term: term}, Want: want}
}

// released is a Released for shard s/0 at ms milliseconds with the given result.
func released(ms int64, term core.Term, want ...core.Action) mc.Step {
	return mc.Step{Input: core.Released{Now: mc.Ms(ms), Space: "s", Term: term}, Want: want}
}

// stopS0 is the StopRun of shard s/0 because the shard moved.
func stopS0(term core.Term) core.Action {
	return core.StopRun{Space: "s", Term: term, Reason: core.StopMoved}
}

// releaseS0 is the Release of shard s/0.
func releaseS0(term core.Term) core.Action { return core.Release{Space: "s", Term: term} }

// holderChanged is a HolderChanged for shard s/0 at ms milliseconds with the given result.
func holderChanged(ms int64, holder string, term core.Term, want ...core.Action) mc.Step {
	return mc.Step{Input: core.HolderChanged{Now: mc.Ms(ms), Space: "s", Holder: holder, Term: term}, Want: want}
}

// acquireFailed is an AcquireFailed naming holder h2 for shard s/0 at ms milliseconds with the given
// result.
func acquireFailed(ms int64, retryAfter time.Duration, want ...core.Action) mc.Step {
	return mc.Step{Input: core.AcquireFailed{Now: mc.Ms(ms), Space: "s", Holder: "h2", RetryAfter: retryAfter}, Want: want}
}

// stopLostS0 is the StopRun of shard s/0's run of term 7 because the record no longer names it.
func stopLostS0() core.Action {
	return core.StopRun{Space: "s", Term: 7, Reason: core.StopLost}
}

// ownedAndLostS0 boots a machine of the one shard s/0, whose run has term 7, and has holder h2 take
// its record at @1: five steps, with the next wake at @2.
func ownedAndLostS0() []mc.Step {
	return append(mc.Boot(7, mc.S("s", 1)), holderChanged(1000, "h2", 8, stopLostS0(), wakeAt(2000)))
}

// lostS0 is the status of a wanted shard whose run of term 7 was told at since milliseconds to stop
// because the record, which now names holder, no longer names it.
func lostS0(since int64, holder string) core.ShardStatus {
	return core.ShardStatus{State: core.ShardStateLost, Since: mc.Ms(since), Term: 7, Reason: core.StopLost, Holder: holder, Wanted: true}
}

// noRunS0 is the status of an unwanted shard that has been releasing a term with no run since since
// milliseconds.
func noRunS0(since int64, term core.Term) core.ShardStatus {
	return core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(since), Term: term, Holder: "h1"}
}

// bootedOnly is the steps that take a machine of the one shard s/0 to a live session with a view that
// does not want it: three steps, with the next wake at @2.
func bootedOnly() []mc.Step {
	return []mc.Step{
		keepAlive(0, 2000),
		renewed(100, 0, extended(10000), wakeAt(2000)),
		viewed(200, mc.Vm(1, "m2"), wakeAt(2000)),
	}
}

// acquiringS0 is the steps that take a machine of the one shard s/0 to a live session and an Acquire
// at @0.2: three steps, with the next wake at @2.
func acquiringS0() []mc.Step {
	return []mc.Step{
		keepAlive(0, 2000),
		renewed(100, 0, extended(10000), wakeAt(2000)),
		viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
	}
}

// movedS0 boots a machine of the one shard s/0, whose run has term 7, and moves the shard away at @1:
// five steps, with the next wake at @2.
func movedS0() []mc.Step {
	return append(mc.Boot(7, mc.S("s", 1)), viewed(1000, mc.Vm(2, "m2"), stopS0(7), wakeAt(2000)))
}

// ownedS0 is the status of a wanted shard whose run of the given term started at since milliseconds.
func ownedS0(since int64, term core.Term) core.ShardStatus {
	return core.ShardStatus{State: core.ShardStateOwned, Since: mc.Ms(since), Term: term, Holder: "h1", Wanted: true}
}

// stoppingS0 is the status of an unwanted shard whose run was told at since milliseconds to stop
// because it moved.
func stoppingS0(since int64, term core.Term) core.ShardStatus {
	return core.ShardStatus{State: core.ShardStateStopping, Since: mc.Ms(since), Term: term, Reason: core.StopMoved, Holder: "h1"}
}

// releasingS0 is the status of an unwanted shard that moved and has been releasing its term since
// since milliseconds.
func releasingS0(since int64, term core.Term) core.ShardStatus {
	return core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(since), Term: term, Reason: core.StopMoved, Holder: "h1"}
}

// shardIs checks one shard of Status exactly, on a machine whose only space is s.
func shardIs(shard int, want core.ShardStatus) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		if got := m.Status().Spaces[0].Shards[shard]; got != want {
			t.Errorf("shard %d = %+v, want %+v", shard, got, want)
		}
	}
}

// assignmentIs checks Assignment(space) exactly, and that it is not nil.
func assignmentIs(space string, want ...string) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		if got := m.Assignment(space); got == nil || !slices.Equal(got, want) {
			t.Errorf("Assignment(%q) = %q, want %q", space, got, want)
		}
	}
}

// viewVersionIs checks ViewVersion.
func viewVersionIs(want uint64) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		if got := m.Status().ViewVersion; got != want {
			t.Errorf("ViewVersion = %d, want %d", got, want)
		}
	}
}

// all runs the checks in order.
func all(checks ...func(*testing.T, *core.Machine)) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		for _, check := range checks {
			check(t, m)
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

func TestATMCH13GrantThatCannotRun(t *testing.T) {
	runCase(t, "AT-MCH-13", stepChecks{
		4: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200)}),
		5: shardIs(0, noRunS0(400, 9)),
		6: shardIs(0, core.ShardStatus{Since: mc.Ms(500), Holder: "h1"}),
	})
}

func TestATMCH16AnotherHolderStopsTheRun(t *testing.T) {
	for i, s := range mc.Get("AT-MCH-16").Steps {
		for _, a := range s.Want {
			if _, ok := a.(core.Release); ok {
				t.Errorf("step %d has %#v, and the record names another holder", i+1, a)
			}
		}
	}
	runCase(t, "AT-MCH-16", stepChecks{
		4: shardIs(0, ownedS0(300, 7)),
		5: shardIs(0, lostS0(1000, "h2")),
		6: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(1500), Holder: "h2", Wanted: true}),
		7: shardIs(0, core.ShardStatus{Since: mc.Ms(1600), Holder: "h2", Wanted: true, RetryAt: mc.Ms(4600)}),
	})
}

func TestATMCH17RetryAtStaysInItsBounds(t *testing.T) {
	first := core.ShardStatus{Since: mc.Ms(3300), Holder: "h2", Wanted: true, RetryAt: mc.Ms(18300)}
	second := core.ShardStatus{Since: mc.Ms(3400), Holder: "h3", Wanted: true, RetryAt: mc.Ms(5400)}
	last := len(mc.Get("AT-MCH-17").Steps)
	runCase(t, "AT-MCH-17", stepChecks{
		6: shardIs(0, first),
		7: all(shardIs(0, first), shardIs(1, second)),
		9: all(shardIs(0, first), shardIs(1, second)),
		10: all(
			shardIs(0, first),
			shardIs(1, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(5400), Holder: "h3", Wanted: true}),
		),
		11:       shardIs(1, ownedS0(5500, 9)),
		last - 1: all(shardIs(0, first), shardIs(1, ownedS0(5500, 9))),
		last: shardIs(0, core.ShardStatus{
			State: core.ShardStateAcquiring, Since: mc.Ms(18300), Holder: "h2", Wanted: true,
		}),
	})
}

func TestATMCH19AnswersWithoutAGrantRetry(t *testing.T) {
	for _, tc := range []struct{ name, id string }{
		{"a", "AT-MCH-19 (a)"},
		{"b", "AT-MCH-19 (b)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			waiting := core.ShardStatus{Since: mc.Ms(200), Wanted: true, RetryAt: mc.Ms(2200)}
			runCase(t, tc.id, stepChecks{
				3: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}),
				4: shardIs(0, waiting),
				6: shardIs(0, waiting),
				7: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(2200), Wanted: true}),
				8: shardIs(0, ownedS0(2300, 7)),
			})
		})
	}
}

func TestATMCH22ReleasingALargerTerm(t *testing.T) {
	sixty := noRunS0(1500, 60)
	runCase(t, "AT-MCH-22", stepChecks{
		5: shardIs(0, stoppingS0(1000, 50)),
		6: shardIs(0, releasingS0(1500, 50)),
		7: shardIs(0, sixty),
		8: shardIs(0, sixty),
		9: shardIs(0, core.ShardStatus{Since: mc.Ms(1800), Holder: "h1"}),
	})
}

func TestATMCH51AdoptedGrantAndLeftover(t *testing.T) {
	adopted := ownedS0(400, 100)
	runCase(t, "AT-MCH-51", stepChecks{
		4: shardIs(0, core.ShardStatus{Since: mc.Ms(300), Holder: "h2", Wanted: true, RetryAt: mc.Ms(2300)}),
		5: all(shardIs(0, adopted), shardIs(1, core.ShardStatus{})),
		6: all(shardIs(0, adopted), shardIs(1, noRunS0(500, 90))),
		7: all(shardIs(0, adopted), shardIs(1, core.ShardStatus{Since: mc.Ms(600), Holder: "h1"})),
	})
}

func TestATMCH14MoveStopsTheRun(t *testing.T) {
	runCase(t, "AT-MCH-14", stepChecks{
		3: all(
			assignmentIs("s", "m1", "m2"),
			shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}),
			shardIs(1, core.ShardStatus{}),
		),
		4: all(shardIs(0, ownedS0(300, 7)), shardIs(1, core.ShardStatus{})),
		5: all(
			assignmentIs("s", "m2", "m3"),
			shardIs(0, stoppingS0(1000, 7)),
			shardIs(1, core.ShardStatus{}),
		),
	})
}

func TestATMCH15MoveToItsEnd(t *testing.T) {
	first, c := mc.Get("AT-MCH-14").Steps, mc.Get("AT-MCH-15").Steps
	if len(c) <= len(first) || !reflect.DeepEqual(c[:len(first)], first) {
		t.Errorf("AT-MCH-15 does not start with the steps of AT-MCH-14: %+v", c)
	}
	idle := core.ShardStatus{Since: mc.Ms(1600), Holder: "h1"}
	runCase(t, "AT-MCH-15", stepChecks{
		5: shardIs(0, stoppingS0(1000, 7)),
		6: all(shardIs(0, releasingS0(1500, 7)), shardIs(1, core.ShardStatus{})),
		7: all(shardIs(0, idle), shardIs(1, core.ShardStatus{})),
		8: shardIs(0, idle),
	})
}

func TestATMCH20OtherTermsAndStates(t *testing.T) {
	owned, unwanted := shardIs(0, ownedS0(300, 7)), shardIs(0, stoppingS0(1000, 7))
	releasing := shardIs(0, releasingS0(1500, 7))
	idle := shardIs(0, core.ShardStatus{Since: mc.Ms(1900), Holder: "h1"})
	runCase(t, "AT-MCH-20", stepChecks{
		4:  owned,
		5:  owned,
		6:  owned,
		7:  unwanted,
		8:  unwanted,
		9:  unwanted,
		10: unwanted,
		11: releasing,
		12: releasing,
		13: releasing,
		14: releasing,
		15: idle,
		16: idle,
	})
}

func TestATMCH23RepeatsUntilAnswered(t *testing.T) {
	t.Run("a", func(t *testing.T) {
		releasing := shardIs(0, releasingS0(1500, 7))
		idle := shardIs(0, core.ShardStatus{Since: mc.Ms(12500), Holder: "h1"})
		runCase(t, "AT-MCH-23 (a)", stepChecks{
			6:  releasing,
			8:  releasing,
			14: all(releasing, sessionIs(9500, core.SessionStateLive, 10000, core.CauseNone)),
			16: all(releasing, sessionIs(11500, core.SessionStateLate, 10000, core.CauseNone)),
			18: idle,
			19: idle,
		})
	})
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

func TestATMCH01BootToFirstGrant(t *testing.T) {
	const (
		opening = core.SessionStateOpening
		live    = core.SessionStateLive
		none    = core.CauseNone
	)
	runCase(t, "AT-MCH-01", stepChecks{
		1: all(
			sessionIs(0, opening, 0, none),
			viewVersionIs(0),
			shardIs(0, core.ShardStatus{}),
			assignmentIs("s", ""),
		),
		2: all(
			sessionIs(100, live, 10000, none),
			shardIs(0, core.ShardStatus{}),
		),
		3: all(
			sessionIs(200, live, 10000, none),
			viewVersionIs(1),
			shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}),
			assignmentIs("s", "m1"),
		),
		4: all(
			sessionIs(300, live, 10000, none),
			shardIs(0, core.ShardStatus{State: core.ShardStateOwned, Since: mc.Ms(300), Term: 7, Holder: "h1", Wanted: true}),
		),
	})

	first := mc.Get("AT-MCH-01").Steps[0].Want
	if len(first) == 0 || first[0] != core.Action(core.KeepAlive{}) {
		t.Errorf("first action of step 1 = %#v, want core.KeepAlive{}", first)
	}
	if got := fmt.Sprintf("%T%+v", first[0], first[0]); got != "core.KeepAlive{}" {
		t.Errorf("text of the first action = %q, want core.KeepAlive{}", got)
	}
}

func TestATMCH02OlderAndInvalidViews(t *testing.T) {
	const m2 = "m2"
	wanting := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(500), Wanted: true}
	runCase(t, "AT-MCH-02", stepChecks{
		3: all(viewVersionIs(5), assignmentIs("s", m2, m2), shardIs(0, core.ShardStatus{})),
		4: all(viewVersionIs(5), assignmentIs("s", m2, m2), shardIs(0, core.ShardStatus{})),
		5: all(viewVersionIs(5), assignmentIs("s", m2, m2), shardIs(0, core.ShardStatus{})),
		6: all(viewVersionIs(5), assignmentIs("s", "m1", "m1"), shardIs(0, wanting), shardIs(1, wanting)),
	})
}

func TestATMCH03AcquireOnlyWhileLive(t *testing.T) {
	wanted := core.ShardStatus{Wanted: true}
	t.Run("opening", func(t *testing.T) {
		runCase(t, "AT-MCH-03 (a)", stepChecks{
			2: all(sessionIs(100, core.SessionStateOpening, 0, core.CauseNone), shardIs(0, wanted)),
			3: all(sessionIs(2000, core.SessionStateOpening, 0, core.CauseNone), shardIs(0, wanted)),
			4: all(
				sessionIs(2100, core.SessionStateLive, 12000, core.CauseNone),
				shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(2100), Wanted: true}),
			),
		})
	})
	t.Run("late", func(t *testing.T) {
		runCase(t, "AT-MCH-03 (b)", stepChecks{
			4: all(sessionIs(10100, core.SessionStateLate, 10000, core.CauseNone), shardIs(0, wanted)),
			5: all(sessionIs(12000, core.SessionStateLate, 10000, core.CauseNone), shardIs(0, wanted)),
			6: all(
				sessionIs(12100, core.SessionStateLive, 22000, core.CauseNone),
				shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(12100), Wanted: true}),
			),
		})
	})
}

func TestATMCH07SharedView(t *testing.T) {
	spaces := func(a2, b1 core.ShardStatus) []core.SpaceStatus {
		return []core.SpaceStatus{
			{Name: "a", Shards: []core.ShardStatus{{}, {}, a2}},
			{Name: "b", Shards: []core.ShardStatus{{}, b1}},
		}
	}
	statusIs := func(want []core.SpaceStatus) func(*testing.T, *core.Machine) {
		return func(t *testing.T, m *core.Machine) {
			t.Helper()
			if got := m.Status().Spaces; !reflect.DeepEqual(got, want) {
				t.Errorf("Spaces = %+v, want %+v", got, want)
			}
		}
	}
	acquiring := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}
	runCase(t, "AT-MCH-07", stepChecks{
		2: statusIs(spaces(core.ShardStatus{}, core.ShardStatus{})),
		3: all(
			viewVersionIs(1),
			statusIs(spaces(acquiring, acquiring)),
			assignmentIs("a", "m2", "m2", "m1"),
			assignmentIs("b", "m2", "m1"),
		),
		4: statusIs(spaces(acquiring, core.ShardStatus{
			State: core.ShardStateOwned, Since: mc.Ms(300), Term: 4, Holder: "h1", Wanted: true,
		})),
	})
}

func TestATMCH24AnswersNoAcquireWaitsFor(t *testing.T) {
	owned := func(since int64, term core.Term) core.ShardStatus {
		return core.ShardStatus{State: core.ShardStateOwned, Since: mc.Ms(since), Term: term, Holder: "h1", Wanted: true}
	}
	t.Run("ignored", func(t *testing.T) {
		acquiring := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}
		runCase(t, "AT-MCH-24", stepChecks{
			3:  shardIs(0, core.ShardStatus{}),
			5:  all(shardIs(0, owned(300, 7)), shardIs(1, acquiring)),
			6:  shardIs(0, owned(300, 7)),
			7:  shardIs(0, owned(300, 7)),
			10: all(shardIs(0, owned(300, 7)), shardIs(1, acquiring)),
			11: all(shardIs(0, owned(300, 7)), shardIs(1, owned(500, 7))),
		})
	})

	// After a run of term 7 has ended, a grant of that term or an earlier one cannot start: it is
	// released with no run, and the shard asks again once that is answered. Only a higher term starts.
	t.Run("highest started term", func(t *testing.T) {
		tests := []struct {
			name  string
			term  core.Term
			start bool
		}{
			{name: "below", term: 6},
			{name: "equal", term: 7},
			{name: "above", term: 8, start: true},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				steps := append(movedS0(),
					viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
					runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
					released(1300, 7, acquireS0(), wakeAt(2000)),
				)
				if tc.start {
					steps = append(steps, acquired(1400, tc.term, startedS0(tc.term, 10000), wakeAt(2000)))
					runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, owned(1400, tc.term))})
					return
				}
				releasing := noRunS0(1400, tc.term)
				releasing.Wanted = true
				steps = append(steps,
					acquired(1400, tc.term, releaseS0(tc.term), wakeAt(2000)),
					released(1500, tc.term, acquireS0(), wakeAt(2000)),
				)
				runSteps(t, scenario(steps...), stepChecks{
					len(steps) - 1: shardIs(0, releasing),
					len(steps): shardIs(0, core.ShardStatus{
						State: core.ShardStateAcquiring, Since: mc.Ms(1500), Holder: "h1", Wanted: true,
					}),
				})
			})
		}
	})
}

func TestATMCH29StuckRun(t *testing.T) {
	c := mc.Get("AT-MCH-29")
	for i, s := range c.Steps[:len(c.Steps)-1] {
		for _, a := range s.Want {
			if _, ok := a.(core.Release); ok {
				t.Errorf("step %d, before the RunStopped, has %#v", i+1, a)
			}
		}
	}
	running := stoppingS0(1000, 7)
	stuck := running
	stuck.Stuck = true
	runCase(t, "AT-MCH-29", stepChecks{
		5:  shardIs(0, running),
		15: shardIs(0, running),
		16: all(shardIs(0, running), sessionIs(10999, core.SessionStateLive, 20000, core.CauseNone)),
		17: all(shardIs(0, stuck), sessionIs(11000, core.SessionStateLive, 20000, core.CauseNone)),
		18: shardIs(0, releasingS0(11500, 7)),
	})
}

func TestATMCH41EachViewMakesANewAssignment(t *testing.T) {
	c := mc.Get("AT-MCH-41")
	want := [][]core.Action{
		{core.KeepAlive{}, wakeAt(2000)},
		{wakeAt(2000)},
		{wakeAt(2000)},
	}
	if len(c.Steps) != len(want) {
		t.Fatalf("AT-MCH-41 has %d slices, want %d", len(c.Steps), len(want))
	}
	for i, s := range c.Steps {
		if !reflect.DeepEqual(s.Want, want[i]) {
			t.Errorf("slice %d = %#v, want %#v", i+1, s.Want, want[i])
		}
	}

	first := []string{"m1", "m1", "m1", "m1"}
	var kept []string
	runCase(t, "AT-MCH-41", stepChecks{
		1: assignmentIs("s", "", "", "", ""),
		2: func(t *testing.T, m *core.Machine) {
			t.Helper()
			kept = m.Assignment("s")
			assignmentIs("s", first...)(t, m)
		},
		3: func(t *testing.T, m *core.Machine) {
			t.Helper()
			assignmentIs("s", "m1", "m2", "m2", "m1")(t, m)
			if !slices.Equal(kept, first) {
				t.Errorf("the earlier slice changed to %q, want %q", kept, first)
			}
		},
	})
}

func TestATMCH42PanicLeavesTheMachineAlone(t *testing.T) {
	m := runSteps(t, mc.Get("AT-MCH-42"), nil)
	status := m.Status()
	at, armed := m.NextWake()
	assignment := slices.Clone(m.Assignment("s"))

	const text = "core: Step: nil Input"
	if err := panicError(t, func() { m.Step(nil) }); err.Error() != text {
		t.Fatalf("Step(nil) panicked with %q, want %q", err, text)
	}

	if got := m.Status(); !reflect.DeepEqual(got, status) {
		t.Errorf("Status() after the panic = %+v, want %+v", got, status)
	}
	if gotAt, gotArmed := m.NextWake(); gotAt != at || gotArmed != armed {
		t.Errorf("NextWake() after the panic = (%v, %v), want (%v, %v)", gotAt, gotArmed, at, armed)
	}
	if got := m.Assignment("s"); !slices.Equal(got, assignment) {
		t.Errorf("Assignment after the panic = %q, want %q", got, assignment)
	}
}

func TestATMCH43ExactStatus(t *testing.T) {
	m := runSteps(t, mc.Get("AT-MCH-43"), nil)
	want := core.Status{
		Now:         mc.Ms(500),
		Session:     core.SessionStateLive,
		Deadline:    mc.Ms(10000),
		LastFailure: core.CauseUnreachable,
		Slot:        -1,
		ViewVersion: 7,
		Spaces: []core.SpaceStatus{
			{
				Name:   "audit",
				Fenced: true,
				Shards: []core.ShardStatus{
					{State: core.ShardStateOwned, Since: mc.Ms(400), Term: 5, Holder: "h1", Wanted: true},
					{},
				},
			},
			{
				Name: "orders",
				Shards: []core.ShardStatus{
					{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true},
					{State: core.ShardStateOwned, Since: mc.Ms(300), Term: 12, Holder: "h1", Wanted: true},
					{},
					{},
				},
			},
		},
	}
	if got := m.Status(); !reflect.DeepEqual(got, want) {
		t.Errorf("Status() = %+v, want %+v", got, want)
	}
}

// TestViewAssignments checks what a view does to the assignment, the Wanted flags and the shards on
// a live session.
func TestViewAssignments(t *testing.T) {
	live := []mc.Step{keepAlive(0, 2000), renewed(100, 0, extended(10000), wakeAt(2000))}
	wall := mc.Wall(mc.Ms(200))
	unwanted := shardIs(0, core.ShardStatus{})

	tests := []struct {
		name  string
		view  core.View
		want  []core.Action
		check func(*testing.T, *core.Machine)
	}{
		{
			name:  "a view without members gives no shard a key",
			view:  core.View{Version: 1},
			want:  []core.Action{wakeAt(2000)},
			check: all(viewVersionIs(1), assignmentIs("s", ""), unwanted),
		},
		{
			name:  "a view whose members are all draining gives no shard a key",
			view:  core.View{Version: 1, Members: []core.Member{{ID: "m1", Slot: -1, Draining: true}, {ID: "m2", Slot: -1, Draining: true}}},
			want:  []core.Action{wakeAt(2000)},
			check: all(viewVersionIs(1), assignmentIs("s", ""), unwanted),
		},
		{
			name:  "a view that leaves this member out gives it nothing",
			view:  mc.Vm(1, "m2", "m3"),
			want:  []core.Action{wakeAt(2000)},
			check: all(viewVersionIs(1), assignmentIs("s", "m3"), unwanted),
		},
		{
			name:  "this member can be in the view and get nothing",
			view:  mc.Vm(1, "m1", "m2"),
			want:  []core.Action{wakeAt(2000)},
			check: all(viewVersionIs(1), assignmentIs("s", "m2"), unwanted),
		},
		{
			name:  "the placement counts from Wall: a member before its JoinAt gets nothing",
			view:  core.View{Version: 1, Members: []core.Member{{ID: "m1", Slot: -1, JoinAt: wall.Add(time.Second)}}},
			want:  []core.Action{wakeAt(2000)},
			check: all(assignmentIs("s", ""), unwanted),
		},
		{
			name:  "the placement counts from Wall: a member at its LeaveAt gets nothing",
			view:  core.View{Version: 1, Members: []core.Member{{ID: "m1", Slot: -1, LeaveAt: wall}}},
			want:  []core.Action{wakeAt(2000)},
			check: all(assignmentIs("s", ""), unwanted),
		},
		{
			name: "the placement counts from Wall: a member inside its window gets the shard",
			view: core.View{Version: 1, Members: []core.Member{{ID: "m1", Slot: -1, JoinAt: wall, LeaveAt: wall.Add(time.Second)}}},
			want: []core.Action{acquireS0(), wakeAt(2000)},
			check: all(
				assignmentIs("s", "m1"),
				shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}),
			),
		},
		{
			name:  "a view with slots gives keys of slots and wants nothing",
			view:  mc.Vs(1, []string{"m1/1"}, "m1"),
			want:  []core.Action{wakeAt(2000)},
			check: all(viewVersionIs(1), assignmentIs("s", "slot-0"), unwanted),
		},
		{
			name: "a view that says this member holds a slot still wants nothing",
			view: core.View{
				Version:     1,
				Members:     []core.Member{{ID: "m1", Slot: 0}},
				Slots:       1,
				SlotHolders: []string{"m1/1"},
			},
			want:  []core.Action{wakeAt(2000)},
			check: all(assignmentIs("s", "slot-0"), unwanted),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			steps := append(slices.Clone(live), viewed(200, tc.view, tc.want...))
			runSteps(t, scenario(steps...), stepChecks{len(steps): tc.check})
		})
	}

	t.Run("vacant slots get keys too", func(t *testing.T) {
		runSteps(t, scenarioIn(mc.C(mc.S("s", 2)),
			keepAlive(0, 2000),
			viewed(100, mc.Vs(1, []string{"m1/1", ""}, "m1"), wakeAt(2000)),
		), stepChecks{2: assignmentIs("s", "slot-1", "slot-0")})
	})

	t.Run("Wanted follows the assignment", func(t *testing.T) {
		wanted := core.ShardStatus{Wanted: true}
		runSteps(t, scenario(
			keepAlive(0, 2000),
			viewed(100, mc.Vm(1, "m1"), wakeAt(2000)),
			viewed(200, mc.Vm(2, "m2"), wakeAt(2000)),
			viewed(300, mc.Vm(3, "m1"), wakeAt(2000)),
		), stepChecks{
			2: shardIs(0, wanted),
			3: shardIs(0, core.ShardStatus{}),
			4: shardIs(0, wanted),
		})
	})

	t.Run("the first view may have version 0", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(0, 2000),
			viewed(100, mc.Vm(0, "m1"), wakeAt(2000)),
		), stepChecks{2: all(viewVersionIs(0), assignmentIs("s", "m1"))})
	})

	t.Run("a view with an older reading runs at the machine's reading", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(5000, 7000),
			viewed(1000, mc.Vm(1, "m1"), wakeAt(7000)),
		), stepChecks{2: all(sessionIs(5000, core.SessionStateOpening, 0, core.CauseNone), assignmentIs("s", "m1"))})
	})
}

func TestViewChangedInvalidViews(t *testing.T) {
	tests := []struct {
		name string
		view core.View
	}{
		{"unsorted members", core.View{Version: 2, Members: []core.Member{{ID: "m2", Slot: -1}, {ID: "m1", Slot: -1}}}},
		{"duplicate members", core.View{Version: 2, Members: []core.Member{{ID: "m1", Slot: -1}, {ID: "m1", Slot: -1}}}},
		{"an invalid member", core.View{Version: 2, Members: []core.Member{{ID: "", Slot: -1}}}},
		{"negative slots", core.View{Version: 2, Slots: -1}},
		{"too few slot holders", core.View{Version: 2, Slots: 2, SlotHolders: []string{"m1/1"}}},
		{"a member in a vacant slot", core.View{Version: 2, Members: []core.Member{{ID: "m1", Slot: 0}}, Slots: 1, SlotHolders: []string{""}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.view.Validate() == nil {
				t.Fatal("the view is valid")
			}
			// An invalid view must not reach PlacementKeys or Balanced, which panic on some, and
			// must change nothing.
			runSteps(t, scenario(
				keepAlive(0, 2000),
				viewed(100, mc.Vm(1, "m2"), wakeAt(2000)),
				viewed(200, tc.view, wakeAt(2000)),
			), stepChecks{3: all(viewVersionIs(1), assignmentIs("s", "m2"))})
		})
	}
}

// TestAssignmentRecompute checks that only an applied view makes a new assignment. A slice's
// identity shows this: a recompute stores a new one even when the keys are the same.
func TestAssignmentRecompute(t *testing.T) {
	base := []mc.Step{
		keepAlive(0, 2000),
		renewed(100, 0, extended(10000), wakeAt(2000)),
		viewed(200, mc.Vm(5, "m2"), wakeAt(2000)),
	}
	tests := []struct {
		name      string
		step      mc.Step
		recompute bool
	}{
		{name: "a plain tick", step: tick(300, wakeAt(2000))},
		{name: "a tick with a KeepAlive", step: keepAlive(2000, 4000)},
		{name: "a tick when the session is late", step: keepAlive(10000, 12000)},
		{name: "a renewal", step: renewed(300, 200, extended(10200), wakeAt(2000))},
		{name: "a failed renewal", step: failed(300, core.CauseUnreachable, wakeAt(2000))},
		{name: "an answer", step: acquired(300, 7, wakeAt(2000))},
		{name: "a drain", step: mc.Step{Input: core.Drain{Now: mc.Ms(300)}, Want: []core.Action{wakeAt(2000)}}},
		{name: "an older view", step: viewed(300, mc.Vm(4, "m2"), wakeAt(2000))},
		{name: "an invalid view", step: viewed(300, core.View{Version: 6, Slots: -1}, wakeAt(2000))},
		{name: "a view of the same version", step: viewed(300, mc.Vm(5, "m2"), wakeAt(2000)), recompute: true},
		{name: "a view of a newer version", step: viewed(300, mc.Vm(6, "m2"), wakeAt(2000)), recompute: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var before []string
			runSteps(t, scenario(append(slices.Clone(base), tc.step)...), stepChecks{
				3: func(_ *testing.T, m *core.Machine) { before = m.Assignment("s") },
				4: func(t *testing.T, m *core.Machine) {
					t.Helper()
					after := m.Assignment("s")
					if same := &after[0] == &before[0]; same == tc.recompute {
						t.Errorf("stored the same slice = %v, want %v", same, !tc.recompute)
					}
					if !slices.Equal(after, []string{"m2"}) {
						t.Errorf("Assignment = %q, want [m2]", after)
					}
				},
			})
		})
	}
}

// TestGrantThatCannotStart checks a grant that fails the start conditions: it is released with no
// StartRun, its Release repeats until answered, and the shard can acquire again only after the answer.
func TestGrantThatCannotStart(t *testing.T) {
	const holder = "h1"
	t.Run("the session is late, and the shard acquires again after the answer", func(t *testing.T) {
		waiting := noRunS0(10100, 7)
		waiting.Wanted = true
		runSteps(t, scenario(
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
			keepAlive(10000, 12000),
			acquired(10100, 7, releaseS0(7), wakeAt(12000)),
			keepAlive(12000, 12100),
			renewed(12050, 12000, extended(22000), wakeAt(12100)),
			tick(12100, releaseS0(7), wakeAt(14000)),
			released(12150, 7, acquireS0(), wakeAt(14000)),
			acquired(12200, 8, startedS0(8, 22000), wakeAt(14000)),
		), stepChecks{
			4:  shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}),
			5:  shardIs(0, waiting),
			7:  shardIs(0, waiting),
			8:  shardIs(0, waiting),
			9:  shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(12150), Holder: holder, Wanted: true}),
			10: shardIs(0, ownedS0(12200, 8)),
		})
	})

	t.Run("the shard is no longer wanted", func(t *testing.T) {
		wanted := noRunS0(400, 7)
		wanted.Wanted = true
		runSteps(t, scenario(
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
			viewed(300, mc.Vm(2, "m2"), wakeAt(2000)),
			acquired(400, 7, releaseS0(7), wakeAt(2000)),
			viewed(500, mc.Vm(3, "m1"), wakeAt(2000)),
			released(600, 7, acquireS0(), wakeAt(2000)),
		), stepChecks{
			4: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200)}),
			5: shardIs(0, noRunS0(400, 7)),
			6: shardIs(0, wanted),
			7: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(600), Holder: holder, Wanted: true}),
		})
	})

	t.Run("the member is draining", func(t *testing.T) {
		wanted := noRunS0(300, 7)
		wanted.Wanted = true
		runSteps(t, scenario(append(acquiringS0(), acquired(300, 7, releaseS0(7), wakeAt(2000)))...), stepChecks{
			3: func(_ *testing.T, m *core.Machine) { core.ForceDraining(m) },
			4: shardIs(0, wanted),
		})
	})

	t.Run("a record naming this member before the session is live", func(t *testing.T) {
		wanted := noRunS0(200, 9)
		wanted.Wanted = true
		runSteps(t, scenario(
			keepAlive(0, 2000),
			viewed(100, mc.Vm(1, "m1"), wakeAt(2000)),
			holderChanged(200, holder, 9, releaseS0(9), wakeAt(2000)),
		), stepChecks{3: shardIs(0, wanted)})
	})

	t.Run("a term released with no run can still start later", func(t *testing.T) {
		steps := append(bootedOnly(),
			holderChanged(300, holder, 9, releaseS0(9), wakeAt(2000)),
			released(400, 9, wakeAt(2000)),
			viewed(500, mc.Vm(2, "m1"), acquireS0(), wakeAt(2000)),
			acquired(600, 9, startedS0(9, 10000), wakeAt(2000)),
		)
		runSteps(t, scenario(steps...), stepChecks{7: shardIs(0, ownedS0(600, 9))})
	})

	t.Run("the session is lost", func(t *testing.T) {
		m := runSteps(t, scenario(
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
			failed(300, core.CauseLost),
			acquired(400, 7),
		), nil)
		if at, ok := m.NextWake(); at != 0 || ok {
			t.Errorf("NextWake() = (%v, %v), want (0, false)", at, ok)
		}
	})
}

// TestAnswersThatStayNoOps checks the inputs that say nothing to an acquiring shard: a record naming
// this member with no term, and an answer for a shard the machine does not have. The shard goes on
// acquiring, and the rest of the step still runs.
func TestAnswersThatStayNoOps(t *testing.T) {
	inputs := []core.Input{
		core.HolderChanged{Now: mc.Ms(2000), Space: "s", Holder: "h1", Term: 0},
		core.HolderChanged{Now: mc.Ms(2000), Space: "s", Shard: 1, Holder: "h1", Term: 7},
		core.Acquired{Now: mc.Ms(2000), Space: "s", Shard: 1, Term: 7},
		core.AcquireFailed{Now: mc.Ms(2000), Space: "other", Holder: "h2", RetryAfter: time.Second},
	}
	for _, in := range inputs {
		t.Run(fmt.Sprintf("%T%+v", in, in), func(t *testing.T) {
			runSteps(t, scenario(
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
				mc.Step{Input: in, Want: []core.Action{core.KeepAlive{}, wakeAt(4000)}},
			), stepChecks{
				4: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}),
			})
		})
	}
}

// TestRetryAtWake checks wake item 3: the timer is armed only for a shard that can acquire once it
// passes, and an earlier timer wins.
func TestRetryAtWake(t *testing.T) {
	dropped := []mc.Step{
		keepAlive(0, 2000),
		renewed(100, 0, extended(10000), wakeAt(2000)),
		viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
		viewed(300, mc.Vm(2, "m2"), wakeAt(2000)),
		acquired(400, 0, wakeAt(2000)), // RetryAt is @2.4.
	}
	tests := []struct {
		name  string
		steps []mc.Step
	}{
		{
			name:  "not wanted: no timer",
			steps: []mc.Step{keepAlive(2000, 4000), tick(2400, wakeAt(4000))},
		},
		{
			name:  "wanted with the KeepAlive first: the KeepAlive's timer, then RetryAt",
			steps: []mc.Step{viewed(500, mc.Vm(3, "m1"), wakeAt(2000)), keepAlive(2000, 2400)},
		},
		{
			name: "wanted with RetryAt first: RetryAt",
			steps: []mc.Step{
				keepAlive(2000, 4000),
				viewed(2100, mc.Vm(3, "m1"), wakeAt(2400)),
			},
		},
		{
			name: "wanted with RetryAt passed: Acquire at once",
			steps: []mc.Step{
				keepAlive(2000, 4000),
				tick(3000, wakeAt(4000)),
				viewed(3100, mc.Vm(3, "m1"), acquireS0(), wakeAt(4000)),
			},
		},
		{
			name: "RetryAt reached exactly: Acquire",
			steps: []mc.Step{
				viewed(500, mc.Vm(3, "m1"), wakeAt(2000)),
				tick(2400, core.KeepAlive{}, acquireS0(), wakeAt(4400)),
			},
		},
		{
			name: "not armed while the session is late",
			steps: []mc.Step{
				keepAlive(2000, 4000),
				viewed(2100, mc.Vm(3, "m1"), wakeAt(2400)),
				failed(2200, core.CauseUnreachable, wakeAt(2400)),
				keepAlive(10000, 12000),
			},
		},
		{
			name: "the session lost: no timer",
			steps: []mc.Step{
				viewed(500, mc.Vm(3, "m1"), wakeAt(2000)),
				failed(600, core.CauseLost),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSteps(t, scenario(slices.Concat(dropped, tc.steps)...), nil)
		})
	}
}

// adversaries returns inputs at the reading now that must not end the run of term 7 on shard s/0,
// nor release it: other terms, the answer to a Release nobody sent, other shards, grants and failures
// nobody is waiting for, records of the run's own term or older, and a pointer, which is not an input
// of the machine at all.
func adversaries(now time.Duration) []core.Input {
	return []core.Input{
		core.Released{Now: now, Space: "s", Term: 7},
		core.Released{Now: now, Space: "s", Term: 6},
		core.Released{Now: now, Space: "s", Term: 8},
		core.Released{Now: now, Space: "s", Term: 0},
		core.RunStopped{Now: now, Space: "s", Term: 6},
		core.RunStopped{Now: now, Space: "s", Term: 8},
		core.RunStopped{Now: now, Space: "s", Term: 0},
		core.RunStopped{Now: now, Space: "s", Shard: 1, Term: 7},
		core.RunStopped{Now: now, Space: "s", Shard: -1, Term: 7},
		core.RunStopped{Now: now, Space: "other", Term: 7},
		&core.RunStopped{Now: now, Space: "s", Term: 7},
		core.Acquired{Now: now, Space: "s", Term: 7},
		core.Acquired{Now: now, Space: "s", Term: 8},
		core.Acquired{Now: now, Space: "s", Term: 0},
		core.AcquireFailed{Now: now, Space: "s", Holder: "h2", RetryAfter: time.Second},
		core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 7},
		core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 6},
		core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 0},
		core.HolderChanged{Now: now, Space: "s", Holder: "h2", Term: 6},
		core.HolderChanged{Now: now, Space: "s", Holder: "", Term: 6},
	}
}

// recordsOfAnotherRun returns the records at the reading now that end the claim of the run of term 7
// on shard s/0: another holder or none, and a larger term naming this member. Each tells the run to
// stop if it has not been told, and none may lead to a Release.
func recordsOfAnotherRun(now time.Duration) []core.Input {
	return []core.Input{
		core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 8},
		core.HolderChanged{Now: now, Space: "s", Holder: "h2", Term: 7},
		core.HolderChanged{Now: now, Space: "s", Holder: "h2", Term: 8},
		core.HolderChanged{Now: now, Space: "s", Holder: "h2", Term: 0},
		core.HolderChanged{Now: now, Space: "s", Holder: "", Term: 0},
		core.HolderChanged{Now: now, Space: "s", Holder: "", Term: 8},
	}
}

// stepWithoutRelease steps m and fails the test if the result has a Release.
func stepWithoutRelease(t *testing.T, m *core.Machine, in core.Input) {
	t.Helper()
	got := m.Step(in)
	for _, a := range got {
		if _, ok := a.(core.Release); ok {
			t.Errorf("Step(%#v) = %#v, with a Release of a run that is still running", in, got)
		}
	}
}

// TestRunStillRunningIsNeverReleased checks the safety promise: whatever arrives, a run that has not
// reported its own stop after its StopRun is not released, for as long as the clock runs, and its
// RunStopped still works afterwards.
func TestRunStillRunningIsNeverReleased(t *testing.T) {
	const after = 90 * time.Second

	// Each start leaves the shard with a live run of term 7, told to stop or not.
	starts := []struct {
		name  string
		steps []mc.Step
		at    time.Duration
		want  core.ShardStatus
	}{
		{
			name:  "owned",
			steps: mc.Boot(7, mc.S("s", 1)),
			at:    mc.Ms(1000),
			want:  ownedS0(300, 7),
		},
		{
			name:  "stopping",
			steps: movedS0(),
			at:    mc.Ms(1100),
			want:  stoppingS0(1000, 7),
		},
		{
			name:  "stuck",
			steps: append(movedS0(), keepAlive(11000, 13000)),
			at:    mc.Ms(11100),
			want: func() core.ShardStatus {
				s := stoppingS0(1000, 7)
				s.Stuck = true
				return s
			}(),
		},
	}
	for _, start := range starts {
		inputs := adversaries(start.at)
		if start.name == "owned" {
			// An owned shard's run was never told to stop, so even its own term does not count.
			inputs = append(inputs, core.RunStopped{Now: start.at, Space: "s", Term: 7})
		}
		for _, in := range inputs {
			t.Run(fmt.Sprintf("%s/%T%+v", start.name, in, in), func(t *testing.T) {
				// The second shard makes Shard: 1 a shard the machine has, with no run to report on.
				m := core.NewMachine(mc.C(mc.S("s", 2)))
				for _, s := range start.steps {
					m.Step(s.Input)
				}
				stepWithoutRelease(t, m, in)
				if got := m.Status().Spaces[0].Shards[0]; got != start.want {
					t.Fatalf("shard 0 after the input = %+v, want %+v", got, start.want)
				}
				if start.want.State == core.ShardStateOwned {
					m.Step(core.ViewChanged{Now: start.at, View: mc.Vm(9, "m2", "m3"), Wall: mc.Wall(start.at)})
				}

				now := start.at
				for now < after {
					now += time.Second
					stepWithoutRelease(t, m, core.Tick{Now: now})
					stepWithoutRelease(t, m, core.Released{Now: now, Space: "s", Term: 7})
				}
				sh := m.Status().Spaces[0].Shards[0]
				if sh.State != core.ShardStateStopping || sh.Term != 7 || !sh.Stuck {
					t.Fatalf("shard 0 after the wait = %+v, want it stopping in term 7 and stuck", sh)
				}

				got := m.Step(core.RunStopped{Now: now, Space: "s", Term: 7})
				if !slices.Contains(got, releaseS0(7)) {
					t.Errorf("the RunStopped returned %#v, want a Release of term 7", got)
				}
			})
		}
	}

	// A record that ends the run's claim, a larger term of this member's included, sends no Release of
	// either term: not while the run goes on, however long, and not at its RunStopped, which is still
	// the old term's.
	for _, start := range starts {
		for _, in := range recordsOfAnotherRun(start.at) {
			t.Run(fmt.Sprintf("the claim ended/%s/%+v", start.name, in), func(t *testing.T) {
				m := core.NewMachine(mc.C(mc.S("s", 2)))
				for _, s := range start.steps {
					m.Step(s.Input)
				}
				stepWithoutRelease(t, m, in)

				now := start.at
				for now < after {
					now += time.Second
					stepWithoutRelease(t, m, core.Tick{Now: now})
					stepWithoutRelease(t, m, core.Released{Now: now, Space: "s", Term: 7})
					stepWithoutRelease(t, m, core.Released{Now: now, Space: "s", Term: 8})
					stepWithoutRelease(t, m, core.RunStopped{Now: now, Space: "s", Term: 8})
				}
				sh := m.Status().Spaces[0].Shards[0]
				toldToStop := sh.State == core.ShardStateLost || sh.State == core.ShardStateStopping
				if !toldToStop || sh.Term != 7 || !sh.Stuck {
					t.Fatalf("shard 0 after the wait = %+v, want it told to stop in term 7 and stuck", sh)
				}

				stepWithoutRelease(t, m, core.RunStopped{Now: now, Space: "s", Term: 7})
				sh = m.Status().Spaces[0].Shards[0]
				if sh.State != core.ShardStateIdle || sh.Term != 0 || sh.Stuck || sh.Reason != core.StopNone {
					t.Fatalf("shard 0 after the RunStopped = %+v, want it idle", sh)
				}
				stepWithoutRelease(t, m, core.Tick{Now: now + time.Minute})
			})
		}
	}

	t.Run("a repeated RunStopped sends no extra Release and moves no timer", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			runStopped(1500, 7, releaseS0(7), wakeAt(2000)),
			runStopped(1600, 7, wakeAt(2000)),
			keepAlive(2000, 3500),
			runStopped(3000, 7, wakeAt(3500)),
			tick(3500, releaseS0(7), wakeAt(4000)),
		)...), nil)
	})

	t.Run("the RunStopped of an old run does not release a new one", func(t *testing.T) {
		running := stoppingS0(1600, 8)
		runSteps(t, scenario(append(movedS0(),
			viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
			runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
			released(1300, 7, acquireS0(), wakeAt(2000)),
			acquired(1400, 8, startedS0(8, 10000), wakeAt(2000)),
			runStopped(1450, 7, wakeAt(2000)),
			released(1460, 7, wakeAt(2000)),
			viewed(1600, mc.Vm(4, "m2"), stopS0(8), wakeAt(2000)),
			runStopped(1700, 7, wakeAt(2000)),
			released(1800, 7, wakeAt(2000)),
			released(1900, 8, wakeAt(2000)),
			runStopped(1950, 8, releaseS0(8), wakeAt(2000)),
		)...), stepChecks{
			10: shardIs(0, ownedS0(1400, 8)),
			11: shardIs(0, ownedS0(1400, 8)),
			15: shardIs(0, running),
			16: shardIs(0, releasingS0(1950, 8)),
		})
	})

	t.Run("a shard with no run takes no report", func(t *testing.T) {
		acquiring := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}
		runSteps(t, scenario(
			keepAlive(0, 2000),
			runStopped(10, 7, wakeAt(2000)),
			released(20, 7, wakeAt(2000)),
			runStopped(30, 0, wakeAt(2000)),
			released(40, 0, wakeAt(2000)),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
			runStopped(300, 7, wakeAt(2000)),
			released(400, 7, wakeAt(2000)),
			runStopped(500, 0, wakeAt(2000)),
			released(600, 0, wakeAt(2000)),
		), stepChecks{
			5:  shardIs(0, core.ShardStatus{}),
			11: shardIs(0, acquiring),
		})
	})

	t.Run("the report for one shard does not move another", func(t *testing.T) {
		// Both shards run term 7. Only the named shard moves.
		steps := []mc.Step{
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), core.Acquire{Space: "s"}, core.Acquire{Space: "s", Shard: 1}, wakeAt(2000)),
			mc.Grant(mc.Ms(300), "s", 0, 7, mc.Ms(10000), mc.Ms(2000)),
			mc.Grant(mc.Ms(400), "s", 1, 7, mc.Ms(10000), mc.Ms(2000)),
			viewed(1000, mc.Vm(2, "m2", "m3"),
				core.StopRun{Space: "s", Term: 7, Reason: core.StopMoved},
				core.StopRun{Space: "s", Shard: 1, Term: 7, Reason: core.StopMoved},
				wakeAt(2000),
			),
			{
				Input: core.RunStopped{Now: mc.Ms(1100), Space: "s", Shard: 1, Term: 7},
				Want:  []core.Action{core.Release{Space: "s", Shard: 1, Term: 7}, wakeAt(2000)},
			},
			{Input: core.Released{Now: mc.Ms(1200), Space: "s", Term: 7}, Want: []core.Action{wakeAt(2000)}},
		}
		runSteps(t, scenarioIn(mc.C(mc.S("s", 2)), steps...), stepChecks{
			7: all(shardIs(0, stoppingS0(1000, 7)), shardIs(1, releasingS0(1100, 7))),
			8: all(shardIs(0, stoppingS0(1000, 7)), shardIs(1, releasingS0(1100, 7))),
		})
	})

	t.Run("a lost session sends nothing", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			steps []mc.Step
		}{
			{name: "owned", steps: mc.Boot(7, mc.S("s", 1))},
			{name: "stopping", steps: movedS0()},
			{name: "releasing", steps: append(movedS0(), runStopped(1500, 7, releaseS0(7), wakeAt(2000)))},
		} {
			t.Run(tc.name, func(t *testing.T) {
				steps := append(slices.Clone(tc.steps),
					failed(1600, core.CauseLost),
					viewed(1700, mc.Vm(5, "m3")),
					runStopped(1800, 7),
					released(1900, 7),
					tick(60000),
				)
				m := runSteps(t, scenario(steps...), nil)
				if at, ok := m.NextWake(); at != 0 || ok {
					t.Errorf("NextWake() = (%v, %v), want (0, false)", at, ok)
				}
			})
		}
	})
}

// TestNoReleaseBeforeRunStoppedOnAnyWalk feeds walks of arbitrary inputs to a machine and keeps a
// record of what the driver has been told and has reported. It checks that no Release of a shard goes
// out while that shard has a run whose RunStopped has not arrived after its StopRun, whatever term the
// Release names, and that a term starts and stops once. A term that never started may be released
// without a RunStopped, as may a started term whose run has returned, but only while the shard has no
// run at all: that is the whole of the exception for a grant's no-run Release. The walks are the same
// on every run.
func TestNoReleaseBeforeRunStoppedOnAnyWalk(t *testing.T) {
	type run struct {
		shard int
		term  core.Term
	}
	views := [][]string{{"m1"}, {"m1", "m2"}, {"m1", "m4"}, {"m2", "m3"}}
	holders := []string{"h1", "h1", "h2", ""}

	var starts, stops, releases, repeats, restarts, stuck int
	var noRun, oldTerm, adopted, lostRuns, goneIdle int
	for seed := range uint64(300) {
		state := seed
		next := func(n uint64) uint64 {
			state = state*6364136223846793005 + 1442695040888963407
			return (state >> 33) % n
		}

		m := core.NewMachine(mc.C(mc.S("s", 2)))
		started, stopSent, stopped := map[run]bool{}, map[run]bool{}, map[run]bool{}
		runsOf := [2]int{}

		// running holds each shard's run the driver has not reported over, or 0 for none. A run is
		// over only by a RunStopped of its term after its StopRun.
		running := [2]core.Term{}
		var history []core.Input
		var now time.Duration
		version := uint64(0)
		term := core.Term(1)

		step := func(in core.Input) {
			history = append(history, in)
			if v, ok := in.(core.RunStopped); ok && v.Shard >= 0 && v.Shard < 2 && stopSent[run{v.Shard, v.Term}] {
				stopped[run{v.Shard, v.Term}] = true
				if running[v.Shard] == v.Term {
					running[v.Shard] = 0
				}
			}
			before := m.Status().Spaces[0].Shards
			got := m.Step(in)
			at, armed := m.NextWake()
			st := m.Status()
			for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
				t.Fatalf("seed %d: %s, after %+v", seed, v, history)
			}
			for _, a := range got {
				switch v := a.(type) {
				case core.StartRun:
					starts++
					runsOf[v.Shard]++
					if runsOf[v.Shard] > 1 {
						restarts++
					}
					k := run{v.Shard, v.Term}
					if started[k] {
						t.Fatalf("seed %d: a second %+v after %+v", seed, v, history)
					}
					if running[v.Shard] != 0 {
						t.Fatalf("seed %d: %+v while term %d runs, after %+v", seed, v, running[v.Shard], history)
					}
					started[k] = true
					running[v.Shard] = v.Term
					if _, ok := in.(core.HolderChanged); ok {
						adopted++
					}
				case core.StopRun:
					stops++
					k := run{v.Shard, v.Term}
					if !started[k] || stopSent[k] {
						t.Fatalf("seed %d: %+v of a run not running, after %+v", seed, v, history)
					}
					stopSent[k] = true
					if v.Reason == core.StopLost {
						lostRuns++
					}
				case core.Release:
					releases++
					if running[v.Shard] != 0 {
						t.Fatalf("seed %d: %+v while term %d runs, before its RunStopped, after %+v",
							seed, v, running[v.Shard], history)
					}
					k := run{v.Shard, v.Term}
					if !started[k] {
						noRun++
					} else if _, ok := in.(core.RunStopped); !ok && before[v.Shard].State != core.ShardStateReleasing {
						oldTerm++
					}
					if b := before[v.Shard]; b.State == core.ShardStateReleasing && b.Term == v.Term {
						repeats++
					}
				}
			}
			for shard, sh := range m.Status().Spaces[0].Shards {
				k := run{shard, sh.Term}
				switch sh.State {
				case core.ShardStateOwned:
					if !started[k] || stopSent[k] || running[shard] != sh.Term {
						t.Fatalf("seed %d: shard %d is %+v, after %+v", seed, shard, sh, history)
					}
				case core.ShardStateStopping, core.ShardStateLost:
					if !stopSent[k] || stopped[k] || running[shard] != sh.Term {
						t.Fatalf("seed %d: shard %d is %+v, after %+v", seed, shard, sh, history)
					}
				case core.ShardStateReleasing, core.ShardStateIdle, core.ShardStateAcquiring:
					if running[shard] != 0 {
						t.Fatalf("seed %d: shard %d is %+v while term %d runs, after %+v", seed, shard, sh, running[shard], history)
					}
				}
				if sh.Stuck {
					stuck++
					if sh.State != core.ShardStateStopping && sh.State != core.ShardStateLost {
						t.Fatalf("seed %d: shard %d is %+v, after %+v", seed, shard, sh, history)
					}
				}
				if b := before[shard]; sh.State == core.ShardStateIdle && (b.State == core.ShardStateLost || b.State == core.ShardStateStopping) {
					goneIdle++
				}
			}
		}

		step(core.Tick{})
		step(core.SessionRenewed{SentAt: 0})
		for range 200 {
			shard := 0
			if next(2) == 0 {
				shard = 1
			}
			// The shard's own term half the time, so that reports often match, and otherwise any
			// term near the ones in use.
			answer := m.Status().Spaces[0].Shards[shard].Term
			if next(2) == 0 {
				answer = core.Term(next(uint64(term) + 2))
			}
			switch next(14) {
			case 0:
				now += 100 * time.Millisecond
				step(core.Tick{Now: now})
			case 1:
				now += 2 * time.Second
				step(core.Tick{Now: now})
			case 2:
				if next(8) == 0 {
					now += 11 * time.Second
					step(core.Tick{Now: now})
				}
			case 3:
				now += time.Millisecond
				step(core.SessionRenewed{Now: now, SentAt: now})
			case 4:
				version++
				now += time.Millisecond
				step(core.ViewChanged{Now: now, View: mc.Vm(version, views[next(4)]...), Wall: mc.Wall(now)})
			case 5, 6:
				now += time.Millisecond
				step(core.RunStopped{Now: now, Space: "s", Shard: shard, Term: answer})
			case 7:
				now += time.Millisecond
				step(core.Released{Now: now, Space: "s", Shard: shard, Term: answer})
			case 8:
				retryAfters := []time.Duration{math.MinInt64, -time.Second, 0, time.Second, time.Hour, math.MaxInt64}
				now += time.Millisecond
				step(core.AcquireFailed{
					Now: now, Space: "s", Shard: shard, Holder: holders[next(4)], RetryAfter: retryAfters[next(6)],
				})
			case 9, 10:
				// The shard's term, a term near it, a new term or none.
				record := answer
				switch next(4) {
				case 0:
					term++
					record = term
				case 1:
					record = answer + 1
				case 2:
					record = 0
				}
				now += time.Millisecond
				step(core.HolderChanged{Now: now, Space: "s", Shard: shard, Holder: holders[next(4)], Term: record})
			default:
				// Mostly a new term, sometimes one that may have run already.
				grant := answer
				if next(4) != 0 {
					term++
					grant = term
				}
				now += time.Millisecond
				step(core.Acquired{Now: now, Space: "s", Shard: shard, Term: grant})
			}
		}
	}

	// The walks are only worth something if they reach the paths the promise is about.
	for _, c := range []struct {
		name  string
		count int
	}{
		{"StartRun", starts},
		{"StopRun", stops},
		{"Release", releases},
		{"Release repeat", repeats},
		{"second run on a shard", restarts},
		{"stuck status", stuck},
		{"no-run Release of a term that never started", noRun},
		{"no-run Release of a term whose run has returned", oldTerm},
		{"grant adopted from a record", adopted},
		{"run stopped as lost", lostRuns},
		{"run that went to idle with no Release", goneIdle},
	} {
		if c.count == 0 {
			t.Errorf("no walk reached a %s", c.name)
		}
	}
}

// TestAcquireFailedRetryAt checks the wait after an AcquireFailed: RetryAfter, but at least RetryPeriod
// and at most LeaseDuration, whatever a driver that breaks its contract says.
func TestAcquireFailedRetryAt(t *testing.T) {
	tests := []struct {
		name       string
		retryAfter time.Duration
		wait       time.Duration
	}{
		{"the most negative", math.MinInt64, 2 * time.Second},
		{"negative", -time.Second, 2 * time.Second},
		{"none", 0, 2 * time.Second},
		{"below RetryPeriod", 2*time.Second - 1, 2 * time.Second},
		{"RetryPeriod", 2 * time.Second, 2 * time.Second},
		{"above RetryPeriod", 2*time.Second + 1, 2*time.Second + 1},
		{"between", 7 * time.Second, 7 * time.Second},
		{"LeaseDuration", 15 * time.Second, 15 * time.Second},
		{"above LeaseDuration", 15*time.Second + 1, 15 * time.Second},
		{"the largest", math.MaxInt64, 15 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			at := mc.Ms(300) + tc.wait
			runSteps(t, scenario(append(acquiringS0(),
				acquireFailed(300, tc.retryAfter, wakeAt(2000)),
			)...), stepChecks{
				4: shardIs(0, core.ShardStatus{Since: mc.Ms(300), Holder: "h2", Wanted: true, RetryAt: at}),
			})
		})
	}

	t.Run("the wait counts from the machine's clock", func(t *testing.T) {
		runSteps(t, scenario(append(acquiringS0(),
			keepAlive(2000, 4000),
			acquireFailed(300, 0, wakeAt(4000)),
		)...), stepChecks{
			5: shardIs(0, core.ShardStatus{Since: mc.Ms(2000), Holder: "h2", Wanted: true, RetryAt: mc.Ms(4000)}),
		})
	})

	t.Run("a shard no longer wanted keeps its RetryAt and arms no timer", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
			viewed(300, mc.Vm(2, "m2"), wakeAt(2000)),
			acquireFailed(400, time.Second, wakeAt(2000)),
			keepAlive(2000, 4000),
		), stepChecks{
			5: shardIs(0, core.ShardStatus{Since: mc.Ms(400), Holder: "h2", RetryAt: mc.Ms(2400)}),
		})
	})
}

// TestAnswersIgnoredWhenNotAcquiring checks that an answer changes nothing for a shard that is not
// waiting for one, whatever state it is in, and that the wake stays what it was.
func TestAnswersIgnoredWhenNotAcquiring(t *testing.T) {
	starts := []struct {
		name  string
		steps []mc.Step
		want  core.ShardStatus
	}{
		{name: "idle", steps: bootedOnly(), want: core.ShardStatus{}},
		{name: "owned", steps: mc.Boot(7, mc.S("s", 1)), want: ownedS0(300, 7)},
		{name: "stopping", steps: movedS0(), want: stoppingS0(1000, 7)},
		{name: "lost", steps: ownedAndLostS0(), want: lostS0(1000, "h2")},
		{
			name:  "releasing a run's term",
			steps: append(movedS0(), runStopped(1200, 7, releaseS0(7), wakeAt(2000))),
			want:  releasingS0(1200, 7),
		},
		{
			name:  "releasing with no run",
			steps: append(bootedOnly(), holderChanged(300, "h1", 9, releaseS0(9), wakeAt(2000))),
			want:  noRunS0(300, 9),
		},
	}
	const now = 1500
	answers := []core.Input{
		core.Acquired{Now: mc.Ms(now), Space: "s", Term: 0},
		core.Acquired{Now: mc.Ms(now), Space: "s", Term: 7},
		core.Acquired{Now: mc.Ms(now), Space: "s", Term: 20},
		core.AcquireFailed{Now: mc.Ms(now), Space: "s", Holder: "h3", RetryAfter: time.Minute},
	}
	for _, start := range starts {
		for _, in := range answers {
			t.Run(fmt.Sprintf("%s/%T%+v", start.name, in, in), func(t *testing.T) {
				steps := append(slices.Clone(start.steps), mc.Step{Input: in, Want: []core.Action{wakeAt(2000)}})
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, start.want)})
			})
		}
	}
}

// TestHolderChangedTable checks each state against each kind of record, after the rule that a record
// older than the shard's term is ignored. The shard is at @1.5 and every result ends with the
// KeepAlive's Wake at @2.
func TestHolderChangedTable(t *testing.T) {
	idle := core.ShardStatus{}
	acquiring := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}
	owned, stopping := ownedS0(300, 7), stoppingS0(1000, 7)
	lost := lostS0(1000, "h2")
	releasing := releasingS0(1200, 7)
	holding := func(s core.ShardStatus, holder string) core.ShardStatus {
		s.Holder = holder
		return s
	}
	idleAt := func(holder string) core.ShardStatus {
		return core.ShardStatus{Since: mc.Ms(1500), Holder: holder}
	}
	lostNow := func(holder string) core.ShardStatus { return lostS0(1500, holder) }
	releasingSteps := append(movedS0(), runStopped(1200, 7, releaseS0(7), wakeAt(2000)))

	type row struct {
		holder string
		term   core.Term
		want   core.ShardStatus
		action core.Action
	}
	add := func(name string, steps []mc.Step, rows ...row) {
		for _, r := range rows {
			t.Run(fmt.Sprintf("%s/%q term %d", name, r.holder, r.term), func(t *testing.T) {
				want := []core.Action{wakeAt(2000)}
				if r.action != nil {
					want = []core.Action{r.action, wakeAt(2000)}
				}
				list := append(slices.Clone(steps), holderChanged(1500, r.holder, r.term, want...))
				runSteps(t, scenario(list...), stepChecks{len(list): shardIs(0, r.want)})
			})
		}
	}

	add("idle", bootedOnly(),
		row{"h1", 9, noRunS0(1500, 9), releaseS0(9)},
		row{"h1", 0, idle, nil},
		row{"h2", 9, holding(idle, "h2"), nil},
		row{"h2", 0, holding(idle, "h2"), nil},
		row{"", 0, idle, nil},
	)
	add("acquiring", acquiringS0(),
		row{"h1", 9, ownedS0(1500, 9), startedS0(9, 10000)},
		row{"h1", 0, acquiring, nil},
		row{"h2", 9, holding(acquiring, "h2"), nil},
		row{"", 0, acquiring, nil},
	)
	add("owned", mc.Boot(7, mc.S("s", 1)),
		row{"h1", 7, owned, nil},
		row{"h1", 0, owned, nil},
		row{"h1", 6, owned, nil},
		row{"h2", 6, owned, nil},
		row{"", 6, owned, nil},
		row{"h1", 8, lostNow("h1"), stopLostS0()},
		row{"h2", 7, lostNow("h2"), stopLostS0()},
		row{"h2", 8, lostNow("h2"), stopLostS0()},
		row{"h2", 0, lostNow("h2"), stopLostS0()},
		row{"", 0, lostNow(""), stopLostS0()},
		row{"", 8, lostNow(""), stopLostS0()},
	)
	add("stopping", movedS0(),
		row{"h1", 7, stopping, nil},
		row{"h1", 0, stopping, nil},
		row{"h1", 6, stopping, nil},
		row{"h2", 6, stopping, nil},
		row{"", 6, stopping, nil},
		row{"h1", 8, stopping, nil},
		row{"h2", 7, holding(stopping, "h2"), nil},
		row{"h2", 8, holding(stopping, "h2"), nil},
		row{"h2", 0, holding(stopping, "h2"), nil},
		row{"", 0, holding(stopping, ""), nil},
	)
	add("lost", ownedAndLostS0(),
		row{"h1", 7, lost, nil},
		row{"h1", 9, lost, nil},
		row{"h1", 0, lost, nil},
		row{"h3", 9, lost, nil},
		row{"", 0, lost, nil},
	)
	add("releasing", releasingSteps,
		row{"h1", 7, releasing, nil},
		row{"h1", 0, releasing, nil},
		row{"h1", 6, releasing, nil},
		row{"h2", 6, releasing, nil},
		row{"", 6, releasing, nil},
		row{"h1", 8, core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(1200), Term: 8, Holder: "h1"}, releaseS0(8)},
		row{"h2", 7, idleAt("h2"), nil},
		row{"h2", 8, idleAt("h2"), nil},
		row{"h2", 0, idleAt("h2"), nil},
		row{"", 0, idleAt(""), nil},
		row{"", 8, idleAt(""), nil},
	)

	t.Run("an unknown space or shard does nothing", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)),
			mc.Step{Input: core.HolderChanged{Now: mc.Ms(400), Space: "other", Holder: "h2", Term: 8}, Want: []core.Action{wakeAt(2000)}},
			mc.Step{Input: core.HolderChanged{Now: mc.Ms(500), Space: "s", Shard: 1, Holder: "h2", Term: 8}, Want: []core.Action{wakeAt(2000)}},
			mc.Step{Input: core.HolderChanged{Now: mc.Ms(600), Space: "s", Shard: -1, Holder: "h2", Term: 8}, Want: []core.Action{wakeAt(2000)}},
		)
		runSteps(t, scenario(steps...), stepChecks{7: shardIs(0, owned)})
	})
}

// TestRecordWithNoHolder checks that a record with no holder ends the wait for RetryAt, so that the
// Acquire goes in the same step, and that a record naming another holder does not.
func TestRecordWithNoHolder(t *testing.T) {
	waiting := append(acquiringS0(), acquireFailed(300, 5*time.Second, wakeAt(2000))) // RetryAt is @5.3.
	acquiring := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(1000), Wanted: true}

	t.Run("the Acquire goes in the same step", func(t *testing.T) {
		runSteps(t, scenario(append(slices.Clone(waiting),
			holderChanged(1000, "", 0, acquireS0(), wakeAt(2000)),
		)...), stepChecks{5: shardIs(0, acquiring)})
	})

	t.Run("whatever term the record has", func(t *testing.T) {
		runSteps(t, scenario(append(slices.Clone(waiting),
			holderChanged(1000, "", 8, acquireS0(), wakeAt(2000)),
		)...), stepChecks{5: shardIs(0, acquiring)})
	})

	t.Run("another holder leaves RetryAt as it is", func(t *testing.T) {
		runSteps(t, scenario(append(slices.Clone(waiting),
			holderChanged(1000, "h3", 8, wakeAt(2000)),
		)...), stepChecks{
			5: shardIs(0, core.ShardStatus{Since: mc.Ms(300), Holder: "h3", Wanted: true, RetryAt: mc.Ms(5300)}),
		})
	})

	t.Run("a late session clears RetryAt and waits to be live", func(t *testing.T) {
		runSteps(t, scenario(append(slices.Clone(waiting),
			keepAlive(10000, 12000),
			holderChanged(10100, "", 0, wakeAt(12000)),
			keepAlive(12000, 14000),
			renewed(12100, 12000, extended(22000), acquireS0(), wakeAt(14000)),
		)...), stepChecks{
			6: shardIs(0, core.ShardStatus{Since: mc.Ms(300), Wanted: true}),
		})
	})

	t.Run("a shard not wanted clears RetryAt and stays idle", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
			viewed(250, mc.Vm(2, "m2"), wakeAt(2000)),
			acquireFailed(300, 5*time.Second, wakeAt(2000)),
			holderChanged(1000, "", 0, wakeAt(2000)),
		), stepChecks{6: shardIs(0, core.ShardStatus{Since: mc.Ms(300)})})
	})
}

// TestRecordNamingThisMemberWhileAcquiring checks that a record naming this member answers an Acquire
// that is out, and that the Acquire's own answer, coming later, finds nothing to do.
func TestRecordNamingThisMemberWhileAcquiring(t *testing.T) {
	lateAnswers := []core.Input{
		core.Acquired{Now: mc.Ms(600), Space: "s", Term: 9},
		core.Acquired{Now: mc.Ms(600), Space: "s", Term: 10},
		core.Acquired{Now: mc.Ms(600), Space: "s", Term: 0},
		core.AcquireFailed{Now: mc.Ms(600), Space: "s", Holder: "h2", RetryAfter: time.Minute},
	}
	for _, in := range lateAnswers {
		t.Run(fmt.Sprintf("a grant it can run/%T%+v", in, in), func(t *testing.T) {
			runSteps(t, scenario(append(acquiringS0(),
				holderChanged(500, "h1", 9, startedS0(9, 10000), wakeAt(2000)),
				mc.Step{Input: in, Want: []core.Action{wakeAt(2000)}},
			)...), stepChecks{
				4: shardIs(0, ownedS0(500, 9)),
				5: shardIs(0, ownedS0(500, 9)),
			})
		})
		t.Run(fmt.Sprintf("a grant it cannot run/%T%+v", in, in), func(t *testing.T) {
			runSteps(t, scenario(
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
				viewed(300, mc.Vm(2, "m2"), wakeAt(2000)),
				holderChanged(500, "h1", 9, releaseS0(9), wakeAt(2000)),
				mc.Step{Input: in, Want: []core.Action{wakeAt(2000)}},
				keepAlive(2000, 2500),
				tick(2500, releaseS0(9), wakeAt(4000)),
			), stepChecks{
				5: shardIs(0, noRunS0(500, 9)),
				6: shardIs(0, noRunS0(500, 9)),
			})
		})
	}
}

// TestAdoptedGrantClearsRetryAt checks a grant adopted while the shard waits for RetryAt: the wait is
// over, so no timer of it stays armed and no later idle shard inherits it.
func TestAdoptedGrantClearsRetryAt(t *testing.T) {
	runSteps(t, scenario(append(acquiringS0(),
		acquired(300, 0, wakeAt(2000)), // RetryAt is @2.3.
		holderChanged(400, "h1", 9, startedS0(9, 10000), wakeAt(2000)),
		keepAlive(2000, 4000),
		tick(2300, wakeAt(4000)),
	)...), stepChecks{
		5: shardIs(0, ownedS0(400, 9)),
		7: shardIs(0, ownedS0(400, 9)),
	})

	t.Run("and so does a leftover", func(t *testing.T) {
		leftover := noRunS0(10200, 9)
		leftover.Wanted = true
		runSteps(t, scenario(append(acquiringS0(),
			keepAlive(10000, 12000),
			acquired(10100, 0, wakeAt(12000)),
			holderChanged(10200, "h1", 9, releaseS0(9), wakeAt(12000)),
		)...), stepChecks{6: shardIs(0, leftover)})
	})
}

// TestLeftoverNeverStartsASecondRun checks a record of a term at or below the highest started: it is
// released with no run, in any state with no run, and StartRun comes only with a higher term.
func TestLeftoverNeverStartsASecondRun(t *testing.T) {
	over := append(movedS0(),
		viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
		runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
	)
	for _, term := range []core.Term{7, 6, 1} {
		t.Run(fmt.Sprintf("while acquiring, term %d", term), func(t *testing.T) {
			wanted := noRunS0(1400, term)
			wanted.Wanted = true
			runSteps(t, scenario(append(slices.Clone(over),
				released(1300, 7, acquireS0(), wakeAt(2000)),
				holderChanged(1400, "h1", term, releaseS0(term), wakeAt(2000)),
				released(1500, term, acquireS0(), wakeAt(2000)),
				acquired(1600, 8, startedS0(8, 10000), wakeAt(2000)),
			)...), stepChecks{
				9:  shardIs(0, wanted),
				11: shardIs(0, ownedS0(1600, 8)),
			})
		})
	}

	t.Run("while idle, in a shard that is not wanted", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
			released(1300, 7, wakeAt(2000)),
			holderChanged(1400, "h1", 7, releaseS0(7), wakeAt(2000)),
			released(1500, 7, wakeAt(2000)),
		)...), stepChecks{
			8: shardIs(0, noRunS0(1400, 7)),
			9: shardIs(0, core.ShardStatus{Since: mc.Ms(1500), Holder: "h1"}),
		})
	})

	t.Run("the same record again and again releases each time and starts nothing", func(t *testing.T) {
		runSteps(t, scenario(append(slices.Clone(over),
			released(1300, 7, acquireS0(), wakeAt(2000)),
			holderChanged(1400, "h1", 7, releaseS0(7), wakeAt(2000)),
			holderChanged(1450, "h1", 7, wakeAt(2000)),
			released(1500, 7, acquireS0(), wakeAt(2000)),
			acquired(1550, 7, releaseS0(7), wakeAt(2000)),
			released(1600, 7, acquireS0(), wakeAt(2000)),
		)...), nil)
	})

	t.Run("a run's own echo while it runs does nothing", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(400, "h1", 7, wakeAt(2000)),
			holderChanged(500, "h1", 6, wakeAt(2000)),
		)...), stepChecks{6: shardIs(0, ownedS0(300, 7))})
	})
}

// TestKnownGone checks a run told to stop that the record then stops naming: its RunStopped takes the
// shard to idle with no Release, and the mark does not outlive that run.
func TestKnownGone(t *testing.T) {
	t.Run("from stopping, to idle with no Release and no timer of the shard", func(t *testing.T) {
		gone := stoppingS0(1000, 7)
		gone.Holder = "h2"
		runSteps(t, scenario(append(movedS0(),
			holderChanged(1100, "h2", 8, wakeAt(2000)),
			runStopped(1200, 6, wakeAt(2000)),
			runStopped(1300, 8, wakeAt(2000)),
			runStopped(1400, 7, wakeAt(2000)),
			runStopped(1500, 7, wakeAt(2000)),
			keepAlive(2000, 4000),
			keepAlive(4000, 6000),
		)...), stepChecks{
			6: shardIs(0, gone),
			7: shardIs(0, gone),
			8: shardIs(0, gone),
			9: shardIs(0, core.ShardStatus{Since: mc.Ms(1400), Holder: "h2"}),
		})
	})

	t.Run("no second StopRun, and a stuck run stays unreleased", func(t *testing.T) {
		stuck := stoppingS0(1000, 7)
		stuck.Holder, stuck.Stuck = "h2", true
		runSteps(t, scenario(append(movedS0(),
			holderChanged(1100, "h2", 8, wakeAt(2000)),
			keepAlive(11000, 13000),
			holderChanged(11100, "", 0, wakeAt(13000)),
			runStopped(11200, 7, wakeAt(13000)),
		)...), stepChecks{
			7: shardIs(0, stuck),
			9: shardIs(0, core.ShardStatus{Since: mc.Ms(11200)}),
		})
	})

	t.Run("wanted again, it acquires in the RunStopped's step", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
			holderChanged(1200, "h2", 8, wakeAt(2000)),
			runStopped(1300, 7, acquireS0(), wakeAt(2000)),
		)...), stepChecks{
			8: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(1300), Holder: "h2", Wanted: true}),
		})
	})

	t.Run("a stale record of the run's own term does not bring it back", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			holderChanged(1100, "h2", 8, wakeAt(2000)),
			holderChanged(1200, "h1", 7, wakeAt(2000)),
			runStopped(1300, 7, wakeAt(2000)),
		)...), nil)
	})

	t.Run("the next run releases as usual", func(t *testing.T) {
		for _, name := range []string{"after stopping", "after lost"} {
			t.Run(name, func(t *testing.T) {
				steps := append(movedS0(),
					viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
					holderChanged(1200, "h2", 8, wakeAt(2000)),
				)
				if name == "after lost" {
					steps = ownedAndLostS0()
				}
				steps = append(steps,
					runStopped(1300, 7, acquireS0(), wakeAt(2000)),
					acquired(1400, 9, startedS0(9, 10000), wakeAt(2000)),
					viewed(1500, mc.Vm(4, "m2"), stopS0(9), wakeAt(2000)),
					runStopped(1600, 9, releaseS0(9), wakeAt(2000)),
				)
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, releasingS0(1600, 9))})
			})
		}
	})
}

// TestLostRun checks a run stopped because the record no longer names it: it counts as stuck like any
// other run told to stop, one StopRun is all it gets, and a move changes nothing.
func TestLostRun(t *testing.T) {
	lost := lostS0(1000, "h2")
	stuck := lost
	stuck.Stuck = true

	t.Run("stuck at exactly stopAt plus RenewDeadline, then to idle", func(t *testing.T) {
		const justBefore = 11*time.Second - 1
		runSteps(t, scenario(append(ownedAndLostS0(),
			mc.Step{Input: core.Tick{Now: justBefore}, Want: []core.Action{core.KeepAlive{}, wakeAt(11000)}},
			mc.Step{Input: core.Tick{Now: mc.Ms(11000)}, Want: []core.Action{core.Wake{At: justBefore + 2*time.Second}}},
			mc.Step{Input: core.RunStopped{Now: mc.Ms(11500), Space: "s", Term: 7}, Want: []core.Action{core.Wake{At: justBefore + 2*time.Second}}},
		)...), stepChecks{
			6: shardIs(0, lost),
			7: shardIs(0, stuck),
			8: shardIs(0, core.ShardStatus{Since: mc.Ms(11500), Holder: "h2", Wanted: true}),
		})
	})

	t.Run("a view that moves the shard sends no second StopRun", func(t *testing.T) {
		moved := lost
		moved.Wanted = false
		runSteps(t, scenario(append(ownedAndLostS0(),
			viewed(1100, mc.Vm(2, "m2"), wakeAt(2000)),
			runStopped(1200, 7, wakeAt(2000)),
		)...), stepChecks{
			6: shardIs(0, moved),
			7: shardIs(0, core.ShardStatus{Since: mc.Ms(1200), Holder: "h2"}),
		})
	})

	t.Run("on a late session too", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			keepAlive(10000, 12000),
			holderChanged(10100, "h2", 8, stopLostS0(), wakeAt(12000)),
			runStopped(10200, 7, wakeAt(12000)),
		)...), stepChecks{
			6: shardIs(0, lostS0(10100, "h2")),
			7: shardIs(0, core.ShardStatus{Since: mc.Ms(10200), Holder: "h2", Wanted: true}),
		})
	})
}

// TestLargerOwnTermWhileARunIsGoing checks the promise for a record naming this member in a larger
// term while a run of the smaller one is going. The run's term stays the shard's, only that term's
// RunStopped ends it, and nothing of the larger term goes out before that: no Release, no StartRun.
func TestLargerOwnTermWhileARunIsGoing(t *testing.T) {
	const larger = 9
	lost := lostS0(1000, "h1")

	t.Run("owned: told to stop, and the larger term starts only after the RunStopped", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(1000, "h1", larger, stopLostS0(), wakeAt(2000)),
			holderChanged(1100, "h1", larger, wakeAt(2000)),
			holderChanged(1150, "h1", larger+1, wakeAt(2000)),
			acquired(1200, larger, wakeAt(2000)),
			runStopped(1300, larger, wakeAt(2000)),
			released(1400, larger, wakeAt(2000)),
			released(1450, 7, wakeAt(2000)),
			keepAlive(2000, 4000),
			runStopped(2100, 7, acquireS0(), wakeAt(4000)),
			acquired(2200, larger, startedS0(larger, 10000), wakeAt(4000)),
		)...), stepChecks{
			5:  shardIs(0, lost),
			12: shardIs(0, lost),
			14: shardIs(0, ownedS0(2200, larger)),
		})
	})

	t.Run("owned and moved: the larger term's Release comes only after the RunStopped", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(1000, "h1", larger, stopLostS0(), wakeAt(2000)),
			viewed(1100, mc.Vm(2, "m2"), wakeAt(2000)),
			runStopped(1200, 7, wakeAt(2000)),
			holderChanged(1300, "h1", larger, releaseS0(larger), wakeAt(2000)),
		)...), stepChecks{
			8: shardIs(0, noRunS0(1300, larger)),
		})
	})

	t.Run("stopping: no second StopRun, and the run's term stays", func(t *testing.T) {
		running := stoppingS0(1000, 7)
		runSteps(t, scenario(append(movedS0(),
			holderChanged(1100, "h1", larger, wakeAt(2000)),
			runStopped(1200, larger, wakeAt(2000)),
			released(1250, larger, wakeAt(2000)),
			runStopped(1300, 7, wakeAt(2000)),
			holderChanged(1400, "h1", larger, releaseS0(larger), wakeAt(2000)),
		)...), stepChecks{
			6:  shardIs(0, running),
			7:  shardIs(0, running),
			8:  shardIs(0, running),
			9:  shardIs(0, core.ShardStatus{Since: mc.Ms(1300), Holder: "h1"}),
			10: shardIs(0, noRunS0(1400, larger)),
		})
	})

	t.Run("releasing: the run has returned, so the larger term goes at once", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			runStopped(1100, 7, releaseS0(7), wakeAt(2000)),
			holderChanged(1200, "h1", larger, releaseS0(larger), wakeAt(2000)),
			keepAlive(2000, 3200),
			tick(3200, releaseS0(larger), wakeAt(4000)),
		)...), nil)
	})
}

// TestHandoverTimersAtTheLargestReadings checks that the handover's timers fit in a Duration and stay
// after the clock, with the longest timing fields too, at readings a driver never reaches.
func TestHandoverTimersAtTheLargestReadings(t *testing.T) {
	const (
		largest = time.Duration(math.MaxInt64)
		ceiling = largest - 24*time.Hour
	)
	timings := []core.Timing{
		core.DefaultTiming,
		{LeaseDuration: 24 * time.Hour, RenewDeadline: 21 * time.Hour, RetryPeriod: 17 * time.Hour, SlotGrace: 24 * time.Hour},
	}
	readings := []time.Duration{
		largest,
		ceiling,
		ceiling - 1,
		ceiling - time.Second,
		ceiling - 3*time.Second,
		ceiling - time.Minute,
		ceiling - 48*time.Hour,
	}
	for _, timing := range timings {
		for _, now := range readings {
			cfg := mc.C(mc.S("s", 1))
			cfg.Timing = timing
			m := core.NewMachine(cfg)
			step := func(in core.Input) []core.Action {
				t.Helper()
				got := m.Step(in)
				at, armed := m.NextWake()
				st := m.Status()
				for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
					t.Errorf("timing %v, reading %v: Step(%#v): %s", timing, now, in, v)
				}
				return got
			}
			for _, s := range mc.Boot(7, mc.S("s", 1)) {
				step(s.Input)
			}

			moved := core.ViewChanged{Now: now, View: mc.Vm(2, "m2"), Wall: mc.Wall(0)}
			if got := step(moved); !slices.Contains(got, stopS0(7)) {
				t.Errorf("timing %v, reading %v: the view returned %#v, want a StopRun", timing, now, got)
			}
			step(core.Tick{Now: now})
			if got := step(core.RunStopped{Now: now, Space: "s", Term: 7}); !slices.Contains(got, releaseS0(7)) {
				t.Errorf("timing %v, reading %v: the RunStopped returned %#v, want a Release", timing, now, got)
			}
			step(core.Tick{Now: largest})
			step(core.Tick{Now: largest})
			step(core.Released{Now: largest, Space: "s", Term: 7})
			if got := m.Status().Spaces[0].Shards[0].State; got != core.ShardStateIdle {
				t.Errorf("timing %v, reading %v: state = %v, want idle", timing, now, got)
			}
		}
	}
}

// TestStopping checks what a move does to a run, and what does not undo it.
func TestStopping(t *testing.T) {
	t.Run("the reason is draining while the member drains", func(t *testing.T) {
		draining := stoppingS0(1000, 7)
		draining.Reason = core.StopDraining
		steps := append(mc.Boot(7, mc.S("s", 1)),
			viewed(1000, mc.Vm(2, "m2"), core.StopRun{Space: "s", Term: 7, Reason: core.StopDraining}, wakeAt(2000)),
			runStopped(1500, 7, releaseS0(7), wakeAt(2000)),
		)
		releasing := releasingS0(1500, 7)
		releasing.Reason = core.StopDraining
		runSteps(t, scenario(steps...), stepChecks{
			4: func(_ *testing.T, m *core.Machine) { core.ForceDraining(m) },
			5: shardIs(0, draining),
			6: shardIs(0, releasing),
		})
	})

	t.Run("a late session stops a run too", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)),
			keepAlive(10000, 12000),
			viewed(10100, mc.Vm(2, "m2"), stopS0(7), wakeAt(12000)),
		)
		runSteps(t, scenario(steps...), stepChecks{6: shardIs(0, stoppingS0(10100, 7))})
	})

	t.Run("a view that keeps the shard does not stop the run", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)), viewed(1000, mc.Vm(2, "m1"), wakeAt(2000)))
		runSteps(t, scenario(steps...), stepChecks{5: shardIs(0, ownedS0(300, 7))})
	})

	t.Run("a view with an older reading stops the run at the machine's reading", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)),
			keepAlive(5000, 7000),
			viewed(1000, mc.Vm(2, "m2"), stopS0(7), wakeAt(7000)),
		)
		runSteps(t, scenario(steps...), stepChecks{6: shardIs(0, stoppingS0(5000, 7))})
	})

	t.Run("there is one StopRun however the view changes", func(t *testing.T) {
		wanted := stoppingS0(1000, 7)
		wanted.Wanted = true
		runSteps(t, scenario(append(movedS0(),
			viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
			viewed(1200, mc.Vm(4, "m2"), wakeAt(2000)),
			viewed(1300, mc.Vm(5, "m1"), wakeAt(2000)),
		)...), stepChecks{
			6: shardIs(0, wanted),
			7: shardIs(0, stoppingS0(1000, 7)),
			8: shardIs(0, wanted),
		})
	})
}

// TestStuck checks when a run counts as stuck: at exactly RenewDeadline after its StopRun, until its
// RunStopped.
func TestStuck(t *testing.T) {
	running := stoppingS0(1000, 7)
	stuck := running
	stuck.Stuck = true

	t.Run("exactly at stopAt plus RenewDeadline", func(t *testing.T) {
		const justBefore = 11*time.Second - 1
		runSteps(t, scenario(append(movedS0(),
			mc.Step{Input: core.Tick{Now: justBefore}, Want: []core.Action{core.KeepAlive{}, wakeAt(11000)}},
			mc.Step{Input: core.Tick{Now: mc.Ms(11000)}, Want: []core.Action{core.Wake{At: justBefore + 2*time.Second}}},
		)...), stepChecks{
			6: shardIs(0, running),
			7: shardIs(0, stuck),
		})
	})

	t.Run("a reading past it, with no timer left", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(), keepAlive(30000, 32000))...), stepChecks{6: shardIs(0, stuck)})
	})

	t.Run("any input marks it", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(), runStopped(30000, 6, core.KeepAlive{}, wakeAt(32000)))...),
			stepChecks{6: shardIs(0, stuck)})
	})

	t.Run("false again after RunStopped, and never stuck while releasing", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			keepAlive(11000, 13000),
			runStopped(11500, 7, releaseS0(7), wakeAt(13000)),
			keepAlive(13000, 13500),
			tick(13500, releaseS0(7), wakeAt(15000)),
			tick(40000, core.KeepAlive{}, releaseS0(7), wakeAt(42000)),
		)...), stepChecks{
			6: shardIs(0, stuck),
			7: shardIs(0, releasingS0(11500, 7)),
			9: shardIs(0, releasingS0(11500, 7)),
		})
	})

	t.Run("a RunStopped at that very reading is never stuck", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			runStopped(11000, 7, core.KeepAlive{}, releaseS0(7), wakeAt(13000)),
		)...), stepChecks{6: shardIs(0, releasingS0(11000, 7))})
	})

	t.Run("a stop that comes in time is not stuck", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			runStopped(1500, 7, releaseS0(7), wakeAt(2000)),
			released(1600, 7, wakeAt(2000)),
			keepAlive(30000, 32000),
		)...), stepChecks{6: shardIs(0, releasingS0(1500, 7))})
	})

	t.Run("each run counts from its own StopRun", func(t *testing.T) {
		second := stoppingS0(20000, 8)
		secondStuck := second
		secondStuck.Stuck = true
		runSteps(t, scenario(append(movedS0(),
			keepAlive(11000, 13000),
			viewed(11100, mc.Vm(3, "m1"), wakeAt(13000)),
			runStopped(11200, 7, releaseS0(7), wakeAt(13000)),
			renewed(11300, 11300, extended(21300), wakeAt(13000)),
			released(11400, 7, acquireS0(), wakeAt(13000)),
			acquired(11500, 8, startedS0(8, 21300), wakeAt(13000)),
			viewed(20000, mc.Vm(4, "m2"), core.KeepAlive{}, stopS0(8), wakeAt(21300)),
			tick(21300, wakeAt(22000)),
			keepAlive(22000, 24000),
			keepAlive(24000, 26000),
			keepAlive(26000, 28000),
			keepAlive(28000, 30000),
			tick(29999, wakeAt(30000)),
			keepAlive(30000, 32000),
		)...), stepChecks{
			12: shardIs(0, second),
			18: shardIs(0, second),
			19: shardIs(0, secondStuck),
		})
	})
}

// TestReleaseRepeat checks the Release cadence: a repeat at exactly releaseAt, counted from the last
// Release, whichever input brings the clock there.
func TestReleaseRepeat(t *testing.T) {
	stopped := append(movedS0(), runStopped(1500, 7, releaseS0(7), wakeAt(2000)), keepAlive(2000, 3500))
	releasing := releasingS0(1500, 7)
	tests := []struct {
		name  string
		steps []mc.Step
	}{
		{
			name: "not before releaseAt, and exactly at it",
			steps: []mc.Step{
				{Input: core.Tick{Now: mc.Ms(3500) - 1}, Want: []core.Action{wakeAt(3500)}},
				tick(3500, releaseS0(7), wakeAt(4000)),
			},
		},
		{
			name: "each repeat counts from itself, not from the last due reading",
			steps: []mc.Step{
				tick(4200, core.KeepAlive{}, releaseS0(7), wakeAt(6200)),
				tick(6199, wakeAt(6200)),
				tick(6200, core.KeepAlive{}, releaseS0(7), wakeAt(8200)),
			},
		},
		{
			name: "one Release for a reading many periods late",
			steps: []mc.Step{
				tick(30000, core.KeepAlive{}, releaseS0(7), wakeAt(32000)),
				tick(31000, wakeAt(32000)),
			},
		},
		{
			name:  "an ignored input at releaseAt still repeats",
			steps: []mc.Step{runStopped(3500, 7, releaseS0(7), wakeAt(4000))},
		},
		{
			name:  "a wrong answer at releaseAt still repeats",
			steps: []mc.Step{released(3500, 6, releaseS0(7), wakeAt(4000))},
		},
		{
			name:  "the answer at releaseAt comes first, so no repeat goes",
			steps: []mc.Step{released(3500, 7, wakeAt(4000))},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSteps(t, scenario(slices.Concat(stopped, tc.steps)...), stepChecks{6: shardIs(0, releasing)})
		})
	}

	t.Run("an older reading counts from the machine's clock", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			keepAlive(5000, 7000),
			runStopped(1500, 7, releaseS0(7), wakeAt(7000)),
			tick(6999, wakeAt(7000)),
			tick(7000, core.KeepAlive{}, releaseS0(7), wakeAt(9000)),
		)...), stepChecks{7: shardIs(0, releasingS0(5000, 7))})
	})

	t.Run("the term and the reason stay while it repeats", func(t *testing.T) {
		runSteps(t, scenario(slices.Concat(stopped, []mc.Step{
			tick(3500, releaseS0(7), wakeAt(4000)),
			keepAlive(4000, 5500),
			tick(5500, releaseS0(7), wakeAt(6000)),
		})...), stepChecks{
			8:  shardIs(0, releasing),
			10: shardIs(0, releasing),
		})
	})
}

// TestWantedAgain checks a shard the assignment gives back during its handover: it finishes the
// handover first, and acquires again for a new term.
func TestWantedAgain(t *testing.T) {
	acquiring := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(1300), Holder: "h1", Wanted: true}

	t.Run("while stopping", func(t *testing.T) {
		stopping, releasing := stoppingS0(1000, 7), releasingS0(1200, 7)
		stopping.Wanted, releasing.Wanted = true, true
		runSteps(t, scenario(append(movedS0(),
			viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
			runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
			released(1300, 7, acquireS0(), wakeAt(2000)),
			acquired(1400, 8, startedS0(8, 10000), wakeAt(2000)),
		)...), stepChecks{
			6: shardIs(0, stopping),
			7: shardIs(0, releasing),
			8: shardIs(0, acquiring),
			9: shardIs(0, ownedS0(1400, 8)),
		})
	})

	t.Run("while releasing", func(t *testing.T) {
		releasing := releasingS0(1100, 7)
		releasing.Wanted = true
		runSteps(t, scenario(append(movedS0(),
			runStopped(1100, 7, releaseS0(7), wakeAt(2000)),
			viewed(1200, mc.Vm(3, "m1"), wakeAt(2000)),
			released(1300, 7, acquireS0(), wakeAt(2000)),
			acquired(1400, 8, startedS0(8, 10000), wakeAt(2000)),
		)...), stepChecks{
			7: shardIs(0, releasing),
			8: shardIs(0, acquiring),
			9: shardIs(0, ownedS0(1400, 8)),
		})
	})

	t.Run("the repeats go on while it is wanted", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			runStopped(1100, 7, releaseS0(7), wakeAt(2000)),
			viewed(1200, mc.Vm(3, "m1"), wakeAt(2000)),
			keepAlive(2000, 3100),
			tick(3100, releaseS0(7), wakeAt(4000)),
		)...), nil)
	})

	t.Run("a grant of the old term does not start a second run", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
			runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
			released(1300, 7, acquireS0(), wakeAt(2000)),
			acquired(1400, 7, releaseS0(7), wakeAt(2000)),
		)...), nil)
	})

	t.Run("released while the session is late, it acquires when the session is live", func(t *testing.T) {
		idle := core.ShardStatus{Since: mc.Ms(10100), Holder: "h1", Wanted: true}
		runSteps(t, scenario(append(movedS0(),
			runStopped(1100, 7, releaseS0(7), wakeAt(2000)),
			viewed(1200, mc.Vm(3, "m1"), wakeAt(2000)),
			tick(10000, core.KeepAlive{}, releaseS0(7), wakeAt(12000)),
			released(10100, 7, wakeAt(12000)),
			keepAlive(12000, 14000),
			renewed(12100, 12000, extended(22000), acquireS0(), wakeAt(14000)),
		)...), stepChecks{
			9:  shardIs(0, idle),
			10: shardIs(0, idle),
			11: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(12100), Holder: "h1", Wanted: true}),
		})
	})

	t.Run("not wanted, it stays idle", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			runStopped(1100, 7, releaseS0(7), wakeAt(2000)),
			released(1200, 7, wakeAt(2000)),
			keepAlive(2000, 4000),
		)...), stepChecks{8: shardIs(0, core.ShardStatus{Since: mc.Ms(1200), Holder: "h1"})})
	})
}

// TestHandoverOutputOrder checks that a handover's actions come in the order of their groups, not of
// their shards. Only a view returns a StopRun and only a grant a StartRun, so no one step has both.
func TestHandoverOutputOrder(t *testing.T) {
	stop := func(shard int, term core.Term) core.Action {
		return core.StopRun{Space: "s", Shard: shard, Term: term, Reason: core.StopMoved}
	}
	release := func(shard int, term core.Term) core.Action {
		return core.Release{Space: "s", Shard: shard, Term: term}
	}
	// With m1 and m3 this member wants only shard 2, and with m1 and m4 only shard 1.
	steps := append(mc.Boot(7, mc.S("s", 3)),
		viewed(1000, mc.Vm(2, "m1", "m3"), stop(0, 7), stop(1, 8), wakeAt(2000)),
		mc.Step{
			Input: core.RunStopped{Now: mc.Ms(1500), Space: "s", Shard: 0, Term: 7},
			Want:  []core.Action{release(0, 7), wakeAt(2000)},
		},
		mc.Step{
			Input: core.RunStopped{Now: mc.Ms(1600), Space: "s", Shard: 1, Term: 8},
			Want:  []core.Action{release(1, 8), wakeAt(2000)},
		},
		mc.Step{
			Input: core.Released{Now: mc.Ms(1700), Space: "s", Shard: 1, Term: 8},
			Want:  []core.Action{wakeAt(2000)},
		},
		keepAlive(2000, 3500),
		// One step returns a StopRun of the last shard, a Release of the first and an Acquire of
		// the one between.
		viewed(3500, mc.Vm(3, "m1", "m4"),
			stop(2, 9),
			release(0, 7),
			core.Acquire{Space: "s", Shard: 1},
			wakeAt(4000),
		),
		keepAlive(4000, 5500),
		// One step returns a StartRun of a later shard, then the Release of the first.
		mc.Step{
			Input: core.Acquired{Now: mc.Ms(5500), Space: "s", Shard: 1, Term: 10},
			Want: []core.Action{
				core.StartRun{Space: "s", Shard: 1, Term: 10, Deadline: mc.Ms(10000)},
				release(0, 7),
				wakeAt(6000),
			},
		},
		// A KeepAlive comes before them all, and an answer's Acquire after the Release of an
		// earlier shard.
		mc.Step{
			Input: core.RunStopped{Now: mc.Ms(6000), Space: "s", Shard: 2, Term: 9},
			Want:  []core.Action{core.KeepAlive{}, release(2, 9), wakeAt(7500)},
		},
	)
	runSteps(t, scenarioIn(mc.C(mc.S("s", 3)), steps...), nil)

	t.Run("by space name, then shard", func(t *testing.T) {
		a0 := core.Release{Space: "a", Term: 7}
		b0 := core.Release{Space: "b", Term: 8}
		spaces := []core.SpaceSpec{mc.S("b", 1), mc.S("a", 1)}
		runSteps(t, scenarioIn(mc.C(spaces...), append(mc.Boot(7, spaces...),
			viewed(1000, mc.Vm(2, "m2"),
				core.StopRun{Space: "a", Term: 7, Reason: core.StopMoved},
				core.StopRun{Space: "b", Term: 8, Reason: core.StopMoved},
				wakeAt(2000),
			),
			mc.Step{Input: core.RunStopped{Now: mc.Ms(1500), Space: "b", Term: 8}, Want: []core.Action{b0, wakeAt(2000)}},
			mc.Step{Input: core.RunStopped{Now: mc.Ms(1500), Space: "a", Term: 7}, Want: []core.Action{a0, wakeAt(2000)}},
			keepAlive(2000, 3500),
			tick(3500, a0, b0, wakeAt(4000)),
		)...), nil)
	})
}

func TestAcquireOrderAcrossSpaces(t *testing.T) {
	runSteps(t, scenarioIn(mc.C(mc.S("b", 2), mc.S("a", 2)),
		keepAlive(0, 2000),
		renewed(100, 0, extended(10000), wakeAt(2000)),
		viewed(200, mc.Vm(1, "m1"),
			core.Acquire{Space: "a", Shard: 0},
			core.Acquire{Space: "a", Shard: 1},
			core.Acquire{Space: "b", Shard: 0},
			core.Acquire{Space: "b", Shard: 1},
			wakeAt(2000),
		),
	), nil)
}

// TestOutputOrder checks that the actions of one step come in the order of their groups, whatever
// order the phases emit them in: a due KeepAlive comes before the StartRun.
func TestOutputOrder(t *testing.T) {
	runSteps(t, scenarioIn(mc.C(mc.S("s", 2)),
		keepAlive(0, 2000),
		renewed(100, 0, extended(10000), wakeAt(2000)),
		viewed(200, mc.Vm(1, "m1"), acquireS0(), core.Acquire{Space: "s", Shard: 1}, wakeAt(2000)),
		acquired(2000, 7, core.KeepAlive{}, startedS0(7, 10000), wakeAt(4000)),
	), nil)

	// Shard 0 emits its Acquire before shard 1 emits its StartRun, and the StartRun still comes
	// first.
	t.Run("a StartRun comes before an Acquire of an earlier shard", func(t *testing.T) {
		runSteps(t, scenarioIn(mc.C(mc.S("s", 2)),
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), core.Acquire{Space: "s", Shard: 1}, wakeAt(2000)),
			acquired(300, 0, wakeAt(2000)), // RetryAt of shard 0 is @2.3.
			keepAlive(2000, 2300),
			mc.Step{
				Input: core.Acquired{Now: mc.Ms(2300), Space: "s", Shard: 1, Term: 8},
				Want:  []core.Action{core.StartRun{Space: "s", Shard: 1, Term: 8, Deadline: mc.Ms(10000)}, acquireS0(), wakeAt(4000)},
			},
		), nil)
	})

	// Shard 0 emits its Release before shard 1 emits its StopRun, and the StopRun still comes first.
	t.Run("a StopRun comes before a repeated Release of an earlier shard", func(t *testing.T) {
		runSteps(t, scenarioIn(mc.C(mc.S("s", 2)),
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), core.Acquire{Space: "s", Shard: 1}, wakeAt(2000)),
			mc.Step{
				Input: core.Acquired{Now: mc.Ms(300), Space: "s", Shard: 1, Term: 8},
				Want:  []core.Action{core.StartRun{Space: "s", Shard: 1, Term: 8, Deadline: mc.Ms(10000)}, wakeAt(2000)},
			},
			keepAlive(10000, 12000),
			acquired(10100, 9, releaseS0(9), wakeAt(12000)), // The repeat is due at @12.1.
			keepAlive(12000, 12100),
			mc.Step{
				Input: core.HolderChanged{Now: mc.Ms(12100), Space: "s", Shard: 1, Holder: "h2", Term: 9},
				Want: []core.Action{
					core.StopRun{Space: "s", Shard: 1, Term: 8, Reason: core.StopLost},
					releaseS0(9),
					wakeAt(14000),
				},
			},
		), nil)
	})
}

// TestViewPlacementTime checks the time placement reads: exactly W in the step that applies a view,
// and W plus the time since that step in a later recompute.
func TestViewPlacementTime(t *testing.T) {
	wall := mc.Wall(mc.Ms(200))
	joins := func(version uint64, at time.Time) core.View {
		return core.View{Version: version, Members: []core.Member{{ID: "m1", Slot: -1, JoinAt: at}}}
	}
	emptyThenMark := func(t *testing.T, m *core.Machine) {
		t.Helper()
		assignmentIs("s", "")(t, m)
		core.MarkViewDirty(m)
	}

	t.Run("at the step that applies the view", func(t *testing.T) {
		tests := []struct {
			name   string
			member core.Member
			want   string
		}{
			{"before JoinAt", core.Member{ID: "m1", Slot: -1, JoinAt: wall.Add(1)}, ""},
			{"at JoinAt", core.Member{ID: "m1", Slot: -1, JoinAt: wall}, "m1"},
			{"before LeaveAt", core.Member{ID: "m1", Slot: -1, LeaveAt: wall.Add(1)}, "m1"},
			{"at LeaveAt", core.Member{ID: "m1", Slot: -1, LeaveAt: wall}, ""},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				m := core.NewMachine(mc.C(mc.S("s", 1)))
				m.Step(core.Tick{Now: mc.Ms(200)})
				m.Step(core.ViewChanged{
					Now:  mc.Ms(200),
					View: core.View{Version: 1, Members: []core.Member{tc.member}},
					Wall: wall,
				})
				assignmentIs("s", tc.want)(t, m)
			})
		}
	})

	// The member becomes eligible one second after the view applied. An ignored view changes neither
	// W, nor A, nor the applied view.
	t.Run("at a later recompute", func(t *testing.T) {
		ignoredView := func(v core.View) []mc.Step {
			return []mc.Step{{
				Input: core.ViewChanged{Now: mc.Ms(300), View: v, Wall: wall.Add(time.Hour)},
				Want:  []core.Action{wakeAt(2000)},
			}}
		}
		tests := []struct {
			name    string
			ignored []mc.Step
		}{
			{name: "no other view"},
			{name: "an older view", ignored: ignoredView(mc.Vm(4, "m2"))},
			{name: "an invalid view", ignored: ignoredView(core.View{Version: 9, Slots: -1})},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				steps := slices.Concat(
					[]mc.Step{
						keepAlive(0, 2000),
						renewed(100, 0, extended(10000), wakeAt(2000)),
						viewed(200, joins(5, wall.Add(time.Second)), wakeAt(2000)),
					},
					tc.ignored,
					[]mc.Step{tick(1199, wakeAt(2000))},
				)
				n := len(steps)
				steps = append(steps, tick(1200, acquireS0(), wakeAt(2000)))
				runSteps(t, scenario(steps...), stepChecks{
					n - 1: emptyThenMark,
					n:     emptyThenMark,
					n + 1: assignmentIs("s", "m1"),
				})
			})
		}
	})

	t.Run("A is the machine's reading when the input's is older", func(t *testing.T) {
		older := mc.Wall(mc.Ms(1000))
		runSteps(t, scenario(
			keepAlive(5000, 7000),
			mc.Step{
				Input: core.ViewChanged{Now: mc.Ms(1000), View: joins(1, older.Add(time.Second)), Wall: older},
				Want:  []core.Action{wakeAt(7000)},
			},
			tick(5999, wakeAt(7000)),
			tick(6000, wakeAt(7000)),
		), stepChecks{
			2: emptyThenMark,
			3: emptyThenMark,
			4: assignmentIs("s", "m1"),
		})
	})

	t.Run("W has no monotonic reading", func(t *testing.T) {
		now := time.Now()
		if !strings.Contains(now.String(), "m=") {
			t.Skip("this platform's clock has no monotonic reading")
		}
		m := core.NewMachine(mc.C(mc.S("s", 1)))
		m.Step(core.ViewChanged{View: mc.Vm(1, "m1"), Wall: now})
		got := core.ViewWall(m)
		if !got.Equal(now) {
			t.Errorf("W = %v, want %v", got, now)
		}
		if strings.Contains(got.String(), "m=") {
			t.Errorf("W = %v still has a monotonic reading", got)
		}
	})
}

// agreementViews returns valid views of every shape the machine can apply: with and without slots,
// with members that are draining, standing by or outside their window at wall.
func agreementViews(t *testing.T, wall time.Time) []core.View {
	t.Helper()
	ids := []string{"m0", "m1", "m2", "m3", "m4", "m5"}
	var views []core.View
	for n := range len(ids) + 1 {
		// shape picks, per member, nothing or one way of not being eligible.
		for shape := range 5 {
			members := make([]core.Member, n)
			for i := range members {
				members[i] = core.Member{ID: ids[i], Slot: -1}
				switch (i + shape) % 5 {
				case 1:
					members[i].Draining = true
				case 2:
					members[i].JoinAt = wall.Add(1)
				case 3:
					members[i].LeaveAt = wall
				}
			}
			views = append(views, core.View{Version: uint64(len(views)), Members: members})
		}
	}
	for slots := 1; slots <= 4; slots++ {
		for vacant := range 1 << slots {
			holders := make([]string, slots)
			first := -1
			for s := range holders {
				if vacant&(1<<s) == 0 {
					holders[s] = "h/" + strconv.Itoa(s)
					if first < 0 {
						first = s
					}
				}
			}
			members := []core.Member{{ID: "m1", Slot: first}, {ID: "m2", Slot: -1}}
			views = append(views, core.View{Version: uint64(len(views)), Members: members, Slots: slots, SlotHolders: holders})
		}
	}
	for _, v := range views {
		if err := v.Validate(); err != nil {
			t.Fatalf("view %+v is not valid: %v", v, err)
		}
	}
	return views
}

// TestViewAgreesWithPlacement checks that the assignment and the Wanted flags are what calling
// PlacementKeys and Balanced directly gives, for every valid shape of view, and that no such view
// panics. Each view has its own machine, and all of them run on one machine too, so a recompute
// from an earlier view leaves nothing behind.
func TestViewAgreesWithPlacement(t *testing.T) {
	const reading = 200 * time.Millisecond
	wall := mc.Wall(reading)
	spaces := []core.SpaceSpec{mc.S("a", 1), mc.S("b", 7), mc.S("c", 64)}

	check := func(t *testing.T, m *core.Machine, v core.View) {
		t.Helper()
		var expired []bool
		if v.Slots > 0 {
			expired = make([]bool, v.Slots)
		}
		keys, _ := core.PlacementKeys(v, wall, expired)
		st := m.Status()
		if st.ViewVersion != v.Version {
			t.Fatalf("ViewVersion = %d, want %d", st.ViewVersion, v.Version)
		}
		for i, s := range spaces {
			want := make([]string, s.Shards)
			for shard, k := range core.Balanced(s.Name, s.Shards, keys) {
				if k >= 0 {
					want[shard] = keys[k]
				}
			}
			assignmentIs(s.Name, want...)(t, m)
			for shard, sh := range st.Spaces[i].Shards {
				wanted := v.Slots == 0 && want[shard] == "m1"
				if sh.Wanted != wanted {
					t.Errorf("view %+v: %s/%d Wanted = %v, want %v", v, s.Name, shard, sh.Wanted, wanted)
				}
			}
		}
		if got := m.Assignment("unknown"); got != nil {
			t.Errorf("Assignment(unknown) = %q, want nil", got)
		}
	}

	shared := core.NewMachine(mc.C(spaces...))
	shared.Step(core.Tick{Now: reading})
	for _, v := range agreementViews(t, wall) {
		m := core.NewMachine(mc.C(spaces...))
		m.Step(core.Tick{Now: reading})
		in := core.ViewChanged{Now: reading, View: v, Wall: wall}
		m.Step(in)
		check(t, m, v)

		shared.Step(in)
		check(t, shared, v)
	}
}

// TestSpaceOrderDoesNotMatter runs one sequence on the same spaces given in opposite orders, and
// expects equal results, Status and assignments after every step.
func TestSpaceOrderDoesNotMatter(t *testing.T) {
	spaces := []core.SpaceSpec{mc.S("a", 5), mc.S("b", 3), {Name: "c", Shards: 8, Fenced: true}}
	reversed := slices.Clone(spaces)
	slices.Reverse(reversed)
	one, other := core.NewMachine(mc.C(spaces...)), core.NewMachine(mc.C(reversed...))

	step := func(in core.Input) []core.Action {
		t.Helper()
		got, gotOther := one.Step(in), other.Step(in)
		if !reflect.DeepEqual(got, gotOther) {
			t.Fatalf("Step(%#v) = %#v and %#v", in, got, gotOther)
		}
		if st, stOther := one.Status(), other.Status(); !reflect.DeepEqual(st, stOther) {
			t.Fatalf("after Step(%#v): Status = %+v and %+v", in, st, stOther)
		}
		for _, s := range spaces {
			if a, b := one.Assignment(s.Name), other.Assignment(s.Name); !slices.Equal(a, b) {
				t.Fatalf("after Step(%#v): Assignment(%s) = %q and %q", in, s.Name, a, b)
			}
		}
		return got
	}

	step(core.Tick{Now: mc.Ms(0)})
	step(core.SessionRenewed{Now: mc.Ms(100), SentAt: mc.Ms(0)})
	result := step(core.ViewChanged{Now: mc.Ms(200), View: mc.Vm(1, "m1", "m2"), Wall: mc.Wall(mc.Ms(200))})
	var grants []core.Input
	for _, a := range result {
		if acquire, ok := a.(core.Acquire); ok {
			grants = append(grants, core.Acquired{
				Now: mc.Ms(300), Space: acquire.Space, Shard: acquire.Shard, Term: core.Term(len(grants) + 1),
			})
		}
	}
	if len(grants) == 0 {
		t.Fatal("the view gave the member no shard")
	}
	for _, in := range grants {
		step(in)
	}
	step(core.ViewChanged{Now: mc.Ms(400), View: mc.Vm(2, "m1"), Wall: mc.Wall(mc.Ms(400))})
}

// TestMemberNamedLikeASlotKey checks that a member whose ID looks like a slot's key gets nothing in
// a view with slots, where such a key names the slot and not the member, and is an ordinary member
// without slots.
func TestMemberNamedLikeASlotKey(t *testing.T) {
	cfg := core.MachineConfig{Holder: "h1", Member: "slot-0", Spaces: []core.SpaceSpec{mc.S("s", 1)}}
	runSteps(t, scenarioIn(cfg,
		keepAlive(0, 2000),
		renewed(100, 0, extended(10000), wakeAt(2000)),
		viewed(200, mc.Vs(1, []string{"h/0"}, "slot-0"), wakeAt(2000)),
		viewed(300, mc.Vm(2, "slot-0"), acquireS0(), wakeAt(2000)),
	), stepChecks{
		3: all(assignmentIs("s", "slot-0"), shardIs(0, core.ShardStatus{})),
		4: all(
			assignmentIs("s", "slot-0"),
			shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(300), Wanted: true}),
		),
	})
}

// TestRetryAtAtTheClockCeiling checks that RetryAt follows the rule of timers near the highest
// reading the clock takes: it fits in a Duration, and an armed timer is after the clock.
func TestRetryAtAtTheClockCeiling(t *testing.T) {
	const (
		retry = 2 * time.Second
		lease = 15 * time.Second
		below = maxReading - 1
	)
	wall := mc.Wall(0)
	wake := core.Wake{At: below + retry}
	idleWaiting := core.ShardStatus{Since: below, Wanted: true, RetryAt: below + retry}
	view := func(now time.Duration, v core.View, want ...core.Action) mc.Step {
		return mc.Step{Input: core.ViewChanged{Now: now, View: v, Wall: wall}, Want: want}
	}
	failedAtTheTop := func(shard int, holder string, retryAfter time.Duration) mc.Step {
		return mc.Step{
			Input: core.AcquireFailed{Now: math.MaxInt64, Space: "s", Shard: shard, Holder: holder, RetryAfter: retryAfter},
			Want:  []core.Action{wake},
		}
	}
	runSteps(t, scenarioIn(mc.C(mc.S("s", 4)),
		mc.Step{Input: core.Tick{Now: below}, Want: []core.Action{core.KeepAlive{}, wake}},
		mc.Step{
			Input: core.SessionRenewed{Now: below, SentAt: below},
			Want:  []core.Action{core.ExtendDeadline{Deadline: below + 10*time.Second}, wake},
		},
		view(below, mc.Vm(1, "m1"),
			acquireS0(),
			core.Acquire{Space: "s", Shard: 1},
			core.Acquire{Space: "s", Shard: 2},
			core.Acquire{Space: "s", Shard: 3},
			wake,
		),
		view(below, mc.Vm(2, "m2"), wake),
		mc.Step{Input: core.Acquired{Now: below, Space: "s", Term: 0}, Want: []core.Action{wake}},
		view(below, mc.Vm(3, "m1"), wake),
		mc.Step{Input: core.Tick{Now: math.MaxInt64}, Want: []core.Action{wake}},
		failedAtTheTop(1, "h2", math.MaxInt64),
		failedAtTheTop(2, "h3", math.MinInt64),
		mc.Step{Input: core.Acquired{Now: math.MaxInt64, Space: "s", Shard: 3, Term: 0}, Want: []core.Action{wake}},
	), stepChecks{
		6:  shardIs(0, idleWaiting),
		7:  shardIs(0, idleWaiting),
		8:  shardIs(1, core.ShardStatus{Since: maxReading, Holder: "h2", Wanted: true, RetryAt: maxReading + lease}),
		9:  shardIs(2, core.ShardStatus{Since: maxReading, Holder: "h3", Wanted: true, RetryAt: maxReading + retry}),
		10: shardIs(3, core.ShardStatus{Since: maxReading, Wanted: true, RetryAt: maxReading + retry}),
	})
}

// TestAnswerTimersAtTheLargestReadings checks that the timers of the answers and of a record fit in a
// Duration and stay after the clock, with the longest timing fields too, whatever RetryAfter says, at
// readings a driver never reaches.
func TestAnswerTimersAtTheLargestReadings(t *testing.T) {
	const (
		largest = time.Duration(math.MaxInt64)
		ceiling = largest - 24*time.Hour
	)
	timings := []core.Timing{
		core.DefaultTiming,
		{LeaseDuration: 24 * time.Hour, RenewDeadline: 21 * time.Hour, RetryPeriod: 17 * time.Hour, SlotGrace: 24 * time.Hour},
	}
	readings := []time.Duration{largest, ceiling, ceiling - 1, ceiling - time.Second, ceiling - time.Hour, ceiling - 48*time.Hour}
	answers := func(now time.Duration) []core.Input {
		return []core.Input{
			core.Acquired{Now: now, Space: "s", Term: 0},
			core.Acquired{Now: now, Space: "s", Term: 9},
			core.AcquireFailed{Now: now, Space: "s", Holder: "h2", RetryAfter: math.MaxInt64},
			core.AcquireFailed{Now: now, Space: "s", Holder: "h2", RetryAfter: math.MinInt64},
			core.AcquireFailed{Now: now, Space: "s", Holder: "h2", RetryAfter: 0},
			core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 9},
			core.HolderChanged{Now: now, Space: "s", Holder: "", Term: 0},
		}
	}
	for _, timing := range timings {
		for _, now := range readings {
			for _, in := range answers(now) {
				cfg := mc.C(mc.S("s", 1))
				cfg.Timing = timing
				m := core.NewMachine(cfg)
				step := func(in core.Input) {
					t.Helper()
					got := m.Step(in)
					at, armed := m.NextWake()
					st := m.Status()
					for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
						t.Errorf("timing %v, reading %v, %+v: Step(%#v): %s", timing, now, in, in, v)
					}
				}
				step(core.Tick{})
				step(core.SessionRenewed{SentAt: 0})
				step(core.ViewChanged{View: mc.Vm(1, "m1"), Wall: mc.Wall(0)})
				step(core.SessionRenewed{Now: now, SentAt: now})
				step(in)
				st := m.Status()
				if at := st.Spaces[0].Shards[0].RetryAt; at != 0 && (at <= st.Now || at-st.Now > timing.LeaseDuration) {
					t.Errorf("timing %v, reading %v, %+v: RetryAt %v with the clock at %v", timing, now, in, at, st.Now)
				}
				step(core.Tick{Now: largest})
				step(core.HolderChanged{Now: largest, Space: "s", Holder: "h2", Term: 10})
				step(core.Tick{Now: largest})
			}
		}
	}
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
