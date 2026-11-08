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
// readings in milliseconds, and that Done means a closed session, or a lost one with every shard idle.
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
		want := state == core.SessionStateLost
		for _, space := range st.Spaces {
			for _, sh := range space.Shards {
				want = want && sh.State == core.ShardStateIdle
			}
		}
		want = want || state == core.SessionStateClosed
		if st.Done != want {
			t.Errorf("Done = %v, want %v", st.Done, want)
		}
	}
}

// drainingIs checks Draining.
func drainingIs(want bool) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		if got := m.Status().Draining; got != want {
			t.Errorf("Draining = %v, want %v", got, want)
		}
	}
}

// doneIs checks Done exactly, and that a done machine has no timer armed.
func doneIs(want bool) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		if got := m.Status().Done; got != want {
			t.Errorf("Done = %v, want %v", got, want)
		}
		if at, ok := m.NextWake(); want && (at != 0 || ok) {
			t.Errorf("NextWake() on a done machine = (%v, %v), want (0, false)", at, ok)
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

// drained is a Drain at ms milliseconds with the given result.
func drained(ms int64, want ...core.Action) mc.Step {
	return mc.Step{Input: core.Drain{Now: mc.Ms(ms)}, Want: want}
}

// sessionClosed is a SessionClosed at ms milliseconds with the given result.
func sessionClosed(ms int64, want ...core.Action) mc.Step {
	return mc.Step{Input: core.SessionClosed{Now: mc.Ms(ms)}, Want: want}
}

// stopDrainingS0 is the StopRun of shard s/0's run of term 7 because the member drains.
func stopDrainingS0() core.Action {
	return core.StopRun{Space: "s", Term: 7, Reason: core.StopDraining}
}

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

// stopExpiredS0 is the StopRun of shard s/0's run because the session deadline passed.
func stopExpiredS0(term core.Term) core.Action {
	return core.StopRun{Space: "s", Term: term, Reason: core.StopExpired}
}

// expiredS0Steps boots a machine of the one shard s/0, whose run has term 7, and lets the session
// deadline pass with a Tick at @10: five steps, with the next wake at @12 and the run stuck at @20.
func expiredS0Steps() []mc.Step {
	return append(mc.Boot(7, mc.S("s", 1)), tick(10000, core.KeepAlive{}, stopExpiredS0(7), wakeAt(12000)))
}

// expiredS0 is the status of a wanted shard whose run of the given term was told at since milliseconds
// to stop because the session deadline passed.
func expiredS0(since int64, term core.Term) core.ShardStatus {
	return core.ShardStatus{State: core.ShardStateExpired, Since: mc.Ms(since), Term: term, Reason: core.StopExpired, Holder: "h1", Wanted: true}
}

// degradeS0 is the Degrade of shard s/0.
func degradeS0(term core.Term) core.Action { return core.Degrade{Space: "s", Term: term} }

// restoreS0 is the Restore of shard s/0.
func restoreS0(term core.Term) core.Action { return core.Restore{Space: "s", Term: term} }

// confirmS0 is the Confirm of shard s/0.
func confirmS0(term core.Term) core.Action { return core.Confirm{Space: "s", Term: term} }

// fencedS0 returns a case that is not in the shared lists, on one fenced space of one shard.
func fencedS0(steps ...mc.Step) mc.Case {
	return scenarioIn(mc.CF(mc.S("s", 1)), steps...)
}

// degradedS0Steps boots a fenced machine of the one shard s/0, whose run has term 7, and lets the
// session deadline pass at @10: five steps, with the session late and the next wake at @12.
func degradedS0Steps() []mc.Step { return mc.BootF(7, mc.S("s", 1)) }

// confirmingS0Steps goes on from degradedS0Steps with a renewal at @10.5, sent at @10.4, and so a
// Confirm: six steps, with the deadline at @20.4, the next wake at @12 and the next Confirm at @12.5.
func confirmingS0Steps() []mc.Step {
	return append(degradedS0Steps(), renewed(10500, 10400, extended(20400), confirmS0(7), wakeAt(12000)))
}

// degradedS0 is the status of a wanted shard whose run of the given term went degraded at since
// milliseconds.
func degradedS0(since int64, term core.Term) core.ShardStatus {
	return core.ShardStatus{State: core.ShardStateDegraded, Since: mc.Ms(since), Term: term, Holder: "h1", Wanted: true}
}

// confirmPendingIs checks whether shard s/0 has a Confirm pending.
func confirmPendingIs(want bool) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		if got := core.ConfirmPending(m, 0, 0); got != want {
			t.Errorf("ConfirmPending = %v, want %v", got, want)
		}
	}
}

// countActions returns how many actions of type A the steps of c return.
func countActions[A core.Action](c mc.Case) int {
	n := 0
	for _, s := range c.Steps {
		for _, a := range s.Want {
			if _, ok := a.(A); ok {
				n++
			}
		}
	}
	return n
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

// shardsAre checks every shard of Status exactly, by space name and then by shard.
func shardsAre(want ...core.ShardStatus) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		var got []core.ShardStatus
		for _, space := range m.Status().Spaces {
			got = append(got, space.Shards...)
		}
		if !slices.Equal(got, want) {
			t.Errorf("shards = %+v, want %+v", got, want)
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

func TestATMCH04ExpiryUnderItsOwnTiming(t *testing.T) {
	const late = core.SessionStateLate
	idleA := core.ShardStatus{Since: mc.Ms(26500), Holder: "h1", Wanted: true}
	acquiringA := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(41500), Holder: "h1", Wanted: true}
	stuckB := expiredS0(20000, 8)
	stuckB.Stuck = true
	runCase(t, "AT-MCH-04", stepChecks{
		10: all(
			sessionIs(19999, core.SessionStateLive, 20000, core.CauseUnreachable),
			shardsAre(
				core.ShardStatus{State: core.ShardStateOwned, Since: mc.Ms(300), Term: 7, Holder: "h1", Wanted: true},
				core.ShardStatus{State: core.ShardStateOwned, Since: mc.Ms(400), Term: 8, Holder: "h1", Wanted: true},
			),
		),
		11: all(
			sessionIs(20000, late, 20000, core.CauseUnreachable),
			shardsAre(expiredS0(20000, 7), expiredS0(20000, 8)),
		),
		15: all(
			sessionIs(26500, late, 20000, core.CauseUnreachable),
			shardsAre(idleA, expiredS0(20000, 8)),
		),
		17: shardsAre(idleA, expiredS0(20000, 8)),
		18: shardsAre(idleA, stuckB),
		20: all(
			sessionIs(41500, core.SessionStateLive, 61000, core.CauseNone),
			shardsAre(acquiringA, core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(41000), Term: 8, Reason: core.StopExpired, Holder: "h1", Wanted: true}),
		),
		22: shardsAre(
			core.ShardStatus{State: core.ShardStateOwned, Since: mc.Ms(43000), Term: 9, Holder: "h1", Wanted: true},
			core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(42000), Holder: "h1", Wanted: true},
		),
	})
}

func TestATMCH06OutageReachesTheDeadline(t *testing.T) {
	const late = core.SessionStateLate
	stoppedS1 := expiredS0(10000, 8)
	runCase(t, "AT-MCH-06", stepChecks{
		13: sessionIs(8000, core.SessionStateLive, 10000, core.CauseUnreachable),
		14: all(
			sessionIs(10000, late, 10000, core.CauseUnreachable),
			shardIs(0, expiredS0(10000, 7)),
			shardIs(1, stoppedS1),
		),
		16: shardIs(0, core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(10100), Term: 7, Reason: core.StopExpired, Holder: "h1", Wanted: true}),
		18: all(
			sessionIs(11000, late, 10000, core.CauseUnreachable),
			shardIs(0, core.ShardStatus{Since: mc.Ms(10200), Holder: "h1", Wanted: true}),
			shardIs(1, stoppedS1),
			doneIs(false),
		),
	})
}

func TestATMCH18GrantAfterTheSession(t *testing.T) {
	t.Run("a", func(t *testing.T) {
		const late = core.SessionStateLate
		released := noRunS0(10100, 9)
		released.Wanted = true
		runCase(t, "AT-MCH-18 (a)", stepChecks{
			5: all(sessionIs(10100, late, 10000, core.CauseNone), shardIs(0, released)),
			6: all(sessionIs(10200, late, 10000, core.CauseNone), shardIs(0, core.ShardStatus{Since: mc.Ms(10200), Holder: "h1", Wanted: true})),
		})
	})
	t.Run("b", func(t *testing.T) {
		const lost = core.SessionStateLost
		runCase(t, "AT-MCH-18 (b)", stepChecks{
			4: all(
				sessionIs(300, lost, 10000, core.CauseLost),
				shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}),
				doneIs(false),
			),
			5: all(
				sessionIs(400, lost, 10000, core.CauseLost),
				shardIs(0, core.ShardStatus{Since: mc.Ms(400), Wanted: true}),
				doneIs(true),
			),
		})
	})
}

func TestATMCH44LostSessionEndsInDone(t *testing.T) {
	const lost = core.SessionStateLost
	goneS0 := core.ShardStatus{Since: mc.Ms(1300), Holder: "h1", Wanted: true}
	goneS1 := core.ShardStatus{Since: mc.Ms(1200), Holder: "h2", Wanted: true}
	answeredS2 := core.ShardStatus{Since: mc.Ms(1100), Wanted: true}
	lostS1 := core.ShardStatus{State: core.ShardStateLost, Since: mc.Ms(400), Term: 8, Reason: core.StopLost, Holder: "h2", Wanted: true}
	stoppedS0 := lostS0(1000, "h1")
	waiting := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}
	runCase(t, "AT-MCH-44", stepChecks{
		7:  all(sessionIs(1000, lost, 10000, core.CauseLost), shardsAre(stoppedS0, lostS1, waiting), doneIs(false)),
		8:  all(shardsAre(stoppedS0, lostS1, answeredS2), doneIs(false)),
		9:  all(shardsAre(stoppedS0, goneS1, answeredS2), doneIs(false)),
		10: all(sessionIs(1300, lost, 10000, core.CauseLost), shardsAre(goneS0, goneS1, answeredS2), doneIs(true)),
		11: all(sessionIs(60000, lost, 10000, core.CauseLost), shardsAre(goneS0, goneS1, answeredS2), doneIs(true)),
	})
	for i, s := range mc.Get("AT-MCH-44").Steps {
		for _, a := range s.Want {
			switch a.(type) {
			case core.Release, core.CloseSession:
				t.Errorf("step %d returns %#v", i+1, a)
			}
		}
	}
}

func TestATMCH08AnsweredConfirmRestores(t *testing.T) {
	const (
		late = core.SessionStateLate
		live = core.SessionStateLive
		none = core.CauseNone
	)
	degraded := degradedS0(10000, 7)
	runCase(t, "AT-MCH-08", stepChecks{
		4: shardIs(0, ownedS0(300, 7)),
		5: all(sessionIs(10000, late, 10000, none), shardIs(0, degraded)),
		6: all(sessionIs(11000, late, 10000, none), shardIs(0, degraded)),
		7: all(sessionIs(12000, late, 10000, none), shardIs(0, degraded)),
		8: all(sessionIs(12100, live, 22000, none), shardIs(0, degraded)),
		9: all(sessionIs(12300, live, 22000, none), shardIs(0, ownedS0(12300, 7))),
	})
	c := mc.Get("AT-MCH-08")
	if n := countActions[core.StopRun](c); n != 0 {
		t.Errorf("the case returns %d StopRuns, want none", n)
	}
	if n := countActions[core.Restore](c); n != 1 {
		t.Errorf("the case returns %d Restores, want 1", n)
	}
}

func TestATMCH09AnotherHolderStopsADegradedRun(t *testing.T) {
	const late = core.SessionStateLate
	idle := core.ShardStatus{Since: mc.Ms(11000), Holder: "h2", Wanted: true}
	runCase(t, "AT-MCH-09", stepChecks{
		5: shardIs(0, degradedS0(10000, 7)),
		6: all(sessionIs(10500, late, 10000, core.CauseNone), shardIs(0, lostS0(10500, "h2"))),
		7: all(sessionIs(11000, late, 10000, core.CauseNone), shardIs(0, idle)),
		8: all(sessionIs(12000, late, 10000, core.CauseNone), shardIs(0, idle)),
	})
	if n := countActions[core.Release](mc.Get("AT-MCH-09")); n != 0 {
		t.Errorf("the case returns %d Releases, want none", n)
	}
}

func TestATMCH10LostSessionEndsADegradedRun(t *testing.T) {
	const lost = core.SessionStateLost
	idle := core.ShardStatus{Since: mc.Ms(11500), Holder: "h1", Wanted: true}
	runCase(t, "AT-MCH-10", stepChecks{
		5: all(shardIs(0, degradedS0(10000, 7)), doneIs(false)),
		6: all(sessionIs(11000, lost, 10000, core.CauseLost), shardIs(0, lostS0(11000, "h1")), doneIs(false)),
		7: all(sessionIs(11500, lost, 10000, core.CauseLost), shardIs(0, idle), doneIs(true)),
		8: all(sessionIs(60000, lost, 10000, core.CauseLost), shardIs(0, idle), doneIs(true)),
	})
	if n := countActions[core.Release](mc.Get("AT-MCH-10")); n != 0 {
		t.Errorf("the case returns %d Releases, want none", n)
	}
}

func TestATMCH11ConfirmCadenceEndsLate(t *testing.T) {
	const (
		late = core.SessionStateLate
		live = core.SessionStateLive
		none = core.CauseNone
	)
	degraded := degradedS0(10000, 7)
	runCase(t, "AT-MCH-11", stepChecks{
		7:  all(sessionIs(12100, live, 22000, none), shardIs(0, degraded)),
		15: all(sessionIs(20100, live, 22000, none), shardIs(0, degraded)),
		16: all(sessionIs(22000, late, 22000, none), shardIs(0, degraded)),
		17: all(sessionIs(22100, late, 22000, none), shardIs(0, degraded), confirmPendingIs(false)),
	})
	c := mc.Get("AT-MCH-11")
	if n := countActions[core.Confirm](c); n != 5 {
		t.Errorf("the case returns %d Confirms, want 5", n)
	}
	if n := countActions[core.Restore](c) + countActions[core.StopRun](c); n != 0 {
		t.Errorf("the case returns %d Restores and StopRuns, want none", n)
	}
}

// drainingS0 is the status of an unwanted shard whose run of the given term was told at since
// milliseconds to stop because the member drains.
func drainingS0(since int64, term core.Term) core.ShardStatus {
	sh := stoppingS0(since, term)
	sh.Reason = core.StopDraining
	return sh
}

// releasingDrainedS0 is the status of an unwanted shard whose drained run returned, and which has been
// releasing its term since since milliseconds.
func releasingDrainedS0(since int64, term core.Term) core.ShardStatus {
	sh := releasingS0(since, term)
	sh.Reason = core.StopDraining
	return sh
}

// idleS0 is the status of an idle, unwanted shard that this member last held, since since
// milliseconds.
func idleS0(since int64) core.ShardStatus {
	return core.ShardStatus{Since: mc.Ms(since), Holder: "h1"}
}

// slotIs checks Slot: the held slot, or -1 for none.
func slotIs(want int) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		if got := m.Status().Slot; got != want {
			t.Errorf("Slot = %d, want %d", got, want)
		}
	}
}

// nextWakeIs checks NextWake: a timer at ms milliseconds, or none for 0.
func nextWakeIs(ms int64) func(*testing.T, *core.Machine) {
	return func(t *testing.T, m *core.Machine) {
		t.Helper()
		at, armed := m.NextWake()
		if want := ms != 0; at != mc.Ms(ms) || armed != want {
			t.Errorf("NextWake() = (%v, %v), want (%v, %v)", at, armed, mc.Ms(ms), want)
		}
	}
}

// closedAt checks a closed machine whose clock stopped at ms milliseconds: Done, with no timer.
func closedAt(ms, deadline int64) func(*testing.T, *core.Machine) {
	return all(sessionIs(ms, core.SessionStateClosed, deadline, core.CauseNone), doneIs(true), nextWakeIs(0))
}

// closeSessionsBefore returns how many CloseSessions the steps of c return before step n, counting
// from 1.
func closeSessionsBefore(c mc.Case, n int) int {
	return countActions[core.CloseSession](mc.Case{Steps: c.Steps[:n-1]})
}

func TestATMCH12DrainADegradedRun(t *testing.T) {
	const (
		late    = core.SessionStateLate
		live    = core.SessionStateLive
		closing = core.SessionStateClosing
		none    = core.CauseNone
	)
	t.Run("a", func(t *testing.T) {
		runCase(t, "AT-MCH-12 (a)", stepChecks{
			5:  all(sessionIs(10000, late, 10000, none), shardIs(0, degradedS0(10000, 7)), drainingIs(false)),
			6:  all(sessionIs(10500, late, 10000, none), shardIs(0, drainingS0(10500, 7)), drainingIs(true)),
			8:  all(sessionIs(12500, late, 10000, none), shardIs(0, releasingDrainedS0(12500, 7))),
			10: all(sessionIs(14500, late, 10000, none), shardIs(0, releasingDrainedS0(12500, 7))),
			11: all(sessionIs(15000, closing, 10000, none), shardIs(0, idleS0(15000)), nextWakeIs(17000)),
			12: all(closedAt(15500, 10000), shardIs(0, idleS0(15000))),
		})
		if n := closeSessionsBefore(mc.Get("AT-MCH-12 (a)"), 11); n != 0 {
			t.Errorf("%d CloseSessions before the Release is answered, want none", n)
		}
	})
	t.Run("b", func(t *testing.T) {
		runCase(t, "AT-MCH-12 (b)", stepChecks{
			6:  all(sessionIs(10500, live, 20400, none), shardIs(0, degradedS0(10000, 7)), confirmPendingIs(true)),
			7:  all(sessionIs(11000, live, 20400, none), shardIs(0, drainingS0(11000, 7)), drainingIs(true)),
			8:  shardIs(0, drainingS0(11000, 7)),
			10: all(sessionIs(12100, live, 22000, none), shardIs(0, drainingS0(11000, 7))),
			11: shardIs(0, releasingDrainedS0(12500, 7)),
			12: all(sessionIs(12600, closing, 22000, none), shardIs(0, idleS0(12600)), nextWakeIs(14600)),
			13: closedAt(13000, 22000),
		})
		c := mc.Get("AT-MCH-12 (b)")
		if n := countActions[core.Confirm](c); n != 1 {
			t.Errorf("the case returns %d Confirms, want 1", n)
		}
		if n := countActions[core.Restore](c); n != 0 {
			t.Errorf("the case returns %d Restores, want none", n)
		}
	})
}

func TestATMCH21StaleInputsWhileClosing(t *testing.T) {
	const closing = core.SessionStateClosing
	checks := stepChecks{}
	// Steps 7 to 18: the step that starts closing at @1.4, then the eleven stale inputs from @1.5 on.
	for n := 7; n <= 18; n++ {
		now := int64(1400)
		if n > 7 {
			now = 1500 + int64(n-8)*100
		}
		checks[n] = all(
			sessionIs(now, closing, 10000, core.CauseNone),
			shardIs(0, idleS0(1400)),
			drainingIs(true),
			viewVersionIs(1),
			assignmentIs("s", "m1"),
			nextWakeIs(3400),
		)
	}
	// Steps 19 to 32: the SessionClosed at @2.5, then every input after it.
	for n := 19; n <= 32; n++ {
		checks[n] = all(closedAt(2500, 10000), shardIs(0, idleS0(1400)), drainingIs(true), viewVersionIs(1))
	}
	runCase(t, "AT-MCH-21", checks)
	c := mc.Get("AT-MCH-21")
	if n := countActions[core.CloseSession](c); n != 1 {
		t.Errorf("the case returns %d CloseSessions, want 1", n)
	}
	for i, s := range c.Steps[7:] {
		for _, a := range s.Want {
			if _, ok := a.(core.Wake); !ok {
				t.Errorf("step %d returns %#v", i+8, a)
			}
		}
	}
}

func TestATMCH25CloseSessionRepeatsUntilClosed(t *testing.T) {
	const closing = core.SessionStateClosing
	runCase(t, "AT-MCH-25", stepChecks{
		5:  all(sessionIs(1000, core.SessionStateLive, 10000, core.CauseNone), shardIs(0, drainingS0(1000, 7)), drainingIs(true)),
		7:  all(sessionIs(1400, closing, 10000, core.CauseNone), shardIs(0, idleS0(1400)), nextWakeIs(3400), doneIs(false)),
		8:  all(sessionIs(2000, closing, 10000, core.CauseNone), nextWakeIs(3400)),
		9:  all(sessionIs(3400, closing, 10000, core.CauseNone), nextWakeIs(5400)),
		10: closedAt(3500, 10000),
		11: closedAt(3500, 10000),
	})
	c := mc.Get("AT-MCH-25")
	if n := countActions[core.CloseSession](c); n != 2 {
		t.Errorf("the case returns %d CloseSessions, want 2", n)
	}
	if n := countActions[core.KeepAlive](mc.Case{Steps: c.Steps[6:]}); n != 0 {
		t.Errorf("the case returns %d KeepAlives from the step that starts closing on, want none", n)
	}
}

func TestATMCH27DrainWithAStuckRun(t *testing.T) {
	const live = core.SessionStateLive
	stuck := drainingS0(1000, 7)
	stuck.Stuck = true
	runCase(t, "AT-MCH-27", stepChecks{
		5:  all(sessionIs(1000, live, 10000, core.CauseNone), shardIs(0, drainingS0(1000, 7))),
		15: all(sessionIs(10100, live, 20000, core.CauseNone), shardIs(0, drainingS0(1000, 7)), nextWakeIs(11000)),
		16: all(sessionIs(11000, live, 20000, core.CauseNone), shardIs(0, stuck), nextWakeIs(12000)),
		18: all(sessionIs(12100, live, 22000, core.CauseNone), shardIs(0, stuck)),
		19: all(sessionIs(13000, live, 22000, core.CauseNone), shardIs(0, releasingDrainedS0(13000, 7))),
		20: all(sessionIs(13200, core.SessionStateClosing, 22000, core.CauseNone), shardIs(0, idleS0(13200)), nextWakeIs(15200)),
		21: closedAt(13500, 22000),
	})
	if n := closeSessionsBefore(mc.Get("AT-MCH-27"), 20); n != 0 {
		t.Errorf("%d CloseSessions while the run is going or its Release is out, want none", n)
	}
}

func TestATMCH28DrainLateOrLost(t *testing.T) {
	t.Run("a", func(t *testing.T) {
		const late = core.SessionStateLate
		expired := expiredS0(10000, 7)
		expired.Wanted = false
		releasing := core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(11000), Term: 7, Reason: core.StopExpired, Holder: "h1"}
		runCase(t, "AT-MCH-28 (a)", stepChecks{
			6:  all(sessionIs(10500, late, 10000, core.CauseNone), shardIs(0, expired), drainingIs(true)),
			7:  all(sessionIs(11000, late, 10000, core.CauseNone), shardIs(0, releasing)),
			9:  all(sessionIs(13000, late, 10000, core.CauseNone), shardIs(0, releasing)),
			10: all(sessionIs(13500, core.SessionStateClosing, 10000, core.CauseNone), shardIs(0, idleS0(13500)), nextWakeIs(15500)),
			11: all(sessionIs(14000, core.SessionStateClosing, 10000, core.CauseNone), nextWakeIs(15500)),
			12: closedAt(14500, 10000),
		})
		c := mc.Get("AT-MCH-28 (a)")
		if n := countActions[core.StopRun](c); n != 1 {
			t.Errorf("the case returns %d StopRuns, want 1", n)
		}
		if n := closeSessionsBefore(c, 10); n != 0 {
			t.Errorf("%d CloseSessions before the Release is answered, want none", n)
		}
	})
	t.Run("b", func(t *testing.T) {
		const late = core.SessionStateLate
		expired := expiredS0(10000, 7)
		expired.Wanted = false
		runCase(t, "AT-MCH-28 (b)", stepChecks{
			5:  all(sessionIs(400, core.SessionStateLive, 10000, core.CauseNone), slotIs(0), shardIs(0, ownedS0(400, 7))),
			6:  all(sessionIs(10000, late, 10000, core.CauseNone), slotIs(0), shardIs(0, expiredS0(10000, 7))),
			7:  all(slotIs(0), shardIs(0, expired), drainingIs(true)),
			10: all(sessionIs(12500, late, 10000, core.CauseNone), slotIs(0), shardIs(0, idleS0(12500)), nextWakeIs(14000)),
			12: all(sessionIs(14500, late, 10000, core.CauseNone), slotIs(0), nextWakeIs(16000)),
			13: all(sessionIs(15000, core.SessionStateClosing, 10000, core.CauseNone), slotIs(-1), nextWakeIs(17000)),
			14: all(closedAt(15500, 10000), slotIs(-1)),
		})
		c := mc.Get("AT-MCH-28 (b)")
		if n := countActions[core.ReleaseSlot](c); n != 2 {
			t.Errorf("the case returns %d ReleaseSlots, want 2", n)
		}
		if n := countActions[core.ReleaseSlot](mc.Case{Steps: c.Steps[:9]}); n != 0 {
			t.Errorf("%d ReleaseSlots before the shard's Release is answered, want none", n)
		}
		if n := closeSessionsBefore(c, 13); n != 0 {
			t.Errorf("%d CloseSessions before the slot is released, want none", n)
		}
	})
	t.Run("c", func(t *testing.T) {
		const lost = core.SessionStateLost
		idle := idleS0(2000)
		runCase(t, "AT-MCH-28 (c)", stepChecks{
			5:  all(sessionIs(1000, core.SessionStateLive, 10000, core.CauseNone), drainingIs(true)),
			6:  all(sessionIs(1500, lost, 10000, core.CauseLost), shardIs(0, drainingS0(1000, 7)), doneIs(false)),
			7:  all(sessionIs(2000, lost, 10000, core.CauseLost), shardIs(0, idle), doneIs(true)),
			8:  all(sessionIs(11000, lost, 10000, core.CauseLost), shardIs(0, idle), doneIs(true)),
			10: all(sessionIs(12000, lost, 10000, core.CauseLost), shardIs(0, idle), doneIs(true), drainingIs(true)),
		})
		c := mc.Get("AT-MCH-28 (c)")
		if n := countActions[core.CloseSession](c) + countActions[core.Release](c); n != 0 {
			t.Errorf("the case returns %d CloseSessions and Releases, want none", n)
		}
		if n := countActions[core.StopRun](c); n != 1 {
			t.Errorf("the case returns %d StopRuns, want 1", n)
		}
	})
}

// slotS1 is the status of shard s/1 in the given state since since milliseconds, in term, with the
// shard wanted, as the run of slot-0 among two or three slots.
func slotS1(state core.ShardState, since int64, term core.Term) core.ShardStatus {
	sh := core.ShardStatus{State: state, Since: mc.Ms(since), Term: term, Wanted: true}
	if state == core.ShardStateOwned {
		sh.Holder = "h1"
	}
	return sh
}

// slotsAre checks Slot after each of the steps first to last, counting from 1.
func slotsAre(checks stepChecks, first, last, slot int) {
	for n := first; n <= last; n++ {
		check := slotIs(slot)
		if prior, ok := checks[n]; ok {
			check = all(prior, check)
		}
		checks[n] = check
	}
}

func TestATMCH26DrainWithASlot(t *testing.T) {
	const live = core.SessionStateLive
	checks := stepChecks{
		5:  all(sessionIs(400, live, 10000, core.CauseNone), shardIs(0, ownedS0(400, 7)), assignmentIs("s", "slot-0")),
		6:  all(shardIs(0, drainingS0(1000, 7)), drainingIs(true)),
		7:  shardIs(0, releasingDrainedS0(1200, 7)),
		8:  all(sessionIs(1400, live, 10000, core.CauseNone), shardIs(0, idleS0(1400)), nextWakeIs(2000)),
		9:  all(sessionIs(1600, core.SessionStateClosing, 10000, core.CauseNone), nextWakeIs(3600)),
		11: closedAt(2500, 10000),
	}
	slotsAre(checks, 4, 8, 0)
	slotsAre(checks, 9, 11, -1)
	runCase(t, "AT-MCH-26", checks)
	c := mc.Get("AT-MCH-26")
	if n := countActions[core.ReleaseSlot](c); n != 1 {
		t.Errorf("the case returns %d ReleaseSlots, want 1", n)
	}
	if n := closeSessionsBefore(c, 9); n != 0 {
		t.Errorf("%d CloseSessions before the slot is released, want none", n)
	}
}

func TestATMCH30DrainWithAStuckRunAndASlot(t *testing.T) {
	const live = core.SessionStateLive
	stuck := drainingS0(1000, 7)
	stuck.Stuck = true
	checks := stepChecks{
		16: all(sessionIs(10100, live, 20000, core.CauseNone), shardIs(0, drainingS0(1000, 7)), nextWakeIs(11000)),
		17: all(sessionIs(11000, live, 20000, core.CauseNone), shardIs(0, stuck), nextWakeIs(12000)),
		18: all(sessionIs(11200, live, 20000, core.CauseNone), shardIs(0, stuck), nextWakeIs(12000)),
		21: all(sessionIs(13000, live, 22000, core.CauseNone), shardIs(0, releasingDrainedS0(13000, 7))),
		22: all(sessionIs(13200, core.SessionStateClosing, 22000, core.CauseNone), shardIs(0, idleS0(13200)), nextWakeIs(15200)),
		23: closedAt(13500, 22000),
	}
	slotsAre(checks, 4, 17, 0)
	slotsAre(checks, 18, 23, -1)
	runCase(t, "AT-MCH-30", checks)
	c := mc.Get("AT-MCH-30")
	if n := countActions[core.ReleaseSlot](mc.Case{Steps: c.Steps[:16]}); n != 0 {
		t.Errorf("%d ReleaseSlots while the run is stopping and not stuck, want none", n)
	}
	if n := closeSessionsBefore(c, 22); n != 0 {
		t.Errorf("%d CloseSessions while the run is going or its Release is out, want none", n)
	}
}

func TestATMCH31StandbyClaimsOnTheCadence(t *testing.T) {
	const live = core.SessionStateLive
	checks := stepChecks{
		3: all(assignmentIs("s", "slot-0"), shardIs(0, core.ShardStatus{}), nextWakeIs(2000)),
		4: nextWakeIs(2200),
		6: all(sessionIs(2200, live, 12000, core.CauseNone), nextWakeIs(4000)),
		7: all(viewVersionIs(2), shardIs(0, core.ShardStatus{}), nextWakeIs(4000)),
		8: all(sessionIs(3200, live, 12000, core.CauseNone), shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(3200), Wanted: true})),
		9: all(shardIs(0, ownedS0(3300, 7)), nextWakeIs(4000)),
	}
	slotsAre(checks, 1, 7, -1)
	slotsAre(checks, 8, 9, 0)
	runCase(t, "AT-MCH-31", checks)
	if n := countActions[core.ClaimSlot](mc.Get("AT-MCH-31")); n != 2 {
		t.Errorf("the case returns %d ClaimSlots, want 2", n)
	}
}

func TestATMCH32ClaimAnswersItCannotKeep(t *testing.T) {
	t.Run("a", func(t *testing.T) {
		checks := stepChecks{
			5:  all(sessionIs(1500, core.SessionStateLive, 10000, core.CauseNone), nextWakeIs(2000)),
			7:  nextWakeIs(3500),
			8:  nextWakeIs(4000),
			11: nextWakeIs(4200),
			12: all(shardIs(0, core.ShardStatus{}), nextWakeIs(6000)),
		}
		slotsAre(checks, 1, 12, -1)
		runCase(t, "AT-MCH-32 (a)", checks)
		c := mc.Get("AT-MCH-32 (a)")
		if n := countActions[core.ClaimSlot](c); n != 3 {
			t.Errorf("the case returns %d ClaimSlots, want 3", n)
		}
		if n := countActions[core.ReleaseSlot](c); n != 2 {
			t.Errorf("the case returns %d ReleaseSlots, want 2", n)
		}
	})
	t.Run("b", func(t *testing.T) {
		owned := shardsAre(core.ShardStatus{}, slotS1(core.ShardStateOwned, 400, 7))
		checks := stepChecks{
			3:  slotIs(-1),
			4:  all(assignmentIs("s", "slot-1", "slot-0"), shardsAre(core.ShardStatus{}, slotS1(core.ShardStateAcquiring, 300, 0))),
			6:  all(owned, nextWakeIs(2000)),
			7:  owned,
			8:  nextWakeIs(3000),
			10: nextWakeIs(4000),
			11: all(owned, nextWakeIs(4000)),
		}
		slotsAre(checks, 4, 11, 0)
		runCase(t, "AT-MCH-32 (b)", checks)
		if n := countActions[core.ReleaseSlot](mc.Get("AT-MCH-32 (b)")); n != 2 {
			t.Errorf("the case returns %d ReleaseSlots, want 2", n)
		}
	})
}

func TestATMCH35AnotherHolderTakesTheSlot(t *testing.T) {
	const live = core.SessionStateLive
	lost := core.ShardStatus{State: core.ShardStateLost, Since: mc.Ms(1300), Term: 7, Reason: core.StopLost, Holder: "h1"}
	releasing := core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(1400), Term: 7, Reason: core.StopLost, Holder: "h1"}
	checks := stepChecks{
		5: all(slotIs(0), shardIs(0, ownedS0(400, 7))),
		6: all(sessionIs(1300, live, 10000, core.CauseNone), drainingIs(false), shardIs(0, lost), assignmentIs("s", "slot-0")),
		7: all(sessionIs(1400, live, 10000, core.CauseNone), drainingIs(false), shardIs(0, releasing)),
		8: shardIs(0, idleS0(1500)),
		9: nextWakeIs(3300),
	}
	slotsAre(checks, 6, 9, -1)
	runCase(t, "AT-MCH-35", checks)
	c := mc.Get("AT-MCH-35")
	if n := countActions[core.Release](mc.Case{Steps: c.Steps[:6]}); n != 0 {
		t.Errorf("%d Releases before the run's RunStopped, want none", n)
	}
	if n := countActions[core.ReleaseSlot](c); n != 0 {
		t.Errorf("the case returns %d ReleaseSlots of a slot another holder has, want none", n)
	}
}

func TestATMCH36RetiredSlot(t *testing.T) {
	stopping := stoppingS0(500, 7)
	checks := stepChecks{
		4: all(assignmentIs("s", "slot-1", "slot-0"), shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(300), Wanted: true})),
		5: shardIs(0, ownedS0(400, 7)),
		6: all(assignmentIs("s", "slot-0", "slot-0"), shardsAre(stopping, core.ShardStatus{})),
		7: shardsAre(releasingS0(600, 7), core.ShardStatus{}),
		8: all(shardsAre(idleS0(700), core.ShardStatus{}), nextWakeIs(2000)),
		9: all(sessionIs(1000, core.SessionStateLive, 10000, core.CauseNone), shardsAre(idleS0(700), core.ShardStatus{}), nextWakeIs(2000)),
	}
	slotsAre(checks, 1, 3, -1)
	slotsAre(checks, 4, 8, 1)
	slotsAre(checks, 9, 9, -1)
	runCase(t, "AT-MCH-36", checks)
	c := mc.Get("AT-MCH-36")
	if n := countActions[core.ReleaseSlot](mc.Case{Steps: c.Steps[:7]}); n != 0 {
		t.Errorf("%d ReleaseSlots before the shard is released, want none", n)
	}
}

func TestATMCH38AdoptTheLowestListed(t *testing.T) {
	checks := stepChecks{
		4: all(
			viewVersionIs(2),
			assignmentIs("s", "slot-2", "slot-0", "slot-1"),
			shardsAre(core.ShardStatus{}, slotS1(core.ShardStateAcquiring, 300, 0), core.ShardStatus{}),
			nextWakeIs(2000),
		),
		6: shardsAre(core.ShardStatus{}, slotS1(core.ShardStateOwned, 500, 7), core.ShardStatus{}),
		7: nextWakeIs(2300),
		8: nextWakeIs(4000),
		9: all(shardsAre(core.ShardStatus{}, slotS1(core.ShardStateOwned, 500, 7), core.ShardStatus{}), nextWakeIs(4000)),
	}
	slotsAre(checks, 1, 3, -1)
	slotsAre(checks, 4, 9, 0)
	runCase(t, "AT-MCH-38", checks)
	if n := countActions[core.ReleaseSlot](mc.Get("AT-MCH-38")); n != 2 {
		t.Errorf("the case returns %d ReleaseSlots, want 2", n)
	}
}

// TestATMCH39SpaceOrderWhileDraining runs AT-MCH-39, with its spaces given as b then a, and its steps
// again on the spaces given as a then b, and expects equal results, Status, assignments and wakes
// after every step.
func TestATMCH39SpaceOrderWhileDraining(t *testing.T) {
	c := mc.Get("AT-MCH-39")
	runSteps(t, c, stepChecks{
		7: all(drainingIs(true), shardsAre(drainingS0(1000, 1), drainingS0(1000, 2), drainingS0(1000, 3))),
		8: shardsAre(drainingS0(1000, 1), drainingS0(1000, 2), releasingDrainedS0(1100, 3)),
		10: all(
			sessionIs(1300, core.SessionStateLive, 10000, core.CauseNone),
			shardsAre(idleS0(1300), drainingS0(1000, 2), releasingDrainedS0(1100, 3)),
		),
		15: all(
			sessionIs(3300, core.SessionStateClosing, 10000, core.CauseNone),
			shardsAre(idleS0(1300), idleS0(3300), idleS0(3200)),
			nextWakeIs(5300),
		),
		16: closedAt(3400, 10000),
	})
	if n := closeSessionsBefore(c, 15); n != 0 {
		t.Errorf("%d CloseSessions before the last Release is answered, want none", n)
	}

	reversed := slices.Clone(c.Config.Spaces)
	slices.Reverse(reversed)
	if reversed[0].Name != "a" {
		t.Fatalf("AT-MCH-39's spaces are %+v, want b then a", c.Config.Spaces)
	}
	one, other := core.NewMachine(c.Config), core.NewMachine(mc.C(reversed...))
	for i, s := range c.Steps {
		got, gotOther := one.Step(s.Input), other.Step(s.Input)
		if !reflect.DeepEqual(got, s.Want) || !reflect.DeepEqual(gotOther, s.Want) {
			t.Fatalf("step %d: Step(%#v) = %#v and %#v, want %#v", i+1, s.Input, got, gotOther, s.Want)
		}
		if st, stOther := one.Status(), other.Status(); !reflect.DeepEqual(st, stOther) {
			t.Fatalf("step %d: Status = %+v and %+v", i+1, st, stOther)
		}
		for _, name := range []string{"a", "b"} {
			if x, y := one.Assignment(name), other.Assignment(name); !slices.Equal(x, y) {
				t.Fatalf("step %d: Assignment(%s) = %q and %q", i+1, name, x, y)
			}
		}
		at, armed := one.NextWake()
		if atOther, armedOther := other.NextWake(); at != atOther || armed != armedOther {
			t.Fatalf("step %d: NextWake = (%v, %v) and (%v, %v)", i+1, at, armed, atOther, armedOther)
		}
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
			name:  "a view with slots gives keys of slots, wants nothing of a standby, and the standby claims",
			view:  mc.Vs(1, []string{"m1/1"}, "m1"),
			want:  []core.Action{core.ClaimSlot{}, wakeAt(2000)},
			check: all(viewVersionIs(1), assignmentIs("s", "slot-0"), unwanted, slotIs(-1)),
		},
		{
			name: "a view whose Member says this member holds a slot changes nothing: only SlotHolders counts",
			view: core.View{
				Version:     1,
				Members:     []core.Member{{ID: "m1", Slot: 0}},
				Slots:       1,
				SlotHolders: []string{"m1/1"},
			},
			want:  []core.Action{core.ClaimSlot{}, wakeAt(2000)},
			check: all(assignmentIs("s", "slot-0"), unwanted, slotIs(-1)),
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
		{name: "a drain", step: drained(300, core.CloseSession{}, wakeAt(2300))},
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

	// The Acquire went out before the Drain, so its answer still comes, and the shard waits for it.
	t.Run("the member is draining", func(t *testing.T) {
		runSteps(t, scenario(append(acquiringS0(),
			drained(250, wakeAt(2000)),
			acquired(300, 7, releaseS0(7), wakeAt(2000)),
			released(400, 7, core.CloseSession{}, wakeAt(2400)),
		)...), stepChecks{
			4: all(drainingIs(true), shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200)})),
			5: all(drainingIs(true), shardIs(0, noRunS0(300, 7)), sessionIs(300, core.SessionStateLive, 10000, core.CauseNone)),
			6: all(shardIs(0, core.ShardStatus{Since: mc.Ms(400), Holder: holder}), sessionIs(400, core.SessionStateClosing, 10000, core.CauseNone)),
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

	// A grant that starts nothing does not count as a started term, whichever way it would move the
	// highest one.
	t.Run("an older term does not lower the highest started term", func(t *testing.T) {
		releasing := noRunS0(1600, 7)
		releasing.Wanted = true
		steps := append(movedS0(),
			viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
			runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
			released(1300, 7, acquireS0(), wakeAt(2000)),
			acquired(1400, 6, releaseS0(6), wakeAt(2000)),
			released(1500, 6, acquireS0(), wakeAt(2000)),
			acquired(1600, 7, releaseS0(7), wakeAt(2000)),
		)
		runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, releasing)})
	})

	t.Run("a newer term that cannot start can still start later", func(t *testing.T) {
		steps := []mc.Step{
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(2000)),
			viewed(300, mc.Vm(2, "m2"), wakeAt(2000)),
			acquired(400, 9, releaseS0(9), wakeAt(2000)),
			viewed(500, mc.Vm(3, "m1"), wakeAt(2000)),
			released(600, 9, acquireS0(), wakeAt(2000)),
			acquired(700, 9, startedS0(9, 10000), wakeAt(2000)),
		}
		runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, ownedS0(700, 9))})
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

	// The second shard makes Shard: 1 a shard the machine has, with no run to report on.
	newMachine := func(fenced bool) *core.Machine {
		if fenced {
			return core.NewMachine(mc.CF(mc.S("s", 2)))
		}
		return core.NewMachine(mc.C(mc.S("s", 2)))
	}

	// Each start leaves the shard with a live run of term 7, told to stop or not.
	starts := []struct {
		name   string
		fenced bool
		steps  []mc.Step
		at     time.Duration
		want   core.ShardStatus
	}{
		{
			name:   "degraded",
			fenced: true,
			steps:  degradedS0Steps(),
			at:     mc.Ms(10100),
			want:   degradedS0(10000, 7),
		},
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
		{
			name:  "expired",
			steps: expiredS0Steps(),
			at:    mc.Ms(10100),
			want:  expiredS0(10000, 7),
		},
		{
			name:  "expired and stuck",
			steps: append(expiredS0Steps(), keepAlive(20000, 22000)),
			at:    mc.Ms(20100),
			want: func() core.ShardStatus {
				s := expiredS0(10000, 7)
				s.Stuck = true
				return s
			}(),
		},
	}
	for _, start := range starts {
		inputs := adversaries(start.at)
		notTold := start.want.State == core.ShardStateOwned || start.want.State == core.ShardStateDegraded
		if notTold {
			// The run of an owned or degraded shard was never told to stop, so even its own term does
			// not count.
			inputs = append(inputs, core.RunStopped{Now: start.at, Space: "s", Term: 7})
		}
		for _, in := range inputs {
			t.Run(fmt.Sprintf("%s/%T%+v", start.name, in, in), func(t *testing.T) {
				m := newMachine(start.fenced)
				for _, s := range start.steps {
					m.Step(s.Input)
				}
				stepWithoutRelease(t, m, in)
				if got := m.Status().Spaces[0].Shards[0]; got != start.want {
					t.Fatalf("shard 0 after the input = %+v, want %+v", got, start.want)
				}
				if notTold {
					m.Step(core.ViewChanged{Now: start.at, View: mc.Vm(9, "m2", "m3"), Wall: mc.Wall(start.at)})
				}

				now := start.at
				for now < after {
					now += time.Second
					stepWithoutRelease(t, m, core.Tick{Now: now})
					stepWithoutRelease(t, m, core.Released{Now: now, Space: "s", Term: 7})
				}
				wantState := start.want.State
				if notTold {
					wantState = core.ShardStateStopping
				}
				sh := m.Status().Spaces[0].Shards[0]
				if sh.State != wantState || sh.Term != 7 || !sh.Stuck {
					t.Fatalf("shard 0 after the wait = %+v, want it %v in term 7 and stuck", sh, wantState)
				}

				got := m.Step(core.RunStopped{Now: now, Space: "s", Term: 7})
				if !slices.Contains(got, releaseS0(7)) {
					t.Errorf("the RunStopped returned %#v, want a Release of term 7", got)
				}
			})
		}
	}

	// A record that ends the run's claim, a larger term of this member's included, sends no Release of
	// either term while the run goes on, however long. At its RunStopped, the session being late by
	// then, a larger term of this member's is released with no run, and any other record leaves
	// nothing to release.
	for _, start := range starts {
		for _, in := range recordsOfAnotherRun(start.at) {
			t.Run(fmt.Sprintf("the claim ended/%s/%+v", start.name, in), func(t *testing.T) {
				m := newMachine(start.fenced)
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
				toldToStop := sh.State == core.ShardStateLost || sh.State == core.ShardStateStopping || sh.State == core.ShardStateExpired
				if !toldToStop || sh.Term != 7 || !sh.Stuck {
					t.Fatalf("shard 0 after the wait = %+v, want it told to stop in term 7 and stuck", sh)
				}

				if record, ok := in.(core.HolderChanged); ok && record.Holder == "h1" {
					got := m.Step(core.RunStopped{Now: now, Space: "s", Term: 7})
					if !slices.Contains(got, releaseS0(8)) {
						t.Errorf("the RunStopped returned %#v, want a Release of term 8", got)
					}
					sh = m.Status().Spaces[0].Shards[0]
					if sh.State != core.ShardStateReleasing || sh.Term != 8 || sh.Stuck || sh.Reason != core.StopNone {
						t.Fatalf("shard 0 after the RunStopped = %+v, want it releasing term 8", sh)
					}
					return
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

	// A lost session sends no Release of anything, however long the run takes and whatever arrives, and
	// the run's RunStopped ends it with none.
	for _, start := range starts {
		t.Run(fmt.Sprintf("the session lost/%s", start.name), func(t *testing.T) {
			m := newMachine(start.fenced)
			for _, s := range start.steps {
				m.Step(s.Input)
			}
			stepWithoutRelease(t, m, core.SessionFailed{Now: start.at, Cause: core.CauseLost})
			for _, in := range slices.Concat(adversaries(start.at), recordsOfAnotherRun(start.at)) {
				stepWithoutRelease(t, m, in)
			}
			now := start.at
			for now < after {
				now += time.Second
				stepWithoutRelease(t, m, core.Tick{Now: now})
				stepWithoutRelease(t, m, core.Released{Now: now, Space: "s", Term: 7})
			}
			sh := m.Status().Spaces[0].Shards[0]
			if !sh.Stuck || sh.Term != 7 {
				t.Fatalf("shard 0 after the wait = %+v, want it stuck in term 7", sh)
			}
			stepWithoutRelease(t, m, core.RunStopped{Now: now, Space: "s", Term: 7})
			sh = m.Status().Spaces[0].Shards[0]
			if sh.State != core.ShardStateIdle || sh.Term != 0 || sh.Stuck || sh.Reason != core.StopNone {
				t.Fatalf("shard 0 after the RunStopped = %+v, want it idle", sh)
			}
			stepWithoutRelease(t, m, core.Tick{Now: now + time.Minute})
		})
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

	// After a lost session the machine sends the StopRun a run is still owed and nothing else: no
	// Release, no KeepAlive, and a Wake only for a run that can still go stuck. The RunStopped of a run
	// told to stop ends in idle with nothing to send, and then the machine is done and returns nil.
	t.Run("a lost session sends only what a run is owed", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			steps []mc.Step
			at    int64
			loss  []core.Action
			wake  []core.Action
		}{
			{
				name:  "owned",
				steps: mc.Boot(7, mc.S("s", 1)),
				at:    1600,
				loss:  []core.Action{stopLostS0(), wakeAt(11600)},
				wake:  []core.Action{wakeAt(11600)},
			},
			{
				name:  "stopping",
				steps: movedS0(),
				at:    1600,
				loss:  []core.Action{wakeAt(11000)},
				wake:  []core.Action{wakeAt(11000)},
			},
			{
				name:  "expired",
				steps: expiredS0Steps(),
				at:    11000,
				loss:  []core.Action{wakeAt(20000)},
				wake:  []core.Action{wakeAt(20000)},
			},
			{
				name:  "lost",
				steps: ownedAndLostS0(),
				at:    1600,
				loss:  []core.Action{wakeAt(11000)},
				wake:  []core.Action{wakeAt(11000)},
			},
			{
				name:  "releasing",
				steps: append(movedS0(), runStopped(1500, 7, releaseS0(7), wakeAt(2000))),
				at:    1600,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				steps := append(slices.Clone(tc.steps),
					failed(tc.at, core.CauseLost, tc.loss...),
					viewed(tc.at+100, mc.Vm(5, "m3"), tc.wake...),
					released(tc.at+150, 7, tc.wake...),
					runStopped(tc.at+200, 7),
					released(tc.at+300, 7),
					tick(60000),
				)
				m := runSteps(t, scenario(steps...), stepChecks{
					len(tc.steps) + 4: doneIs(true),
				})
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
// run at all: that is the whole of the exception for a grant's no-run Release. A deadline may pass and
// the session may fail on any walk, and the same holds through both. Every other walk is on a fenced
// space, where the deadline degrades a run instead, and a degraded run is a run like any other. After a
// loss the machine returns only that step's StopRuns and then Wakes, and it is done, with nil results,
// once every shard is idle.
//
// A walk may also drain, and a SessionClosed may come at any time. Once draining, for good, nothing is
// wanted, started or acquired, and every StopRun says draining unless the run expired or was lost. No
// CloseSession goes out while any run has not reported its end, stuck or not, nor while any shard is
// busy, nor after a loss. While closing the machine returns only CloseSession and Wake, and once closed
// it returns nil and its Status, clock included, never changes. The walks are the same on every run.
//
// Half the walks, the slotted ones, have views with slots, or none, and claim answers and release
// reports of any slot. On those the machine holds at most one slot, and takes it only from a claim answer
// or the lowest slot a view lists for its holder that it is not giving back, never while draining. It
// gives the held slot up only on the answer to a ReleaseSlot of it, a view naming another holder, which
// stops every owned or degraded run of the slot as lost, or a lost session. A StartRun goes only for a
// shard of the held slot, one the view counts and the machine has not retired, or without slots for a
// member holding none. A ReleaseSlot of the held slot goes only when the member drains or the slot is
// retired, and only while every shard is idle or has a stuck run. A ClaimSlot goes only from a live
// standby that is not draining, under a view with slots. No slot action goes out after a loss, while
// closing, or in the step that starts closing, and CloseSession needs the held slot -1.
func TestNoReleaseBeforeRunStoppedOnAnyWalk(t *testing.T) {
	type run struct {
		shard int
		term  core.Term
	}
	views := [][]string{{"m1"}, {"m1", "m2"}, {"m1", "m4"}, {"m2", "m3"}}
	holders := []string{"h1", "h1", "h2", ""}

	var starts, stops, releases, repeats, restarts, stuck int
	var noRun, oldTerm, adopted, lostRuns, goneIdle, startedAfterRun, releasedAfterRun int
	var expiredRuns, lateReleases, losses, sessionLostRuns, stoppedAfterLoss, answeredAfterLoss, doneSteps int
	var degrades, restores, confirms, stoppedDegraded, lateNoRun int
	var drains, drainedRuns, stuckWhileDraining, lostWhileDraining, closings, closeRepeats, closingSteps int
	var closedSteps, earlyClosed, lateClosings int
	var claimsSent, claimsTaken, adoptions, giveBacks, heldReleases, releasesOverStuck, slotReleases int
	var slotLosses, slotLostRuns, retirements, slotStarts, drainedSlots int
	for seed := range uint64(300) {
		state := seed
		next := func(n uint64) uint64 {
			state = state*6364136223846793005 + 1442695040888963407
			return (state >> 33) % n
		}

		fenced := seed%2 == 1
		slotted := seed%4 >= 2
		cfg := mc.C(mc.S("s", 2))
		if fenced {
			cfg = mc.CF(mc.S("s", 2))
		}
		m := core.NewMachine(cfg)
		started, stopSent, stopped := map[run]bool{}, map[run]bool{}, map[run]bool{}
		runsOf := [2]int{}

		// running holds each shard's run the driver has not reported over, or 0 for none. A run is
		// over only by a RunStopped of its term after its StopRun.
		running := [2]core.Term{}
		var history []core.Input
		var now time.Duration
		version := uint64(0)
		term := core.Term(1)

		// The slot model. applied is the view the machine last applied, releaseSent says a ReleaseSlot of
		// the held slot has gone out since the machine took it, retired says a view has retired it since,
		// and givingBack holds the other slots a ReleaseSlot has gone out for that no SlotReleased has
		// answered.
		var applied core.View
		releaseSent, retired := false, false
		givingBack := map[int]bool{}
		checkSlots := func(in core.Input, prior, st core.Status, got []core.Action) {
			t.Helper()
			const lost, closing = core.SessionStateLost, core.SessionStateClosing
			view, isView := in.(core.ViewChanged)
			isView = isView && st.ViewVersion == view.View.Version && prior.Session != closing
			if isView {
				applied = view.View
			}
			if r, ok := in.(core.SlotReleased); ok && prior.Session != closing {
				delete(givingBack, r.Slot)
			}
			h, was := st.Slot, prior.Slot
			sentBefore := releaseSent
			closes, slotActions := false, false
			for _, a := range got {
				switch a.(type) {
				case core.CloseSession:
					closes = true
				case core.ClaimSlot, core.ReleaseSlot:
					slotActions = true
				}
			}
			if slotActions && (st.Session == lost || prior.Session == closing || closes) {
				t.Fatalf("seed %d: %+v with the session %v, after %+v", seed, got, prior.Session, history)
			}
			if st.Session == lost && h != -1 {
				t.Fatalf("seed %d: slot %d after the loss, after %+v", seed, h, history)
			}
			if h != was {
				if was >= 0 {
					r, isRelease := in.(core.SlotReleased)
					switch {
					case st.Session == lost:
					case isRelease && r.Slot == was && releaseSent:
						slotReleases++
						if prior.Draining {
							drainedSlots++
						}
					case isView && was < view.View.Slots && view.View.SlotHolders[was] != "" && view.View.SlotHolders[was] != "h1":
						slotLosses++
						for shard, b := range prior.Spaces[0].Shards {
							if b.State != core.ShardStateOwned && b.State != core.ShardStateDegraded {
								continue
							}
							if !slices.Contains(got, core.Action(core.StopRun{Space: "s", Shard: shard, Term: b.Term, Reason: core.StopLost})) {
								t.Fatalf("seed %d: slot %d lost with shard %d %+v, returning %+v, after %+v", seed, was, shard, b, got, history)
							}
							slotLostRuns++
						}
					default:
						t.Fatalf("seed %d: slot %d became %d, after %+v", seed, was, h, history)
					}
				}
				if h >= 0 {
					c, isClaim := in.(core.SlotClaimed)
					lowest := -1
					if isView {
						for s, holder := range view.View.SlotHolders {
							if holder == "h1" && !givingBack[s] && s != was {
								lowest = s
								break
							}
						}
					}
					switch {
					case st.Draining || givingBack[h]:
						t.Fatalf("seed %d: took slot %d while draining or giving it back, after %+v", seed, h, history)
					case isClaim && c.Slot == h && was == -1 && (applied.Slots > h || st.ViewVersion == 0 && len(applied.Members) == 0):
						claimsTaken++
					case isView && h == lowest:
						adoptions++
					default:
						t.Fatalf("seed %d: took slot %d, after %+v", seed, h, history)
					}
				}
				releaseSent, retired = false, false
			}
			if h >= 0 && isView && applied.Slots <= h {
				if !retired {
					retirements++
				}
				retired = true
			}
			for _, a := range got {
				switch v := a.(type) {
				case core.ReleaseSlot:
					if v.Slot != h {
						giveBacks++
						givingBack[v.Slot] = true
						continue
					}
					if was != h || !(st.Draining || retired) {
						t.Fatalf("seed %d: %+v of a slot taken %d, draining %v, retired %v, after %+v", seed, v, was, st.Draining, retired, history)
					}
					over := false
					for shard, sh := range st.Spaces[0].Shards {
						told := sh.State == core.ShardStateStopping || sh.State == core.ShardStateExpired || sh.State == core.ShardStateLost
						if sh.State != core.ShardStateIdle && !(told && sh.Stuck) {
							t.Fatalf("seed %d: %+v with shard %d %+v, after %+v", seed, v, shard, sh, history)
						}
						over = over || sh.Stuck
					}
					heldReleases++
					if over {
						releasesOverStuck++
					}
					releaseSent = true
				case core.ClaimSlot:
					if h != -1 || applied.Slots == 0 || st.Session != core.SessionStateLive || st.Draining {
						t.Fatalf("seed %d: %+v with Status %+v, after %+v", seed, v, st, history)
					}
					claimsSent++
				case core.StartRun:
					key := m.Assignment("s")[v.Shard]
					switch {
					case h >= 0 && (key != "slot-"+strconv.Itoa(h) || applied.Slots <= h || retired || releaseSent):
						t.Fatalf("seed %d: %+v with slot %d, key %q, after %+v", seed, v, h, key, history)
					case h < 0 && (applied.Slots != 0 || key != "m1"):
						t.Fatalf("seed %d: %+v with no slot, key %q, after %+v", seed, v, key, history)
					case h >= 0:
						slotStarts++
					}
				case core.CloseSession:
					if h != -1 || was != -1 && !sentBefore {
						t.Fatalf("seed %d: %+v while holding slot %d, after %+v", seed, v, was, history)
					}
				}
			}
			if closes || st.Session == lost {
				clear(givingBack)
			}
		}

		step := func(in core.Input) {
			history = append(history, in)
			if v, ok := in.(core.RunStopped); ok && v.Shard >= 0 && v.Shard < 2 && stopSent[run{v.Shard, v.Term}] {
				stopped[run{v.Shard, v.Term}] = true
				if running[v.Shard] == v.Term {
					running[v.Shard] = 0
				}
			}
			prior := m.Status()
			before := prior.Spaces[0].Shards
			got := m.Step(in)
			at, armed := m.NextWake()
			st := m.Status()
			for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
				t.Fatalf("seed %d: %s, after %+v", seed, v, history)
			}
			checkSlots(in, prior, st, got)
			wasLost := prior.Session == core.SessionStateLost
			isLost := st.Session == core.SessionStateLost
			if isLost && !wasLost {
				losses++
			}
			if prior.Done && got != nil {
				t.Fatalf("seed %d: a done machine returned %+v, after %+v", seed, got, history)
			}
			if st.Done {
				doneSteps++
			}
			// A session goes lost and stays lost, closing goes only to closed, and closed stays. Done
			// means a closed session, or a lost one with every shard idle.
			idle := true
			for _, sh := range st.Spaces[0].Shards {
				idle = idle && sh.State == core.ShardStateIdle
			}
			wasClosing, isClosing := prior.Session == core.SessionStateClosing, st.Session == core.SessionStateClosing
			wasClosed, isClosed := prior.Session == core.SessionStateClosed, st.Session == core.SessionStateClosed
			if st.Done != ((isLost && idle) || isClosed) || (wasLost && !isLost) ||
				(wasClosing && !isClosing && !isClosed) || (wasClosed && !isClosed) || (isClosed && !wasClosing && !wasClosed) {
				t.Fatalf("seed %d: Status %+v, after %+v", seed, st, history)
			}
			if wasClosed {
				closedSteps++
				if got != nil || !reflect.DeepEqual(st, prior) {
					t.Fatalf("seed %d: a closed machine returned %+v and moved to %+v, after %+v", seed, got, st, history)
				}
			}
			if _, ok := in.(core.SessionClosed); ok && !wasClosing && !wasClosed {
				earlyClosed++
			}
			if (isClosing || isClosed) && (!idle || running != ([2]core.Term{}) || !st.Draining) {
				t.Fatalf("seed %d: Status %+v while %v run, after %+v", seed, st, running, history)
			}
			// Draining is for good. Nothing is wanted then, and a Drain after a loss does nothing.
			if prior.Draining && !st.Draining {
				t.Fatalf("seed %d: draining ended, after %+v", seed, history)
			}
			if !prior.Draining && st.Draining {
				drains++
				if wasLost {
					t.Fatalf("seed %d: a Drain after the loss took effect, after %+v", seed, history)
				}
			}
			if isLost && !wasLost && prior.Draining {
				lostWhileDraining++
			}
			for shard, sh := range st.Spaces[0].Shards {
				if st.Draining && sh.Wanted {
					t.Fatalf("seed %d: shard %d is %+v while draining, after %+v", seed, shard, sh, history)
				}
				if st.Draining && sh.Stuck {
					stuckWhileDraining++
				}
			}
			if wasClosing {
				closingSteps++
				for _, a := range got {
					switch a.(type) {
					case core.CloseSession, core.Wake:
					default:
						t.Fatalf("seed %d: %#v while closing, after %+v", seed, a, history)
					}
				}
			}
			for _, a := range got {
				switch v := a.(type) {
				case core.CloseSession:
					// Closing makes every record takeable, so no run may be going, not even a stuck one.
					if running != ([2]core.Term{}) || !idle || !isClosing || !st.Draining {
						t.Fatalf("seed %d: CloseSession while %v run, with Status %+v, after %+v", seed, running, st, history)
					}
					closings++
					if wasClosing {
						closeRepeats++
					}
					if prior.Session == core.SessionStateLate {
						lateClosings++
					}
				case core.StartRun, core.Acquire:
					if st.Draining {
						t.Fatalf("seed %d: %#v while draining, after %+v", seed, a, history)
					}
				case core.StopRun:
					if st.Draining && v.Reason == core.StopMoved {
						t.Fatalf("seed %d: %+v while draining, after %+v", seed, v, history)
					}
					if v.Reason == core.StopDraining {
						drainedRuns++
						if !st.Draining {
							t.Fatalf("seed %d: %+v while not draining, after %+v", seed, v, history)
						}
					}
				}
			}
			if st.Done && running != ([2]core.Term{}) {
				t.Fatalf("seed %d: done while %v run, after %+v", seed, running, history)
			}
			if isLost {
				for _, a := range got {
					switch v := a.(type) {
					case core.Wake:
					case core.StopRun:
						if wasLost || v.Reason != core.StopLost {
							t.Fatalf("seed %d: %+v after the session was lost, after %+v", seed, v, history)
						}
						sessionLostRuns++
					default:
						t.Fatalf("seed %d: %#v after the session was lost, after %+v", seed, a, history)
					}
				}
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
					switch in.(type) {
					case core.HolderChanged:
						adopted++
					case core.RunStopped:
						startedAfterRun++
					}
				case core.StopRun:
					stops++
					k := run{v.Shard, v.Term}
					if !started[k] || stopSent[k] {
						t.Fatalf("seed %d: %+v of a run not running, after %+v", seed, v, history)
					}
					stopSent[k] = true
					if before[v.Shard].State == core.ShardStateDegraded {
						stoppedDegraded++
					}
					switch v.Reason {
					case core.StopLost:
						lostRuns++
					case core.StopExpired:
						expiredRuns++
						if st.Session != core.SessionStateLate || fenced {
							t.Fatalf("seed %d: %+v with the session %v, fenced %v, after %+v", seed, v, st.Session, fenced, history)
						}
					}
				case core.Degrade:
					degrades++
					if !fenced || running[v.Shard] != v.Term {
						t.Fatalf("seed %d: %+v with term %d running, fenced %v, after %+v", seed, v, running[v.Shard], fenced, history)
					}
				case core.Restore:
					restores++
					if running[v.Shard] != v.Term {
						t.Fatalf("seed %d: %+v with term %d running, after %+v", seed, v, running[v.Shard], history)
					}
				case core.Confirm:
					confirms++
					if running[v.Shard] != v.Term {
						t.Fatalf("seed %d: %+v with term %d running, after %+v", seed, v, running[v.Shard], history)
					}
				case core.Release:
					releases++
					if st.Session == core.SessionStateLate {
						lateReleases++
					}
					if running[v.Shard] != 0 {
						t.Fatalf("seed %d: %+v while term %d runs, before its RunStopped, after %+v",
							seed, v, running[v.Shard], history)
					}
					k := run{v.Shard, v.Term}
					if fenced && st.Session == core.SessionStateLate && !started[k] {
						lateNoRun++
					}
					if _, ok := in.(core.RunStopped); ok && before[v.Shard].Term != v.Term {
						releasedAfterRun++
					}
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
				case core.ShardStateOwned, core.ShardStateDegraded:
					if !started[k] || stopSent[k] || running[shard] != sh.Term {
						t.Fatalf("seed %d: shard %d is %+v, after %+v", seed, shard, sh, history)
					}
				case core.ShardStateStopping, core.ShardStateExpired, core.ShardStateLost:
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
					if sh.State != core.ShardStateStopping && sh.State != core.ShardStateExpired && sh.State != core.ShardStateLost {
						t.Fatalf("seed %d: shard %d is %+v, after %+v", seed, shard, sh, history)
					}
				}
				b := before[shard]
				told := b.State == core.ShardStateLost || b.State == core.ShardStateStopping || b.State == core.ShardStateExpired
				if sh.State == core.ShardStateIdle && told {
					goneIdle++
				}
				if isLost {
					if sh.RetryAt != 0 || sh.State == core.ShardStateOwned || sh.State == core.ShardStateDegraded || sh.State == core.ShardStateReleasing {
						t.Fatalf("seed %d: shard %d is %+v after the session was lost, after %+v", seed, shard, sh, history)
					}
					if sh.State == core.ShardStateIdle && told && wasLost {
						stoppedAfterLoss++
					}
					if sh.State == core.ShardStateIdle && b.State == core.ShardStateAcquiring {
						answeredAfterLoss++
					}
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
			pick := next(16)
			if slotted && (pick == 11 || pick == 12) {
				// A slot input in place of a grant: a claim answer for any slot, or the answer to a
				// ReleaseSlot, of the held slot half the time. Now and then a Drain, which a slotted walk
				// needs more often to release a held slot.
				now += time.Millisecond
				held := m.Status().Slot
				switch v := next(16); {
				case v < 6:
					step(core.SlotClaimed{Now: now, Slot: int(next(5)) - 1})
				case v < 14 && held >= 0 && next(2) == 0:
					step(core.SlotReleased{Now: now, Slot: held})
				case v < 14:
					step(core.SlotReleased{Now: now, Slot: int(next(4)) - 1})
				default:
					step(core.Drain{Now: now})
				}
				continue
			}
			switch pick {
			case 0:
				now += 100 * time.Millisecond
				step(core.Tick{Now: now})
			case 1:
				now += 2 * time.Second
				step(core.Tick{Now: now})
			case 2:
				// One draw, whose value mod 8 picks the long Tick, so a walk that never drains is the
				// walk it was before draining existed. A Drain is rare, since a drained walk mostly
				// closes, and a SessionClosed may come at any time.
				switch v := next(32); {
				case v%8 == 0:
					now += 11 * time.Second
					step(core.Tick{Now: now})
				case v == 1:
					now += time.Millisecond
					step(core.Drain{Now: now})
				case v%8 == 3:
					now += time.Millisecond
					step(core.SessionClosed{Now: now})
				}
			case 3:
				now += time.Millisecond
				step(core.SessionRenewed{Now: now, SentAt: now})
			case 4:
				version++
				now += time.Millisecond
				v := mc.Vm(version, views[next(4)]...)
				if slotted {
					// Slots or none, each slot held by this member, another or no one.
					slotHolders := []string{"h1", "h2", ""}
					var list []string
					for range next(4) {
						list = append(list, slotHolders[next(3)])
					}
					v = mc.Vs(version, list, "m1", "m2")
				}
				step(core.ViewChanged{Now: now, View: v, Wall: mc.Wall(now)})
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
			case 14:
				now += time.Millisecond
				step(core.SessionFailed{Now: now, Cause: core.CauseUnreachable})
			case 15:
				// Rare, because it is final: most of a walk should go by first, and some walks should
				// never see it.
				if next(16) == 0 {
					now += time.Millisecond
					step(core.SessionFailed{Now: now, Cause: core.CauseLost})
				}
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
		{"run stopped as expired", expiredRuns},
		{"Release while the session is late", lateReleases},
		{"lost session", losses},
		{"run stopped by a lost session", sessionLostRuns},
		{"RunStopped after a lost session", stoppedAfterLoss},
		{"answer to an Acquire after a lost session", answeredAfterLoss},
		{"done machine", doneSteps},
		{"StartRun of a term that waited for a RunStopped", startedAfterRun},
		{"Release of a term that waited for a RunStopped", releasedAfterRun},
		{"Degrade", degrades},
		{"Restore", restores},
		{"Confirm", confirms},
		{"StopRun of a degraded run", stoppedDegraded},
		{"no-run Release in a fenced space while the session is late", lateNoRun},
		{"Drain", drains},
		{"StopRun of a drained run", drainedRuns},
		{"stuck run while draining", stuckWhileDraining},
		{"lost session while draining", lostWhileDraining},
		{"CloseSession", closings},
		{"CloseSession repeat", closeRepeats},
		{"CloseSession while the session is late", lateClosings},
		{"step while closing", closingSteps},
		{"step after the session closed", closedSteps},
		{"SessionClosed before closing", earlyClosed},
		{"ClaimSlot", claimsSent},
		{"claim answer taken", claimsTaken},
		{"slot adopted from a view", adoptions},
		{"slot given back", giveBacks},
		{"ReleaseSlot of the held slot", heldReleases},
		{"ReleaseSlot of the held slot over a stuck run", releasesOverStuck},
		{"held slot released", slotReleases},
		{"held slot released while draining", drainedSlots},
		{"held slot lost to another holder", slotLosses},
		{"run stopped as lost with its slot", slotLostRuns},
		{"held slot retired", retirements},
		{"StartRun of a slot's shard", slotStarts},
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
		row{"", 9, idle, nil},
	)
	add("acquiring", acquiringS0(),
		row{"h1", 9, ownedS0(1500, 9), startedS0(9, 10000)},
		row{"h1", 0, acquiring, nil},
		row{"h2", 9, holding(acquiring, "h2"), nil},
		row{"h2", 0, holding(acquiring, "h2"), nil},
		row{"", 0, acquiring, nil},
		row{"", 9, acquiring, nil},
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
		row{"", 8, holding(stopping, ""), nil},
	)
	add("lost", ownedAndLostS0(),
		row{"h1", 7, lost, nil},
		row{"h1", 6, lost, nil},
		row{"h1", 0, lost, nil},
		row{"h1", 9, holding(lost, "h1"), nil}, // TestGrantAfterRunStopped has what comes of it.
		row{"h3", 6, lost, nil},
		row{"h3", 7, lost, nil},
		row{"h3", 9, lost, nil},
		row{"h3", 0, lost, nil},
		row{"", 0, lost, nil},
		row{"", 9, lost, nil},
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

	t.Run("a shard releasing and wanted goes idle and acquires in the same step", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			viewed(1100, mc.Vm(3, "m1"), wakeAt(2000)),
			runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
			holderChanged(1300, "", 0, acquireS0(), wakeAt(2000)),
		)...), stepChecks{
			8: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(1300), Wanted: true}),
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

	// The record arrives in the step that finds the deadline past. D1 comes before D2, so the run is
	// lost rather than expired, and it gets one StopRun.
	t.Run("on a late session too", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(10100, "h2", 8, core.KeepAlive{}, stopLostS0(), wakeAt(12100)),
			runStopped(10200, 7, wakeAt(12100)),
		)...), stepChecks{
			5: shardIs(0, lostS0(10100, "h2")),
			6: shardIs(0, core.ShardStatus{Since: mc.Ms(10200), Holder: "h2", Wanted: true}),
		})
	})
}

// TestLargerOwnTermWhileARunIsGoing checks the promise for a record naming this member in a larger
// term while a run of the smaller one is going. The run's term stays the shard's, only that term's
// RunStopped ends it, and nothing of the larger term goes out before that: no Release, no StartRun.
func TestLargerOwnTermWhileARunIsGoing(t *testing.T) {
	const larger = 9
	lost := lostS0(1000, "h1")

	t.Run("owned: told to stop, and the larger term starts only at the RunStopped", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(1000, "h1", larger, stopLostS0(), wakeAt(2000)),
			holderChanged(1100, "h1", larger, wakeAt(2000)),
			acquired(1200, larger, wakeAt(2000)),
			runStopped(1300, larger, wakeAt(2000)),
			released(1400, larger, wakeAt(2000)),
			released(1450, 7, wakeAt(2000)),
			keepAlive(2000, 4000),
			runStopped(2100, 7, startedS0(larger, 10000), wakeAt(4000)),
		)...), stepChecks{
			5:  shardIs(0, lost),
			11: shardIs(0, lost),
			12: shardIs(0, ownedS0(2100, larger)),
		})
	})

	t.Run("owned and moved: the larger term's Release comes only at the RunStopped", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(1000, "h1", larger, stopLostS0(), wakeAt(2000)),
			viewed(1100, mc.Vm(2, "m2"), wakeAt(2000)),
			runStopped(1200, 7, releaseS0(larger), wakeAt(2000)),
		)...), stepChecks{
			7: shardIs(0, noRunS0(1200, larger)),
		})
	})

	t.Run("stopping: no second StopRun, and the run's term stays", func(t *testing.T) {
		running := stoppingS0(1000, 7)
		runSteps(t, scenario(append(movedS0(),
			holderChanged(1100, "h1", larger, wakeAt(2000)),
			runStopped(1200, larger, wakeAt(2000)),
			released(1250, larger, wakeAt(2000)),
			runStopped(1300, 7, releaseS0(larger), wakeAt(2000)),
		)...), stepChecks{
			6: shardIs(0, running),
			7: shardIs(0, running),
			8: shardIs(0, running),
			9: shardIs(0, noRunS0(1300, larger)),
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

// TestGrantAfterRunStopped checks what becomes of a larger term naming this member that finds a run
// going: the run's RunStopped passes it to grant, so a wanted shard starts it and any other releases
// it with no run, and the record does not stay in this member's name. Every step's result is compared
// whole, so no Release of any term and no StartRun comes while the run is going.
func TestGrantAfterRunStopped(t *testing.T) {
	// Each start takes shard s/0 to a run of term 7 that has seen, at @1.1, the record given to reach.
	starts := []struct {
		name   string
		wanted bool

		// stop is the StopRun the record returns, for a run that has not had one.
		stop core.Action

		// stuckAt is the reading in milliseconds at which the run counts as stuck.
		stuckAt int64

		reach func(record mc.Step) []mc.Step
	}{
		{
			name:    "from owned, wanted",
			wanted:  true,
			stop:    stopLostS0(),
			stuckAt: 11100,
			reach: func(record mc.Step) []mc.Step {
				return append(mc.Boot(7, mc.S("s", 1)), record)
			},
		},
		{
			name:    "from owned, then moved",
			stop:    stopLostS0(),
			stuckAt: 11100,
			reach: func(record mc.Step) []mc.Step {
				return append(mc.Boot(7, mc.S("s", 1)), record, viewed(1150, mc.Vm(2, "m2"), wakeAt(2000)))
			},
		},
		{
			name:    "from stopping, wanted again",
			wanted:  true,
			stuckAt: 11000,
			reach: func(record mc.Step) []mc.Step {
				return append(movedS0(), viewed(1050, mc.Vm(3, "m1"), wakeAt(2000)), record)
			},
		},
		{
			name:    "from stopping, not wanted",
			stuckAt: 11000,
			reach: func(record mc.Step) []mc.Step {
				return append(movedS0(), record)
			},
		},
	}

	for _, start := range starts {
		// record is the record at @1.1, which returns the run's StopRun if it has not had one.
		record := func(holder string, term core.Term) mc.Step {
			if start.stop != nil {
				return holderChanged(1100, holder, term, start.stop, wakeAt(2000))
			}
			return holderChanged(1100, holder, term, wakeAt(2000))
		}

		// granted returns what the RunStopped at @1.5 must return for a term, and the status after it.
		granted := func(term core.Term) (core.Action, core.ShardStatus) {
			if start.wanted {
				return startedS0(term, 10000), ownedS0(1500, term)
			}
			return releaseS0(term), noRunS0(1500, term)
		}

		t.Run(start.name, func(t *testing.T) {
			t.Run("one larger term", func(t *testing.T) {
				action, status := granted(9)
				steps := append(start.reach(record("h1", 9)),
					runStopped(1500, 7, action, wakeAt(2000)),
				)
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, status)})
			})

			t.Run("a second larger term replaces it, and a smaller one does not", func(t *testing.T) {
				action, status := granted(11)
				steps := append(start.reach(record("h1", 9)),
					holderChanged(1200, "h1", 11, wakeAt(2000)),
					holderChanged(1250, "h1", 10, wakeAt(2000)),
					holderChanged(1300, "h1", 11, wakeAt(2000)),
					holderChanged(1350, "h1", 9, wakeAt(2000)),
					holderChanged(1400, "h1", 7, wakeAt(2000)),
					holderChanged(1450, "h1", 0, wakeAt(2000)),
					runStopped(1500, 7, action, wakeAt(2000)),
				)
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, status)})
			})

			t.Run("nothing moves the term before the run's RunStopped", func(t *testing.T) {
				action, status := granted(9)
				steps := append(start.reach(record("h1", 9)),
					acquired(1200, 9, wakeAt(2000)),
					released(1250, 9, wakeAt(2000)),
					runStopped(1300, 9, wakeAt(2000)),
					runStopped(1350, 8, wakeAt(2000)),
					runStopped(1400, 0, wakeAt(2000)),
					runStopped(1500, 7, action, wakeAt(2000)),
				)
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, status)})
			})

			t.Run("one StartRun or Release for the term, whatever comes after", func(t *testing.T) {
				action, status := granted(9)
				steps := append(start.reach(record("h1", 9)),
					runStopped(1500, 7, action, wakeAt(2000)),
					runStopped(1600, 7, wakeAt(2000)),
					holderChanged(1700, "h1", 9, wakeAt(2000)),
					acquired(1800, 9, wakeAt(2000)),
				)
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, status)})
			})

			// A record not older than the term, naming another holder or none, takes the term back:
			// starting or releasing it would act on a record that has moved on.
			for _, later := range []struct {
				holder string
				term   core.Term
			}{{"h2", 9}, {"h2", 10}, {"h2", 0}, {"", 0}, {"", 9}, {"", 10}} {
				t.Run(fmt.Sprintf("a record %q term %d takes the term back", later.holder, later.term), func(t *testing.T) {
					var want []core.Action
					after := core.ShardStatus{Since: mc.Ms(1500), Holder: later.holder, Wanted: start.wanted}
					if start.wanted {
						want = append(want, acquireS0())
						after.State = core.ShardStateAcquiring
					}
					steps := append(start.reach(record("h1", 9)),
						holderChanged(1200, later.holder, later.term, wakeAt(2000)),
						runStopped(1500, 7, append(want, wakeAt(2000))...),
					)
					runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, after)})
				})
			}

			t.Run("an older record of another holder does not take the term back", func(t *testing.T) {
				action, status := granted(9)
				steps := append(start.reach(record("h1", 9)),
					holderChanged(1200, "h2", 8, wakeAt(2000)),
					holderChanged(1300, "", 8, wakeAt(2000)),
					holderChanged(1400, "h2", 6, wakeAt(2000)),
					runStopped(1500, 7, action, wakeAt(2000)),
				)
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, status)})
			})

			t.Run("a larger own term after another holder still counts", func(t *testing.T) {
				action, status := granted(9)
				steps := append(start.reach(record("h2", 8)),
					holderChanged(1200, "h1", 9, wakeAt(2000)),
					runStopped(1500, 7, action, wakeAt(2000)),
				)
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, status)})
			})

			if !start.wanted {
				t.Run("the Release repeats and its answer ends it", func(t *testing.T) {
					steps := append(start.reach(record("h1", 9)),
						runStopped(1500, 7, releaseS0(9), wakeAt(2000)),
						released(1600, 7, wakeAt(2000)),
						keepAlive(2000, 3500),
						tick(3500, releaseS0(9), wakeAt(4000)),
						released(3600, 9, wakeAt(4000)),
					)
					runSteps(t, scenario(steps...), stepChecks{
						len(steps): shardIs(0, core.ShardStatus{Since: mc.Ms(3600), Holder: "h1"}),
					})
				})
			}

			t.Run("a late session at the RunStopped releases with no run", func(t *testing.T) {
				steps := append(start.reach(record("h1", 9)),
					keepAlive(10000, start.stuckAt),
					runStopped(10100, 7, releaseS0(9), wakeAt(12000)),
				)
				want := noRunS0(10100, 9)
				want.Wanted = start.wanted
				runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, want)})
			})

			// The run has its StopRun already, so the Drain stops nothing, and the session closes only
			// once the term is released.
			t.Run("draining at the RunStopped releases with no run", func(t *testing.T) {
				steps := append(start.reach(record("h1", 9)),
					drained(1450, wakeAt(2000)),
					runStopped(1500, 7, releaseS0(9), wakeAt(2000)),
					released(1600, 9, core.CloseSession{}, wakeAt(3600)),
				)
				runSteps(t, scenario(steps...), stepChecks{
					len(steps) - 1: all(drainingIs(true), shardIs(0, noRunS0(1500, 9))),
					len(steps):     sessionIs(1600, core.SessionStateClosing, 10000, core.CauseNone),
				})
			})

			t.Run("a lost session sends nothing of the term", func(t *testing.T) {
				steps := append(start.reach(record("h1", 9)),
					failed(1400, core.CauseLost, wakeAt(start.stuckAt)),
					holderChanged(1450, "h1", 10, wakeAt(start.stuckAt)),
					runStopped(1500, 7),
					tick(60000),
				)
				idle := core.ShardStatus{Since: mc.Ms(1500), Holder: "h1", Wanted: start.wanted}
				runSteps(t, scenario(steps...), stepChecks{
					len(steps) - 1: all(shardIs(0, idle), doneIs(true)),
					len(steps):     all(shardIs(0, idle), doneIs(true)),
				})
			})
		})
	}

	t.Run("a stuck run gets no Release of any term until it returns", func(t *testing.T) {
		stuck := lostS0(1000, "h1")
		stuck.Stuck, stuck.Wanted = true, false
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(1000, "h1", 9, stopLostS0(), wakeAt(2000)),
			viewed(1100, mc.Vm(2, "m2"), wakeAt(2000)),
			keepAlive(11000, 13000),
			holderChanged(11100, "h1", 12, wakeAt(13000)),
			keepAlive(13000, 15000),
			keepAlive(15000, 17000),
			runStopped(15100, 7, releaseS0(12), wakeAt(17000)),
		)...), stepChecks{
			7:  shardIs(0, func() core.ShardStatus { s := stuck; s.Holder = "h1"; return s }()),
			10: shardIs(0, stuck),
			11: shardIs(0, noRunS0(15100, 12)),
		})
	})

	t.Run("the term's own run ends as usual", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(1000, "h1", 9, stopLostS0(), wakeAt(2000)),
			runStopped(1100, 7, startedS0(9, 10000), wakeAt(2000)),
			viewed(1200, mc.Vm(2, "m2"), stopS0(9), wakeAt(2000)),
			runStopped(1300, 9, releaseS0(9), wakeAt(2000)),
		)...), stepChecks{8: shardIs(0, releasingS0(1300, 9))})
	})

	t.Run("the term does not outlive its run", func(t *testing.T) {
		// A term below the one that waited is no older than anything once the shard has no run.
		runSteps(t, scenario(append(movedS0(),
			holderChanged(1100, "h1", 9, wakeAt(2000)),
			runStopped(1200, 7, releaseS0(9), wakeAt(2000)),
			released(1300, 9, wakeAt(2000)),
			holderChanged(1400, "h2", 8, wakeAt(2000)),
			holderChanged(1500, "h1", 8, releaseS0(8), wakeAt(2000)),
		)...), stepChecks{
			9:  shardIs(0, core.ShardStatus{Since: mc.Ms(1300), Holder: "h2"}),
			10: shardIs(0, noRunS0(1500, 8)),
		})
	})

	t.Run("after a term taken back, the next run ends as usual", func(t *testing.T) {
		runSteps(t, scenario(append(movedS0(),
			viewed(1050, mc.Vm(3, "m1"), wakeAt(2000)),
			holderChanged(1100, "h1", 9, wakeAt(2000)),
			holderChanged(1200, "h2", 10, wakeAt(2000)),
			runStopped(1300, 7, acquireS0(), wakeAt(2000)),
			acquired(1400, 11, startedS0(11, 10000), wakeAt(2000)),
			viewed(1500, mc.Vm(4, "m2"), stopS0(11), wakeAt(2000)),
			runStopped(1600, 11, releaseS0(11), wakeAt(2000)),
		)...), nil)
	})

	t.Run("the term waits on its own shard", func(t *testing.T) {
		runSteps(t, scenarioIn(mc.C(mc.S("s", 2)),
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), core.Acquire{Space: "s"}, core.Acquire{Space: "s", Shard: 1}, wakeAt(2000)),
			mc.Grant(mc.Ms(300), "s", 0, 7, mc.Ms(10000), mc.Ms(2000)),
			mc.Grant(mc.Ms(400), "s", 1, 7, mc.Ms(10000), mc.Ms(2000)),
			holderChanged(1000, "h1", 9, stopLostS0(), wakeAt(2000)),
			mc.Step{
				Input: core.HolderChanged{Now: mc.Ms(1100), Space: "s", Shard: 1, Holder: "h2", Term: 8},
				Want:  []core.Action{core.StopRun{Space: "s", Shard: 1, Term: 7, Reason: core.StopLost}, wakeAt(2000)},
			},
			mc.Step{
				Input: core.RunStopped{Now: mc.Ms(1200), Space: "s", Shard: 1, Term: 7},
				Want:  []core.Action{core.Acquire{Space: "s", Shard: 1}, wakeAt(2000)},
			},
			runStopped(1300, 7, startedS0(9, 10000), wakeAt(2000)),
		), nil)
	})

	t.Run("the timers fit at the largest reading", func(t *testing.T) {
		const largest = time.Duration(math.MaxInt64)
		wake := core.Wake{At: largest - 24*time.Hour + 2*time.Second}
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(1000, "h1", 9, stopLostS0(), wakeAt(2000)),
			mc.Step{Input: core.Tick{Now: largest}, Want: []core.Action{core.KeepAlive{}, wake}},
			mc.Step{Input: core.RunStopped{Now: largest, Space: "s", Term: 7}, Want: []core.Action{releaseS0(9), wake}},
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

			// The session is never live at the clock's ceiling, so there the deadline stops the run
			// before the view can move it.
			stop, stopped := stopExpiredS0(7), step(core.SessionRenewed{Now: now, SentAt: now})
			moved := step(core.ViewChanged{Now: now, View: mc.Vm(2, "m2"), Wall: mc.Wall(0)})
			if now < ceiling {
				stop, stopped = stopS0(7), moved
			}
			if !slices.Contains(stopped, stop) {
				t.Errorf("timing %v, reading %v: the step returned %#v, want %#v", timing, now, stopped, stop)
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

// TestExpiryAndLossTimersAtTheLargestReadings checks that the timers of an expired run and of a lost
// session fit in a Duration and stay after the clock, with the longest timing fields too, at readings a
// driver never reaches.
func TestExpiryAndLossTimersAtTheLargestReadings(t *testing.T) {
	const (
		largest = time.Duration(math.MaxInt64)
		ceiling = largest - 24*time.Hour
	)
	timings := []core.Timing{
		core.DefaultTiming,
		{LeaseDuration: 24 * time.Hour, RenewDeadline: 21 * time.Hour, RetryPeriod: 17 * time.Hour, SlotGrace: 24 * time.Hour},
	}
	readings := []time.Duration{largest, ceiling, ceiling - 1, ceiling - time.Second, ceiling - time.Hour, ceiling - 48*time.Hour}
	firsts := []struct {
		name string
		in   func(now time.Duration) core.Input
		want core.Action
	}{
		{
			name: "expired",
			in:   func(now time.Duration) core.Input { return core.Tick{Now: now} },
			want: stopExpiredS0(7),
		},
		{
			name: "lost",
			in:   func(now time.Duration) core.Input { return core.SessionFailed{Now: now, Cause: core.CauseLost} },
			want: stopLostS0(),
		},
	}
	for _, timing := range timings {
		for _, now := range readings {
			for _, first := range firsts {
				cfg := mc.C(mc.S("s", 1))
				cfg.Timing = timing
				m := core.NewMachine(cfg)
				step := func(in core.Input) []core.Action {
					t.Helper()
					got := m.Step(in)
					at, armed := m.NextWake()
					st := m.Status()
					for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
						t.Errorf("%s, timing %v, reading %v: Step(%#v): %s", first.name, timing, now, in, v)
					}
					return got
				}
				for _, s := range mc.Boot(7, mc.S("s", 1)) {
					step(s.Input)
				}
				if got := step(first.in(now)); !slices.Contains(got, first.want) {
					t.Errorf("%s, timing %v, reading %v: the first input returned %#v, want %#v", first.name, timing, now, got, first.want)
				}
				at, armed := m.NextWake()
				if st := m.Status(); !armed || at-st.Now > max(timing.RetryPeriod, timing.RenewDeadline) {
					t.Errorf("%s, timing %v, reading %v: NextWake() = (%v, %v) with the clock at %v", first.name, timing, now, at, armed, st.Now)
				}
				step(core.Tick{Now: largest})
				step(core.SessionFailed{Now: largest, Cause: core.CauseLost})
				step(core.Tick{Now: largest})
				if !m.Status().Spaces[0].Shards[0].Stuck && m.Status().Now-now >= timing.RenewDeadline {
					t.Errorf("%s, timing %v, reading %v: shard = %+v, want it stuck", first.name, timing, now, m.Status().Spaces[0].Shards[0])
				}
				if got := step(core.RunStopped{Now: largest, Space: "s", Term: 7}); got != nil {
					t.Errorf("%s, timing %v, reading %v: the RunStopped returned %#v, want nil", first.name, timing, now, got)
				}
				if !m.Status().Done {
					t.Errorf("%s, timing %v, reading %v: Status = %+v, want Done", first.name, timing, now, m.Status())
				}
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
			drained(1000, stopDrainingS0(), wakeAt(2000)),
			viewed(1200, mc.Vm(2, "m1"), wakeAt(2000)),
			runStopped(1500, 7, releaseS0(7), wakeAt(2000)),
		)
		releasing := releasingS0(1500, 7)
		releasing.Reason = core.StopDraining
		runSteps(t, scenario(steps...), stepChecks{
			5: shardIs(0, draining),
			6: all(shardIs(0, draining), assignmentIs("s", "m1")),
			7: shardIs(0, releasing),
		})
	})

	// A degraded shard is the only one whose run has no StopRun while the session is late.
	t.Run("a late session stops a degraded run too", func(t *testing.T) {
		steps := append(degradedS0Steps(), viewed(10100, mc.Vm(2, "m2"), stopS0(7), wakeAt(12000)))
		runSteps(t, fencedS0(steps...), stepChecks{6: shardIs(0, stoppingS0(10100, 7))})
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

// TestExpiry checks D2: an owned shard of an unfenced space stops at the session deadline itself, and
// goes on to its Release as a moved shard does, whatever the session does next.
func TestExpiry(t *testing.T) {
	const late = core.SessionStateLate

	t.Run("at exactly the deadline, not a moment before", func(t *testing.T) {
		const justBefore = 10*time.Second - 1
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			mc.Step{Input: core.Tick{Now: justBefore}, Want: []core.Action{core.KeepAlive{}, wakeAt(10000)}},
			mc.Step{Input: core.Tick{Now: mc.Ms(10000)}, Want: []core.Action{stopExpiredS0(7), core.Wake{At: justBefore + 2*time.Second}}},
		)...), stepChecks{
			5: shardIs(0, ownedS0(300, 7)),
			6: all(sessionIs(10000, late, 10000, core.CauseNone), shardIs(0, expiredS0(10000, 7))),
		})
	})

	// Any input that finds the deadline past has the same effect, because D2 runs on every step. A
	// RunStopped for the owned run does not count, since that run was never told to stop.
	t.Run("whichever input finds the deadline past", func(t *testing.T) {
		now := mc.Ms(10000)
		for _, in := range []core.Input{
			core.Tick{Now: now},
			core.Released{Now: now, Space: "s", Term: 7},
			core.RunStopped{Now: now, Space: "s", Term: 7},
			core.RunStopped{Now: now, Space: "s", Term: 8},
			core.Acquired{Now: now, Space: "s", Term: 8},
			core.Acquired{Now: now, Space: "s", Term: 0},
			core.AcquireFailed{Now: now, Space: "s", Holder: "h2", RetryAfter: time.Second},
			core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 7},
			core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 0},
			core.HolderChanged{Now: now, Space: "s", Holder: "h2", Term: 6},
			core.SessionFailed{Now: now, Cause: core.CauseUnreachable},
			core.SessionFailed{Now: now, Cause: core.Cause(7)},
			core.SessionRenewed{Now: now, SentAt: 0},
			core.SessionRenewed{Now: now, SentAt: now + time.Second},
			core.ViewChanged{Now: now, View: mc.Vm(2, "m1"), Wall: mc.Wall(now)},
		} {
			t.Run(fmt.Sprintf("%T%+v", in, in), func(t *testing.T) {
				steps := append(mc.Boot(7, mc.S("s", 1)),
					mc.Step{Input: in, Want: []core.Action{core.KeepAlive{}, stopExpiredS0(7), wakeAt(12000)}},
				)
				runSteps(t, scenario(steps...), stepChecks{5: shardIs(0, expiredS0(10000, 7))})
			})
		}
	})

	t.Run("a renewal in that step that moves the deadline past the reading keeps the run", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)), renewed(10000, 5000, extended(15000), core.KeepAlive{}, wakeAt(12000)))
		runSteps(t, scenario(steps...), stepChecks{
			5: all(sessionIs(10000, core.SessionStateLive, 15000, core.CauseNone), shardIs(0, ownedS0(300, 7))),
		})
	})

	t.Run("a renewal in that step that leaves the deadline behind the reading does not", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)),
			renewed(11000, 500, extended(10500), core.KeepAlive{}, stopExpiredS0(7), wakeAt(13000)),
		)
		runSteps(t, scenario(steps...), stepChecks{
			5: all(sessionIs(11000, late, 10500, core.CauseNone), shardIs(0, expiredS0(11000, 7))),
		})
	})

	t.Run("a move in that step gets one StopRun, and it says expired", func(t *testing.T) {
		moved := expiredS0(10000, 7)
		moved.Wanted = false
		steps := append(mc.Boot(7, mc.S("s", 1)),
			viewed(10000, mc.Vm(2, "m2"), core.KeepAlive{}, stopExpiredS0(7), wakeAt(12000)),
			runStopped(10100, 7, releaseS0(7), wakeAt(12000)),
		)
		runSteps(t, scenario(steps...), stepChecks{5: shardIs(0, moved)})
	})

	t.Run("a move afterwards sends no second StopRun", func(t *testing.T) {
		moved := expiredS0(10000, 7)
		moved.Wanted = false
		steps := append(expiredS0Steps(),
			viewed(10100, mc.Vm(2, "m2"), wakeAt(12000)),
			viewed(10200, mc.Vm(3, "m1"), wakeAt(12000)),
			viewed(10300, mc.Vm(4, "m2"), wakeAt(12000)),
			runStopped(10400, 7, releaseS0(7), wakeAt(12000)),
		)
		runSteps(t, scenario(steps...), stepChecks{
			6: shardIs(0, moved),
			7: shardIs(0, expiredS0(10000, 7)),
			8: shardIs(0, moved),
		})
	})

	// A grant at or after the deadline never starts a run, so it needs no StopRun and no RunStopped.
	for _, ms := range []int64{10000, 10500} {
		t.Run(fmt.Sprintf("a grant at @%d goes to a Release with no run", ms), func(t *testing.T) {
			waiting := noRunS0(ms, 7)
			waiting.Wanted = true
			steps := append(acquiringS0(),
				acquired(ms, 7, core.KeepAlive{}, releaseS0(7), wakeAt(ms+2000)),
				released(ms+100, 7, wakeAt(ms+2000)),
				renewed(ms+200, ms+200, extended(ms+10200), acquireS0(), wakeAt(ms+2000)),
				acquired(ms+300, 8, startedS0(8, ms+10200), wakeAt(ms+2000)),
			)
			runSteps(t, scenario(steps...), stepChecks{
				4: all(sessionIs(ms, late, 10000, core.CauseNone), shardIs(0, waiting)),
				5: shardIs(0, core.ShardStatus{Since: mc.Ms(ms + 100), Holder: "h1", Wanted: true}),
				7: shardIs(0, ownedS0(ms+300, 8)),
			})
		})
	}

	t.Run("its RunStopped leads to a Release, repeated while late, and it stays idle after", func(t *testing.T) {
		steps := append(expiredS0Steps(),
			released(10100, 7, wakeAt(12000)),
			runStopped(10200, 6, wakeAt(12000)),
			runStopped(10500, 7, releaseS0(7), wakeAt(12000)),
			keepAlive(12000, 12500),
			tick(12500, releaseS0(7), wakeAt(14000)),
			keepAlive(14000, 14500),
			tick(14500, releaseS0(7), wakeAt(16000)),
			released(15000, 7, wakeAt(16000)),
			keepAlive(16000, 18000),
		)
		idle := core.ShardStatus{Since: mc.Ms(15000), Holder: "h1", Wanted: true}
		runSteps(t, scenario(steps...), stepChecks{
			7:  shardIs(0, expiredS0(10000, 7)),
			8:  shardIs(0, core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(10500), Term: 7, Reason: core.StopExpired, Holder: "h1", Wanted: true}),
			13: shardIs(0, idle),
			14: all(sessionIs(16000, late, 10000, core.CauseNone), shardIs(0, idle)),
		})
	})

	t.Run("a late renewal does not bring the run back: it stops, releases and gets a fresh term", func(t *testing.T) {
		acquiring := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(12200), Holder: "h1", Wanted: true}
		noRun := noRunS0(12300, 7)
		noRun.Wanted = true
		steps := append(expiredS0Steps(),
			renewed(10500, 10400, extended(20400), wakeAt(12000)),
			keepAlive(12000, 14000),
			runStopped(12100, 7, releaseS0(7), wakeAt(14000)),
			released(12200, 7, acquireS0(), wakeAt(14000)),
			acquired(12300, 7, releaseS0(7), wakeAt(14000)),
			released(12400, 7, acquireS0(), wakeAt(14000)),
			acquired(12500, 8, startedS0(8, 20400), wakeAt(14000)),
			tick(20400, core.KeepAlive{}, stopExpiredS0(8), wakeAt(22400)),
		)
		runSteps(t, scenario(steps...), stepChecks{
			6:  all(sessionIs(10500, core.SessionStateLive, 20400, core.CauseNone), shardIs(0, expiredS0(10000, 7))),
			7:  shardIs(0, expiredS0(10000, 7)),
			8:  shardIs(0, core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(12100), Term: 7, Reason: core.StopExpired, Holder: "h1", Wanted: true}),
			9:  shardIs(0, acquiring),
			10: shardIs(0, noRun),
			12: shardIs(0, ownedS0(12500, 8)),
			13: all(sessionIs(20400, late, 20400, core.CauseNone), shardIs(0, expiredS0(20400, 8))),
		})
	})

	t.Run("stuck at exactly stopAt plus RenewDeadline, and its Release waits for the RunStopped", func(t *testing.T) {
		const justBefore = 20*time.Second - 1
		stuck := expiredS0(10000, 7)
		stuck.Stuck = true
		steps := append(expiredS0Steps(),
			mc.Step{Input: core.Tick{Now: justBefore}, Want: []core.Action{core.KeepAlive{}, wakeAt(20000)}},
			mc.Step{Input: core.Tick{Now: mc.Ms(20000)}, Want: []core.Action{core.Wake{At: justBefore + 2*time.Second}}},
			runStopped(20500, 7, releaseS0(7), core.Wake{At: justBefore + 2*time.Second}),
		)
		runSteps(t, scenario(steps...), stepChecks{
			6: shardIs(0, expiredS0(10000, 7)),
			7: shardIs(0, stuck),
			8: shardIs(0, core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(20500), Term: 7, Reason: core.StopExpired, Holder: "h1", Wanted: true}),
		})
	})

	// A record can name this member in a larger term while the expired run is still going. Nothing of that
	// term goes out until the run returns, and then it goes to grant as for any other run told to stop.
	t.Run("a larger own term waits for the RunStopped", func(t *testing.T) {
		gone := expiredS0(10000, 7)
		noRun := noRunS0(10600, 8)
		noRun.Wanted = true

		t.Run("still late: released with no run", func(t *testing.T) {
			steps := append(expiredS0Steps(),
				holderChanged(10500, "h1", 8, wakeAt(12000)),
				runStopped(10600, 7, releaseS0(8), wakeAt(12000)),
				released(10700, 8, wakeAt(12000)),
			)
			runSteps(t, scenario(steps...), stepChecks{
				6: shardIs(0, gone),
				7: shardIs(0, noRun),
				8: shardIs(0, core.ShardStatus{Since: mc.Ms(10700), Holder: "h1", Wanted: true}),
			})
		})

		t.Run("live again: started", func(t *testing.T) {
			steps := append(expiredS0Steps(),
				holderChanged(10500, "h1", 8, wakeAt(12000)),
				renewed(10550, 10550, extended(20550), wakeAt(12000)),
				runStopped(10600, 7, startedS0(8, 20550), wakeAt(12000)),
			)
			runSteps(t, scenario(steps...), stepChecks{7: shardIs(0, gone), 8: shardIs(0, ownedS0(10600, 8))})
		})

		t.Run("live again but moved: released with no run", func(t *testing.T) {
			moved := noRun
			moved.Wanted = false
			steps := append(expiredS0Steps(),
				holderChanged(10500, "h1", 8, wakeAt(12000)),
				renewed(10550, 10550, extended(20550), wakeAt(12000)),
				viewed(10560, mc.Vm(2, "m2"), wakeAt(12000)),
				runStopped(10600, 7, releaseS0(8), wakeAt(12000)),
			)
			runSteps(t, scenario(steps...), stepChecks{9: shardIs(0, moved)})
		})

		t.Run("an older record of another holder leaves it", func(t *testing.T) {
			steps := append(expiredS0Steps(),
				holderChanged(10400, "h1", 9, wakeAt(12000)),
				holderChanged(10500, "h2", 8, wakeAt(12000)),
				runStopped(10600, 7, releaseS0(9), wakeAt(12000)),
			)
			runSteps(t, scenario(steps...), nil)
		})

		t.Run("a later record of another holder takes it back", func(t *testing.T) {
			steps := append(expiredS0Steps(),
				holderChanged(10400, "h1", 8, wakeAt(12000)),
				holderChanged(10500, "h2", 9, wakeAt(12000)),
				runStopped(10600, 7, wakeAt(12000)),
			)
			runSteps(t, scenario(steps...), stepChecks{
				8: shardIs(0, core.ShardStatus{Since: mc.Ms(10600), Holder: "h2", Wanted: true}),
			})
		})

		t.Run("a stuck run still waits for its own RunStopped", func(t *testing.T) {
			stuck := expiredS0(10000, 7)
			stuck.Stuck = true
			steps := append(expiredS0Steps(),
				holderChanged(10500, "h1", 8, wakeAt(12000)),
				keepAlive(12000, 14000),
				keepAlive(14000, 16000),
				keepAlive(16000, 18000),
				keepAlive(18000, 20000),
				tick(20000, core.KeepAlive{}, wakeAt(22000)),
				runStopped(20100, 7, releaseS0(8), wakeAt(22000)),
			)
			runSteps(t, scenario(steps...), stepChecks{11: shardIs(0, stuck)})
		})

		t.Run("a lost session sends nothing of it", func(t *testing.T) {
			idle := core.ShardStatus{Since: mc.Ms(10700), Holder: "h1", Wanted: true}
			steps := append(expiredS0Steps(),
				holderChanged(10500, "h1", 8, wakeAt(12000)),
				failed(10600, core.CauseLost, wakeAt(20000)),
				runStopped(10700, 7),
				tick(10800),
			)
			runSteps(t, scenario(steps...), stepChecks{
				7: all(shardIs(0, gone), doneIs(false)),
				8: all(shardIs(0, idle), doneIs(true)),
				9: all(shardIs(0, idle), doneIs(true)),
			})
		})
	})

	t.Run("a renewal does not move the stuck timer", func(t *testing.T) {
		stuck := expiredS0(10000, 7)
		stuck.Stuck = true
		steps := append(expiredS0Steps(),
			renewed(10500, 10400, extended(20400), wakeAt(12000)),
			mc.Step{Input: core.Tick{Now: mc.Ms(19000)}, Want: []core.Action{core.KeepAlive{}, wakeAt(20000)}},
			tick(20000, wakeAt(20400)),
		)
		runSteps(t, scenario(steps...), stepChecks{8: shardIs(0, stuck)})
	})

	t.Run("records", func(t *testing.T) {
		expired := expiredS0(10000, 7)
		holding := func(holder string) core.ShardStatus {
			s := expired
			s.Holder = holder
			return s
		}
		for _, r := range []struct {
			holder string
			term   core.Term
			want   core.ShardStatus
			gone   bool
		}{
			{holder: "h1", term: 7, want: expired},
			{holder: "h1", term: 0, want: expired},
			{holder: "h1", term: 6, want: expired},
			{holder: "h2", term: 6, want: expired},
			{holder: "", term: 6, want: expired},
			{holder: "h2", term: 7, want: holding("h2"), gone: true},
			{holder: "h2", term: 8, want: holding("h2"), gone: true},
			{holder: "h2", term: 0, want: holding("h2"), gone: true},
			{holder: "", term: 0, want: holding(""), gone: true},
		} {
			t.Run(fmt.Sprintf("%q term %d", r.holder, r.term), func(t *testing.T) {
				steps := append(expiredS0Steps(), holderChanged(10500, r.holder, r.term, wakeAt(12000)))
				after := core.ShardStatus{State: core.ShardStateReleasing, Since: mc.Ms(10600), Term: 7, Reason: core.StopExpired, Holder: r.want.Holder, Wanted: true}
				stopped := runStopped(10600, 7, releaseS0(7), wakeAt(12000))
				if r.gone {
					after = core.ShardStatus{Since: mc.Ms(10600), Holder: r.want.Holder, Wanted: true}
					stopped = runStopped(10600, 7, wakeAt(12000))
				}
				steps = append(steps, stopped)
				runSteps(t, scenario(steps...), stepChecks{6: shardIs(0, r.want), 7: shardIs(0, after)})
			})
		}
	})

	// A fenced space's run does not stop at the deadline: it goes on degraded.
	t.Run("a fenced space degrades instead", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)),
			tick(10000, core.KeepAlive{}, degradeS0(7), wakeAt(12000)),
			keepAlive(12000, 14000),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			6: all(sessionIs(12000, late, 10000, core.CauseNone), shardIs(0, degradedS0(10000, 7))),
		})
	})

	// Each group keeps space order, and a Restore comes before the Release of an earlier space.
	t.Run("the spaces beside a fenced one keep their order", func(t *testing.T) {
		specs := []core.SpaceSpec{mc.S("a", 1), mc.S("b", 1), mc.S("c", 1)}
		cfg := mc.C(mc.S("c", 1), core.SpaceSpec{Name: "b", Shards: 1, Fenced: true}, mc.S("a", 1))
		releaseA := core.Release{Space: "a", Term: 7}
		confirmB := core.Confirm{Space: "b", Term: 8}
		steps := append(mc.Boot(7, specs...),
			tick(10000,
				core.KeepAlive{},
				core.StopRun{Space: "a", Term: 7, Reason: core.StopExpired},
				core.Degrade{Space: "b", Term: 8},
				core.StopRun{Space: "c", Term: 9, Reason: core.StopExpired},
				wakeAt(12000),
			),
			mc.Step{Input: core.RunStopped{Now: mc.Ms(10100), Space: "a", Term: 7}, Want: []core.Action{releaseA, wakeAt(12000)}},
			mc.Step{Input: core.RunStopped{Now: mc.Ms(10100), Space: "c", Term: 9}, Want: []core.Action{core.Release{Space: "c", Term: 9}, wakeAt(12000)}},
			mc.Step{Input: core.Released{Now: mc.Ms(10200), Space: "c", Term: 9}, Want: []core.Action{wakeAt(12000)}},
			keepAlive(12000, 12100),
			renewed(12100, 12000, extended(22000), releaseA, confirmB, core.Acquire{Space: "c"}, wakeAt(14000)),
			keepAlive(14000, 14100),
			mc.Step{
				Input: core.HolderChanged{Now: mc.Ms(14100), Space: "b", Holder: "h1", Term: 8},
				Want:  []core.Action{core.Restore{Space: "b", Term: 8}, releaseA, wakeAt(16000)},
			},
		)
		m := runSteps(t, scenarioIn(cfg, steps...), nil)
		if got, want := m.Status().Spaces[1].Shards[0], ownedS0(14100, 8); got != want {
			t.Errorf("b/0 = %+v, want %+v", got, want)
		}
	})
}

// TestSessionLoss checks what the step that accepts a lost session does to a shard in each state, and
// what the shard does from then on: one StopRun at most, for a run that has none, a Wake only for a run
// that can still go stuck, and idle with nothing to send once its run has returned.
func TestSessionLoss(t *testing.T) {
	stuckStopping := stoppingS0(1000, 7)
	stuckStopping.Stuck = true
	goneStopping := stoppingS0(1000, 7)
	goneStopping.Holder = "h2"

	for _, tc := range []struct {
		name  string
		steps []mc.Step
		at    int64
		stop  core.Action // The one StopRun this loss sends, if any.
		wake  int64       // The stuck timer left armed, if any.
		want  core.ShardStatus
		term  core.Term // The run still going after the loss, if any.
	}{
		{name: "idle", steps: bootedOnly(), at: 1600, want: core.ShardStatus{}},
		{
			name:  "idle after a grant of nothing: RetryAt is dropped",
			steps: append(acquiringS0(), acquired(300, 0, wakeAt(2000))),
			at:    1600,
			want:  core.ShardStatus{Since: mc.Ms(300), Wanted: true},
		},
		{
			name:  "idle after a failed answer: RetryAt is dropped",
			steps: append(acquiringS0(), acquireFailed(300, 5*time.Second, wakeAt(2000))),
			at:    1600,
			want:  core.ShardStatus{Since: mc.Ms(300), Holder: "h2", Wanted: true},
		},
		{
			name:  "owned",
			steps: mc.Boot(7, mc.S("s", 1)),
			at:    1600,
			stop:  stopLostS0(),
			wake:  11600,
			want:  lostS0(1600, "h1"),
			term:  7,
		},
		{
			name:  "stopping keeps its StopRun",
			steps: movedS0(),
			at:    1600,
			wake:  11000,
			want:  stoppingS0(1000, 7),
			term:  7,
		},
		{
			name:  "stopping and known gone",
			steps: append(movedS0(), holderChanged(1100, "h2", 8, wakeAt(2000))),
			at:    1600,
			wake:  11000,
			want:  goneStopping,
			term:  7,
		},
		{
			name:  "stopping and stuck stays stuck, with no timer",
			steps: append(movedS0(), keepAlive(11000, 13000)),
			at:    11500,
			want:  stuckStopping,
			term:  7,
		},
		{
			name:  "expired keeps its StopRun and its reason",
			steps: expiredS0Steps(),
			at:    11000,
			wake:  20000,
			want:  expiredS0(10000, 7),
			term:  7,
		},
		{
			name:  "lost keeps its StopRun",
			steps: ownedAndLostS0(),
			at:    1600,
			wake:  11000,
			want:  lostS0(1000, "h2"),
			term:  7,
		},
		{
			name:  "releasing a run's term",
			steps: append(movedS0(), runStopped(1500, 7, releaseS0(7), wakeAt(2000))),
			at:    1600,
			want:  core.ShardStatus{Since: mc.Ms(1600), Holder: "h1"},
		},
		{
			name:  "releasing an expired run's term",
			steps: append(expiredS0Steps(), runStopped(10500, 7, releaseS0(7), wakeAt(12000))),
			at:    11000,
			want:  core.ShardStatus{Since: mc.Ms(11000), Holder: "h1", Wanted: true},
		},
		{
			name:  "releasing with no run",
			steps: append(bootedOnly(), holderChanged(300, "h1", 9, releaseS0(9), wakeAt(2000))),
			at:    1600,
			want:  core.ShardStatus{Since: mc.Ms(1600), Holder: "h1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var waits []core.Action
			if tc.wake != 0 {
				waits = []core.Action{wakeAt(tc.wake)}
			}
			var loss []core.Action
			if tc.stop != nil {
				loss = append(loss, tc.stop)
			}
			loss = append(loss, waits...)

			steps := append(slices.Clone(tc.steps),
				failed(tc.at, core.CauseLost, loss...),
				failed(tc.at+1, core.CauseLost, waits...),
				tick(tc.at+2, waits...),
				renewed(tc.at+3, tc.at+3, waits...),
				viewed(tc.at+4, core.View{Version: 100, Members: []core.Member{{ID: "m1", Slot: -1}}}, waits...),
			)
			first := len(tc.steps) + 1
			afterView := tc.want
			afterView.Wanted = true
			checks := stepChecks{
				first:     all(shardIs(0, tc.want), doneIs(tc.term == 0)),
				first + 1: all(shardIs(0, tc.want), doneIs(tc.term == 0)),
				first + 2: all(shardIs(0, tc.want), doneIs(tc.term == 0)),
				first + 3: all(shardIs(0, tc.want), doneIs(tc.term == 0)),
				first + 4: all(shardIs(0, afterView), doneIs(tc.term == 0)),
			}
			if tc.term != 0 {
				steps = append(steps,
					runStopped(tc.at+5, tc.term+1, waits...),
					runStopped(tc.at+6, tc.term-1, waits...),
					runStopped(tc.at+7, 0, waits...),
					released(tc.at+8, tc.term, waits...),
					runStopped(tc.at+9, tc.term),
					tick(tc.at+10),
				)
				idle := core.ShardStatus{Since: mc.Ms(tc.at + 9), Holder: tc.want.Holder, Wanted: true}
				checks[first+8] = all(shardIs(0, afterView), doneIs(false))
				checks[first+9] = all(shardIs(0, idle), doneIs(true))
				checks[first+10] = all(shardIs(0, idle), doneIs(true))
			}
			runSteps(t, scenario(steps...), checks)
		})
	}

	t.Run("one StopRun per run, in order, and Done with the last RunStopped", func(t *testing.T) {
		stop := func(space string, shard int, term core.Term) core.StopRun {
			return core.StopRun{Space: space, Shard: shard, Term: term, Reason: core.StopLost}
		}
		ran := func(ms int64, space string, shard int, term core.Term, want ...core.Action) mc.Step {
			return mc.Step{Input: core.RunStopped{Now: mc.Ms(ms), Space: space, Shard: shard, Term: term}, Want: want}
		}
		specs := []core.SpaceSpec{mc.S("b", 1), mc.S("a", 2)}
		steps := append(mc.Boot(7, specs...),
			failed(1000, core.CauseLost, stop("a", 0, 7), stop("a", 1, 8), stop("b", 0, 9), wakeAt(11000)),
			ran(1100, "a", 1, 8, wakeAt(11000)),
			ran(1200, "b", 0, 9, wakeAt(11000)),
			ran(1300, "a", 0, 7),
		)
		runSteps(t, scenarioIn(mc.C(specs...), steps...), stepChecks{
			7:  doneIs(false),
			8:  doneIs(false),
			9:  doneIs(false),
			10: doneIs(true),
		})
	})

	// Done looks at every shard of every space, so the last shard to finish can be anywhere.
	t.Run("not done while the last space still has a run or an Acquire out", func(t *testing.T) {
		ran := func(ms int64, space string, shard int, term core.Term, want ...core.Action) mc.Step {
			return mc.Step{Input: core.RunStopped{Now: mc.Ms(ms), Space: space, Shard: shard, Term: term}, Want: want}
		}
		stop := func(space string, shard int, term core.Term) core.StopRun {
			return core.StopRun{Space: space, Shard: shard, Term: term, Reason: core.StopLost}
		}
		specs := []core.SpaceSpec{mc.S("b", 2), mc.S("a", 1)}

		t.Run("a run", func(t *testing.T) {
			steps := append(mc.Boot(7, specs...),
				failed(1000, core.CauseLost, stop("a", 0, 7), stop("b", 0, 8), stop("b", 1, 9), wakeAt(11000)),
				ran(1100, "a", 0, 7, wakeAt(11000)),
				ran(1200, "b", 0, 8, wakeAt(11000)),
				ran(1300, "b", 1, 9),
			)
			runSteps(t, scenarioIn(mc.C(specs...), steps...), stepChecks{
				8:  doneIs(false),
				9:  doneIs(false),
				10: doneIs(true),
			})
		})

		t.Run("an Acquire", func(t *testing.T) {
			steps := []mc.Step{
				keepAlive(0, 2000),
				renewed(100, 0, extended(10000), wakeAt(2000)),
				viewed(200, mc.Vm(1, "m1"),
					core.Acquire{Space: "a"},
					core.Acquire{Space: "b"},
					core.Acquire{Space: "b", Shard: 1},
					wakeAt(2000),
				),
				failed(300, core.CauseLost),
				{Input: core.Acquired{Now: mc.Ms(400), Space: "a", Term: 7}},
				{Input: core.AcquireFailed{Now: mc.Ms(500), Space: "b", Holder: "h2"}},
				{Input: core.Acquired{Now: mc.Ms(600), Space: "b", Shard: 1}},
			}
			runSteps(t, scenarioIn(mc.C(specs...), steps...), stepChecks{
				5: doneIs(false),
				6: doneIs(false),
				7: doneIs(true),
			})
		})
	})

	// An acquiring shard stays until the driver's answer. Whatever the answer says, it ends the wait and
	// nothing more: no run, no Release, no RetryAt.
	t.Run("an Acquire out", func(t *testing.T) {
		waiting := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true}
		for _, tc := range []struct {
			name   string
			answer mc.Step
			want   core.ShardStatus
		}{
			{name: "granted", answer: acquired(61000, 9), want: core.ShardStatus{Since: mc.Ms(61000), Wanted: true}},
			{name: "granted a term below the highest", answer: acquired(61000, 1), want: core.ShardStatus{Since: mc.Ms(61000), Wanted: true}},
			{name: "no grant", answer: acquired(61000, 0), want: core.ShardStatus{Since: mc.Ms(61000), Wanted: true}},
			{name: "held by another", answer: acquireFailed(61000, time.Second), want: core.ShardStatus{Since: mc.Ms(61000), Holder: "h2", Wanted: true}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				steps := append(acquiringS0(),
					failed(300, core.CauseLost),
					tick(60000),
					runStopped(60100, 9),
					released(60200, 9),
					holderChanged(60300, "h1", 9),
					tc.answer,
					tick(61100),
				)
				runSteps(t, scenario(steps...), stepChecks{
					4: all(shardIs(0, waiting), doneIs(false)),
					5: all(shardIs(0, waiting), doneIs(false)),
					8: all(shardIs(0, waiting), doneIs(false)),
					9: all(shardIs(0, tc.want), doneIs(true)),
				})
			})
		}
	})

	t.Run("records have no effect", func(t *testing.T) {
		starts := []struct {
			name  string
			steps []mc.Step
			at    int64
			want  core.ShardStatus
			wake  []core.Action
		}{
			{
				name:  "acquiring",
				steps: append(acquiringS0(), failed(1600, core.CauseLost)),
				at:    1700,
				want:  core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200), Wanted: true},
			},
			{
				name:  "stopping",
				steps: append(movedS0(), failed(1600, core.CauseLost, wakeAt(11000))),
				at:    1700,
				want:  stoppingS0(1000, 7),
				wake:  []core.Action{wakeAt(11000)},
			},
			{
				name:  "expired",
				steps: append(expiredS0Steps(), failed(11000, core.CauseLost, wakeAt(20000))),
				at:    11100,
				want:  expiredS0(10000, 7),
				wake:  []core.Action{wakeAt(20000)},
			},
			{
				name:  "lost",
				steps: append(mc.Boot(7, mc.S("s", 1)), failed(1000, core.CauseLost, stopLostS0(), wakeAt(11000))),
				at:    1700,
				want:  lostS0(1000, "h1"),
				wake:  []core.Action{wakeAt(11000)},
			},
			{
				name:  "done",
				steps: append(bootedOnly(), failed(1600, core.CauseLost)),
				at:    1700,
				want:  core.ShardStatus{},
			},
		}
		records := []struct {
			holder string
			term   core.Term
		}{{"h1", 9}, {"h1", 7}, {"h1", 0}, {"h2", 9}, {"h2", 7}, {"h2", 0}, {"", 0}, {"", 9}}
		for _, start := range starts {
			for _, r := range records {
				t.Run(fmt.Sprintf("%s/%q term %d", start.name, r.holder, r.term), func(t *testing.T) {
					steps := append(slices.Clone(start.steps), holderChanged(start.at, r.holder, r.term, start.wake...))
					runSteps(t, scenario(steps...), stepChecks{len(steps): shardIs(0, start.want)})
				})
			}
		}
	})

	t.Run("a view that takes the shard or gives it asks for nothing", func(t *testing.T) {
		moved := lostS0(1000, "h1")
		moved.Wanted = false
		steps := append(mc.Boot(7, mc.S("s", 1)),
			failed(1000, core.CauseLost, stopLostS0(), wakeAt(11000)),
			viewed(1100, mc.Vm(2, "m2"), wakeAt(11000)),
			viewed(1200, mc.Vm(3, "m1"), wakeAt(11000)),
			viewed(1300, mc.Vm(4, "m2"), wakeAt(11000)),
			runStopped(1400, 7),
			viewed(1500, mc.Vm(5, "m1")),
		)
		runSteps(t, scenario(steps...), stepChecks{
			6:  shardIs(0, moved),
			7:  shardIs(0, lostS0(1000, "h1")),
			10: all(shardIs(0, core.ShardStatus{Since: mc.Ms(1400), Holder: "h1", Wanted: true}), doneIs(true)),
		})
	})

	t.Run("a lost run goes stuck at exactly stopAt plus RenewDeadline", func(t *testing.T) {
		const justBefore = 11*time.Second - 1
		running := lostS0(1000, "h1")
		stuck := running
		stuck.Stuck = true
		steps := append(mc.Boot(7, mc.S("s", 1)),
			failed(1000, core.CauseLost, stopLostS0(), wakeAt(11000)),
			mc.Step{Input: core.Tick{Now: justBefore}, Want: []core.Action{wakeAt(11000)}},
			tick(11000),
			tick(60000),
			runStopped(60100, 7),
		)
		runSteps(t, scenario(steps...), stepChecks{
			6: all(shardIs(0, running), doneIs(false)),
			7: all(shardIs(0, stuck), doneIs(false)),
			8: all(shardIs(0, stuck), doneIs(false)),
			9: all(
				shardIs(0, core.ShardStatus{Since: mc.Ms(60100), Holder: "h1", Wanted: true}),
				doneIs(true),
			),
		})
	})

	t.Run("the stuck timer of a run told to stop earlier survives the loss", func(t *testing.T) {
		stopped := stoppingS0(1000, 7)
		stopped.Stuck = true
		runSteps(t, scenario(append(movedS0(),
			failed(1600, core.CauseLost, wakeAt(11000)),
			tick(11000),
		)...), stepChecks{7: all(shardIs(0, stopped), doneIs(false))})

		expired := expiredS0(10000, 7)
		expired.Stuck = true
		runSteps(t, scenario(append(expiredS0Steps(),
			failed(11000, core.CauseLost, wakeAt(20000)),
			tick(20000),
		)...), stepChecks{7: all(shardIs(0, expired), doneIs(false))})
	})

	t.Run("a fenced owned shard goes lost like any other", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)),
			failed(1000, core.CauseLost, stopLostS0(), wakeAt(11000)),
			runStopped(1100, 7),
		)
		runSteps(t, scenarioIn(mc.CF(mc.S("s", 1)), steps...), stepChecks{
			5: all(shardIs(0, lostS0(1000, "h1")), doneIs(false)),
			6: doneIs(true),
		})
	})
}

// TestFencedDeadline checks D2 in a fenced space: an owned shard goes degraded at the session deadline
// itself, and its run keeps going for as long as nothing else stops it.
func TestFencedDeadline(t *testing.T) {
	const late = core.SessionStateLate

	t.Run("at exactly the deadline, not a moment before", func(t *testing.T) {
		const justBefore = 10*time.Second - 1
		runSteps(t, fencedS0(append(mc.Boot(7, mc.S("s", 1)),
			mc.Step{Input: core.Tick{Now: justBefore}, Want: []core.Action{core.KeepAlive{}, wakeAt(10000)}},
			mc.Step{Input: core.Tick{Now: mc.Ms(10000)}, Want: []core.Action{degradeS0(7), core.Wake{At: justBefore + 2*time.Second}}},
		)...), stepChecks{
			5: shardIs(0, ownedS0(300, 7)),
			6: all(sessionIs(10000, late, 10000, core.CauseNone), shardIs(0, degradedS0(10000, 7))),
		})
	})

	t.Run("whichever input finds the deadline past", func(t *testing.T) {
		now := mc.Ms(10000)
		for _, in := range []core.Input{
			core.Tick{Now: now},
			core.Released{Now: now, Space: "s", Term: 7},
			core.RunStopped{Now: now, Space: "s", Term: 7},
			core.RunStopped{Now: now, Space: "s", Term: 8},
			core.Acquired{Now: now, Space: "s", Term: 8},
			core.Acquired{Now: now, Space: "s", Term: 0},
			core.AcquireFailed{Now: now, Space: "s", Holder: "h2", RetryAfter: time.Second},
			core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 7},
			core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 0},
			core.HolderChanged{Now: now, Space: "s", Holder: "h2", Term: 6},
			core.SessionFailed{Now: now, Cause: core.CauseUnreachable},
			core.SessionFailed{Now: now, Cause: core.Cause(7)},
			core.SessionRenewed{Now: now, SentAt: 0},
			core.SessionRenewed{Now: now, SentAt: now + time.Second},
			core.ViewChanged{Now: now, View: mc.Vm(2, "m1"), Wall: mc.Wall(now)},
		} {
			t.Run(fmt.Sprintf("%T%+v", in, in), func(t *testing.T) {
				steps := append(mc.Boot(7, mc.S("s", 1)),
					mc.Step{Input: in, Want: []core.Action{core.KeepAlive{}, degradeS0(7), wakeAt(12000)}},
				)
				runSteps(t, fencedS0(steps...), stepChecks{5: shardIs(0, degradedS0(10000, 7))})
			})
		}
	})

	t.Run("the run keeps going: one Degrade, no StopRun, never stuck", func(t *testing.T) {
		steps := degradedS0Steps()
		for ms := int64(12000); ms <= 60000; ms += 2000 {
			steps = append(steps, keepAlive(ms, ms+2000), runStopped(ms, 7, wakeAt(ms+2000)))
		}
		runSteps(t, fencedS0(steps...), stepChecks{
			len(steps): all(sessionIs(60000, late, 10000, core.CauseNone), shardIs(0, degradedS0(10000, 7))),
		})
	})

	t.Run("a renewal in that step that moves the deadline past the reading keeps it owned", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)), renewed(10000, 5000, extended(15000), core.KeepAlive{}, wakeAt(12000)))
		runSteps(t, fencedS0(steps...), stepChecks{
			5: all(sessionIs(10000, core.SessionStateLive, 15000, core.CauseNone), shardIs(0, ownedS0(300, 7))),
		})
	})

	t.Run("a renewal in that step that leaves the deadline behind the reading does not", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)),
			renewed(11000, 500, extended(10500), core.KeepAlive{}, degradeS0(7), wakeAt(13000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			5: all(sessionIs(11000, late, 10500, core.CauseNone), shardIs(0, degradedS0(11000, 7))),
		})
	})

	// D1 comes first, so the run is told to stop and there is nothing left to degrade.
	t.Run("another holder in that step stops the run without a Degrade", func(t *testing.T) {
		steps := append(mc.Boot(7, mc.S("s", 1)),
			holderChanged(10000, "h2", 8, core.KeepAlive{}, stopLostS0(), wakeAt(12000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{5: shardIs(0, lostS0(10000, "h2"))})
	})

	t.Run("a move in that step returns the Degrade, then the StopRun", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			step   func(want ...core.Action) mc.Step
			reason core.StopReason
		}{
			{
				name:   "moved",
				step:   func(want ...core.Action) mc.Step { return viewed(10000, mc.Vm(2, "m2"), want...) },
				reason: core.StopMoved,
			},
			{
				name:   "draining",
				step:   func(want ...core.Action) mc.Step { return drained(10000, want...) },
				reason: core.StopDraining,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				steps := append(mc.Boot(7, mc.S("s", 1)),
					tc.step(
						core.KeepAlive{},
						degradeS0(7),
						core.StopRun{Space: "s", Term: 7, Reason: tc.reason},
						wakeAt(12000),
					),
				)
				want := stoppingS0(10000, 7)
				want.Reason = tc.reason
				runSteps(t, fencedS0(steps...), stepChecks{5: shardIs(0, want)})
			})
		}
	})

	// A late grant has no run to keep going, so it takes the same path as in any other space.
	for _, ms := range []int64{10000, 10500} {
		t.Run(fmt.Sprintf("a grant at @%d goes to a Release with no run", ms), func(t *testing.T) {
			waiting := noRunS0(ms, 7)
			waiting.Wanted = true
			steps := append(acquiringS0(),
				acquired(ms, 7, core.KeepAlive{}, releaseS0(7), wakeAt(ms+2000)),
				holderChanged(ms+50, "h1", 7, wakeAt(ms+2000)),
				released(ms+100, 7, wakeAt(ms+2000)),
				renewed(ms+200, ms+200, extended(ms+10200), acquireS0(), wakeAt(ms+2000)),
				acquired(ms+300, 8, startedS0(8, ms+10200), wakeAt(ms+2000)),
				tick(ms+10200, core.KeepAlive{}, degradeS0(8), wakeAt(ms+12200)),
			)
			runSteps(t, fencedS0(steps...), stepChecks{
				4: all(sessionIs(ms, late, 10000, core.CauseNone), shardIs(0, waiting)),
				5: shardIs(0, waiting),
				6: shardIs(0, core.ShardStatus{Since: mc.Ms(ms + 100), Holder: "h1", Wanted: true}),
				8: shardIs(0, ownedS0(ms+300, 8)),
				9: shardIs(0, degradedS0(ms+10200, 8)),
			})
		})
	}
}

// TestConfirmCadence checks D4 and its timer: a degraded shard's Confirm goes at once when the session
// is live, then every RetryPeriod from the last one, and not at all while the session is not live.
func TestConfirmCadence(t *testing.T) {
	const late = core.SessionStateLate
	degraded := degradedS0(10000, 7)

	t.Run("it stops at the deadline, and its timer goes with it", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			keepAlive(12000, 12500),
			tick(12400, wakeAt(12500)),
			tick(12500, confirmS0(7), wakeAt(14000)),
			keepAlive(14000, 14500),
			tick(14500, confirmS0(7), wakeAt(16000)),
			keepAlive(16000, 16500),
			tick(16500, confirmS0(7), wakeAt(18000)),
			keepAlive(18000, 18500),
			tick(18500, confirmS0(7), wakeAt(20000)),
			keepAlive(20000, 20400),
			tick(20400, wakeAt(22000)),
			tick(20500, wakeAt(22000)),
			keepAlive(22000, 24000),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			6:          confirmPendingIs(true),
			16:         all(sessionIs(20000, core.SessionStateLive, 20400, core.CauseNone), confirmPendingIs(true)),
			17:         all(sessionIs(20400, late, 20400, core.CauseNone), shardIs(0, degraded), confirmPendingIs(false)),
			len(steps): all(sessionIs(22000, late, 20400, core.CauseNone), shardIs(0, degraded)),
		})
	})

	t.Run("a late driver counts from the Confirm it got", func(t *testing.T) {
		runSteps(t, fencedS0(append(confirmingS0Steps(),
			tick(15000, core.KeepAlive{}, confirmS0(7), wakeAt(17000)),
			tick(16900, wakeAt(17000)),
			tick(17000, core.KeepAlive{}, confirmS0(7), wakeAt(19000)),
		)...), nil)
	})

	// The Confirm sent at @20.3 would next be due at @22.3. A renewal does not wait for that.
	t.Run("the first live step after a late one sends it at once", func(t *testing.T) {
		runSteps(t, fencedS0(append(confirmingS0Steps(),
			tick(20300, core.KeepAlive{}, confirmS0(7), wakeAt(20400)),
			tick(20400, wakeAt(22300)),
			renewed(20500, 20450, extended(30450), confirmS0(7), wakeAt(22300)),
			keepAlive(22300, 22500),
			tick(22500, confirmS0(7), wakeAt(24300)),
		)...), nil)
	})

	// A renewal arrives while degraded, and the deadline passes again before the Confirm is answered:
	// no second Degrade, the answer that comes then counts for nothing, and the next renewal starts over.
	t.Run("the deadline passes again before the answer", func(t *testing.T) {
		steps := append(degradedS0Steps(),
			renewed(10500, 1000, extended(11000), confirmS0(7), wakeAt(11000)),
			tick(11000, wakeAt(12000)),
			holderChanged(11100, "h1", 7, wakeAt(12000)),
			keepAlive(12000, 14000),
			holderChanged(12100, "h1", 7, wakeAt(14000)),
			renewed(12200, 12100, extended(22100), confirmS0(7), wakeAt(14000)),
			holderChanged(12300, "h1", 7, restoreS0(7), wakeAt(14000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			6:  all(shardIs(0, degraded), confirmPendingIs(true)),
			7:  all(sessionIs(11000, late, 11000, core.CauseNone), shardIs(0, degraded), confirmPendingIs(false)),
			8:  shardIs(0, degraded),
			10: shardIs(0, degraded),
			11: all(shardIs(0, degraded), confirmPendingIs(true)),
			12: shardIs(0, ownedS0(12300, 7)),
		})
	})

	t.Run("a renewal that leaves the session late sends none", func(t *testing.T) {
		runSteps(t, fencedS0(append(degradedS0Steps(),
			renewed(12000, 1000, extended(11000), core.KeepAlive{}, wakeAt(14000)),
		)...), stepChecks{6: all(sessionIs(12000, late, 11000, core.CauseNone), confirmPendingIs(false))})
	})

	t.Run("an older renewal while live leaves the cadence alone", func(t *testing.T) {
		runSteps(t, fencedS0(append(confirmingS0Steps(),
			renewed(11000, 1000, wakeAt(12000)),
			keepAlive(12000, 12500),
			tick(12500, confirmS0(7), wakeAt(14000)),
		)...), nil)
	})

	t.Run("each shard has its own", func(t *testing.T) {
		confirm := func(shard int, term core.Term) core.Action {
			return core.Confirm{Space: "s", Shard: shard, Term: term}
		}
		steps := append(mc.BootF(7, mc.S("s", 2)),
			renewed(10500, 10400, extended(20400), confirm(0, 7), confirm(1, 8), wakeAt(12000)),
			mc.Step{
				Input: core.HolderChanged{Now: mc.Ms(11000), Space: "s", Shard: 0, Holder: "h1", Term: 7},
				Want:  []core.Action{restoreS0(7), wakeAt(12000)},
			},
			keepAlive(12000, 12500),
			tick(12500, confirm(1, 8), wakeAt(14000)),
			tick(20400, core.KeepAlive{}, degradeS0(7), wakeAt(22400)),
		)
		runSteps(t, scenarioIn(mc.CF(mc.S("s", 2)), steps...), stepChecks{
			8:  shardsAre(ownedS0(11000, 7), degradedS0(10000, 8)),
			11: shardsAre(degradedS0(20400, 7), degradedS0(10000, 8)),
		})
	})

	t.Run("under a Timing of its own", func(t *testing.T) {
		cfg := mc.CF(mc.S("s", 1))
		cfg.Timing = core.Timing{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second, SlotGrace: time.Minute}
		runSteps(t, scenarioIn(cfg,
			keepAlive(0, 5000),
			renewed(100, 0, extended(20000), wakeAt(5000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), wakeAt(5000)),
			acquired(300, 7, startedS0(7, 20000), wakeAt(5000)),
			tick(20000, core.KeepAlive{}, degradeS0(7), wakeAt(25000)),
			renewed(20100, 20050, extended(40050), confirmS0(7), wakeAt(25000)),
			keepAlive(25000, 25100),
			tick(25100, confirmS0(7), wakeAt(30000)),
		), nil)
	})
}

// TestRestoreNeedsAnAnsweredConfirm checks the rule fenced spaces rest on: a degraded shard goes back to
// owned only through a Confirm returned while the session is live and answered, with the session still
// live, with this holder and term. Anything weaker lets a shard taken meanwhile come back to owned.
func TestRestoreNeedsAnAnsweredConfirm(t *testing.T) {
	degraded := degradedS0(10000, 7)

	t.Run("an answer before any Confirm", func(t *testing.T) {
		steps := append(degradedS0Steps(),
			holderChanged(10100, "h1", 7, wakeAt(12000)),
			holderChanged(10200, "h1", 7, wakeAt(12000)),
			keepAlive(12000, 14000),
			holderChanged(12050, "h1", 7, wakeAt(14000)),
			renewed(12100, 12000, extended(22000), confirmS0(7), wakeAt(14000)),
			holderChanged(12300, "h1", 7, restoreS0(7), wakeAt(14000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			6:  all(shardIs(0, degraded), confirmPendingIs(false)),
			7:  all(shardIs(0, degraded), confirmPendingIs(false)),
			9:  all(shardIs(0, degraded), confirmPendingIs(false)),
			10: all(shardIs(0, degraded), confirmPendingIs(true)),
			11: all(shardIs(0, ownedS0(12300, 7)), confirmPendingIs(false)),
		})
	})

	t.Run("an answer after the session went not live again", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			tick(20400, core.KeepAlive{}, wakeAt(22400)),
			holderChanged(20500, "h1", 7, wakeAt(22400)),
			holderChanged(20600, "h1", 7, wakeAt(22400)),
			renewed(20700, 20650, extended(30650), confirmS0(7), wakeAt(22400)),
			holderChanged(20800, "h1", 7, restoreS0(7), wakeAt(22400)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			6:  confirmPendingIs(true),
			7:  all(shardIs(0, degraded), confirmPendingIs(false)),
			8:  all(shardIs(0, degraded), confirmPendingIs(false)),
			9:  all(shardIs(0, degraded), confirmPendingIs(false)),
			10: all(shardIs(0, degraded), confirmPendingIs(true)),
			11: shardIs(0, ownedS0(20800, 7)),
		})
	})

	t.Run("an answer in the step that finds the deadline past", func(t *testing.T) {
		steps := append(confirmingS0Steps(), holderChanged(20400, "h1", 7, core.KeepAlive{}, wakeAt(22400)))
		runSteps(t, fencedS0(steps...), stepChecks{7: all(shardIs(0, degraded), confirmPendingIs(false))})
	})

	t.Run("an answer a moment before the deadline, and then the deadline", func(t *testing.T) {
		const justBefore = 20400*time.Millisecond - 1
		steps := append(confirmingS0Steps(),
			mc.Step{
				Input: core.HolderChanged{Now: justBefore, Space: "s", Holder: "h1", Term: 7},
				Want:  []core.Action{core.KeepAlive{}, restoreS0(7), wakeAt(20400)},
			},
			mc.Step{Input: core.Tick{Now: mc.Ms(20400)}, Want: []core.Action{degradeS0(7), core.Wake{At: justBefore + 2*time.Second}}},
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			7: shardIs(0, core.ShardStatus{State: core.ShardStateOwned, Since: justBefore, Term: 7, Holder: "h1", Wanted: true}),
			8: shardIs(0, degradedS0(20400, 7)),
		})
	})

	// The reading of the input does not make the session live: the machine's clock does.
	t.Run("an answer with an older reading, after the deadline", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			tick(20400, core.KeepAlive{}, wakeAt(22400)),
			holderChanged(15000, "h1", 7, wakeAt(22400)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{8: shardIs(0, degraded)})
	})

	// Where the machine's side of the rule ends. A reading taken before the outage is refused until the
	// renewal's Confirm goes out, and after that it restores the run, even with a reading older than that
	// Confirm: a Confirm carries no ID, so only the driver can tell, and Confirm's doc makes it hold such a
	// record back. If this ever stops restoring, that doc and the driver's filter can change too.
	t.Run("once a Confirm is out, an older reading looks like its answer", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			now  int64
		}{
			{name: "stepped after the Confirm", now: 20800},
			{name: "stamped before the Confirm", now: 15000},
		} {
			t.Run(tc.name, func(t *testing.T) {
				steps := append(confirmingS0Steps(),
					tick(20400, core.KeepAlive{}, wakeAt(22400)),
					holderChanged(20500, "h1", 7, wakeAt(22400)),
					renewed(20700, 20650, extended(30650), confirmS0(7), wakeAt(22400)),
					holderChanged(tc.now, "h1", 7, restoreS0(7), wakeAt(22400)),
				)
				since := max(tc.now, 20700)
				runSteps(t, fencedS0(steps...), stepChecks{
					8:  all(shardIs(0, degraded), confirmPendingIs(false)),
					9:  all(shardIs(0, degraded), confirmPendingIs(true)),
					10: shardIs(0, ownedS0(since, 7)),
				})
			})
		}
	})

	// A record that says nothing about this run leaves the shard degraded, and its Confirm still counts.
	for _, r := range []struct {
		holder string
		term   core.Term
	}{
		{holder: "h1", term: 6},
		{holder: "h1", term: 0},
		{holder: "h2", term: 6},
		{holder: "", term: 6},
	} {
		t.Run(fmt.Sprintf("a Confirm is out and the record is %q term %d", r.holder, r.term), func(t *testing.T) {
			steps := append(confirmingS0Steps(),
				holderChanged(11000, r.holder, r.term, wakeAt(12000)),
				holderChanged(11100, r.holder, r.term, wakeAt(12000)),
				holderChanged(11200, "h1", 7, restoreS0(7), wakeAt(12000)),
			)
			runSteps(t, fencedS0(steps...), stepChecks{
				7: all(shardIs(0, degraded), confirmPendingIs(true)),
				8: all(shardIs(0, degraded), confirmPendingIs(true)),
				9: shardIs(0, ownedS0(11200, 7)),
			})
		})
	}

	// Another holder, or none, ends the run's claim. The answer that comes after it is not one.
	for _, r := range []struct {
		holder string
		term   core.Term
	}{
		{holder: "h2", term: 7},
		{holder: "h2", term: 8},
		{holder: "h2", term: 0},
		{holder: "", term: 0},
		{holder: "", term: 8},
	} {
		t.Run(fmt.Sprintf("a Confirm is out and the shard is taken: %q term %d", r.holder, r.term), func(t *testing.T) {
			steps := append(confirmingS0Steps(),
				holderChanged(11000, r.holder, r.term, stopLostS0(), wakeAt(12000)),
				holderChanged(11100, "h1", 7, wakeAt(12000)),
				keepAlive(12000, 14000),
				tick(12500, wakeAt(14000)),
				runStopped(12600, 7, acquireS0(), wakeAt(14000)),
			)
			runSteps(t, fencedS0(steps...), stepChecks{
				7: shardIs(0, lostS0(11000, r.holder)),
				8: shardIs(0, lostS0(11000, r.holder)),
			})
		})
	}

	t.Run("repeated answers restore once, and none carries over to the next deadline", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			holderChanged(11000, "h1", 7, restoreS0(7), wakeAt(12000)),
			holderChanged(11100, "h1", 7, wakeAt(12000)),
			holderChanged(11200, "h1", 7, wakeAt(12000)),
			keepAlive(12000, 14000),
			tick(12500, wakeAt(14000)),
			tick(20400, core.KeepAlive{}, degradeS0(7), wakeAt(22400)),
			holderChanged(20500, "h1", 7, wakeAt(22400)),
			renewed(20600, 20550, extended(30550), confirmS0(7), wakeAt(22400)),
			holderChanged(20700, "h1", 7, restoreS0(7), wakeAt(22400)),
			holderChanged(20800, "h1", 7, wakeAt(22400)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			7:  shardIs(0, ownedS0(11000, 7)),
			9:  shardIs(0, ownedS0(11000, 7)),
			12: shardIs(0, degradedS0(20400, 7)),
			13: shardIs(0, degradedS0(20400, 7)),
			15: shardIs(0, ownedS0(20700, 7)),
			16: shardIs(0, ownedS0(20700, 7)),
		})
	})

	t.Run("no other input restores", func(t *testing.T) {
		now := mc.Ms(11000)
		for _, in := range append(adversaries(now), core.RunStopped{Now: now, Space: "s", Term: 7}) {
			if v, ok := in.(core.HolderChanged); ok && v.Holder == "h1" && v.Term == 7 {
				continue
			}
			t.Run(fmt.Sprintf("%T%+v", in, in), func(t *testing.T) {
				m := core.NewMachine(mc.CF(mc.S("s", 2)))
				for _, s := range confirmingS0Steps() {
					m.Step(s.Input)
				}
				stepWithoutRelease(t, m, in)
				if got := m.Status().Spaces[0].Shards[0]; got != degraded {
					t.Errorf("shard 0 = %+v, want %+v", got, degraded)
				}
				if !core.ConfirmPending(m, 0, 0) {
					t.Error("the Confirm is no longer pending")
				}
			})
		}
	})

	t.Run("an answer for another shard or space restores nothing here", func(t *testing.T) {
		now := mc.Ms(11000)
		for _, in := range []core.Input{
			core.HolderChanged{Now: now, Space: "other", Holder: "h1", Term: 7},
			core.HolderChanged{Now: now, Space: "s", Shard: 5, Holder: "h1", Term: 7},
			core.HolderChanged{Now: now, Space: "s", Shard: -1, Holder: "h1", Term: 7},
			core.HolderChanged{Now: now, Space: "s", Shard: 1, Holder: "h1", Term: 7},
		} {
			m := core.NewMachine(mc.CF(mc.S("s", 2)))
			for _, s := range mc.BootF(7, mc.S("s", 2)) {
				m.Step(s.Input)
			}
			m.Step(core.SessionRenewed{Now: mc.Ms(10500), SentAt: mc.Ms(10400)})
			want := []core.Action{wakeAt(12000)}
			if got := m.Step(in); !reflect.DeepEqual(got, want) {
				t.Errorf("Step(%+v) = %#v, want %#v", in, got, want)
			}
			if got, want := m.Status().Spaces[0].Shards, []core.ShardStatus{degraded, degradedS0(10000, 8)}; !slices.Equal(got, want) {
				t.Errorf("after %+v, shards = %+v, want %+v", in, got, want)
			}
		}
	})

	// No input reaches this state, because every live step sends what is due. The check on a pending
	// Confirm must still hold by itself.
	t.Run("live with no Confirm pending", func(t *testing.T) {
		steps := append(confirmingS0Steps(), holderChanged(11000, "h1", 7, wakeAt(12000)))
		runSteps(t, fencedS0(steps...), stepChecks{
			6: func(_ *testing.T, m *core.Machine) { core.ClearConfirmPending(m, 0, 0) },
			7: shardIs(0, degraded),
		})
	})

	// The Confirm of a run that was then told to stop means nothing to that run or to the next one.
	t.Run("a Confirm does not outlive its run", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			viewed(11000, mc.Vm(2, "m2"), stopS0(7), wakeAt(12000)),
			holderChanged(11100, "h1", 7, wakeAt(12000)),
			runStopped(11200, 7, releaseS0(7), wakeAt(12000)),
			viewed(11300, mc.Vm(3, "m1"), wakeAt(12000)),
			released(11400, 7, acquireS0(), wakeAt(12000)),
			acquired(11500, 8, startedS0(8, 20400), wakeAt(12000)),
			holderChanged(11600, "h1", 8, wakeAt(12000)),
			tick(20400, core.KeepAlive{}, degradeS0(8), wakeAt(22400)),
			holderChanged(20500, "h1", 8, wakeAt(22400)),
			renewed(20600, 20550, extended(30550), confirmS0(8), wakeAt(22400)),
			holderChanged(20700, "h1", 7, wakeAt(22400)),
			holderChanged(20800, "h1", 8, restoreS0(8), wakeAt(22400)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			8:  shardIs(0, stoppingS0(11000, 7)),
			12: shardIs(0, ownedS0(11500, 8)),
			13: shardIs(0, ownedS0(11500, 8)),
			15: shardIs(0, degradedS0(20400, 8)),
			17: shardIs(0, degradedS0(20400, 8)),
			18: shardIs(0, ownedS0(20800, 8)),
		})
	})
}

// TestDegradedRecords checks what a record does to a degraded shard while the session is late, with no
// Confirm out: it can end the run's claim, and nothing else.
func TestDegradedRecords(t *testing.T) {
	for _, r := range []struct {
		holder string
		term   core.Term
		lost   bool
	}{
		{holder: "h1", term: 7},
		{holder: "h1", term: 0},
		{holder: "h1", term: 6},
		{holder: "h2", term: 6},
		{holder: "", term: 6},
		{holder: "h2", term: 7, lost: true},
		{holder: "h2", term: 8, lost: true},
		{holder: "h2", term: 0, lost: true},
		{holder: "", term: 0, lost: true},
		{holder: "", term: 8, lost: true},
	} {
		t.Run(fmt.Sprintf("%q term %d", r.holder, r.term), func(t *testing.T) {
			if !r.lost {
				runSteps(t, fencedS0(append(degradedS0Steps(),
					holderChanged(10500, r.holder, r.term, wakeAt(12000)),
					runStopped(10600, 7, wakeAt(12000)),
				)...), stepChecks{6: shardIs(0, degradedS0(10000, 7)), 7: shardIs(0, degradedS0(10000, 7))})
				return
			}
			idle := core.ShardStatus{Since: mc.Ms(10700), Holder: r.holder, Wanted: true}
			runSteps(t, fencedS0(append(degradedS0Steps(),
				holderChanged(10500, r.holder, r.term, stopLostS0(), wakeAt(12000)),
				runStopped(10600, 6, wakeAt(12000)),
				runStopped(10700, 7, wakeAt(12000)),
			)...), stepChecks{
				6: shardIs(0, lostS0(10500, r.holder)),
				7: shardIs(0, lostS0(10500, r.holder)),
				8: shardIs(0, idle),
			})
		})
	}
}

// TestDegradedLargerOwnTerm checks a record naming this member in a larger term while a degraded run is
// going. The shard goes lost and waits for the run's RunStopped, which passes the term to grant. A record
// that is not older than the waiting term and names another holder, or none, takes it back. Older ones
// than that say nothing, so the run's own term is no answer to its Confirm any more.
func TestDegradedLargerOwnTerm(t *testing.T) {
	const larger = 9

	t.Run("while late: the term is released with no run at the RunStopped", func(t *testing.T) {
		waiting := noRunS0(10900, larger)
		waiting.Wanted = true
		steps := append(degradedS0Steps(),
			holderChanged(10500, "h1", larger, stopLostS0(), wakeAt(12000)),
			holderChanged(10600, "h1", larger, wakeAt(12000)),
			holderChanged(10650, "h1", 7, wakeAt(12000)),
			acquired(10700, larger, wakeAt(12000)),
			runStopped(10800, larger, wakeAt(12000)),
			released(10850, larger, wakeAt(12000)),
			runStopped(10900, 7, releaseS0(larger), wakeAt(12000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			6:  shardIs(0, lostS0(10500, "h1")),
			11: shardIs(0, lostS0(10500, "h1")),
			12: shardIs(0, waiting),
		})
	})

	t.Run("while live: the term starts at the RunStopped, and goes through the rule like any run", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			holderChanged(11000, "h1", larger, stopLostS0(), wakeAt(12000)),
			holderChanged(11100, "h1", 7, wakeAt(12000)),
			keepAlive(12000, 14000),
			tick(12500, wakeAt(14000)),
			runStopped(12600, 7, startedS0(larger, 20400), wakeAt(14000)),
			holderChanged(12700, "h1", larger, wakeAt(14000)),
			tick(20400, core.KeepAlive{}, degradeS0(larger), wakeAt(22400)),
			holderChanged(20500, "h1", larger, wakeAt(22400)),
			renewed(20600, 20550, extended(30550), confirmS0(larger), wakeAt(22400)),
			holderChanged(20700, "h1", larger, restoreS0(larger), wakeAt(22400)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			7:  shardIs(0, lostS0(11000, "h1")),
			8:  shardIs(0, lostS0(11000, "h1")),
			10: shardIs(0, lostS0(11000, "h1")),
			11: shardIs(0, ownedS0(12600, larger)),
			12: shardIs(0, ownedS0(12600, larger)),
			13: shardIs(0, degradedS0(20400, larger)),
			14: shardIs(0, degradedS0(20400, larger)),
			16: shardIs(0, ownedS0(20700, larger)),
		})
	})

	t.Run("and moved: the term is released with no run at the RunStopped", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			holderChanged(11000, "h1", larger, stopLostS0(), wakeAt(12000)),
			viewed(11100, mc.Vm(2, "m2"), wakeAt(12000)),
			runStopped(11200, 7, releaseS0(larger), wakeAt(12000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{9: shardIs(0, noRunS0(11200, larger))})
	})

	t.Run("after another holder: the term still waits for the RunStopped", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			holderChanged(11000, "h2", 8, stopLostS0(), wakeAt(12000)),
			holderChanged(11100, "h1", larger, wakeAt(12000)),
			runStopped(11200, 7, startedS0(larger, 20400), wakeAt(12000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			7: shardIs(0, lostS0(11000, "h2")),
			8: shardIs(0, lostS0(11000, "h1")),
			9: shardIs(0, ownedS0(11200, larger)),
		})
	})

	// The record is at or above the waiting term, or has no term to compare, so the wait is over. The
	// RunStopped has nothing to grant, and the shard asks for its record in the ordinary way.
	for _, r := range []struct {
		holder string
		term   core.Term
	}{
		{holder: "h2", term: larger},
		{holder: "h2", term: larger + 1},
		{holder: "h2", term: 0},
		{holder: "", term: larger},
		{holder: "", term: larger + 1},
		{holder: "", term: 0},
	} {
		t.Run(fmt.Sprintf("taken back by %q term %d", r.holder, r.term), func(t *testing.T) {
			steps := append(confirmingS0Steps(),
				holderChanged(11000, "h1", larger, stopLostS0(), wakeAt(12000)),
				holderChanged(11100, r.holder, r.term, wakeAt(12000)),
				runStopped(11200, 7, acquireS0(), wakeAt(12000)),
			)
			runSteps(t, fencedS0(steps...), stepChecks{
				8: shardIs(0, lostS0(11000, r.holder)),
				9: shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(11200), Holder: r.holder, Wanted: true}),
			})
		})

		t.Run(fmt.Sprintf("taken back while late by %q term %d", r.holder, r.term), func(t *testing.T) {
			steps := append(degradedS0Steps(),
				holderChanged(10500, "h1", larger, stopLostS0(), wakeAt(12000)),
				holderChanged(10600, r.holder, r.term, wakeAt(12000)),
				runStopped(10700, 7, wakeAt(12000)),
			)
			runSteps(t, fencedS0(steps...), stepChecks{
				8: shardIs(0, core.ShardStatus{Since: mc.Ms(10700), Holder: r.holder, Wanted: true}),
			})
		})
	}

	// The record is older than the waiting term, though not older than the run's: the ignore rule looks
	// at the larger of the two.
	for _, r := range []struct {
		holder string
		term   core.Term
	}{
		{holder: "h2", term: 7},
		{holder: "h2", term: 8},
		{holder: "", term: 7},
		{holder: "", term: 8},
	} {
		t.Run(fmt.Sprintf("not taken back by %q term %d", r.holder, r.term), func(t *testing.T) {
			steps := append(confirmingS0Steps(),
				holderChanged(11000, "h1", larger, stopLostS0(), wakeAt(12000)),
				holderChanged(11100, r.holder, r.term, wakeAt(12000)),
				runStopped(11200, 7, startedS0(larger, 20400), wakeAt(12000)),
			)
			runSteps(t, fencedS0(steps...), stepChecks{
				8: shardIs(0, lostS0(11000, "h1")),
				9: shardIs(0, ownedS0(11200, larger)),
			})
		})
	}

	t.Run("taken back, then a term larger still waits again", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			holderChanged(11000, "h1", larger, stopLostS0(), wakeAt(12000)),
			holderChanged(11100, "h2", larger, wakeAt(12000)),
			holderChanged(11200, "h1", larger+1, wakeAt(12000)),
			runStopped(11300, 7, startedS0(larger+1, 20400), wakeAt(12000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{10: shardIs(0, ownedS0(11300, larger+1))})
	})

	t.Run("a lost session drops the term: no StartRun and no Release", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			holderChanged(11000, "h1", larger, stopLostS0(), wakeAt(12000)),
			failed(11100, core.CauseLost, wakeAt(21000)),
			runStopped(11200, 7),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			8: all(shardIs(0, lostS0(11000, "h1")), doneIs(false)),
			9: all(shardIs(0, core.ShardStatus{Since: mc.Ms(11200), Holder: "h1", Wanted: true}), doneIs(true)),
		})
	})
}

// TestDegradedMove checks D3 on a degraded shard: a move stops the run, live or not, with the reason the
// move has, and the shard then goes the way any moved shard goes.
func TestDegradedMove(t *testing.T) {
	t.Run("while late: it stops, releases and gets a new term", func(t *testing.T) {
		wanted := stoppingS0(10100, 7)
		wanted.Wanted = true
		releasing := releasingS0(10500, 7)
		releasing.Wanted = true
		steps := append(degradedS0Steps(),
			viewed(10100, mc.Vm(2, "m2"), stopS0(7), wakeAt(12000)),
			renewed(10200, 10150, extended(20150), wakeAt(12000)),
			viewed(10300, mc.Vm(3, "m1"), wakeAt(12000)),
			holderChanged(10400, "h1", 7, wakeAt(12000)),
			runStopped(10500, 7, releaseS0(7), wakeAt(12000)),
			released(10600, 7, acquireS0(), wakeAt(12000)),
			acquired(10700, 8, startedS0(8, 20150), wakeAt(12000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			6:  shardIs(0, stoppingS0(10100, 7)),
			7:  shardIs(0, stoppingS0(10100, 7)),
			8:  shardIs(0, wanted),
			9:  shardIs(0, wanted),
			10: shardIs(0, releasing),
			12: shardIs(0, ownedS0(10700, 8)),
		})
	})

	t.Run("while live with a Confirm out: no more Confirms, and the answer does nothing", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			viewed(11000, mc.Vm(2, "m2"), stopS0(7), wakeAt(12000)),
			keepAlive(12000, 14000),
			tick(12500, wakeAt(14000)),
			holderChanged(12600, "h1", 7, wakeAt(14000)),
			keepAlive(14000, 16000),
			tick(14500, wakeAt(16000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			7:          shardIs(0, stoppingS0(11000, 7)),
			len(steps): shardIs(0, stoppingS0(11000, 7)),
		})
	})

	// D3 comes before D4, so a run told to stop asks for nothing in the same step.
	t.Run("a move at the reading a Confirm is due: the StopRun and no Confirm", func(t *testing.T) {
		steps := append(confirmingS0Steps(),
			keepAlive(12000, 12500),
			viewed(12500, mc.Vm(2, "m2"), stopS0(7), wakeAt(14000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{8: shardIs(0, stoppingS0(12500, 7))})
	})

	for _, tc := range []struct {
		name  string
		steps []mc.Step
		at    int64
	}{
		{name: "while late", steps: degradedS0Steps(), at: 10100},
		{name: "while live", steps: confirmingS0Steps(), at: 11000},
	} {
		t.Run("the reason is draining while the member drains, "+tc.name, func(t *testing.T) {
			draining := stoppingS0(tc.at, 7)
			draining.Reason = core.StopDraining
			steps := append(slices.Clone(tc.steps), drained(tc.at, stopDrainingS0(), wakeAt(12000)))
			runSteps(t, fencedS0(steps...), stepChecks{len(steps): shardIs(0, draining)})
		})
	}

	t.Run("the StopRun starts the count toward Stuck, and only it does", func(t *testing.T) {
		stuck := stoppingS0(10100, 7)
		stuck.Stuck = true
		steps := append(degradedS0Steps(),
			viewed(10100, mc.Vm(2, "m2"), stopS0(7), wakeAt(12000)),
			tick(20000, core.KeepAlive{}, wakeAt(20100)),
			tick(20100, wakeAt(22000)),
		)
		runSteps(t, fencedS0(steps...), stepChecks{
			7: shardIs(0, stoppingS0(10100, 7)),
			8: shardIs(0, stuck),
		})
	})
}

// TestDegradedSessionLoss checks that a lost session ends a degraded shard's run as it does an owned
// one's, whether or not a Confirm is out, and that nothing about the Confirm survives it.
func TestDegradedSessionLoss(t *testing.T) {
	idle := func(since int64) core.ShardStatus {
		return core.ShardStatus{Since: mc.Ms(since), Holder: "h1", Wanted: true}
	}

	t.Run("while late", func(t *testing.T) {
		runSteps(t, fencedS0(append(degradedS0Steps(),
			failed(11000, core.CauseLost, stopLostS0(), wakeAt(21000)),
			tick(12000, wakeAt(21000)),
			runStopped(12100, 7),
		)...), stepChecks{
			6: all(shardIs(0, lostS0(11000, "h1")), doneIs(false)),
			8: all(shardIs(0, idle(12100)), doneIs(true)),
		})
	})

	t.Run("with a Confirm out", func(t *testing.T) {
		runSteps(t, fencedS0(append(confirmingS0Steps(),
			failed(11000, core.CauseLost, stopLostS0(), wakeAt(21000)),
			holderChanged(11100, "h1", 7, wakeAt(21000)),
			tick(12500, wakeAt(21000)),
			renewed(12600, 12500, wakeAt(21000)),
			runStopped(12700, 7),
			holderChanged(12800, "h1", 7),
		)...), stepChecks{
			7:  all(shardIs(0, lostS0(11000, "h1")), confirmPendingIs(false), doneIs(false)),
			8:  shardIs(0, lostS0(11000, "h1")),
			11: all(shardIs(0, idle(12700)), doneIs(true)),
			12: all(shardIs(0, idle(12700)), doneIs(true)),
		})
	})

	t.Run("the run stuck after it keeps its timer", func(t *testing.T) {
		stuck := lostS0(11000, "h1")
		stuck.Stuck = true
		runSteps(t, fencedS0(append(degradedS0Steps(),
			failed(11000, core.CauseLost, stopLostS0(), wakeAt(21000)),
			tick(21000),
		)...), stepChecks{7: all(shardIs(0, stuck), doneIs(false))})
	})
}

// TestConfirmTimersAtTheLargestReadings checks that a Confirm's timer fits in a Duration and stays after
// the clock, with the longest timing fields too, at readings a driver never reaches. The session is
// never live at the clock's ceiling, so no Confirm goes out there and the answer restores nothing.
func TestConfirmTimersAtTheLargestReadings(t *testing.T) {
	const (
		largest = time.Duration(math.MaxInt64)
		ceiling = largest - 24*time.Hour
	)
	for _, timing := range []core.Timing{
		core.DefaultTiming,
		{LeaseDuration: 24 * time.Hour, RenewDeadline: 21 * time.Hour, RetryPeriod: 17 * time.Hour, SlotGrace: 24 * time.Hour},
	} {
		for _, now := range []time.Duration{largest, ceiling, ceiling - 1, ceiling - time.Second, ceiling - time.Hour, ceiling - 48*time.Hour} {
			cfg := mc.CF(mc.S("s", 1))
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
			if got := step(core.Tick{Now: now}); !slices.Contains(got, degradeS0(7)) {
				t.Errorf("timing %v, reading %v: the deadline step returned %#v, want a Degrade", timing, now, got)
			}
			got := step(core.SessionRenewed{Now: now, SentAt: now})
			live := m.Status().Session == core.SessionStateLive
			if slices.Contains(got, confirmS0(7)) != live {
				t.Errorf("timing %v, reading %v: the renewal returned %#v with the session live %v", timing, now, got, live)
			}
			if at, armed := m.NextWake(); live && (!armed || at-m.Status().Now > timing.RetryPeriod) {
				t.Errorf("timing %v, reading %v: NextWake() = (%v, %v) with the clock at %v", timing, now, at, armed, m.Status().Now)
			}
			got = step(core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 7})
			if slices.Contains(got, restoreS0(7)) != live {
				t.Errorf("timing %v, reading %v: the answer returned %#v with the session live %v", timing, now, got, live)
			}
			step(core.Tick{Now: largest})
			if got := m.Status().Spaces[0].Shards[0].State; got != core.ShardStateDegraded {
				t.Errorf("timing %v, reading %v: at the ceiling the shard is %v, want degraded", timing, now, got)
			}
			step(core.SessionRenewed{Now: largest, SentAt: largest})
			step(core.SessionFailed{Now: largest, Cause: core.CauseLost})
			step(core.RunStopped{Now: largest, Space: "s", Term: 7})
			if !m.Status().Done {
				t.Errorf("timing %v, reading %v: Status = %+v, want Done", timing, now, m.Status())
			}
		}
	}
}

// TestRunChangesOnAnyWalk checks, on seeded walks of a fenced machine, the order of what a run is told.
// StartRun comes first and once, Degrade and Restore alternate starting with a Degrade, and neither
// comes after the run's StopRun. It also checks the rule against a record of what the driver has been
// told, not the machine's own flag: a Restore answers a Confirm returned while the session was live,
// with no step since in which it was not, and names this holder and the run's term. While a degraded
// shard is live, its Confirm is pending and its next one is no more than RetryPeriod from the last.
// The walks are the same on every run.
func TestRunChangesOnAnyWalk(t *testing.T) {
	type run struct {
		shard int
		term  core.Term
	}
	retry := core.DefaultTiming.RetryPeriod
	holders := []string{"h1", "h2", ""}

	var starts, degrades, degradedAgain, restores, confirms, stoppedDegraded, refused, forgotten int
	for seed := range uint64(300) {
		state := seed
		next := func(n uint64) uint64 {
			state = state*6364136223846793005 + 1442695040888963407
			return (state >> 33) % n
		}

		m := core.NewMachine(mc.CF(mc.S("s", 2)))
		started, degraded, wasDegraded, stopSent := map[run]bool{}, map[run]bool{}, map[run]bool{}, map[run]bool{}
		// pending holds a Confirm the driver may still answer so that it counts. stale holds one it
		// may still answer, but sent before the session last stopped being live.
		var pending, stale [2]bool
		var lastConfirm [2]time.Duration
		var history []core.Input
		var now time.Duration
		version := uint64(1)
		term := core.Term(1)

		step := func(in core.Input) {
			history = append(history, in)
			before := m.Status().Spaces[0].Shards
			got := m.Step(in)
			st := m.Status()
			live := st.Session == core.SessionStateLive
			if !live {
				for shard := range pending {
					stale[shard] = stale[shard] || pending[shard]
					pending[shard] = false
				}
			}
			answer, isRecord := in.(core.HolderChanged)
			if isRecord && answer.Shard >= 0 && answer.Shard < 2 {
				b := before[answer.Shard]
				if b.State == core.ShardStateDegraded && answer.Holder == "h1" && answer.Term == b.Term && !pending[answer.Shard] {
					refused++
					if stale[answer.Shard] {
						forgotten++
					}
				}
			}
			for _, a := range got {
				switch v := a.(type) {
				case core.StartRun:
					starts++
					k := run{v.Shard, v.Term}
					if started[k] || !live {
						t.Fatalf("seed %d: %+v with the session %v, started %v, after %+v", seed, v, st.Session, started[k], history)
					}
					started[k] = true
				case core.Degrade:
					degrades++
					k := run{v.Shard, v.Term}
					if !started[k] || stopSent[k] || degraded[k] || st.Session != core.SessionStateLate {
						t.Fatalf("seed %d: %+v with the session %v, started %v, stopped %v, degraded %v, after %+v",
							seed, v, st.Session, started[k], stopSent[k], degraded[k], history)
					}
					if wasDegraded[k] {
						degradedAgain++
					}
					degraded[k], wasDegraded[k] = true, true
				case core.Restore:
					restores++
					k := run{v.Shard, v.Term}
					answered := isRecord && answer.Shard == v.Shard && answer.Holder == "h1" && answer.Term == v.Term
					if !answered || !live || !pending[v.Shard] || !degraded[k] || stopSent[k] {
						t.Fatalf("seed %d: %+v for %+v with the session %v, pending %v, degraded %v, stopped %v, after %+v",
							seed, v, in, st.Session, pending[v.Shard], degraded[k], stopSent[k], history)
					}
					degraded[k] = false
					pending[v.Shard] = false
				case core.Confirm:
					confirms++
					k := run{v.Shard, v.Term}
					if !live || !degraded[k] || stopSent[k] {
						t.Fatalf("seed %d: %+v with the session %v, degraded %v, stopped %v, after %+v",
							seed, v, st.Session, degraded[k], stopSent[k], history)
					}
					pending[v.Shard], stale[v.Shard] = true, false
					lastConfirm[v.Shard] = st.Now
				case core.StopRun:
					k := run{v.Shard, v.Term}
					if !started[k] || stopSent[k] || v.Reason == core.StopExpired {
						t.Fatalf("seed %d: %+v with started %v, stopped %v, after %+v", seed, v, started[k], stopSent[k], history)
					}
					if degraded[k] {
						stoppedDegraded++
					}
					stopSent[k] = true
				}
			}
			at, armed := m.NextWake()
			for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
				t.Fatalf("seed %d: %s, after %+v", seed, v, history)
			}
			for shard, sh := range st.Spaces[0].Shards {
				k := run{shard, sh.Term}
				switch sh.State {
				case core.ShardStateOwned:
					if !started[k] || stopSent[k] || degraded[k] {
						t.Fatalf("seed %d: shard %d is %+v, degraded %v, after %+v", seed, shard, sh, degraded[k], history)
					}
				case core.ShardStateDegraded:
					if !started[k] || stopSent[k] || !degraded[k] || sh.Reason != core.StopNone || sh.Stuck {
						t.Fatalf("seed %d: shard %d is %+v, degraded %v, after %+v", seed, shard, sh, degraded[k], history)
					}
					if live && (!pending[shard] || !armed || at > lastConfirm[shard]+retry) {
						t.Fatalf("seed %d: shard %d is %+v with the session live, pending %v, last Confirm %v, wake %v, after %+v",
							seed, shard, sh, pending[shard], lastConfirm[shard], at, history)
					}
				}
			}
		}

		step(core.Tick{})
		step(core.SessionRenewed{SentAt: 0})
		step(core.ViewChanged{View: mc.Vm(version, "m1"), Wall: mc.Wall(0)})
		for range 300 {
			shard := 0
			if next(2) == 0 {
				shard = 1
			}
			sh := m.Status().Spaces[0].Shards[shard]
			now += time.Millisecond
			switch next(16) {
			case 0, 1:
				now += 100 * time.Millisecond
				step(core.Tick{Now: now})
			case 2:
				now += 2 * time.Second
				step(core.Tick{Now: now})
			case 3:
				now += 11 * time.Second
				step(core.Tick{Now: now})
			case 4, 5:
				step(core.SessionRenewed{Now: now, SentAt: now})
			case 6, 7, 8, 9:
				// The answer the rule is about, with or without a Confirm out.
				step(core.HolderChanged{Now: now, Space: "s", Shard: shard, Holder: "h1", Term: sh.Term})
			case 10:
				if next(2) == 0 {
					step(core.HolderChanged{Now: now, Space: "s", Shard: shard, Holder: holders[next(3)], Term: core.Term(next(uint64(term) + 2))})
				}
			case 11:
				if next(4) == 0 {
					version++
					members := [][]string{{"m1"}, {"m1"}, {"m1", "m2"}, {"m2", "m3"}}[next(4)]
					step(core.ViewChanged{Now: now, View: mc.Vm(version, members...), Wall: mc.Wall(now)})
				}
			case 12:
				step(core.RunStopped{Now: now, Space: "s", Shard: shard, Term: sh.Term})
			case 13:
				step(core.Released{Now: now, Space: "s", Shard: shard, Term: sh.Term})
			case 14:
				term++
				step(core.Acquired{Now: now, Space: "s", Shard: shard, Term: term})
			case 15:
				if next(64) == 0 {
					step(core.SessionFailed{Now: now, Cause: core.CauseLost})
				}
			}
		}
	}

	for _, c := range []struct {
		name  string
		count int
	}{
		{"StartRun", starts},
		{"Degrade", degrades},
		{"second Degrade of one run", degradedAgain},
		{"Restore", restores},
		{"Confirm", confirms},
		{"StopRun of a degraded run", stoppedDegraded},
		{"refused answer", refused},
		{"answer whose Confirm the session forgot", forgotten},
	} {
		if c.count == 0 {
			t.Errorf("no walk reached a %s", c.name)
		}
	}
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
// their shards. A StopRun comes from a view or a record and a StartRun from a grant, and no input is
// both, so no one step has both.
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

	// The shard an input names takes its place among the others: its actions do not come first.
	t.Run("the named shard goes in shard order", func(t *testing.T) {
		acquiring := core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(5100), Holder: "h1", Wanted: true}
		steps := append(mc.Boot(7, mc.S("s", 3)),
			viewed(1000, mc.Vm(2, "m2"), stop(0, 7), stop(1, 8), stop(2, 9), wakeAt(2000)),
			mc.Step{
				Input: core.RunStopped{Now: mc.Ms(1100), Space: "s", Shard: 0, Term: 7},
				Want:  []core.Action{release(0, 7), wakeAt(2000)},
			},
			mc.Step{
				Input: core.RunStopped{Now: mc.Ms(1100), Space: "s", Shard: 1, Term: 8},
				Want:  []core.Action{release(1, 8), wakeAt(2000)},
			},
			viewed(1200, mc.Vm(3, "m1", "m4"), wakeAt(2000)),
			keepAlive(2000, 3100),
			// The report for the last shard comes at the repeats of the first two.
			mc.Step{
				Input: core.RunStopped{Now: mc.Ms(3100), Space: "s", Shard: 2, Term: 9},
				Want:  []core.Action{release(0, 7), release(1, 8), release(2, 9), wakeAt(4000)},
			},
			keepAlive(4000, 5100),
			// The answer for the shard between them comes at the repeats of the other two, and the
			// shard, wanted again, acquires in that step.
			mc.Step{
				Input: core.Released{Now: mc.Ms(5100), Space: "s", Shard: 1, Term: 8},
				Want:  []core.Action{release(0, 7), core.Acquire{Space: "s", Shard: 1}, release(2, 9), wakeAt(6000)},
			},
		)
		runSteps(t, scenarioIn(mc.C(mc.S("s", 3)), steps...), stepChecks{len(steps): shardIs(1, acquiring)})
	})

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
			keepAlive(10000, 12000),
			acquired(10100, 9, releaseS0(9), wakeAt(12000)), // The repeat is due at @12.1.
			renewed(10200, 10200, extended(20200), wakeAt(12000)),
			mc.Step{
				Input: core.Acquired{Now: mc.Ms(10300), Space: "s", Shard: 1, Term: 8},
				Want:  []core.Action{core.StartRun{Space: "s", Shard: 1, Term: 8, Deadline: mc.Ms(20200)}, wakeAt(12000)},
			},
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
		viewed(200, mc.Vs(1, []string{"h/0"}, "slot-0"), core.ClaimSlot{}, wakeAt(2000)),
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

// TestSessionLostIsFinal checks that nothing brings a lost session back, from wherever it was lost.
// With no shard busy the machine is done at once. With a run going it first says StopRun, then waits
// for the run with no action but the Wake of its stuck timer, and is done at the RunStopped. Done, it
// returns nil for every input, while its clock keeps moving. A Drain after the loss does nothing, and a
// member that was draining when it lost the session never closes it.
func TestSessionLostIsFinal(t *testing.T) {
	const lostAt = 11000
	starts := []struct {
		name     string
		steps    []mc.Step
		deadline int64
		loss     []core.Action // The loss step's result.
		last     mc.Step       // Ends what is left of the shard's work, when Input is set.
		draining bool
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
		{
			// The deadline has passed but no step has seen it: the loss says why the run stops.
			name:     "with an owned run",
			steps:    mc.Boot(7, mc.S("s", 1)),
			deadline: 10000,
			loss:     []core.Action{stopLostS0(), wakeAt(lostAt + 10000)},
			last:     runStopped(lostAt+15000, 7),
		},
		{
			name:     "with an expired run",
			steps:    expiredS0Steps(),
			deadline: 10000,
			loss:     []core.Action{wakeAt(20000)},
			last:     runStopped(lostAt+15000, 7),
		},
		{
			name:     "with a stopping run",
			steps:    movedS0(),
			deadline: 10000,
			last:     runStopped(lostAt+15000, 7),
		},
		{
			name:     "with an Acquire out",
			steps:    acquiringS0(),
			deadline: 10000,
			last:     acquired(lostAt+15000, 9),
		},
		{
			name:     "with a Release out",
			steps:    append(movedS0(), runStopped(1500, 7, releaseS0(7), wakeAt(2000))),
			deadline: 10000,
		},
		{
			// The run is stuck in the loss step itself, so no timer is left.
			name:     "while draining a run",
			steps:    append(mc.Boot(7, mc.S("s", 1)), drained(1000, stopDrainingS0(), wakeAt(2000))),
			deadline: 10000,
			last:     runStopped(lostAt+15000, 7),
			draining: true,
		},
		{
			name: "while draining with a Release out",
			steps: append(mc.Boot(7, mc.S("s", 1)),
				drained(1000, stopDrainingS0(), wakeAt(2000)),
				runStopped(1500, 7, releaseS0(7), wakeAt(2000)),
			),
			deadline: 10000,
			draining: true,
		},
	}
	for _, start := range starts {
		t.Run(start.name, func(t *testing.T) {
			steps := append(slices.Clone(start.steps), failed(lostAt, core.CauseLost, start.loss...))
			lostIs := func(ms int64) func(*testing.T, *core.Machine) {
				return all(sessionIs(ms, core.SessionStateLost, start.deadline, core.CauseLost), drainingIs(start.draining))
			}
			checks := stepChecks{len(steps): lostIs(lostAt)}

			// Every later input does nothing, at a reading past every old timer, and the clock
			// keeps moving. Checked after every one of them: a later failure must not replace the
			// cause, and the last input must not be what puts it back.
			later := lostAt + core.DefaultTiming.LeaseDuration.Milliseconds()
			inputs := func(ms int64) []core.Input {
				now := mc.Ms(ms)
				return append(allInputs(now),
					core.SessionRenewed{Now: now, SentAt: now},
					core.SessionFailed{Now: now, Cause: core.CauseLost},
					core.SessionFailed{Now: now, Cause: core.CauseNone},
					core.Released{Now: now, Space: "s", Term: 7},
					core.RunStopped{Now: now, Space: "s", Term: 6},
					core.AcquireFailed{Now: now, Space: "other", Holder: "h2", RetryAfter: time.Second},
					core.HolderChanged{Now: now, Space: "s", Holder: "h2", Term: 8},
					core.HolderChanged{Now: now, Space: "s", Holder: "", Term: 0},
					core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 9},
					&core.Tick{Now: now},
				)
			}
			before := len(steps)
			for _, in := range inputs(later) {
				steps = append(steps, mc.Step{Input: in})
				checks[len(steps)] = lostIs(later)
			}
			if start.last.Input != nil {
				checks[before] = all(checks[before], doneIs(false))
				steps = append(steps, start.last)
				checks[len(steps)] = all(lostIs(later), doneIs(true))
				for _, in := range inputs(later + 1) {
					steps = append(steps, mc.Step{Input: in})
					checks[len(steps)] = lostIs(later + 1)
				}
			}
			m := runSteps(t, scenario(steps...), checks)
			if at, ok := m.NextWake(); at != 0 || ok {
				t.Errorf("NextWake() = (%v, %v), want (0, false)", at, ok)
			}
			if got := m.Step(core.Tick{Now: mc.Ms(later + 2)}); got != nil {
				t.Errorf("Step after the loss = %#v, want nil", got)
			}
			if st := m.Status(); st.Now != mc.Ms(later+2) || !st.Done {
				t.Errorf("Status after the loss = %+v, want Now %v and Done", st, mc.Ms(later+2))
			}
		})
	}
}

// TestDrain checks what a Drain does before the session closes: it is for good, nothing is wanted or
// acquired again, a request already out still gets its answer, and the session closes once every shard
// is idle, from whatever state the session is in.
func TestDrain(t *testing.T) {
	const live = core.SessionStateLive

	t.Run("a second Drain changes nothing", func(t *testing.T) {
		var first core.Status
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			drained(1000, stopDrainingS0(), wakeAt(2000)),
			drained(1100, wakeAt(2000)),
			drained(1200, wakeAt(2000)),
		)...), stepChecks{
			5: func(_ *testing.T, m *core.Machine) { first = m.Status() },
			7: func(t *testing.T, m *core.Machine) {
				t.Helper()
				st := m.Status()
				st.Now = first.Now
				if !reflect.DeepEqual(st, first) {
					t.Errorf("Status after a second Drain = %+v, want %+v", st, first)
				}
			},
		})
	})

	t.Run("as the first input it closes at once", func(t *testing.T) {
		runSteps(t, scenario(
			mc.Step{Input: core.Drain{}, Want: []core.Action{core.CloseSession{}, wakeAt(2000)}},
			tick(2000, core.CloseSession{}, wakeAt(4000)),
			sessionClosed(2100),
		), stepChecks{
			1: all(sessionIs(0, core.SessionStateClosing, 0, core.CauseNone), drainingIs(true)),
			3: closedAt(2100, 0),
		})
	})

	// The session never renewed, so there is nothing to hand over, and no KeepAlive is due once it closes.
	t.Run("while opening, before any renewal, it closes at once", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(0, 2000),
			viewed(100, mc.Vm(1, "m1"), wakeAt(2000)),
			drained(500, core.CloseSession{}, wakeAt(2500)),
			tick(2000, wakeAt(2500)),
			renewed(2100, 0, wakeAt(2500)),
			tick(2500, core.CloseSession{}, wakeAt(4500)),
			sessionClosed(2600),
		), stepChecks{
			2: shardIs(0, core.ShardStatus{Wanted: true}),
			3: all(sessionIs(500, core.SessionStateClosing, 0, core.CauseNone), shardIs(0, core.ShardStatus{})),
			5: sessionIs(2100, core.SessionStateClosing, 0, core.CauseNone),
			7: closedAt(2600, 0),
		})
	})

	t.Run("while opening with a Release out, it closes at the answer", func(t *testing.T) {
		runSteps(t, scenario(
			keepAlive(0, 2000),
			viewed(100, mc.Vm(1, "m1"), wakeAt(2000)),
			holderChanged(200, "h1", 9, releaseS0(9), wakeAt(2000)),
			drained(300, wakeAt(2000)),
			keepAlive(2000, 2200),
			tick(2200, releaseS0(9), wakeAt(4000)),
			released(2300, 9, core.CloseSession{}, wakeAt(4300)),
		), stepChecks{
			4: all(sessionIs(300, core.SessionStateOpening, 0, core.CauseNone), drainingIs(true), shardIs(0, noRunS0(200, 9))),
			7: all(sessionIs(2300, core.SessionStateClosing, 0, core.CauseNone), shardIs(0, idleS0(2300))),
		})
	})

	// A shard that has handed over stays idle while another still hands over: no view, record or answer
	// makes it wanted or acquires it.
	t.Run("nothing is wanted or acquired again", func(t *testing.T) {
		stopS1 := core.StopRun{Space: "s", Shard: 1, Term: 8, Reason: core.StopDraining}
		steps := append(mc.Boot(7, mc.S("s", 2)),
			drained(1000, stopDrainingS0(), stopS1, wakeAt(2000)),
			runStopped(1100, 7, releaseS0(7), wakeAt(2000)),
			released(1200, 7, wakeAt(2000)),
			viewed(1300, mc.Vm(2, "m1"), wakeAt(2000)),
			holderChanged(1400, "", 0, wakeAt(2000)),
			acquired(1500, 9, wakeAt(2000)),
			acquireFailed(1600, time.Second, wakeAt(2000)),
			keepAlive(2000, 4000),
			renewed(2100, 2000, extended(12000), wakeAt(4000)),
			keepAlive(4000, 6000),
			mc.Step{Input: core.RunStopped{Now: mc.Ms(4500), Space: "s", Shard: 1, Term: 8}, Want: []core.Action{
				core.Release{Space: "s", Shard: 1, Term: 8}, wakeAt(6000),
			}},
			mc.Step{Input: core.Released{Now: mc.Ms(4600), Space: "s", Shard: 1, Term: 8}, Want: []core.Action{
				core.CloseSession{}, wakeAt(6600),
			}},
		)
		idle := core.ShardStatus{Since: mc.Ms(1200), Holder: ""}
		runSteps(t, scenarioIn(mc.C(mc.S("s", 2)), steps...), stepChecks{
			8:  all(drainingIs(true), shardIs(0, idleS0(1200)), assignmentIs("s", "m1", "m1")),
			9:  all(shardIs(0, idleS0(1200)), assignmentIs("s", "m1", "m1")),
			10: shardIs(0, idle),
			12: shardIs(0, idle),
			14: all(sessionIs(2100, live, 12000, core.CauseNone), shardIs(0, idle)),
			17: all(sessionIs(4600, core.SessionStateClosing, 12000, core.CauseNone), shardIs(0, idle)),
		})
	})

	t.Run("a shard waiting to ask again waits no more", func(t *testing.T) {
		waiting := core.ShardStatus{Since: mc.Ms(400), Holder: "h2", Wanted: true, RetryAt: mc.Ms(2400)}
		steps := []mc.Step{
			keepAlive(0, 2000),
			renewed(100, 0, extended(10000), wakeAt(2000)),
			viewed(200, mc.Vm(1, "m1"), acquireS0(), core.Acquire{Space: "s", Shard: 1}, wakeAt(2000)),
			acquired(300, 7, startedS0(7, 10000), wakeAt(2000)),
			mc.Step{Input: core.AcquireFailed{Now: mc.Ms(400), Space: "s", Shard: 1, Holder: "h2", RetryAfter: time.Second}, Want: []core.Action{wakeAt(2000)}},
			drained(500, stopDrainingS0(), wakeAt(2000)),
			keepAlive(2000, 4000),
		}
		gaveUp := waiting
		gaveUp.Wanted, gaveUp.RetryAt = false, 0
		runSteps(t, scenarioIn(mc.C(mc.S("s", 2)), steps...), stepChecks{
			5: shardIs(1, waiting),
			6: all(shardIs(0, drainingS0(500, 7)), shardIs(1, gaveUp)),
			7: all(shardIs(1, gaveUp), nextWakeIs(4000)),
		})
	})

	// An Acquire that went out before the Drain is answered as usual, and the shard waits for no retry.
	for _, tc := range []struct {
		name   string
		answer []mc.Step
		idle   core.ShardStatus
		at     int64
	}{
		{
			name:   "no grant",
			answer: []mc.Step{acquired(300, 0, core.CloseSession{}, wakeAt(2300))},
			idle:   core.ShardStatus{Since: mc.Ms(300)},
			at:     300,
		},
		{
			name:   "another holder",
			answer: []mc.Step{acquireFailed(300, time.Second, core.CloseSession{}, wakeAt(2300))},
			idle:   core.ShardStatus{Since: mc.Ms(300), Holder: "h2"},
			at:     300,
		},
		{
			name:   "a grant, released with no run",
			answer: []mc.Step{acquired(300, 7, releaseS0(7), wakeAt(2000)), released(400, 7, core.CloseSession{}, wakeAt(2400))},
			idle:   idleS0(400),
			at:     400,
		},
		{
			name:   "a grant through a record, released with no run",
			answer: []mc.Step{holderChanged(300, "h1", 7, releaseS0(7), wakeAt(2000)), released(400, 7, core.CloseSession{}, wakeAt(2400))},
			idle:   idleS0(400),
			at:     400,
		},
	} {
		t.Run("an Acquire out at the Drain: "+tc.name, func(t *testing.T) {
			steps := append(acquiringS0(), drained(250, wakeAt(2000)))
			steps = append(steps, tc.answer...)
			runSteps(t, scenario(steps...), stepChecks{
				4:          all(sessionIs(250, live, 10000, core.CauseNone), shardIs(0, core.ShardStatus{State: core.ShardStateAcquiring, Since: mc.Ms(200)})),
				len(steps): all(sessionIs(tc.at, core.SessionStateClosing, 10000, core.CauseNone), shardIs(0, tc.idle)),
			})
		})
	}
}

// TestSessionClosedBeforeClosing checks that a SessionClosed nobody asked for does nothing, whether the
// member drains or not, and that the session still closes the usual way afterwards.
func TestSessionClosedBeforeClosing(t *testing.T) {
	t.Run("opening", func(t *testing.T) {
		runSteps(t, scenario(keepAlive(0, 2000), sessionClosed(100, wakeAt(2000))), stepChecks{
			2: all(sessionIs(100, core.SessionStateOpening, 0, core.CauseNone), doneIs(false)),
		})
	})
	t.Run("live", func(t *testing.T) {
		runSteps(t, scenario(append(bootedOnly(), sessionClosed(300, wakeAt(2000)))...), stepChecks{
			4: all(sessionIs(300, core.SessionStateLive, 10000, core.CauseNone), doneIs(false)),
		})
	})
	t.Run("late", func(t *testing.T) {
		runSteps(t, scenario(append(bootedOnly(), keepAlive(10000, 12000), sessionClosed(10100, wakeAt(12000)))...), stepChecks{
			5: all(sessionIs(10100, core.SessionStateLate, 10000, core.CauseNone), doneIs(false)),
		})
	})
	t.Run("draining with a run going", func(t *testing.T) {
		runSteps(t, scenario(append(mc.Boot(7, mc.S("s", 1)),
			drained(1000, stopDrainingS0(), wakeAt(2000)),
			sessionClosed(1100, wakeAt(2000)),
			runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
			sessionClosed(1300, wakeAt(2000)),
			released(1400, 7, core.CloseSession{}, wakeAt(3400)),
			sessionClosed(1500),
		)...), stepChecks{
			6:  all(sessionIs(1100, core.SessionStateLive, 10000, core.CauseNone), shardIs(0, drainingS0(1000, 7))),
			8:  all(sessionIs(1300, core.SessionStateLive, 10000, core.CauseNone), shardIs(0, releasingDrainedS0(1200, 7))),
			9:  sessionIs(1400, core.SessionStateClosing, 10000, core.CauseNone),
			10: closedAt(1500, 10000),
		})
	})
}

// closingS0 boots a machine of the one shard s/0, whose run has term 7, drains it at @1 and starts
// closing the session at @1.4: seven steps, with the next CloseSession at @3.4.
func closingS0() []mc.Step {
	return append(mc.Boot(7, mc.S("s", 1)),
		drained(1000, stopDrainingS0(), wakeAt(2000)),
		runStopped(1200, 7, releaseS0(7), wakeAt(2000)),
		released(1400, 7, core.CloseSession{}, wakeAt(3400)),
	)
}

// TestClosing checks the closing session: only a Tick and a SessionClosed take effect, the CloseSession
// repeats every RetryPeriod counted from the last one and is the only timer, and the clock ceiling holds.
func TestClosing(t *testing.T) {
	const closing = core.SessionStateClosing

	t.Run("every other input is ignored", func(t *testing.T) {
		now := mc.Ms(2000)
		inputs := append(allInputs(now),
			core.SessionFailed{Now: now, Cause: core.CauseLost},
			core.SessionRenewed{Now: now, SentAt: mc.Ms(1900)},
			core.ViewChanged{Now: now, View: mc.Vm(5, "m2"), Wall: mc.Wall(now)},
			core.Acquired{Now: now, Space: "s", Term: 8},
			core.Released{Now: now, Space: "s", Term: 7},
			core.RunStopped{Now: now, Space: "s", Term: 7},
			core.HolderChanged{Now: now, Space: "s", Holder: "h1", Term: 9},
			core.HolderChanged{Now: now, Space: "s", Holder: "h2", Term: 9},
			core.AcquireFailed{Now: now, Space: "s", Holder: "h2", RetryAfter: time.Second},
			&core.SessionClosed{Now: now},
		)
		for _, in := range inputs {
			if _, ok := in.(core.SessionClosed); ok {
				continue
			}
			t.Run(fmt.Sprintf("%T%+v", in, in), func(t *testing.T) {
				var before core.Status
				runSteps(t, scenario(append(closingS0(), mc.Step{Input: in, Want: []core.Action{wakeAt(3400)}})...), stepChecks{
					7: func(_ *testing.T, m *core.Machine) { before = m.Status() },
					8: func(t *testing.T, m *core.Machine) {
						t.Helper()
						want := before
						if inputNowOf(in) > before.Now {
							want.Now = inputNowOf(in)
						}
						if got := m.Status(); !reflect.DeepEqual(got, want) {
							t.Errorf("Status = %+v, want %+v", got, want)
						}
					},
				})
			})
		}
	})

	t.Run("a lost session reported while closing is ignored", func(t *testing.T) {
		runSteps(t, scenario(append(closingS0(),
			failed(2000, core.CauseLost, wakeAt(3400)),
			tick(3400, core.CloseSession{}, wakeAt(5400)),
			sessionClosed(3500),
		)...), stepChecks{
			8:  all(sessionIs(2000, closing, 10000, core.CauseNone), doneIs(false)),
			10: closedAt(3500, 10000),
		})
	})

	t.Run("the CloseSession repeats at exactly X + RetryPeriod, counted from the last one", func(t *testing.T) {
		runSteps(t, scenario(append(closingS0(),
			tick(3399, wakeAt(3400)),
			tick(3400, core.CloseSession{}, wakeAt(5400)),
			tick(5399, wakeAt(5400)),
			tick(5400, core.CloseSession{}, wakeAt(7400)),
			tick(9000, core.CloseSession{}, wakeAt(11000)),
			tick(10999, wakeAt(11000)),
		)...), stepChecks{
			8:  nextWakeIs(3400),
			9:  all(sessionIs(3400, closing, 10000, core.CauseNone), nextWakeIs(5400)),
			12: all(sessionIs(9000, closing, 10000, core.CauseNone), nextWakeIs(11000)),
		})
	})

	// An ignored input skips only its own effect, so the CloseSession due at its reading still goes.
	t.Run("an ignored input at the repeat still returns the CloseSession", func(t *testing.T) {
		runSteps(t, scenario(append(closingS0(),
			runStopped(3400, 7, core.CloseSession{}, wakeAt(5400)),
			renewed(5400, 5400, core.CloseSession{}, wakeAt(7400)),
		)...), stepChecks{
			9: all(sessionIs(5400, closing, 10000, core.CauseNone), nextWakeIs(7400)),
		})
	})

	t.Run("the session deadline passing changes nothing", func(t *testing.T) {
		runSteps(t, scenario(append(closingS0(),
			tick(9400, core.CloseSession{}, wakeAt(11400)),
			tick(11400, core.CloseSession{}, wakeAt(13400)),
		)...), stepChecks{
			9: all(sessionIs(11400, closing, 10000, core.CauseNone), shardIs(0, idleS0(1400))),
		})
	})

	// The clock stops at maxReading, so a CloseSession armed after it never comes due: the driver has
	// broken its contract to get there. Each timer is still after the clock and fits in a Duration.
	t.Run("at the clock ceiling", func(t *testing.T) {
		const retry = 2 * time.Second
		steps := []mc.Step{
			{Input: core.Tick{Now: maxReading - time.Second}, Want: []core.Action{core.KeepAlive{}, core.Wake{At: maxReading + time.Second}}},
			{Input: core.Drain{Now: maxReading - time.Millisecond}, Want: []core.Action{core.CloseSession{}, core.Wake{At: maxReading - time.Millisecond + retry}}},
			{Input: core.Tick{Now: math.MaxInt64}, Want: []core.Action{core.Wake{At: maxReading - time.Millisecond + retry}}},
			{Input: core.SessionClosed{Now: math.MaxInt64}},
			{Input: core.Tick{Now: math.MaxInt64}},
		}
		runSteps(t, scenario(steps...), stepChecks{
			3: func(t *testing.T, m *core.Machine) {
				t.Helper()
				if st := m.Status(); st.Now != maxReading || st.Session != closing {
					t.Errorf("Status = %+v, want Now %v and closing", st, maxReading)
				}
			},
			5: func(t *testing.T, m *core.Machine) {
				t.Helper()
				if st := m.Status(); st.Now != maxReading || st.Session != core.SessionStateClosed || !st.Done {
					t.Errorf("Status = %+v, want Now %v, closed and done", st, maxReading)
				}
			},
		})
		runSteps(t, scenario(
			mc.Step{Input: core.Drain{Now: math.MaxInt64}, Want: []core.Action{core.CloseSession{}, core.Wake{At: maxReading + retry}}},
			mc.Step{Input: core.Tick{Now: math.MaxInt64}, Want: []core.Action{core.Wake{At: maxReading + retry}}},
		), nil)
	})
}

// inputNowOf returns the reading of one of the machine's inputs, or 0 for any other value.
func inputNowOf(in core.Input) time.Duration {
	if v := reflect.ValueOf(in); v.Kind() == reflect.Struct {
		if f := v.FieldByName("Now"); f.IsValid() {
			return time.Duration(f.Int())
		}
	}
	return 0
}

// TestCloseSessionWaitsForEveryRun checks the promise that closing rests on: the session closes only once
// every run has returned, stuck or not, and every request is answered, because closing makes every record
// of this member's takeable. From each state a shard can be in, the member drains, a record of any kind
// arrives, and a driver then answers whatever is outstanding, after letting a run go stuck and feeding it
// every input that must not end it. No StartRun or Acquire goes out after the Drain, no CloseSession while
// a run is going or a shard is busy, and the session closes in the end.
func TestCloseSessionWaitsForEveryRun(t *testing.T) {
	starts := []struct {
		name    string
		cfg     core.MachineConfig
		steps   []mc.Step
		running core.Term // The run going when the start ends, or 0.
	}{
		{name: "owned", cfg: mc.C(mc.S("s", 1)), steps: mc.Boot(7, mc.S("s", 1)), running: 7},
		{name: "degraded", cfg: mc.CF(mc.S("s", 1)), steps: degradedS0Steps(), running: 7},
		{name: "degraded with a Confirm out", cfg: mc.CF(mc.S("s", 1)), steps: confirmingS0Steps(), running: 7},
		{name: "stopping", cfg: mc.C(mc.S("s", 1)), steps: movedS0(), running: 7},
		{name: "expired", cfg: mc.C(mc.S("s", 1)), steps: expiredS0Steps(), running: 7},
		{name: "lost", cfg: mc.C(mc.S("s", 1)), steps: ownedAndLostS0(), running: 7},
		{name: "releasing", cfg: mc.C(mc.S("s", 1)), steps: append(movedS0(), runStopped(1500, 7, releaseS0(7), wakeAt(2000)))},
		{name: "releasing with no run", cfg: mc.C(mc.S("s", 1)), steps: append(bootedOnly(), holderChanged(300, "h1", 9, releaseS0(9), wakeAt(2000)))},
		{name: "acquiring", cfg: mc.C(mc.S("s", 1)), steps: acquiringS0()},
		{name: "idle", cfg: mc.C(mc.S("s", 1)), steps: bootedOnly()},
	}
	records := []struct {
		holder string
		term   core.Term
	}{{"h1", 7}, {"h1", 8}, {"h1", 6}, {"h1", 0}, {"h1", 9}, {"h2", 7}, {"h2", 8}, {"h2", 6}, {"h2", 0}, {"", 0}, {"", 8}}

	var stuckRuns, lateClosings int
	for _, start := range starts {
		for _, rec := range records {
			t.Run(fmt.Sprintf("%s, then a record %q term %d", start.name, rec.holder, rec.term), func(t *testing.T) {
				m := runSteps(t, scenarioIn(start.cfg, start.steps...), nil)
				now := m.Status().Now
				running := start.running
				var history []core.Input
				step := func(in core.Input) {
					t.Helper()
					history = append(history, in)
					if v, ok := in.(core.RunStopped); ok && v.Shard == 0 && v.Space == "s" && v.Term == running && running != 0 {
						if sh := m.Status().Spaces[0].Shards[0]; sh.Term == v.Term && (sh.State == core.ShardStateStopping ||
							sh.State == core.ShardStateExpired || sh.State == core.ShardStateLost) {
							running = 0
						}
					}
					got := m.Step(in)
					at, armed := m.NextWake()
					st := m.Status()
					for _, v := range wakeViolations(got, st.Now, st.Done, at, armed) {
						t.Fatalf("%s, after %+v", v, history)
					}
					sh := st.Spaces[0].Shards[0]
					for _, a := range got {
						switch a.(type) {
						case core.StartRun, core.Acquire:
							t.Fatalf("%#v after the Drain, after %+v", a, history)
						case core.Release:
							if running != 0 {
								t.Fatalf("%#v while term %d runs, after %+v", a, running, history)
							}
						case core.CloseSession:
							if running != 0 || sh.State != core.ShardStateIdle || st.Session != core.SessionStateClosing {
								t.Fatalf("CloseSession while term %d runs, shard %+v, after %+v", running, sh, history)
							}
						}
					}
					if (st.Session == core.SessionStateClosing || st.Session == core.SessionStateClosed) &&
						(running != 0 || sh.State != core.ShardStateIdle) {
						t.Fatalf("session %v while term %d runs, shard %+v, after %+v", st.Session, running, sh, history)
					}
				}

				now += time.Millisecond
				step(core.Drain{Now: now})
				now += time.Millisecond
				step(core.HolderChanged{Now: now, Space: "s", Holder: rec.holder, Term: rec.term})
				for range 10 {
					st := m.Status()
					if st.Session == core.SessionStateClosing {
						break
					}
					sh := st.Spaces[0].Shards[0]
					switch sh.State {
					case core.ShardStateStopping, core.ShardStateExpired, core.ShardStateLost:
						// Long enough for the run to go stuck, with every input that must not end it.
						now += core.DefaultTiming.RenewDeadline + time.Second
						step(core.Tick{Now: now})
						if m.Status().Spaces[0].Shards[0].Stuck {
							stuckRuns++
						}
						for _, in := range append(adversaries(now), recordsOfAnotherRun(now)...) {
							step(in)
						}
						now += time.Millisecond
						step(core.RunStopped{Now: now, Space: "s", Term: m.Status().Spaces[0].Shards[0].Term})
					case core.ShardStateReleasing:
						now += time.Millisecond
						step(core.Released{Now: now, Space: "s", Term: sh.Term})
					case core.ShardStateAcquiring:
						now += time.Millisecond
						step(core.Acquired{Now: now, Space: "s", Term: 0})
					default:
						t.Fatalf("shard %+v while draining, after %+v", sh, history)
					}
				}
				st := m.Status()
				if st.Session != core.SessionStateClosing || running != 0 || !st.Draining {
					t.Fatalf("Status = %+v with term %d running, want closing, after %+v", st, running, history)
				}
				if m.Status().Deadline <= st.Now {
					lateClosings++
				}
				now += time.Millisecond
				step(core.SessionClosed{Now: now})
				if st := m.Status(); st.Session != core.SessionStateClosed || !st.Done {
					t.Fatalf("Status = %+v, want closed and done", st)
				}
			})
		}
	}
	if stuckRuns == 0 || lateClosings == 0 {
		t.Errorf("%d stuck runs and %d closings of a late session, want some of each", stuckRuns, lateClosings)
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
// returns nil for every input and its reading stays where it was.
func TestStepClockClosedMachine(t *testing.T) {
	const closedAt = time.Second
	m := runSteps(t, scenario(
		keepAlive(0, 2000),
		drained(500, core.CloseSession{}, wakeAt(2500)),
		sessionClosed(1000),
	), stepChecks{3: all(sessionIs(1000, core.SessionStateClosed, 0, core.CauseNone), doneIs(true))})
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
