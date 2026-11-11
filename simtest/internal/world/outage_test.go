// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
)

// outageSpaces are the spaces of the outage tests: book unfenced, ledger fenced.
var outageSpaces = []core.SpaceSpec{{Name: "book", Shards: 2}, {Name: "ledger", Shards: 2, Fenced: true}}

// The holder and the times of the outage tests: the outage runs from 40 s to 54 s.
const (
	testOutageHolder = "m2/0b"
	testOutageAt     = kernel.Time(40 * time.Second)
	testOutageHeal   = kernel.Time(54 * time.Second)
)

// sec returns s seconds.
func sec(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// started returns a watch of the outage in which testOutageHolder runs book/0 and ledger/0 in
// term 5, with the session deadline 48 s.
func started() *outageWatch {
	o := newOutageWatch(testOutageAt, testOutageHeal, outageSpaces)
	o.step(testOutageHolder, kernel.Time(sec(10)), core.Tick{Now: sec(10)}, []core.Action{
		core.StartRun{Space: "book", Shard: 0, Term: 5, Deadline: sec(20)},
		core.StartRun{Space: "ledger", Shard: 0, Term: 5, Deadline: sec(20)},
		core.ExtendDeadline{Deadline: sec(48)},
	})
	return o
}

// TestOutageMessages checks each rule of tenure/outage and its exact message, and that the
// expected course of an outage passes.
func TestOutageMessages(t *testing.T) {
	stop := func(space string, reason core.StopReason) core.StopRun {
		return core.StopRun{Space: space, Shard: 0, Term: 5, Reason: reason}
	}
	tick := func(o *outageWatch, at float64, acts ...core.Action) {
		o.step(testOutageHolder, kernel.Time(sec(at)), core.Tick{Now: sec(at)}, acts)
	}
	tests := []struct {
		name string
		run  func(o *outageWatch)
		want string
	}{
		{"expected course", func(o *outageWatch) {
			tick(o, 48, stop("book", core.StopExpired), core.Degrade{Space: "ledger", Shard: 0, Term: 5})
			tick(o, 54.01, core.Restore{Space: "ledger", Shard: 0, Term: 5})
		}, ""},
		{"lost after the heal", func(o *outageWatch) {
			tick(o, 48, stop("book", core.StopExpired), core.Degrade{Space: "ledger", Shard: 0, Term: 5})
			tick(o, 54.01, stop("ledger", core.StopMoved))
		}, ""},
		{"stopped during the outage", func(o *outageWatch) {
			tick(o, 47, stop("ledger", core.StopMoved))
		}, "outage: ledger/0: m2/0b stopped its run in term 5 at 47.000000000s, during the outage, with reason moved and no sign of loss"},
		{"stopped after another holder", func(o *outageWatch) {
			o.step(testOutageHolder, kernel.Time(sec(47)), core.HolderChanged{Space: "ledger", Shard: 0, Holder: "m3/0c", Term: 6, Now: sec(47)},
				[]core.Action{stop("ledger", core.StopLost)})
		}, ""},
		{"stopped after a lost session", func(o *outageWatch) {
			o.step(testOutageHolder, kernel.Time(sec(47)), core.SessionFailed{Cause: core.CauseLost, Now: sec(47)},
				[]core.Action{stop("ledger", core.StopLost)})
		}, ""},
		{"own record at another term", func(o *outageWatch) {
			o.step(testOutageHolder, kernel.Time(sec(47)), core.HolderChanged{Space: "ledger", Shard: 0, Holder: testOutageHolder, Term: 7, Now: sec(47)},
				[]core.Action{stop("ledger", core.StopLost)})
		}, ""},
		{"fenced run expired", func(o *outageWatch) {
			tick(o, 60, stop("ledger", core.StopExpired))
		}, "outage: ledger/0: m2/0b stopped its fenced run in term 5 at 60.000000000s with reason expired"},
		{"still degraded", func(o *outageWatch) {
			tick(o, 48, stop("book", core.StopExpired), core.Degrade{Space: "ledger", Shard: 0, Term: 5})
		}, "outage: ledger/0: m2/0b is still degraded in term 5 at the end"},
		{"unfenced run past its deadline", func(o *outageWatch) {
			tick(o, 48.5, core.Degrade{Space: "ledger", Shard: 0, Term: 5})
		}, "outage: book/0: m2/0b still runs in term 5 at 48.500000000s, past its session deadline 48.000000000s"},
		{"unfenced run stopped otherwise", func(o *outageWatch) {
			tick(o, 48, stop("book", core.StopMoved))
		}, "outage: book/0: m2/0b stopped its run in term 5 at its session deadline 48.000000000s with reason moved, want expired"},
		{"deadline outside the outage", func(o *outageWatch) {
			tick(o, 39, core.ExtendDeadline{Deadline: sec(60)})
			tick(o, 60.5, core.Restore{Space: "ledger", Shard: 0, Term: 5})
		}, ""},
		{"no step at the deadline", func(o *outageWatch) {
			o.atHeal(func(string) time.Duration { return sec(54) }, func(string) bool { return true })
		}, "outage: book/0: m2/0b still runs in term 5 at 54.000000000s, past its session deadline 48.000000000s"},
		{"heal within the timer's slack", func(o *outageWatch) {
			o.atHeal(func(string) time.Duration { return sec(48) + timerSlack - 1 }, func(string) bool { return true })
		}, ""},
		{"heal of a holder gone", func(o *outageWatch) {
			o.atHeal(func(string) time.Duration { return sec(54) }, func(string) bool { return false })
		}, ""},
		{"no owner at the bound", func(o *outageWatch) {
			owners := func(space string, shard int) []string {
				if space == "book" && shard == 1 {
					return nil
				}
				return []string{"m1/0a"}
			}
			o.atBound(testOutageHeal.Add(sec(21.045)), sec(21.045), outageSpaces, owners)
		}, "outage: book/1 has no owner at 75.045000000s (heal 54.000000000s + bound 21.045000000s)"},
		{"fenced shard without an owner at the bound", func(o *outageWatch) {
			owners := func(space string, _ int) []string {
				if space == "ledger" {
					return nil
				}
				return []string{"m1/0a"}
			}
			o.atBound(testOutageHeal.Add(sec(21.045)), sec(21.045), outageSpaces, owners)
		}, ""},
		{"first violation kept", func(o *outageWatch) {
			tick(o, 47, stop("ledger", core.StopMoved))
			tick(o, 48.5)
		}, "outage: ledger/0: m2/0b stopped its run in term 5 at 47.000000000s, during the outage, with reason moved and no sign of loss"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := started()
			tt.run(o)
			if got := message(o.final()); got != tt.want {
				t.Errorf("final = %q\nwant    %q", got, tt.want)
			}
		})
	}
}

// TestOutageScript checks the outage's fault script and recovery time: the store is isolated
// at At and the network healed at At + For, and R is the heal, also in a run with faults whose
// quiet window starts earlier.
func TestOutageScript(t *testing.T) {
	evs := outageEvents(testOutageAt, testOutageHeal)
	if !reflect.DeepEqual(evs, []fault.Event{
		{At: testOutageAt, Kind: fault.KindIsolate, Node: storeName},
		{At: testOutageHeal, Kind: fault.KindHeal},
	}) {
		t.Errorf("outageEvents = %+v", evs)
	}
	for _, sc := range []Scenario{
		{Outage: &Outage{At: 40 * time.Second, For: 14 * time.Second}},
		{Outage: &Outage{At: 40 * time.Second, For: 14 * time.Second}, Faults: &fault.Random{Quiet: 120 * time.Second}},
	} {
		st, err := newSetup(sc, kernel.Time(150*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if st.recovery != testOutageHeal {
			t.Errorf("recovery %v, want the heal %v", st.recovery, testOutageHeal)
		}
	}
	if r := recoveryStart(Scenario{}.withDefaults(), 0, kernel.Time(time.Minute)); r != 0 {
		t.Errorf("a scenario without an outage or faults has the recovery time %v", r)
	}
}

// outageEnv makes TestOutageChild run.
const outageEnv = "TENURE_SIM_OUTAGE"

// TestOutageRegistered checks that Build registers tenure/outage, after tenure/g5 and
// tenure/converged, in a world with an outage, and that a run in which a fenced run stops in
// the middle of it fails that final with its message. The child process reports such a
// StopRun to the world's watch at 45 s.
func TestOutageRegistered(t *testing.T) {
	if os.Getenv(outageEnv) != "" {
		t.Skip("in the child process")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestOutageChild$", "-test.v") //nolint:gosec // the test binary itself
	cmd.Env = append(childEnviron(), outageEnv+"=1", "FAULTLINE_ARTIFACTS=off")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("child exited with %v, want a failure; output:\n%s", err, out.String())
	}
	for _, want := range []string{
		`final check "tenure/outage" failed after the run`,
		"outage: ledger/0: m9/09 stopped its run in term 3 at 45.000000000s, during the outage, with reason moved and no sign of loss",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("child's output does not contain %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), `"tenure/g5"`) || strings.Contains(out.String(), `"tenure/converged"`) {
		t.Errorf("child's output names another final:\n%s", out.String())
	}
}

// TestOutageChild is the child process of TestOutageRegistered.
func TestOutageChild(t *testing.T) {
	if os.Getenv(outageEnv) == "" {
		t.Skip("runs only as the child process of TestOutageRegistered")
	}
	sc := Scenario{Spaces: outageSpaces, Outage: &Outage{At: 40 * time.Second, For: 5 * time.Second}}
	faultline.Run(t, faultline.Options{Seeds: 1, Duration: 90 * time.Second}, func(fw *faultline.World) {
		w := Build(fw, sc)
		fw.Sim.At(kernel.Time(45*time.Second), "test/stop", func() {
			o := w.outage
			o.step("m9/09", fw.Sim.Now(), core.Tick{}, []core.Action{core.StartRun{Space: "ledger", Shard: 0, Term: 3}})
			o.step("m9/09", fw.Sim.Now(), core.Tick{}, []core.Action{core.StopRun{Space: "ledger", Shard: 0, Term: 3, Reason: core.StopMoved}})
		})
	})
}
