// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// mutants pairs each planted bug's test with the invariant whose violation must fail it
// (SIM-660).
var mutants = []struct {
	test, check string
}{
	{"TestMutantGrantHeld", "tenure/g4"},
	{"TestMutantSkipRaise", "tenure/g3"},
	{"TestMutantLateDeadline", "tenure/g2"},
	{"TestMutantReadConfirm", "tenure/g2"},
}

// TestMutantsCaught checks AT-SIM-37: each planted bug fails the check it breaks. For each
// TestMutant<Name> it runs
//
//	go test -tags tenure_mutants -run '^TestMutant<Name>$' -count=1 .
//
// in a child process, with no FAULTLINE_ variable of this process (and FAULTLINE_ARTIFACTS=off,
// so that the failing seed leaves no files behind), and requires the child to fail with output
// naming the check, and naming no other failure (caughtFor). The output is matched by
// substring, as a coverage build adds lines to it.
// The first child builds the test binary with the tag; the others reuse the build cache.
func TestMutantsCaught(t *testing.T) {
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("no go command to build the planted bugs with: %v", err)
	}
	env := append(slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "FAULTLINE_") }),
		"FAULTLINE_ARTIFACTS=off")
	for _, m := range mutants {
		t.Run(m.test, func(t *testing.T) {
			// The command is the go tool, from PATH, on this package, with a test name of the
			// fixed list above: no input of the test's caller reaches it.
			cmd := exec.Command(gobin, "test", "-tags", "tenure_mutants", "-run", "^"+m.test+"$", "-count=1", ".") //nolint:gosec // fixed arguments, see above
			cmd.Env = env
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() == 0 {
				t.Fatalf("%s exited with %v, want a failure; output:\n%s", m.test, err, out.String())
			}
			if err := caughtFor(out.String(), m.check); err != "" {
				t.Errorf("%s %s; output:\n%s", m.test, err, out.String())
			}
		})
	}
}

// caughtFor returns "" when out, the output of a TestMutant child, shows its run failing the
// invariant check and nothing else, and otherwise what is wrong: a mutant that fails for
// another reason, such as another invariant, a final, a panic or a setup error, shows nothing
// about the check it was planted to break.
func caughtFor(out, check string) string {
	want := `invariant "` + check + `" violated`
	switch {
	case !strings.Contains(out, want):
		return "does not fail with " + strconv.Quote(want)
	case strings.Count(out, `invariant "`) != strings.Count(out, want):
		return "fails another invariant too"
	case strings.Contains(out, `final check "`):
		return "fails a final check too"
	case strings.Contains(out, "panic:"):
		return "panics"
	}
	return ""
}

// Timing of cutRow's world: m1 is cut off from the store from cutAt to healAt, and the run
// lasts cutEnd, long enough to check tenure/g5 for the bound after the heal.
const (
	cutAt  = 10 * time.Second
	healAt = 40 * time.Second
	cutEnd = 65 * time.Second
)

// cutRow returns the row of the world into which TestMutantSkipRaise and
// TestMutantLateDeadline plant their bugs, with m as the scenario's Mutation: 3 seeds of cutEnd
// of three members under the Server rule, the space book of 8 shards, fenced or not, and every
// invariant asserted, G2 included. Its faults are cutSetup's script, from cutAt to healAt; the
// scenario's Faults has no rule and is quiet from healAt, so the world's Recovery is healAt and
// tenure/g5 judges the shards from then on.
func cutRow(name string, m world.Mutation, fenced bool) row {
	return row{
		name: name,
		sc: world.Scenario{
			Members: 3, StopDelay: time.Second, Mutation: m,
			Spaces: []core.SpaceSpec{{Name: "book", Shards: 8, Fenced: fenced}},
			Faults: &fault.Random{Quiet: cutEnd - healAt},
		},
		seeds: 3, duration: cutEnd, net: defaultNet, g2: true,
		rules: []world.Takeover{world.Server},
	}
}

// cutSetup is the part of cutRow's world function after Build: it checks that the world
// recovers at healAt, and plans m1's cut from the store, both ways, from cutAt to healAt.
func cutSetup(fw *faultline.World, w *world.World) {
	if got := w.Recovery(); got != kernel.Time(healAt) {
		fw.T().Fatalf("cut world: Recovery %v, want the heal at %v", got, kernel.Time(healAt))
	}
	cut, heal := kernel.Time(cutAt), kernel.Time(healAt)
	fw.Plan(fault.Script(
		fault.Event{At: cut, Kind: fault.KindCut, Node: "m1", Peer: "store"},
		fault.Event{At: cut, Kind: fault.KindCut, Node: "store", Peer: "m1"},
		fault.Event{At: heal, Kind: fault.KindHealLink, Node: "m1", Peer: "store"},
		fault.Event{At: heal, Kind: fault.KindHealLink, Node: "store", Peer: "m1"},
	))
}

// TestUnplantedWorlds runs the worlds of TestMutantSkipRaise and TestMutantLateDeadline with no
// bug planted, fenced and unfenced, and every check must hold: the failures TestMutantsCaught
// requires come from the bugs, not from the worlds. (TestMutantGrantHeld plants into
// TestSteady's row and TestMutantReadConfirm into TestConfirmRace's, which run on their own.)
func TestUnplantedWorlds(t *testing.T) {
	for _, fenced := range []bool{true, false} {
		name := "unfenced"
		if fenced {
			name = "fenced"
		}
		t.Run(name, func(t *testing.T) {
			r := cutRow(t.Name(), world.MutationNone, fenced)
			faultline.Run(t, r.options(), r.body(world.Server, cutSetup))
		})
	}
}

// TestCaughtFor checks that caughtFor accepts a failure of the planted check alone and nothing
// else.
func TestCaughtFor(t *testing.T) {
	const g2 = `faultline: invariant "tenure/g2" violated at t=1s (event 9)`
	for _, c := range []struct {
		out  string
		want bool
	}{
		{g2 + "\nFAIL", true},
		{`faultline: invariant "tenure/g3" violated at t=1s` + "\nFAIL", false},
		{g2 + "\n" + `faultline: invariant "tenure/g4" violated at t=2s`, false},
		{g2 + "\n" + `faultline: final check "tenure/race" failed`, false},
		{g2 + "\npanic: boom", false},
		{"row TestMutantX: AssertsG2 false, want true\nFAIL", false},
		{"ok", false},
	} {
		if got := caughtFor(c.out, "tenure/g2") == ""; got != c.want {
			t.Errorf("caughtFor(%q, tenure/g2) accepts: %v, want %v", c.out, got, c.want)
		}
	}
}
