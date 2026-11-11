// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simnet"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// row is one row of the scenario table (SIM-600): a scenario, how many seeds of it run and for
// how long, on what network, and whether its runs assert that no two members own a shard at
// once.
type row struct {
	// name is the name of the test that runs the row, such as "TestSteady".
	name string
	// sc is the scenario. Its Takeover is set per rule by the runner.
	sc world.Scenario
	// seeds is the full seed count of each rule; under -short faultline runs at most 5.
	seeds int
	// duration is the virtual length of each run.
	duration time.Duration
	// net is the network; the zero value is faultline's default, 1 ms of latency and up to 4
	// ms of jitter.
	net simnet.Config
	// g2 is the row's G2 column: whether world.Build must find that the runs assert G2.
	g2 bool
	// rules are the takeover rules the row runs under; nil means both, Server then Observer.
	rules []world.Takeover
}

// defaultNet is the network of every row that names no other: faultline's default, written out
// so that the table says what each row runs on.
var defaultNet = simnet.Config{Default: simnet.Link{Latency: time.Millisecond, Jitter: 4 * time.Millisecond}}

// rows is the scenario table, by test name. A test finds its row with rowNamed, so each row is
// also reachable by name from the tests that pin its traces or replay its seeds.
var rows = []row{
	{
		name: "TestSteady", sc: world.Scenario{StopDelay: time.Second},
		seeds: 20, duration: 60 * time.Second, net: defaultNet, g2: true,
	},
	// The store ends each first session once, at about 40 s, and the members recover. The run
	// lasts 75 s, not 90 s: the recovered sessions, opened at about 40 s, would end at about
	// 80 s, and that second round of ends leaves no bound to recover in before 90 s.
	{
		name: "TestSessionEnds", sc: world.Scenario{EndAfter: 40 * time.Second},
		seeds: 20, duration: 75 * time.Second, net: defaultNet, g2: true,
	},
}

// rowNamed returns the row of the table whose name is name, and fails t when there is none.
func rowNamed(t testing.TB, name string) row {
	t.Helper()
	for _, r := range rows {
		if r.name == name {
			return r
		}
	}
	t.Fatalf("no row named %q", name)
	return row{}
}

// takeovers returns the rules the row runs under, in order.
func (r row) takeovers() []world.Takeover {
	if r.rules == nil {
		return []world.Takeover{world.Server, world.Observer}
	}
	return r.rules
}

// options returns the faultline options of a run of the row: its seeds, its duration, its
// network, and determinism checking, which runs every passing seed twice and compares the trace
// hashes.
func (r row) options() faultline.Options {
	return faultline.Options{Seeds: r.seeds, Duration: r.duration, Net: r.net, CheckDeterminism: true}
}

// body returns the function faultline runs for each run of the row under rule: it builds the
// world of the row's scenario under rule, which registers the invariants and the finals, fails
// the run when the world's AssertsG2 is not the row's G2 column, then calls more, when it is
// not nil, with the world. The run goes on to the end after body returns.
func (r row) body(rule world.Takeover, more func(fw *faultline.World, w *world.World)) func(*faultline.World) {
	sc := r.sc
	sc.Takeover = rule
	return func(fw *faultline.World) {
		w := world.Build(fw, sc)
		if w.AssertsG2() != r.g2 {
			fw.T().Fatalf("row %s: AssertsG2 %v, want %v", r.name, w.AssertsG2(), r.g2)
		}
		if more != nil {
			more(fw, w)
		}
	}
}

// runRow runs the row: a subtest per rule, named "server" or "observer", each a faultline.Run
// of the row's options and body. more is passed to body.
func runRow(t *testing.T, r row, more func(fw *faultline.World, w *world.World)) {
	t.Helper()
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			faultline.Run(t, r.options(), r.body(rule, more))
		})
	}
}

// TestRowsNamed checks the scenario table: every row has a name of its own, seeds, a duration,
// and a scenario that world.SetupError accepts for that duration under each of its rules.
func TestRowsNamed(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range rows {
		if r.name == "" || seen[r.name] {
			t.Errorf("row %q: the name is empty or not unique", r.name)
		}
		seen[r.name] = true
		if r.seeds < 1 || r.duration <= 0 {
			t.Errorf("row %s: %d seeds of %v", r.name, r.seeds, r.duration)
		}
		for _, rule := range r.takeovers() {
			sc := r.sc
			sc.Takeover = rule
			if err := world.SetupError(sc, kernel.Time(r.duration)); err != nil {
				t.Errorf("row %s, %v: %v", r.name, rule, err)
			}
			if got := sc.AssertsG2(); got != r.g2 {
				t.Errorf("row %s, %v: AssertsG2 %v, want %v", r.name, rule, got, r.g2)
			}
		}
	}
	if got := rowNamed(t, "TestSteady").seeds; got != 20 {
		t.Errorf("TestSteady has %d seeds, want 20", got)
	}
}
