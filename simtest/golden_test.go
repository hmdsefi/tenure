// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/hmdsefi/faultline"

	"github.com/hmdsefi/tenure/internal/dettest"
	"github.com/hmdsefi/tenure/simtest/internal/world"
)

var update = flag.Bool("update", false, "rewrite testdata/hashes.txt")

// goldenFile pins the trace hashes of the golden scenarios (DET-520).
const goldenFile = "testdata/hashes.txt"

// goldenGo is the Go minor version the golden hashes are pinned under: the sort algorithms and
// the formatting of the standard library may change between minor versions, and so may a hash.
const goldenGo = "go1.26"

// goldenSeeds are the seeds every golden scenario runs.
var goldenSeeds = []uint64{0x1, 0x2, 0x3}

// goldenHint follows the differences when the hashes changed (DET-520).
const goldenHint = `simtest golden hashes changed: if intended, run "GOWORK=off go -C simtest test -run TestGoldenHashes -update ." and add a "Golden-Update: <reason>" line to the commit message`

// The skip messages of TestGoldenHashes (SIM-615).
const (
	goldenScheduleSkip = "simtest: golden hashes skipped while FAULTLINE_SCHEDULE replays a schedule"
	goldenVersionSkip  = "simtest: golden hashes are pinned under " + goldenGo + "; this is "
)

// goldenScenario is one golden scenario: a row of the scenario table under one rule.
type goldenScenario struct {
	name string // the name in the golden file
	row  string // the row's test name
	rule world.Takeover
}

// goldenScenarios are the scenarios TestGoldenHashes pins, each the matching row of the table,
// run as the row runs: for the row's duration, on its network. A shorter run would pin less: at
// 30 s, the fault rows leave their planners 7 s or 8 s before the recovery time, less than one
// interval of their rules, and no fault happens.
var goldenScenarios = []goldenScenario{
	{"steady/server", "TestSteady", world.Server},
	{"steady/observer", "TestSteady", world.Observer},
	{"crash/server", "TestCrashRestart", world.Server},
	{"crash/observer", "TestCrashRestart", world.Observer},
	{"partitions/server", "TestPartitions", world.Server},
	{"wallclock/observer", "TestWallClock", world.Observer},
	{"rollout/server", "TestSlotRollout", world.Server},
	{"outage/observer", "TestFencedStoreOutage", world.Observer},
}

// goldenSkip returns why TestGoldenHashes does not run with the Go version version and the
// value schedule of FAULTLINE_SCHEDULE, or "" when it runs.
func goldenSkip(version, schedule string) string {
	switch {
	case schedule != "":
		return goldenScheduleSkip
	case version != goldenGo && !strings.HasPrefix(version, goldenGo+"."):
		return goldenVersionSkip + version
	}
	return ""
}

// TestGoldenHashes pins the trace hash and the event count of each golden scenario at the
// seeds 0x1, 0x2 and 0x3 in testdata/hashes.txt (SIM-615, DET-520). A change in tenure's core
// or in the harness that changes a run changes its hash, and the test fails with the
// differences and the command that rewrites the file. The hashes are pinned under Go 1.26, and
// the test skips under another minor version and while FAULTLINE_SCHEDULE replays a schedule.
// The variables that choose seeds change nothing: faultline.RunSeeds ignores them.
func TestGoldenHashes(t *testing.T) {
	if why := goldenSkip(runtime.Version(), os.Getenv("FAULTLINE_SCHEDULE")); why != "" {
		t.Skip(why)
	}
	var got []dettest.Entry
	for _, g := range goldenScenarios {
		t.Run(g.name, func(t *testing.T) {
			got = append(got, goldenEntries(t, g)...)
		})
	}
	if t.Failed() {
		return
	}
	if *update {
		if err := os.WriteFile(goldenFile, dettest.FormatGolden(got), 0o644); err != nil { //nolint:gosec // a file of the repository
			t.Fatal(err)
		}
		t.Logf("wrote %s: %d entries", goldenFile, len(got))
		return
	}
	data, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatalf("%v\n%s", err, goldenHint)
	}
	want, err := dettest.ParseGolden(goldenFile, data)
	if err != nil {
		t.Fatalf("%v\n%s", err, goldenHint)
	}
	if lines := dettest.CompareGolden(got, want); lines != nil {
		t.Errorf("%s\n%s", strings.Join(lines, "\n"), goldenHint)
	}
}

// goldenEntries runs the golden seeds of g, with faultline's results written to a file of its
// own, and returns an entry per seed: the name of g, the seed, the trace hash and the event
// count of the seed's first run.
func goldenEntries(t *testing.T, g goldenScenario) []dettest.Entry {
	t.Helper()
	r := rowNamed(t, g.row)
	opts := r.options()
	results := filepath.Join(t.TempDir(), "results.jsonl")
	t.Setenv("FAULTLINE_RESULTS", results)
	faultline.RunSeeds(t, opts, r.body(g.rule, nil), goldenSeeds...)
	if t.Failed() {
		return nil
	}
	data, err := os.ReadFile(results) //nolint:gosec // the test's own temporary file
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := dettest.ParseResults(data)
	if err != nil {
		t.Fatal(err)
	}
	var entries []dettest.Entry
	for _, res := range parsed {
		if res.Status != "pass" {
			t.Fatalf("seed %#016x: status %q", res.Seed, res.Status)
		}
		entries = append(entries, dettest.Entry{Name: g.name, Seed: res.Seed, Hash: res.Hash, Count: res.Events})
	}
	if len(entries) != len(goldenSeeds) {
		t.Fatalf("%d results, want one per seed (%d)", len(entries), len(goldenSeeds))
	}
	return entries
}

// TestGoldenSkip checks when TestGoldenHashes skips, and with which message.
func TestGoldenSkip(t *testing.T) {
	tests := []struct {
		version, schedule, want string
	}{
		{"go1.26", "", ""},
		{"go1.26.0", "", ""},
		{"go1.26.10", "", ""},
		{"go1.27.3", "", "simtest: golden hashes are pinned under go1.26; this is go1.27.3"},
		{"go1.260", "", "simtest: golden hashes are pinned under go1.26; this is go1.260"},
		{"go1.25.7", "", "simtest: golden hashes are pinned under go1.26; this is go1.25.7"},
		{"devel go1.27-abc", "", "simtest: golden hashes are pinned under go1.26; this is devel go1.27-abc"},
		{"go1.26.0", "/tmp/schedule.json", "simtest: golden hashes skipped while FAULTLINE_SCHEDULE replays a schedule"},
		{"go1.27.3", "/tmp/schedule.json", "simtest: golden hashes skipped while FAULTLINE_SCHEDULE replays a schedule"},
	}
	for _, tc := range tests {
		if got := goldenSkip(tc.version, tc.schedule); got != tc.want {
			t.Errorf("goldenSkip(%q, %q) = %q, want %q", tc.version, tc.schedule, got, tc.want)
		}
	}
}

// TestGoldenScenarios checks the golden scenarios: eight names, each unique, each of a row of
// the table under one of the row's rules, which the name ends with.
func TestGoldenScenarios(t *testing.T) {
	if len(goldenScenarios) != 8 {
		t.Errorf("%d golden scenarios, want 8", len(goldenScenarios))
	}
	seen := map[string]bool{}
	for _, g := range goldenScenarios {
		if seen[g.name] {
			t.Errorf("golden name %q is not unique", g.name)
		}
		seen[g.name] = true
		r := rowNamed(t, g.row)
		if !slices.Contains(r.takeovers(), g.rule) {
			t.Errorf("%s: row %s does not run under %v", g.name, g.row, g.rule)
		}
		if !strings.HasSuffix(g.name, "/"+g.rule.String()) {
			t.Errorf("%s: rule %v is not in the name", g.name, g.rule)
		}
	}
}
