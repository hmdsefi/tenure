// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"flag"
	"fmt"
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
	name   string // the name in the golden file
	row    string // the row's test name
	rule   world.Takeover
	change func(*row) // a change to the row, or nil
}

// goldenScenarios are the scenarios TestGoldenHashes pins, each the matching row of the table,
// run as the row runs: for the row's duration, on its network. A shorter run would pin less: at
// 30 s, the fault rows leave their planners 7 s or 8 s before the recovery time, less than one
// interval of their rules, and no fault happens.
var goldenScenarios = []goldenScenario{
	{"steady/server", "TestSteady", world.Server, nil},
	{"steady/observer", "TestSteady", world.Observer, nil},
	{"crash/server", "TestCrashRestart", world.Server, nil},
	{"crash/observer", "TestCrashRestart", world.Observer, nil},
	{"partitions/server", "TestPartitions", world.Server, nil},
	{"wallclock/observer", "TestWallClock", world.Observer, nil},
	{"rollout/server", "TestSlotRollout", world.Server, nil},
	{"outage/observer", "TestFencedStoreOutage", world.Observer, nil},
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
	var ran []string
	for _, g := range goldenScenarios {
		t.Run(g.name, func(t *testing.T) {
			ran = append(ran, g.name)
			got = append(got, goldenEntries(t, g)...)
		})
	}
	if t.Failed() {
		return
	}
	if failure := checkGolden(goldenFile, got, ran, *update); failure != "" {
		t.Fatal(failure)
	}
	if *update {
		t.Logf("wrote %s: %d entries", goldenFile, len(got))
	}
}

// checkGolden compares got, the entries of the golden scenarios named ran, with the golden file
// at path, or with update rewrites the file with got. It returns the failure message, or "" when
// the test passes. A run that a -run pattern limited to some of the scenarios is compared with
// their entries alone, and may not rewrite the file.
func checkGolden(path string, got []dettest.Entry, ran []string, update bool) string {
	partial := len(ran) < len(goldenScenarios)
	if update {
		if partial {
			return fmt.Sprintf("-update rewrites all of %s, but %d of the %d golden scenarios ran: run TestGoldenHashes without a subtest pattern",
				path, len(ran), len(goldenScenarios))
		}
		if err := os.WriteFile(path, dettest.FormatGolden(got), 0o644); err != nil { //nolint:gosec // a file of the repository
			return err.Error()
		}
		return ""
	}
	data, err := os.ReadFile(path) //nolint:gosec // a file of the repository
	if err != nil {
		return err.Error() + "\n" + goldenHint
	}
	want, err := dettest.ParseGolden(path, data)
	if err != nil {
		return err.Error() + "\n" + goldenHint
	}
	if partial {
		want = slices.DeleteFunc(want, func(e dettest.Entry) bool { return !slices.Contains(ran, e.Name) })
	}
	if lines := dettest.CompareGolden(got, want); lines != nil {
		return strings.Join(lines, "\n") + "\n" + goldenHint
	}
	return ""
}

// goldenEntries runs the golden seeds of g, with faultline's results written to a file of its
// own, and returns an entry per seed: the name of g, the seed, the trace hash and the event
// count of the seed's first run.
func goldenEntries(t *testing.T, g goldenScenario) []dettest.Entry {
	t.Helper()
	r := rowNamed(t, g.row)
	if g.change != nil {
		g.change(&r)
	}
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

// TestCheckGolden checks TestGoldenHashes' compare and update paths on a file of its own: a
// full run rewrites the file and then matches it; a changed hash, a missing entry and an extra
// one are reported with the hint; a run of some of the scenarios is compared with their entries
// alone and may not rewrite the file; a missing file is reported with the hint.
func TestCheckGolden(t *testing.T) {
	var all []string
	var entries []dettest.Entry
	for i, g := range goldenScenarios {
		all = append(all, g.name)
		for _, seed := range goldenSeeds {
			entries = append(entries, dettest.Entry{Name: g.name, Seed: seed, Hash: uint64(i)<<8 | seed, Count: 100 + seed})
		}
	}
	path := filepath.Join(t.TempDir(), "hashes.txt")
	if failure := checkGolden(path, entries, all, true); failure != "" {
		t.Fatalf("update: %s", failure)
	}
	data, err := os.ReadFile(path) //nolint:gosec // the test's own temporary file
	if err != nil {
		t.Fatal(err)
	}
	if want := dettest.FormatGolden(entries); string(data) != string(want) {
		t.Fatalf("update wrote\n%s\nwant\n%s", data, want)
	}
	if failure := checkGolden(path, entries, all, false); failure != "" {
		t.Errorf("after update: %s", failure)
	}

	changed := slices.Clone(entries)
	changed[0].Hash = 0xabc
	if got, want := checkGolden(path, changed, all, false),
		"golden: steady/server seed 0x0000000000000001: got hash 0x0000000000000abc count 101, want hash 0x0000000000000001 count 101\n"+goldenHint; got != want {
		t.Errorf("changed hash:\n got %q\nwant %q", got, want)
	}
	if got, want := checkGolden(path, entries[1:], all, false),
		"golden: steady/server seed 0x0000000000000001: in the golden file but not produced\n"+goldenHint; got != want {
		t.Errorf("missing entry:\n got %q\nwant %q", got, want)
	}
	extra := append(slices.Clone(entries), dettest.Entry{Name: "steady/server", Seed: 4, Hash: 1, Count: 1})
	if got, want := checkGolden(path, extra, all, false),
		"golden: steady/server seed 0x0000000000000004: missing from the golden file\n"+goldenHint; got != want {
		t.Errorf("extra entry:\n got %q\nwant %q", got, want)
	}

	// A -run pattern that chose the first scenario alone.
	first := entries[:len(goldenSeeds)]
	if failure := checkGolden(path, first, all[:1], false); failure != "" {
		t.Errorf("some scenarios: %s", failure)
	}
	if got, want := checkGolden(path, changed[:len(goldenSeeds)], all[:1], false),
		"golden: steady/server seed 0x0000000000000001: got hash 0x0000000000000abc count 101, want hash 0x0000000000000001 count 101\n"+goldenHint; got != want {
		t.Errorf("some scenarios, changed hash:\n got %q\nwant %q", got, want)
	}
	if got, want := checkGolden(path, first, all[:1], true),
		"-update rewrites all of "+path+", but 1 of the 8 golden scenarios ran: run TestGoldenHashes without a subtest pattern"; got != want {
		t.Errorf("update of some scenarios:\n got %q\nwant %q", got, want)
	}
	if after, err := os.ReadFile(path); err != nil || string(after) != string(data) { //nolint:gosec // the test's own temporary file
		t.Errorf("update of some scenarios changed the file to\n%s(%v)", after, err)
	}

	missing := filepath.Join(t.TempDir(), "none.txt")
	if got := checkGolden(missing, entries, all, false); !strings.HasSuffix(got, "\n"+goldenHint) || !strings.Contains(got, "none.txt") {
		t.Errorf("missing file: %q", got)
	}
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
