// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite golden files")

// digestCase is one case of TestCoreDigests (DET-440): lines calls add once per text line, in a
// fixed order.
type digestCase struct {
	name  string
	lines func(add func(line string))
}

// digestCases lists the digest cases of DET-450 and DET-460. Each is added by the change that
// adds the core function it calls.
var digestCases []digestCase

// digestEntries computes one golden entry per case, in list order, each from a fresh Digest.
func digestEntries(cases []digestCase) ([]Entry, error) {
	entries := make([]Entry, 0, len(cases))
	seen := make(map[string]bool, len(cases))
	for _, c := range cases {
		if seen[c.name] {
			return nil, fmt.Errorf("duplicate digest case %s", c.name)
		}
		seen[c.name] = true
		var d Digest
		c.lines(d.Add)
		entries = append(entries, d.Entry(c.name))
	}
	return entries, nil
}

// coreDigestsHint follows the differences TestCoreDigests reports (DET-440).
const coreDigestsHint = `core digests changed: if intended, run "GOWORK=off go test ./internal/dettest -run TestCoreDigests -update" and add a "Golden-Update: <reason>" line to the commit message`

// TestCoreDigests pins the digests of core's pure functions in testdata/core-digests.txt
// (DET-440).
func TestCoreDigests(t *testing.T) {
	entries, err := digestEntries(digestCases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "core-digests.txt")
	if *update {
		if err := os.WriteFile(path, FormatGolden(entries), 0o644); err != nil { //nolint:gosec // the golden file is committed and readable by everyone
			t.Fatal(err)
		}
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := ParseGolden("core-digests.txt", data)
	if err != nil {
		t.Fatal(err)
	}
	if lines := CompareGolden(entries, want); lines != nil {
		t.Errorf("%s\n%s", strings.Join(lines, "\n"), coreDigestsHint)
	}
}

// TestATDET12DigestWorkflow checks the case list rules of DET-440 (AT-DET-12, AT-DET-13).
func TestATDET12DigestWorkflow(t *testing.T) {
	if entries, err := digestEntries(nil); err != nil || len(entries) != 0 {
		t.Errorf("digestEntries(nil) = %v, %v; want no entries and no error", entries, err)
	}

	lines := func(ls ...string) func(add func(string)) {
		return func(add func(string)) {
			for _, l := range ls {
				add(l)
			}
		}
	}
	entries, err := digestEntries([]digestCase{
		{name: "second", lines: lines("a")},
		{name: "first", lines: lines("a", "b")},
		{name: "empty", lines: lines()},
	})
	if err != nil {
		t.Fatalf("digestEntries: %v", err)
	}
	want := []Entry{
		{Name: "second", Hash: 0x089bdc07b544e7b2, Count: 1},
		{Name: "first", Hash: 0x78ed6781f136a14e, Count: 2},
		{Name: "empty", Hash: 0xcbf29ce484222325, Count: 0},
	}
	if !slices.Equal(entries, want) {
		t.Errorf("digestEntries = %+v, want %+v", entries, want)
	}

	_, err = digestEntries([]digestCase{
		{name: "score", lines: lines("a")},
		{name: "keyshard", lines: lines("b")},
		{name: "score", lines: lines("c")},
	})
	if want := "duplicate digest case score"; err == nil || err.Error() != want {
		t.Errorf("digestEntries with two cases named score: error %v, want %q", err, want)
	}
}
