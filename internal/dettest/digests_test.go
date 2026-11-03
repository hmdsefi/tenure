// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/tenure/core"
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
var digestCases = []digestCase{
	{name: "keyshard", lines: func(add func(line string)) { keyshardLines(add, keyshardKeys()) }},
	{name: "score", lines: scoreLines},
	{name: "rank", lines: rankLines},
	{name: "tree", lines: treeLines},
}

// keyshardKeys returns the keys of the keyshard case: six fixed keys, then k0 … k9999.
func keyshardKeys() []string {
	keys := []string{"", "a", "é", "\x00", "orderbook/42", strings.Repeat("x", 1000)}
	for i := range 10000 {
		keys = append(keys, "k"+strconv.Itoa(i))
	}
	return keys
}

// keyshardLines adds the keyshard case's lines for keys: for each n, for each key,
// Quote(key) n core.KeyShard(key, n) (DET-450).
func keyshardLines(add func(line string), keys []string) {
	for _, n := range []int{1, 2, 7, 64, 1000, 65536} {
		for _, key := range keys {
			add(strconv.Quote(key) + " " + strconv.Itoa(n) + " " + strconv.Itoa(core.KeyShard(key, n)))
		}
	}
}

// scoreLines adds the score case's lines (DET-450): for each namespace, key and member ID,
// the three quoted strings and core.Score as 0x%016x.
func scoreLines(add func(line string)) {
	keys := []string{"", "alice", "bob"}
	for i := range 100 {
		keys = append(keys, "k"+strconv.Itoa(i))
	}
	ids := []string{"", "n1", "n2", "n3"}
	for i := range 16 {
		ids = append(ids, "m"+strconv.Itoa(i))
	}
	for _, ns := range []string{"", "account", "orderbook"} {
		for _, key := range keys {
			for _, id := range ids {
				add(strconv.Quote(ns) + " " + strconv.Quote(key) + " " + strconv.Quote(id) + " " +
					fmt.Sprintf("0x%016x", core.Score(ns, key, id)))
			}
		}
	}
}

// rankLines adds the rank case's lines (DET-450): over members m7 … m0 (descending IDs), for
// each n and key, n, key and the IDs of core.Rank("account", key, members, n) joined by ",".
func rankLines(add func(line string)) {
	members := make([]core.Member, 8)
	for i := range members {
		members[i] = core.Member{ID: "m" + strconv.Itoa(7-i), Slot: -1}
	}
	for _, n := range []int{1, 3, 8} {
		for i := range 200 {
			key := "k" + strconv.Itoa(i)
			ranked := core.Rank("account", key, members, n)
			ids := make([]string, len(ranked))
			for j, m := range ranked {
				ids[j] = m.ID
			}
			add(strconv.Itoa(n) + " " + key + " " + strings.Join(ids, ","))
		}
	}
}

// treeLines adds the tree case's lines (PTR-150): for each Degree, each partition's key,
// parent and children, then picks of 1000 multiples of the golden-ratio constant.
func treeLines(add func(line string)) {
	for _, d := range []int{1, 2, 3, 20} {
		t := core.Tree{Name: "tq", Read: 100, Write: 50, Degree: d}
		for i := range 100 {
			parent := "-"
			if i > 0 {
				parent = strconv.Itoa(t.Parent(i))
			}
			children := "-"
			if cs := t.Children(i); cs != nil {
				strs := make([]string, len(cs))
				for k, c := range cs {
					strs[k] = strconv.Itoa(c)
				}
				children = strings.Join(strs, ",")
			}
			add(strconv.Itoa(d) + " " + strconv.Itoa(i) + " " + t.Key(i) + " " + parent + " " + children)
		}
	}
	u := core.Tree{Name: "tq", Read: 7, Write: 3, Degree: 2}
	for j := range 1000 {
		r := uint64(j) * 0x9e3779b97f4a7c15 // wraps
		add("pick " + strconv.FormatUint(r, 10) + " " + strconv.Itoa(u.Pick(r)) + " " + strconv.Itoa(u.PickRead(r)))
	}
}

// caseLines collects the lines of the digest case called name.
func caseLines(t *testing.T, name string) []string {
	t.Helper()
	for _, c := range digestCases {
		if c.name == name {
			var lines []string
			c.lines(func(line string) { lines = append(lines, line) })
			return lines
		}
	}
	t.Fatalf("no digest case %q", name)
	return nil
}

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
	failure, err := checkCoreDigests(entries, filepath.Join("testdata", "core-digests.txt"), *update)
	if err != nil {
		t.Fatal(err)
	}
	if failure != "" {
		t.Error(failure)
	}
}

// checkCoreDigests runs DET-440 items 2 and 3 for entries against the golden file at path,
// whose base name ParseGolden reports. With update it rewrites the file. It returns the
// failure message (the CompareGolden lines followed by the update hint), or "" when the test
// passes; err is a read, parse or write error, for t.Fatal.
func checkCoreDigests(entries []Entry, path string, update bool) (failure string, err error) {
	if update {
		return "", os.WriteFile(path, FormatGolden(entries), 0o644) //nolint:gosec // the golden file is committed and readable by everyone
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	want, err := ParseGolden(filepath.Base(path), data)
	if err != nil {
		return "", err
	}
	if lines := CompareGolden(entries, want); lines != nil {
		return strings.Join(lines, "\n") + "\n" + coreDigestsHint, nil
	}
	return "", nil
}

// TestCheckCoreDigests checks TestCoreDigests' compare and update paths (DET-440, AT-DET-12).
func TestCheckCoreDigests(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "core-digests.txt")
	entries := []Entry{
		{Name: "keyshard", Hash: 0x1, Count: 60042},
		{Name: "score", Hash: 0x2, Count: 6180},
	}

	if failure, err := checkCoreDigests(entries, path, false); err == nil {
		t.Errorf("checkCoreDigests without a golden file = %q, nil; want a read error", failure)
	}

	if failure, err := checkCoreDigests(entries, path, true); err != nil || failure != "" {
		t.Fatalf("checkCoreDigests(update) = %q, %v; want a pass", failure, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := FormatGolden(entries); string(data) != string(want) {
		t.Errorf("update wrote\n%s\nwant\n%s", data, want)
	}
	if failure, err := checkCoreDigests(entries, path, false); err != nil || failure != "" {
		t.Errorf("checkCoreDigests after update = %q, %v; want a pass", failure, err)
	}

	golden := goldenHeader +
		"keyshard 0x0000000000000000 0x0000000000000001 60036\n" +
		"machine 0x0000000000000000 0x0000000000000003 7\n" +
		"score 0x0000000000000000 0x0000000000000002 6180\n"
	if err := os.WriteFile(path, []byte(golden), 0o600); err != nil {
		t.Fatal(err)
	}
	failure, err := checkCoreDigests(entries, path, false)
	if err != nil {
		t.Fatalf("checkCoreDigests: %v", err)
	}
	want := "golden: keyshard seed 0x0000000000000000: got hash 0x0000000000000001 count 60042, want hash 0x0000000000000001 count 60036\n" +
		"golden: machine seed 0x0000000000000000: in the golden file but not produced\n" +
		`core digests changed: if intended, run "GOWORK=off go test ./internal/dettest -run TestCoreDigests -update" and add a "Golden-Update: <reason>" line to the commit message`
	if failure != want {
		t.Errorf("checkCoreDigests failure =\n%s\nwant\n%s", failure, want)
	}
	if failure, _ := checkCoreDigests(entries[:1], path, false); !strings.Contains(failure, "golden: score seed 0x0000000000000000: in the golden file but not produced") {
		t.Errorf("checkCoreDigests with a case missing from the list = %q, want a not-produced line", failure)
	}
	if failure, _ := checkCoreDigests(append(slices.Clone(entries), Entry{Name: "tree", Count: 1}), path, false); !strings.Contains(failure, "golden: tree seed 0x0000000000000000: missing from the golden file") {
		t.Errorf("checkCoreDigests with a new case = %q, want a missing line", failure)
	}

	if err := os.WriteFile(path, []byte("# tenure golden v1\nbad\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := checkCoreDigests(entries, path, false); err == nil || err.Error() != "core-digests.txt:2: malformed entry" {
		t.Errorf("checkCoreDigests of a malformed file: error %v, want %q", err, "core-digests.txt:2: malformed entry")
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

// TestDigestCaseKeyshard checks the keyshard case's count and first line (AT-DET-13).
func TestDigestCaseKeyshard(t *testing.T) {
	lines := caseLines(t, "keyshard")
	if len(lines) != 60036 {
		t.Fatalf("keyshard has %d lines, want 60036", len(lines))
	}
	if lines[0] != `"" 1 0` {
		t.Errorf("first keyshard line = %q, want %q", lines[0], `"" 1 0`)
	}
}

// TestDigestCaseScore checks the score case's count and lockstep's vector (AT-DET-13).
func TestDigestCaseScore(t *testing.T) {
	lines := caseLines(t, "score")
	if len(lines) != 6180 {
		t.Errorf("score has %d lines, want 6180", len(lines))
	}
	const want = `"account" "alice" "n1" 0x15f291e4c62474b8`
	if !slices.Contains(lines, want) {
		t.Errorf("score has no line %q", want)
	}
}

// TestDigestCaseRank checks the rank case's count and that every line with n = 8 lists all
// eight IDs (AT-DET-13).
func TestDigestCaseRank(t *testing.T) {
	lines := caseLines(t, "rank")
	if len(lines) != 600 {
		t.Errorf("rank has %d lines, want 600", len(lines))
	}
	all := []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7"}
	eights := 0
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "8" {
			continue
		}
		eights++
		got := strings.Split(fields[2], ",")
		slices.Sort(got)
		if !slices.Equal(got, all) {
			t.Errorf("rank line %q does not list all eight IDs", line)
		}
	}
	if eights != 200 {
		t.Errorf("rank has %d lines with n = 8, want 200", eights)
	}
}

// TestATDET12KeyshardDrift checks that one more keyshard key changes exactly the keyshard
// entry of the committed file (AT-DET-12).
func TestATDET12KeyshardDrift(t *testing.T) {
	keys := append(keyshardKeys(), "k10000")
	var d Digest
	keyshardLines(d.Add, keys)
	got := []Entry{d.Entry("keyshard")}

	data, err := os.ReadFile(filepath.Join("testdata", "core-digests.txt"))
	if err != nil {
		t.Fatal(err)
	}
	all, err := ParseGolden("core-digests.txt", data)
	if err != nil {
		t.Fatal(err)
	}
	var want []Entry
	for _, e := range all {
		if e.Name == "keyshard" {
			want = append(want, e)
		}
	}

	lines := CompareGolden(got, want)
	if len(lines) != 1 {
		t.Fatalf("CompareGolden = %q, want one line", lines)
	}
	re := regexp.MustCompile(`^golden: keyshard seed 0x0000000000000000: got hash 0x[0-9a-f]{16} count 60042, want hash 0x[0-9a-f]{16} count 60036$`)
	if !re.MatchString(lines[0]) {
		t.Errorf("CompareGolden line = %q, want a match of %s", lines[0], re)
	}
}

// TestATPTR12TreeDigest checks the tree case's count and lines (AT-PTR-12).
func TestATPTR12TreeDigest(t *testing.T) {
	lines := caseLines(t, "tree")
	if len(lines) != 1400 {
		t.Fatalf("tree has %d lines, want 1400", len(lines))
	}
	for _, c := range []struct {
		n    int // 1-based line number
		want string
	}{
		{1, "1 0 tq - 1"},
		{100, "1 99 tq/99 98 -"},
		{401, "pick 0 0 0"},
		{402, "pick 11400714819323198485 1 4"},
	} {
		if lines[c.n-1] != c.want {
			t.Errorf("tree line %d = %q, want %q", c.n, lines[c.n-1], c.want)
		}
	}
	const want = "3 1 tq/1 0 4,5,6"
	if !slices.Contains(lines, want) {
		t.Errorf("tree has no line %q", want)
	}
}
