// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// fixtureCore is the corePath TestCheckFixtures gives CheckDir (DET-320).
const fixtureCore = tenureModule + "/core"

// findingLines returns f's String values, one per line, for failure messages.
func findingLines(fs []Finding) string {
	if len(fs) == 0 {
		return "(none)"
	}
	lines := make([]string, len(fs))
	for i, f := range fs {
		lines[i] = f.String()
	}
	return strings.Join(lines, "\n")
}

// keepRule returns the findings of fs whose rule is r (keep) or is not r (!keep), in order.
func keepRule(fs []Finding, r Rule, keep bool) []Finding {
	var out []Finding
	for _, f := range fs {
		if (f.Rule == r) == keep {
			out = append(out, f)
		}
	}
	return out
}

// TestATDET04FindingString checks Finding.String's format (AT-DET-04).
func TestATDET04FindingString(t *testing.T) {
	f := Finding{File: "core/machine.go", Line: 12, Col: 3, Rule: RuleConcurrency, Msg: "go statement: core starts no goroutines"}
	const want = "core/machine.go:12:3: DT002: go statement: core starts no goroutines"
	if got := f.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// TestCoreImports checks the imports of every core package against the allow-list (DET-300).
func TestCoreImports(t *testing.T) {
	fs, err := Check(repoRoot(t), tenureModule)
	if err != nil {
		t.Fatal(err)
	}
	if fs = keepRule(fs, RuleImport, true); len(fs) > 0 {
		t.Errorf("core import allow-list: %d finding(s):\n%s", len(fs), findingLines(fs))
	}
}

// TestCoreDeterminism checks every core package against the rules other than the import
// allow-list (DET-310).
func TestCoreDeterminism(t *testing.T) {
	fs, err := Check(repoRoot(t), tenureModule)
	if err != nil {
		t.Fatal(err)
	}
	if fs = keepRule(fs, RuleImport, false); len(fs) > 0 {
		t.Errorf("core determinism rules: %d finding(s):\n%s", len(fs), findingLines(fs))
	}
}

// wantComment matches a fixture expectation "// want DT001 [DT005 …]" (DET-320), or the same
// text as a block comment "/* want DT000 */", which a line holding a directive needs: a directive
// is a line comment that runs to the end of its line.
var wantComment = regexp.MustCompile(`^(?:// want((?:[ \t]+DT[0-9]{3})+)[ \t]*|/\* want((?:[ \t]+DT[0-9]{3})+)[ \t]*\*/)$`)

// fixtureWants returns the sorted "<file>:<line> <rule>" expectations of the .go files in dir.
func fixtureWants(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var wants []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range f.Comments {
			for _, c := range g.List {
				m := wantComment.FindStringSubmatch(c.Text)
				if m == nil {
					continue
				}
				line := fset.Position(c.Pos()).Line
				for _, r := range strings.Fields(m[1] + m[2]) {
					wants = append(wants, fmt.Sprintf("%s:%d %s", e.Name(), line, r))
				}
			}
		}
	}
	slices.Sort(wants)
	return slices.Compact(wants)
}

// findingKeys returns the sorted, distinct "<file>:<line> <rule>" keys of fs.
func findingKeys(fs []Finding) []string {
	keys := make([]string, len(fs))
	for i, f := range fs {
		keys[i] = fmt.Sprintf("%s:%d %s", f.File, f.Line, f.Rule)
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// keyDiff returns one line per key of want missing from got and per key of got not in want,
// both sorted and distinct.
func keyDiff(got, want []string) []string {
	var out []string
	for _, k := range want {
		if _, ok := slices.BinarySearch(got, k); !ok {
			out = append(out, "missing: "+k)
		}
	}
	for _, k := range got {
		if _, ok := slices.BinarySearch(want, k); !ok {
			out = append(out, "unexpected: "+k)
		}
	}
	return out
}

// atDET03 is AT-DET-03's list of the findings of fixture dt010, in order.
var atDET03 = []string{
	"b_linux.go:0:0: DT010: platform-specific file b_linux.go: every platform compiles the same files",
	"c.go:4:1: DT010: build constraint in core: every platform compiles the same files",
	"d_amd64.go:0:0: DT010: platform-specific file d_amd64.go: every platform compiles the same files",
	"e.s:0:0: DT010: non-Go source file e.s: core is plain Go",
}

// TestCheckFixtures runs CheckDir on each fixture package of testdata/src and compares the findings
// with its "// want" comments; dt010 compares with AT-DET-03's list (DET-320, AT-DET-01, AT-DET-03).
func TestCheckFixtures(t *testing.T) {
	src := filepath.Join("testdata", "src")
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(src, name)
			fs, err := CheckDir(dir, "fixture/"+name, fixtureCore)
			if err != nil {
				t.Fatal(err)
			}
			if name == "dt010" {
				got := strings.Split(findingLines(fs), "\n")
				if !slices.Equal(got, atDET03) {
					t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(atDET03, "\n  "))
				}
				return
			}
			if diff := keyDiff(findingKeys(fs), fixtureWants(t, dir)); len(diff) > 0 {
				t.Errorf("%s\nall findings:\n%s", strings.Join(diff, "\n"), findingLines(fs))
			}
		})
	}
}

// atDET06Module returns the files of AT-DET-06's temp module example.com/m. The skipped files
// also import "os", so that they would give a DT001 finding if the walk entered them; core/_x.go,
// core/.x.go and core/_e.s are files the go tool ignores (DET-010).
func atDET06Module() map[string]string {
	const clock = "\nimport (\n\t_ \"os\"\n\t\"time\"\n)\n\nfunc f() { _ = time.Now() }\n"
	return map[string]string{
		"go.mod":               "module example.com/m\n\ngo 1.26.0\n",
		"m.go":                 "package m\n\nimport \"time\"\n\nfunc F() { _ = time.Now() }\n",
		"core/a.go":            "package core\n\n// A is clean.\nfunc A() int { return 1 }\n",
		"core/a_test.go":       "package core\n" + clock,
		"core/ringcompat/r.go": "package ringcompat\n\nimport _ \"example.com/m/core\"\n",
		"core/x/x.go":          "package x\n\nimport _ \"example.com/m\"\n",
		"core/testdata/t.go":   "package t\n" + clock,
		"core/_old/o.go":       "package o\n" + clock,
		"core/.h/h.go":         "package h\n" + clock,
		"core/sub/go.mod":      "module example.com/m/core/sub\n\ngo 1.26.0\n",
		"core/sub/s.go":        "package sub\n" + clock,
		"core/_x.go":           "package core\n" + clock,
		"core/.x.go":           "package core\n" + clock,
		"core/_e.s":            "// ignored by the go tool\n",
	}
}

// TestATDET06WalkAndImportPaths checks the walk of Check and the import paths it gives packages
// (AT-DET-06).
func TestATDET06WalkAndImportPaths(t *testing.T) {
	root := t.TempDir()
	writeFiles(t, root, atDET06Module())
	fs, err := Check(root, "example.com/m")
	if err != nil {
		t.Fatal(err)
	}
	want := []Finding{{
		File: "core/x/x.go", Line: 3, Col: 10, Rule: RuleImport,
		Msg: `import "example.com/m" is not allowed in core; core imports only cmp, errors, fmt, hash/fnv, math, math/bits, slices, sort, strconv, strings, time and packages of example.com/m/core`,
	}}
	if !slices.Equal(fs, want) {
		t.Errorf("Check:\n%s\nwant:\n%s", findingLines(fs), findingLines(want))
	}

	empty := t.TempDir()
	writeFiles(t, empty, map[string]string{"go.mod": "module example.com/m\n\ngo 1.26.0\n", "m.go": "package m\n"})
	_, err = Check(empty, "example.com/m")
	if wantErr := "dettest: " + filepath.Join(empty, "core") + ": no such directory"; err == nil || err.Error() != wantErr {
		t.Errorf("Check without core: error %v, want %q", err, wantErr)
	}
}

// TestCheckSymlinkedRoot checks that Check and CheckDir walk a root reached through a symbolic
// link, and that their other errors start with "dettest: ".
func TestCheckSymlinkedRoot(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target")
	writeFiles(t, target, atDET06Module())
	link := filepath.Join(tmp, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("no symbolic links: %v", err)
	}
	fs, err := Check(link, "example.com/m")
	if err != nil {
		t.Fatal(err)
	}
	if got := findingKeys(fs); !slices.Equal(got, []string{"core/x/x.go:3 DT001"}) {
		t.Errorf("Check through a link:\n%s\nwant the DT001 finding of core/x/x.go", findingLines(fs))
	}

	fixture, err := filepath.Abs(filepath.Join("testdata", "src", "dt001"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := CheckDir(fixture, "fixture/dt001", fixtureCore)
	if err != nil {
		t.Fatal(err)
	}
	fixtureLink := filepath.Join(tmp, "dt001")
	if err := os.Symlink(fixture, fixtureLink); err != nil {
		t.Fatal(err)
	}
	got, err := CheckDir(fixtureLink, "fixture/dt001", fixtureCore)
	if err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 || !slices.Equal(got, want) {
		t.Errorf("CheckDir through a link:\n%s\nwant:\n%s", findingLines(got), findingLines(want))
	}

	missing := filepath.Join(tmp, "missing")
	if _, err := Check(missing, "example.com/m"); err == nil || err.Error() != "dettest: "+filepath.Join(missing, "core")+": no such directory" {
		t.Errorf("Check(%s): error %v, want the no-such-directory error of its core", missing, err)
	}
	if _, err := CheckDir(missing, "fixture/missing", fixtureCore); err == nil || !strings.HasPrefix(err.Error(), "dettest: ") {
		t.Errorf("CheckDir(%s): error %v, want one starting \"dettest: \"", missing, err)
	}
}

// TestATDET07TypeErrors checks that a type error is returned without findings (AT-DET-07).
func TestATDET07TypeErrors(t *testing.T) {
	root := t.TempDir()
	files := atDET06Module()
	files["core/a.go"] = "package core\n\n// A refers to an undefined identifier.\nfunc A() int { return undefined }\n"
	writeFiles(t, root, files)
	fs, err := Check(root, "example.com/m")
	const prefix = "dettest: type-checking example.com/m/core: "
	if err == nil || !strings.HasPrefix(err.Error(), prefix) {
		t.Errorf("Check: error %v, want one starting %q", err, prefix)
	}
	if fs != nil {
		t.Errorf("Check returned findings with an error:\n%s", findingLines(fs))
	}
}

// tempModule writes files into a temp module example.com/m and returns its root.
func tempModule(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	files["go.mod"] = "module example.com/m\n\ngo 1.26.0\n"
	writeFiles(t, root, files)
	return root
}

// importMsg is DET-100's message for path in the temp module example.com/m.
func importMsg(path string) string {
	return fmt.Sprintf("import %q is not allowed in core; core imports only %s and packages of example.com/m/core",
		path, strings.Join(CoreImports, ", "))
}

// TestCheckCgoFile checks that a file importing "C" gets DT001 and is not type-checked (DET-020).
func TestCheckCgoFile(t *testing.T) {
	root := tempModule(t, map[string]string{
		"core/a.go": "package core\n\nimport \"C\"\n",
		"core/b.go": "package core\n\n// B is clean.\nfunc B() int { return 1 }\n",
	})
	fs, err := Check(root, "example.com/m")
	if err != nil {
		t.Fatal(err)
	}
	want := []Finding{{File: "core/a.go", Line: 3, Col: 8, Rule: RuleImport, Msg: importMsg("C")}}
	if !slices.Equal(fs, want) {
		t.Errorf("Check:\n%s\nwant:\n%s", findingLines(fs), findingLines(want))
	}
}

// TestCheckUnwalkedImports checks that an import of a package below core whose directory the walk
// does not enter is DT001 (DET-100).
func TestCheckUnwalkedImports(t *testing.T) {
	root := tempModule(t, map[string]string{
		"core/a.go": "package core\n\nimport (\n" +
			"\t_ \"example.com/m/core/_x\"\n" +
			"\t_ \"example.com/m/core/link\"\n" +
			"\t_ \"example.com/m/core/ok\"\n" +
			"\t_ \"example.com/m/core/sub\"\n" +
			"\t_ \"example.com/m/core/testdata/t\"\n" +
			")\n",
		"core/_x/x.go":         "package x\n",
		"core/ok/ok.go":        "package ok\n",
		"core/sub/go.mod":      "module example.com/m/core/sub\n\ngo 1.26.0\n",
		"core/sub/s.go":        "package sub\n",
		"core/testdata/t/t.go": "package t\n",
		"other/l.go":           "package link\n",
	})
	if err := os.Symlink(filepath.Join(root, "other"), filepath.Join(root, "core", "link")); err != nil {
		t.Skipf("no symbolic links: %v", err)
	}
	fs, err := Check(root, "example.com/m")
	if err != nil {
		t.Fatal(err)
	}
	want := []Finding{
		{File: "core/a.go", Line: 4, Col: 4, Rule: RuleImport, Msg: importMsg("example.com/m/core/_x")},
		{File: "core/a.go", Line: 5, Col: 4, Rule: RuleImport, Msg: importMsg("example.com/m/core/link")},
		{File: "core/a.go", Line: 7, Col: 4, Rule: RuleImport, Msg: importMsg("example.com/m/core/sub")},
		{File: "core/a.go", Line: 8, Col: 4, Rule: RuleImport, Msg: importMsg("example.com/m/core/testdata/t")},
	}
	if !slices.Equal(fs, want) {
		t.Errorf("Check:\n%s\nwant:\n%s", findingLines(fs), findingLines(want))
	}
}

// TestCheckModulePackageErrors checks that an error in a module package loaded as an import names
// that package or file in the forms of DET-020.
func TestCheckModulePackageErrors(t *testing.T) {
	tests := []struct {
		name, b, prefix string
	}{
		{name: "type error", b: "package b\n\nvar B int = \"x\"\n", prefix: "dettest: type-checking example.com/m/core/b: "},
		{name: "parse error", b: "package b\n\nfunc {\n", prefix: "dettest: parsing core/b/b.go: "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := tempModule(t, map[string]string{
				"core/a/a.go": "package a\n\nimport _ \"example.com/m/core/b\"\n",
				"core/b/b.go": tc.b,
			})
			fs, err := Check(root, "example.com/m")
			if err == nil || !strings.HasPrefix(err.Error(), tc.prefix) {
				t.Errorf("Check: error %v, want one starting %q", err, tc.prefix)
			}
			if fs != nil {
				t.Errorf("Check returned findings with an error:\n%s", findingLines(fs))
			}
		})
	}
}

// TestCheckPlatformForms checks DT010's // +build form and more non-Go extensions (DET-190).
func TestCheckPlatformForms(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"a.go":    "package p\n",
		"b.go":    "// +build linux\n\npackage p\n",
		"c.S":     "// assembly\n",
		"d.syso":  "",
		"e.F":     "",
		"f.txt":   "",
		"g_386.s": "",
	})
	fs, err := CheckDir(dir, "fixture/p", fixtureCore)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"b.go:1:1: DT010: build constraint in core: every platform compiles the same files",
		"c.S:0:0: DT010: non-Go source file c.S: core is plain Go",
		"d.syso:0:0: DT010: non-Go source file d.syso: core is plain Go",
		"e.F:0:0: DT010: non-Go source file e.F: core is plain Go",
		"g_386.s:0:0: DT010: non-Go source file g_386.s: core is plain Go",
	}
	if got := strings.Split(findingLines(fs), "\n"); !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestCheckPositions checks that findings name the position on disk, not one a //line directive
// gives, and that findings on one line are ordered by column (DET-030).
func TestCheckPositions(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"a.go": "package p\n\n//line other.go:100\nimport _ \"sync\"; import _ \"os\"\n",
	})
	fs, err := CheckDir(dir, "fixture/p", fixtureCore)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range fs {
		got = append(got, fmt.Sprintf("%s:%d:%d %s", f.File, f.Line, f.Col, f.Rule))
	}
	want := []string{"a.go:4:10 DT001", "a.go:4:27 DT001"}
	if !slices.Equal(got, want) || !strings.Contains(fs[0].Msg, `"sync"`) {
		t.Errorf("findings:\n%s\nwant %q, the first for \"sync\"", findingLines(fs), want)
	}
}

// TestSortFindings checks the order of DET-030, including the Col, Rule and Msg tie-breaks.
func TestSortFindings(t *testing.T) {
	want := []Finding{
		{File: "a.go", Line: 1, Col: 1, Rule: RuleImport, Msg: "a"},
		{File: "a.go", Line: 1, Col: 1, Rule: RuleImport, Msg: "b"},
		{File: "a.go", Line: 1, Col: 1, Rule: RulePlatform, Msg: "a"},
		{File: "a.go", Line: 1, Col: 2, Rule: RuleImport, Msg: "a"},
		{File: "a.go", Line: 2, Col: 1, Rule: RuleImport, Msg: "a"},
		{File: "b.go", Line: 0, Col: 0, Rule: RuleImport, Msg: "a"},
	}
	got := slices.Clone(want)
	slices.Reverse(got)
	got[1], got[4] = got[4], got[1]
	sortFindings(got)
	if !slices.Equal(got, want) {
		t.Errorf("sortFindings:\n%s\nwant:\n%s", findingLines(got), findingLines(want))
	}
}

// fixtureFindings returns the findings of CheckDir on the fixture testdata/src/<name>.
func fixtureFindings(t *testing.T, name string) []Finding {
	t.Helper()
	fs, err := CheckDir(filepath.Join("testdata", "src", name), "fixture/"+name, fixtureCore)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

// TestATDET02Messages checks the exact text of the clock findings of fixture dt003 and of the
// directive findings of fixture directives (AT-DET-02).
func TestATDET02Messages(t *testing.T) {
	const clockTail = " is not allowed in core: core reads no clock and uses time only for Time and Duration values and their arithmetic"
	dt003 := strings.Split(findingLines(fixtureFindings(t, "dt003")), "\n")
	for _, want := range []string{
		"a.go:12:11: DT003: time.Now" + clockTail,
		"a.go:17:8: DT003: time.Time.Year" + clockTail,
		"a.go:26:11: DT003: time.Unix" + clockTail, // the function; the method t.Unix() on line 27 is allowed
	} {
		if !slices.Contains(dt003, want) {
			t.Errorf("dt003: no finding %q in:\n%s", want, strings.Join(dt003, "\n"))
		}
	}
	for _, line := range dt003 {
		if strings.HasPrefix(line, "a.go:23:") {
			t.Errorf("dt003: t.Unix() is allowed, got %s", line)
		}
	}

	meta := keepRule(fixtureFindings(t, "directives"), RuleMeta, true)
	want := []string{
		"a.go:16:19: DT000: //tenure:maporder needs a reason",
		"a.go:19:28: DT000: unknown directive //tenure:sortorder",
		"a.go:22:19: DT000: //tenure:maporder suppresses no finding",
		"a.go:25:19: DT000: //tenure:totalorder suppresses no finding",
	}
	if got := strings.Split(findingLines(meta), "\n"); !slices.Equal(got, want) {
		t.Errorf("directives: DT000 findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestRuleTypes checks the type-dependent cases of DT003 and DT005 that the fixtures do not
// show: methods promoted from an embedded time.Time, fields of time's struct types, methods of
// Duration, ranges over named map types and over type parameters whose core type is a map.
func TestRuleTypes(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"a.go": `package p

import "time"

type stamp struct{ time.Time }

type set map[string]bool

func f[M ~map[string]int, S ~[]int](s stamp, e *time.ParseError, d time.Duration, m M, xs S, k set) {
	_ = s.Format("")
	_ = s.UnixMilli()
	_ = e.Layout
	_ = d.Hours()
	for range m {
	}
	for range xs {
	}
	for range k {
	}
}
`})
	fs, err := CheckDir(dir, "example.com/p", fixtureCore)
	if err != nil {
		t.Fatal(err)
	}
	const clockTail = " is not allowed in core: core reads no clock and uses time only for Time and Duration values and their arithmetic"
	const rangeTail = ": iteration order is random; range over sorted keys or annotate //tenure:maporder <reason>"
	want := []string{
		"a.go:9:54: DT003: time.ParseError" + clockTail,
		"a.go:10:8: DT003: time.Time.Format" + clockTail,
		"a.go:12:8: DT003: time.ParseError.Layout" + clockTail,
		"a.go:13:8: DT003: time.Duration.Hours" + clockTail,
		"a.go:14:2: DT005: range over map M" + rangeTail,
		"a.go:18:2: DT005: range over map p.set" + rangeTail,
	}
	if got := strings.Split(findingLines(fs), "\n"); !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestRuleMessages checks the exact text and position of the DT002, DT004 and DT008 findings of
// their fixtures (DET-110, DET-130, DET-170).
func TestRuleMessages(t *testing.T) {
	const ioTail = " is not allowed in core: core performs no I/O; return values and errors instead"
	const sortTail = " is not stable: elements that compare equal may come out in any order, and the order can change " +
		"between Go releases; make the comparison a total order and annotate //tenure:totalorder <reason>, or use a stable sort"
	for _, tc := range []struct {
		name string
		want []string
	}{
		{"dt002", []string{
			"a.go:8:11: DT002: channel type: core uses no channels",
			"a.go:9:2: DT002: go statement: core starts no goroutines",
			"a.go:10:2: DT002: channel send: core uses no channels",
			"a.go:11:6: DT002: channel receive: core uses no channels",
			"a.go:15:2: DT002: select statement: core uses no channels",
		}},
		{"dt004", []string{
			"a.go:11:6: DT004: fmt.Println" + ioTail,
			"a.go:12:6: DT004: fmt.Fprint" + ioTail,
			"a.go:15:2: DT004: println" + ioTail,
			"a.go:16:2: DT004: print" + ioTail,
		}},
		{"dt008", []string{
			"a.go:22:7: DT008: sort.Slice" + sortTail,
			"a.go:23:7: DT008: sort.Sort" + sortTail,
			"a.go:24:9: DT008: slices.SortFunc" + sortTail,
			"a.go:28:7: DT008: sort.Float64s" + sortTail,
			"a.go:29:24: DT008: sort.Float64Slice.Sort" + sortTail,
			"a.go:30:9: DT008: slices.Sort" + sortTail,
			"a.go:31:13: DT008: slices.Sorted" + sortTail,
			"a.go:32:16: DT008: slices.BinarySearch" + sortTail,
		}},
	} {
		if got := strings.Split(findingLines(fixtureFindings(t, tc.name)), "\n"); !slices.Equal(got, tc.want) {
			t.Errorf("%s: findings:\n  %s\nwant:\n  %s", tc.name, strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
		}
	}
}

// indirectSource is a package that reaches forbidden names in the ways the type information
// resolves: method expressions and values, a renamed import, an explicit instantiation in a
// variable, ranges over a call result and an alias of a map type, conversions of a time.Time to
// interfaces at each kind of site, type arguments, directives on a node's end line and after a
// //line comment, and a float type parameter sorted. It also holds the allowed cases: Durations
// and structs with a Time field as interfaces, Times passed and returned as time.Time, fmt
// arguments (DET-150 walks them), any-constrained slices functions and an allowed constraint.
// b.go adds a dot import; c.go uses errors.Is.
const indirectSource = `package p

import (
	"fmt"
	"slices"
	"time"
	tm "time"
)

type yearer interface{ Year() int }

type box struct{ v any }

type am = map[string]int

func mk() map[string]int { return nil }

func takes(any) {}

func va(int, ...any) {}

func ret(t time.Time) any { return t }

func send(ch chan any, t time.Time) { ch <- t }

func eqer[X interface{ Equal(time.Time) bool }](X) {}

func f(t time.Time, d time.Duration, ts []time.Time, xs []any, m map[string]int, a am) {
	_ = time.Time.Year
	_ = t.Location
	_ = tm.Now()
	sf := slices.SortFunc[[]int, int]
	_ = sf
	for range mk() {
	}
	for range a {
	}
	_ = fmt.Stringer(t).String()
	_ = slices.Contains(ts, t)
	var y yearer
	y = t
	_ = y
	takes(t)
	va(1, t)
	va(1, xs...)
	_ = []any{t}
	_ = map[any]any{t: t}
	_ = box{v: t}
	_ = box{t}
	var _ fmt.Stringer = d
	_ = any(d)
	_ = fmt.Sprint(d)
	eqer(t)
	for range m {
	} //tenure:maporder applies to this line
	for range m {
	}
//line other.go:100
	for range m { //tenure:maporder order unused
	}
	_ = tm.Since(t)
	var yv yearer
	for _, yv = range ts {
	}
	_ = yv
	ma := map[any]int{}
	_ = ma[t]
	var x any
	switch x {
	case t:
	}
	_ = eq(t, t)
	st := stamp{}
	_ = fmt.Sprint(any(st))
	_ = fmt.Sprintf("%v", yearer(st))
	slices.SortStableFunc(ts, func(a, b time.Time) int { return a.Compare(b) })
	_ = slices.Clone(ts)
	keep(t)
	_ = retT(t)
	r := rec{At: t}
	r.At = t
	var _ any = r
	var _ any = d
	var _ Set[time.Time]
	var _ Eqs[time.Time]
	_ = [1]any{t}
	var v any
	v, _ = tuple()
	tmap := map[string]time.Time{}
	v, _ = tmap["k"]
	_ = v
}

type stamp struct{ time.Time }

type rec struct{ At time.Time }

type Set[T any] struct{}

type Eqs[T interface{ Equal(time.Time) bool }] struct{}

func keep(time.Time) {}

func retT(t time.Time) time.Time { return t }

func tuple() (time.Time, error) { return time.Time{}, nil }

func eq[T interface {
	comparable
	Equal(time.Time) bool
}](a, b T) bool {
	return a == b
}

func fsort[F ~float64](xs []F) { slices.Sort(xs) }
`

// TestRuleBypasses checks that renamed and dot imports, method values and expressions,
// instantiations, aliases, conversions to interfaces and //line comments do not hide findings,
// and that a directive on the line where a node ends applies to that line (DET-120, DET-130,
// DET-140, DET-170, DET-200).
func TestRuleBypasses(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"a.go": indirectSource,
		"b.go": "package p\n\nimport . \"fmt\"\n\nfunc g() { Println() }\n",
		"c.go": "package p\n\nimport \"errors\"\n\nvar errX = errors.New(\"x\")\n\nfunc h(err error) bool { return errors.Is(err, errX) }\n",
	})
	fs, err := CheckDir(dir, "example.com/p", fixtureCore)
	if err != nil {
		t.Fatal(err)
	}
	const clockTail = " is not allowed in core: core reads no clock and uses time only for Time and Duration values and their arithmetic"
	const convTail = " can reach time methods that read the zone or the clock; pass t.UnixMilli() or a Duration"
	const rangeTail = ": iteration order is random; range over sorted keys or annotate //tenure:maporder <reason>"
	const sortTail = " is not stable: elements that compare equal may come out in any order, and the order can change " +
		"between Go releases; make the comparison a total order and annotate //tenure:totalorder <reason>, or use a stable sort"
	want := []string{
		"a.go:22:36: DT003: time.Time used as any" + convTail,
		"a.go:24:14: DT002: channel type: core uses no channels",
		"a.go:24:39: DT002: channel send: core uses no channels",
		"a.go:24:45: DT003: time.Time used as any" + convTail,
		"a.go:29:16: DT003: time.Time.Year" + clockTail,
		"a.go:30:8: DT003: time.Time.Location" + clockTail,
		"a.go:31:9: DT003: time.Now" + clockTail,
		"a.go:32:15: DT008: slices.SortFunc" + sortTail,
		"a.go:34:2: DT005: range over map map[string]int" + rangeTail,
		"a.go:36:2: DT005: range over map p.am" + rangeTail,
		"a.go:38:19: DT003: time.Time used as fmt.Stringer" + convTail,
		"a.go:39:13: DT003: time.Time used as comparable" + convTail,
		"a.go:41:6: DT003: time.Time used as p.yearer" + convTail,
		"a.go:43:8: DT003: time.Time used as any" + convTail,
		"a.go:44:8: DT003: time.Time used as any" + convTail,
		"a.go:46:12: DT003: time.Time used as any" + convTail,
		"a.go:47:18: DT003: time.Time used as any" + convTail,
		"a.go:47:21: DT003: time.Time used as any" + convTail,
		"a.go:48:13: DT003: time.Time used as any" + convTail,
		"a.go:49:10: DT003: time.Time used as any" + convTail,
		"a.go:54:2: DT005: range over map map[string]int" + rangeTail,
		"a.go:55:4: DT000: //tenure:maporder suppresses no finding",
		"a.go:56:2: DT005: range over map map[string]int" + rangeTail,
		"a.go:61:9: DT003: time.Since" + clockTail,
		"a.go:63:9: DT003: time.Time used as p.yearer" + convTail,
		"a.go:67:9: DT003: time.Time used as any" + convTail,
		"a.go:70:7: DT003: time.Time used as any" + convTail,
		"a.go:72:6: DT003: time.Time used as interface{Equal(time.Time) bool; comparable}" + convTail,
		"a.go:84:8: DT003: time.Time used as any" + convTail,
		"a.go:86:13: DT003: time.Time used as any" + convTail,
		"a.go:88:9: DT003: time.Time used as any" + convTail,
		"a.go:90:9: DT003: time.Time used as any" + convTail,
		"a.go:115:41: DT008: slices.Sort" + sortTail,
		"b.go:5:12: DT004: fmt.Println is not allowed in core: core performs no I/O; return values and errors instead",
	}
	if got := strings.Split(findingLines(fs), "\n"); !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestValueRules checks the messages of DT006, DT007 and DT009 and the cases the fixtures do not
// show: the fmt argument walk through nested and recursive types, spread and multi-valued
// arguments, %p after flags and an index, conversions around a product, one complex finding per
// line, constant products, writes through qualified identifiers, dereferences, range clauses and
// embedded fields, and method values; and the fixture dt006's messages.
func TestValueRules(t *testing.T) {
	dt006 := strings.Split(findingLines(fixtureFindings(t, "dt006")), "\n")
	for _, want := range []string{
		"a.go:21:7: DT006: map key type fixture.stamp contains time.Time, whose == compares time zone pointers; key by an integer such as UnixNano",
		"a.go:27:17: DT006: argument of type fixture.holder to fmt.Sprint may print a memory address",
		"a.go:30:9: DT006: comparing time.Time with == compares time zone pointers and monotonic readings; use Time.Equal or Time.Compare",
		"a.go:32:8: DT006: uintptr in core: no value may depend on a memory address",
	} {
		if !slices.Contains(dt006, want) {
			t.Errorf("dt006: no finding %q in:\n%s", want, strings.Join(dt006, "\n"))
		}
	}

	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"a.go": `package p

import (
	"errors"
	"fmt"
	"math"
	"time"
)

type name struct{ s string }

func (n name) String() string { return n.s }

type event struct{ at time.Time }

type tree struct{ kids []tree }

type ppm float64

func two() (int, *int) { return 0, nil }

func fmts(xs []any, ev event, n name, tr tree, m map[*int]bool, ch chan int, fn func(), err error) {
	_ = fmt.Sprint()
	_ = fmt.Sprint(xs...)
	_ = fmt.Sprint(two())
	_ = fmt.Errorf("%%p %-[1]p %d", xs, 1)
	_ = fmt.Sprintln(ev, n, tr, err, nil)
	_ = fmt.Append(nil, m, ch, fn)
	_ = ev == event{}
}

func floats(a, b float64) {
	_ = float64((a * b))
	_ = ppm(a * b)
	_ = (a * b)
	_ = math.Pi * 2
	_ = math.Log(a)
	_, _ = complex(a, b), complex(b, a)
}

type counter struct{ n int }

func (c *counter) Inc() { c.n++ }

type byValue struct{ counter }

type byPointer struct{ *counter }

var (
	total int
	ptr   *int
	bv    byValue
	bp    byPointer
	addr  = &total
)

var _ = func() int { total = 1; return 0 }()

func writes(xs []int) {
	errors.ErrUnsupported = nil
	*ptr = 1
	for total = range xs {
	}
	bv.Inc()
	bp.Inc()
	_ = addr
	inc := bv.Inc
	_ = (bv.Inc)
	(bv.Inc)()
	_ = bp.Inc
	inc()
}

var _ = bv.Inc
`})
	fs, err := CheckDir(dir, "example.com/p", fixtureCore)
	if err != nil {
		t.Fatal(err)
	}
	const (
		fused = "floating-point multiplication may be fused with an addition on some platforms; " +
			"convert the product explicitly, for example float64(a*b)"
		mutable = ": core keeps no mutable package state"
	)
	want := []string{
		"a.go:22:68: DT002: channel type: core uses no channels",
		"a.go:25:17: DT006: argument of type *int to fmt.Sprint may print a memory address",
		"a.go:26:17: DT006: %p prints a memory address",
		"a.go:27:19: DT003: argument of type p.event to fmt.Sprintln formats a time.Time in the local time zone; pass t.UnixMilli() or a Duration",
		"a.go:28:22: DT006: argument of type map[*int]bool to fmt.Append may print a memory address",
		"a.go:28:25: DT006: argument of type chan int to fmt.Append may print a memory address",
		"a.go:28:29: DT006: argument of type func() to fmt.Append may print a memory address",
		"a.go:29:6: DT006: comparing p.event with == compares time zone pointers and monotonic readings; use Time.Equal or Time.Compare",
		"a.go:35:7: DT007: " + fused,
		"a.go:37:11: DT007: math.Log may round differently on other platforms; core uses only math's exactly rounded functions",
		"a.go:38:9: DT007: complex type complex128 in core",
		"a.go:57:22: DT009: write to package-level variable p.total" + mutable,
		"a.go:60:2: DT009: write to package-level variable errors.ErrUnsupported" + mutable,
		"a.go:61:2: DT009: write to package-level variable p.ptr" + mutable,
		"a.go:62:2: DT009: write to package-level variable p.total" + mutable,
		"a.go:64:2: DT009: write to package-level variable p.bv" + mutable,
		"a.go:67:9: DT009: write to package-level variable p.bv" + mutable,
		"a.go:68:7: DT009: write to package-level variable p.bv" + mutable,
		"a.go:69:2: DT009: write to package-level variable p.bv" + mutable,
	}
	if got := strings.Split(findingLines(fs), "\n"); !slices.Equal(got, want) {
		t.Errorf("findings:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
