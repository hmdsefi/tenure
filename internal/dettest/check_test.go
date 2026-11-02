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

// wantComment matches a fixture expectation "// want DT001 [DT005 …]" (DET-320).
var wantComment = regexp.MustCompile(`^// want((?:[ \t]+DT[0-9]{3})+)[ \t]*$`)

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
				for _, r := range strings.Fields(m[1]) {
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
