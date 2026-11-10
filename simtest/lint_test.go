// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"cmp"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The harness rules keep a run a function of its seed. The packages that build the simulated
// world and check it take the time from the simulation's kernel and every random choice from
// the kernel's seeded streams, and run on the kernel's one goroutine. So their code, tests
// aside, has
//
//   - no go statement;
//   - no import of sync, sync/atomic, math/rand or crypto/rand;
//   - no use of the functions of package time that read the wall clock or start a timer;
//   - no use of the package-level functions of math/rand/v2, which draw from a source that no
//     seed controls: only its constructors and types, for the streams the kernel hands out.
//
// The rules look at syntax only: a file is parsed, not type-checked. A local name that hides
// an imported package would be reported as the package, which fails on the safe side.

// harnessDirs are the directories the rules hold in, with their subdirectories.
var harnessDirs = []string{"internal/world", "internal/check"}

// lintFixture is a directory of files that break every rule on marked lines.
const lintFixture = "testdata/lintbad"

// wantMarker starts the comment that marks a planted line of the fixture; the rest of the
// comment is the rule the line breaks.
const wantMarker = "// want: "

// The rules, as a finding prints them. The last four take the name of a package or of a
// function.
const (
	ruleGo        = "go statement"
	ruleImport    = "import of "
	ruleDotImport = "dot import of "
	ruleTime      = "use of time."
	ruleRand      = "use of math/rand/v2."
)

// forbiddenImports are the packages the harness must not import.
var forbiddenImports = []string{"crypto/rand", "math/rand", "sync", "sync/atomic"}

// forbiddenTime are the functions of package time the harness must not use.
var forbiddenTime = []string{
	"After", "AfterFunc", "NewTicker", "NewTimer", "Now", "Since", "Sleep", "Tick", "Until",
}

// allowedRand are the names of package math/rand/v2 the harness may use: its constructors and
// its types. Every other name of the package is a function that draws from the global source.
var allowedRand = []string{
	"New", "NewChaCha8", "NewPCG", "NewZipf",
	"ChaCha8", "PCG", "Rand", "Source", "Zipf",
}

// The packages whose names the rules follow through a file.
const (
	pathTime = "time"
	pathRand = "math/rand/v2"
)

// finding is one broken rule.
type finding struct {
	file string // slash-separated, as the directory was given
	line int
	rule string
}

// String prints the finding as <file>:<line>: <rule>.
func (f finding) String() string {
	return f.file + ":" + strconv.Itoa(f.line) + ": " + f.rule
}

// compareFindings orders findings by file, line and rule.
func compareFindings(a, b finding) int {
	return cmp.Or(cmp.Compare(a.file, b.file), cmp.Compare(a.line, b.line), cmp.Compare(a.rule, b.rule))
}

// lintTree parses every Go file below dir that is not a test and returns, in order, the rules
// the files break and the lines marked with wantMarker, as the findings the markers ask for.
// files is the number of files parsed. Directories named testdata are skipped.
func lintTree(dir string) (found, wanted []finding, files int, err error) {
	fset := token.NewFileSet()
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != dir && d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		files++
		name := filepath.ToSlash(path)
		line := func(pos token.Pos) int { return fset.Position(pos).Line }
		found = append(found, lintFile(f, name, line)...)
		for _, group := range f.Comments {
			for _, c := range group.List {
				if rule, ok := strings.CutPrefix(c.Text, wantMarker); ok {
					wanted = append(wanted, finding{file: name, line: line(c.Pos()), rule: rule})
				}
			}
		}
		return nil
	})
	slices.SortFunc(found, compareFindings)
	slices.SortFunc(wanted, compareFindings)
	return found, wanted, files, err
}

// lintFile returns the rules a parsed file breaks. name is the file's name in the findings and
// line gives the line of a position.
func lintFile(f *ast.File, name string, line func(token.Pos) int) []finding {
	var found []finding
	report := func(pos token.Pos, rule string) {
		found = append(found, finding{file: name, line: line(pos), rule: rule})
	}

	// The names under which the file imports time and math/rand/v2. A package may be imported
	// more than once, under different names.
	local := make(map[string]string)
	for _, spec := range f.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue // the parser accepts only string literals
		}
		if slices.Contains(forbiddenImports, path) {
			report(spec.Pos(), ruleImport+path)
		}
		if path != pathTime && path != pathRand {
			continue
		}
		switch {
		case spec.Name == nil && path == pathRand:
			local["rand"] = path // the package's name, not the last element of its path
		case spec.Name == nil:
			local[path] = path
		case spec.Name.Name == ".":
			// The package's functions would be plain identifiers, which syntax cannot tell
			// from the file's own.
			report(spec.Pos(), ruleDotImport+path)
		case spec.Name.Name != "_":
			local[spec.Name.Name] = path
		}
	}

	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GoStmt:
			report(n.Pos(), ruleGo)
		case *ast.SelectorExpr:
			pkg, ok := n.X.(*ast.Ident)
			if !ok {
				break
			}
			// A use is a call or the function as a value: both reach the function.
			switch local[pkg.Name] {
			case pathTime:
				if slices.Contains(forbiddenTime, n.Sel.Name) {
					report(n.Pos(), ruleTime+n.Sel.Name)
				}
			case pathRand:
				if !slices.Contains(allowedRand, n.Sel.Name) {
					report(n.Pos(), ruleRand+n.Sel.Name)
				}
			}
		}
		return true
	})
	return found
}

// TestATSIM03HarnessRules checks that the packages of the harness break no harness rule, and
// that the rules find every line planted in the fixture and nothing else there.
func TestATSIM03HarnessRules(t *testing.T) {
	for _, dir := range harnessDirs {
		found, _, files, err := lintTree(dir)
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		if files == 0 {
			t.Errorf("%s: no Go file to check", dir)
		}
		for _, f := range found {
			t.Errorf("%v", f)
		}
	}

	found, wanted, files, err := lintTree(lintFixture)
	if err != nil {
		t.Fatalf("%s: %v", lintFixture, err)
	}
	if files == 0 {
		t.Fatalf("%s: no Go file to check", lintFixture)
	}
	for _, f := range wanted {
		if !slices.Contains(found, f) {
			t.Errorf("planted and not found: %v", f)
		}
	}
	for _, f := range found {
		if !slices.Contains(wanted, f) {
			t.Errorf("found and not planted: %v", f)
		}
	}

	// The fixture plants every rule: each forbidden import and function by name, and for
	// math/rand/v2, whose forbidden functions are all but a few, at least one.
	planted := func(rule string) bool {
		return slices.ContainsFunc(wanted, func(f finding) bool { return f.rule == rule })
	}
	rules := []string{ruleGo, ruleDotImport + pathTime, ruleDotImport + pathRand}
	for _, path := range forbiddenImports {
		rules = append(rules, ruleImport+path)
	}
	for _, name := range forbiddenTime {
		rules = append(rules, ruleTime+name)
	}
	for _, rule := range rules {
		if !planted(rule) {
			t.Errorf("%s plants no %q", lintFixture, rule)
		}
	}
	if !slices.ContainsFunc(wanted, func(f finding) bool { return strings.HasPrefix(f.rule, ruleRand) }) {
		t.Errorf("%s plants no %q", lintFixture, ruleRand+"<function>")
	}
}

// TestHarnessFindingFormat checks how a finding prints.
func TestHarnessFindingFormat(t *testing.T) {
	f := finding{file: "internal/world/build.go", line: 41, rule: ruleTime + "Now"}
	const want = "internal/world/build.go:41: use of time.Now"
	if got := fmt.Sprint(f); got != want {
		t.Errorf("finding prints as %q, want %q", got, want)
	}
}
