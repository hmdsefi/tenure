// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"cmp"
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Rule identifies a check.
type Rule string

const (
	RuleMeta        Rule = "DT000" // directive problems
	RuleImport      Rule = "DT001" // import not on the allow-list
	RuleConcurrency Rule = "DT002" // go, select, channels
	RuleClock       Rule = "DT003" // time beyond values and arithmetic
	RuleIO          Rule = "DT004" // fmt functions that read or write
	RuleMapRange    Rule = "DT005" // range over a map
	RulePointer     Rule = "DT006" // behavior that depends on memory addresses
	RuleFloat       Rule = "DT007" // floating-point results that differ by platform
	RuleSort        Rule = "DT008" // unstable sort
	RuleGlobal      Rule = "DT009" // write to package-level state
	RulePlatform    Rule = "DT010" // platform-specific or non-Go files
)

// Finding is one violation.
type Finding struct {
	File string // slash-separated path, relative to the root Check was given or the dir CheckDir was given
	Line int    // 1-based; 0 for a finding about a whole file
	Col  int    // 1-based; 0 for a finding about a whole file
	Rule Rule
	Msg  string
}

// String returns "<File>:<Line>:<Col>: <Rule>: <Msg>".
func (f Finding) String() string {
	return fmt.Sprintf("%s:%d:%d: %s: %s", f.File, f.Line, f.Col, f.Rule, f.Msg)
}

// sortFindings sorts fs by (File, Line, Col, Rule, Msg) (DET-030).
func sortFindings(fs []Finding) {
	slices.SortFunc(fs, func(a, b Finding) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Line, b.Line), cmp.Compare(a.Col, b.Col),
			cmp.Compare(a.Rule, b.Rule), cmp.Compare(a.Msg, b.Msg))
	})
}

// FindRoot walks up from dir to the first directory whose go.mod declares module modulePath.
func FindRoot(dir, modulePath string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("dettest: find root of %s: %w", dir, err)
	}
	for d := abs; ; d = filepath.Dir(d) {
		data, err := os.ReadFile(filepath.Join(d, "go.mod")) //nolint:gosec // G304: reading go.mod files above dir is the point
		switch {
		case err == nil:
			if goModModule(string(data)) == modulePath {
				return d, nil
			}
		case !errors.Is(err, fs.ErrNotExist):
			return "", fmt.Errorf("dettest: find root of %s: %w", abs, err)
		}
		if filepath.Dir(d) == d {
			return "", fmt.Errorf("dettest: no go.mod for module %s above %s", modulePath, abs)
		}
	}
}

// goModModule returns the path of the module directive in the go.mod text gomod, without quotes
// or a trailing comment, or "" if there is none.
func goModModule(gomod string) string {
	for line := range strings.SplitSeq(gomod, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module")
		if !ok || rest == "" || (rest[0] != ' ' && rest[0] != '\t') {
			continue
		}
		rest, _, _ = strings.Cut(rest, "//")
		rest = strings.TrimSpace(rest)
		if p, err := strconv.Unquote(rest); err == nil {
			return p
		}
		return rest
	}
	return ""
}

// Check runs every rule on each package directory at or below <root>/core of the module
// modulePath and returns the findings sorted by (File, Line, Col, Rule, Msg).
func Check(root, modulePath string) ([]Finding, error) {
	out, err := check(root, modulePath)
	return out, dettestError(err)
}

// CheckDir runs every rule on the single package in dir, whose import path is importPath.
// Imports of corePath and of packages below it are allowed. File paths in findings are
// relative to dir.
func CheckDir(dir, importPath, corePath string) ([]Finding, error) {
	out, err := checkDir(dir, importPath, corePath)
	return out, dettestError(err)
}

// dettestError returns err with the prefix "dettest: " unless it has it already.
func dettestError(err error) error {
	if err == nil || strings.HasPrefix(err.Error(), "dettest: ") {
		return err
	}
	return fmt.Errorf("dettest: %w", err)
}

// resolveDir returns the absolute path of dir with symbolic links resolved, so that a walk
// through a linked directory sees its entries as directories and files.
func resolveDir(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

func check(root, modulePath string) ([]Finding, error) {
	abs, err := resolveDir(root)
	if err != nil {
		return nil, err
	}
	coreDir := filepath.Join(abs, "core")
	if st, err := os.Stat(coreDir); err != nil || !st.IsDir() {
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("dettest: %s: no such directory", filepath.Join(root, "core"))
	}
	dirs, err := packageDirs(coreDir)
	if err != nil {
		return nil, err
	}
	c := newChecker(abs, modulePath)
	var out []Finding
	for _, dir := range dirs {
		rel, err := filepath.Rel(abs, dir)
		if err != nil {
			return nil, err
		}
		found, err := c.checkPackage(abs, dir, modulePath+"/"+filepath.ToSlash(rel), modulePath+"/core")
		if err != nil {
			return nil, err
		}
		out = append(out, found...)
	}
	sortFindings(out)
	return out, nil
}

func checkDir(dir, importPath, corePath string) ([]Finding, error) {
	abs, err := resolveDir(dir)
	if err != nil {
		return nil, err
	}
	out, err := newChecker("", "").checkPackage(abs, abs, importPath, corePath)
	if err != nil {
		return nil, err
	}
	sortFindings(out)
	return out, nil
}

// packageDirs returns the absolute package directories at or below the absolute directory dir,
// in os.ReadDir order, parents before children: the directories that hold a non-test .go file.
// It does not enter testdata, vendor, directories whose name starts with "." or "_", or
// directories with their own go.mod (DET-010).
func packageDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	if slices.ContainsFunc(entries, func(e fs.DirEntry) bool { return !e.IsDir() && checkedGoFile(e.Name()) }) {
		out = append(out, dir)
	}
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
			continue
		}
		sub := filepath.Join(dir, name)
		if _, err := os.Stat(filepath.Join(sub, "go.mod")); err == nil {
			continue // its own module
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		subs, err := packageDirs(sub)
		if err != nil {
			return nil, err
		}
		out = append(out, subs...)
	}
	return out, nil
}

// checkedGoFile reports whether the file named name is a checked file: a .go file that is not a
// test file.
func checkedGoFile(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}

// checker holds the FileSet and the importer that every package of one Check or CheckDir call
// shares, so that each imported package is type-checked once. The source importer resolves
// packages with go list in the process's working directory, so the packages of the module being
// checked are loaded here instead, from the module's root directory.
type checker struct {
	fset       *token.FileSet
	std        types.Importer // the source importer, for packages outside the module
	root       string         // absolute root directory of the module; "" for CheckDir
	modulePath string
	pkgs       map[string]*types.Package // module packages by import path; nil while loading
}

func newChecker(root, modulePath string) *checker {
	fset := token.NewFileSet()
	return &checker{fset: fset, std: importer.ForCompiler(fset, "source", nil), root: root, modulePath: modulePath,
		pkgs: map[string]*types.Package{}}
}

// Import implements types.Importer.
func (c *checker) Import(path string) (*types.Package, error) {
	return c.ImportFrom(path, "", 0)
}

// ImportFrom implements types.ImporterFrom: it type-checks a package of the module from the host
// files of its directory and passes every other path to the source importer.
func (c *checker) ImportFrom(path, dir string, mode types.ImportMode) (*types.Package, error) {
	rel, ok := c.moduleRel(path)
	if !ok {
		if from, ok := c.std.(types.ImporterFrom); ok {
			return from.ImportFrom(path, dir, mode)
		}
		return c.std.Import(path)
	}
	if pkg, ok := c.pkgs[path]; ok {
		if pkg == nil {
			return nil, fmt.Errorf("import cycle through %s", path)
		}
		return pkg, nil
	}
	c.pkgs[path] = nil
	pkg, err := c.loadModulePackage(path, filepath.Join(c.root, filepath.FromSlash(rel)))
	if err != nil {
		delete(c.pkgs, path)
		return nil, err
	}
	c.pkgs[path] = pkg
	return pkg, nil
}

// moduleRel returns the slash path of the package path relative to the module root, and whether
// path is a package of the module.
func (c *checker) moduleRel(path string) (string, bool) {
	if c.root == "" {
		return "", false
	}
	if path == c.modulePath {
		return ".", true
	}
	return strings.CutPrefix(path, c.modulePath+"/")
}

// loadModulePackage parses and type-checks the host files of the module package path in the
// absolute directory dir.
func (c *checker) loadModulePackage(path, dir string) (*types.Package, error) {
	bp, err := hostContext().ImportDir(dir, 0)
	if err != nil {
		return nil, err
	}
	files := make([]*ast.File, len(bp.GoFiles))
	for i, name := range bp.GoFiles {
		if files[i], err = parser.ParseFile(c.fset, filepath.Join(dir, name), nil, 0); err != nil {
			return nil, err
		}
	}
	conf := types.Config{Importer: c}
	return conf.Check(path, c.fset, files, nil)
}

// hostContext returns build.Default without cgo: it selects the host files, which are
// type-checked (DET-020).
func hostContext() *build.Context {
	c := build.Default
	c.CgoEnabled = false
	return &c
}

// pkgRun is the check of one package.
type pkgRun struct {
	c        *checker
	base     string // absolute directory that finding paths are relative to
	corePath string
	out      []Finding
}

// add appends a finding about the file at the absolute path file.
func (r *pkgRun) add(file string, line, col int, rule Rule, msg string) error {
	rel, err := filepath.Rel(r.base, file)
	if err != nil {
		return err
	}
	r.out = append(r.out, Finding{File: filepath.ToSlash(rel), Line: line, Col: col, Rule: rule, Msg: msg})
	return nil
}

// addAt appends a finding at pos.
func (r *pkgRun) addAt(pos token.Pos, rule Rule, msg string) error {
	p := r.c.fset.Position(pos)
	return r.add(p.Filename, p.Line, p.Column, rule, msg)
}

// checkPackage runs the rules on the package in the absolute directory dir, whose import path is
// importPath. Finding paths are relative to the absolute directory base. Files are parsed with
// their absolute names, which go/types passes on to the importer as the importing directory.
func (c *checker) checkPackage(base, dir, importPath, corePath string) ([]Finding, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	r := &pkgRun{c: c, base: base, corePath: corePath}
	var host []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		if !strings.HasSuffix(name, ".go") {
			if err := r.nonGoFile(path); err != nil {
				return nil, err
			}
			continue
		}
		f, err := parser.ParseFile(c.fset, path, nil, parser.ParseComments)
		if err != nil {
			rel, _ := filepath.Rel(base, path)
			return nil, fmt.Errorf("dettest: parsing %s: %w", filepath.ToSlash(rel), err)
		}
		if err := r.imports(f); err != nil {
			return nil, err
		}
		if err := r.platformFile(dir, name, f); err != nil {
			return nil, err
		}
		ok, err := hostContext().MatchFile(dir, name)
		if err != nil {
			return nil, err
		}
		if ok {
			host = append(host, f)
		}
	}
	if len(host) > 0 {
		conf := types.Config{Importer: c}
		pkg, err := conf.Check(importPath, c.fset, host, nil)
		if err != nil {
			return nil, fmt.Errorf("dettest: type-checking %s: %w", importPath, err)
		}
		if _, ok := c.moduleRel(importPath); ok {
			if _, loaded := c.pkgs[importPath]; !loaded {
				c.pkgs[importPath] = pkg // packages below reuse it
			}
		}
	}
	return r.out, nil
}
