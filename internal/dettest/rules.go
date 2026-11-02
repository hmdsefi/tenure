// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/build/constraint"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// CoreImports lists the standard-library packages a core package may import, sorted.
var CoreImports = []string{"cmp", "errors", "fmt", "hash/fnv", "math", "math/bits", "slices", "sort", "strconv", "strings", "time"}

// imports reports DT001 for every import of f whose path is neither in CoreImports nor
// corePath nor below it, and for every import below corePath whose directory the walk does not
// enter, at the import path literal (DET-100).
func (r *pkgRun) imports(f *ast.File) {
	for _, spec := range f.Imports {
		p, _ := strconv.Unquote(spec.Path.Value) // the parser accepts only valid string literals
		if slices.Contains(CoreImports, p) || p == r.corePath || (strings.HasPrefix(p, r.corePath+"/") && !r.unwalked(p)) {
			continue
		}
		r.addAt(spec.Path.Pos(), RuleImport, fmt.Sprintf("import %q is not allowed in core; core imports only %s and packages of %s",
			p, strings.Join(CoreImports, ", "), r.corePath))
	}
}

// unwalked reports whether the walk does not enter the directory of the package path below
// corePath: a path element it skips, or, in a Check run, a directory on the way that is a
// symbolic link or holds its own go.mod.
func (r *pkgRun) unwalked(path string) bool {
	dir := ""
	if r.c.root != "" {
		dir = filepath.Join(r.c.root, "core")
	}
	for elem := range strings.SplitSeq(strings.TrimPrefix(path, r.corePath+"/"), "/") {
		if skippedDir(elem) {
			return true
		}
		if dir == "" {
			continue
		}
		dir = filepath.Join(dir, elem)
		st, err := os.Lstat(dir)
		if err != nil {
			dir = "" // missing: type-checking reports it
			continue
		}
		if st.Mode()&fs.ModeSymlink != 0 {
			return true
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return true
		}
	}
	return false
}

// platformMsg ends the DT010 messages about .go files.
const platformMsg = "every platform compiles the same files"

// platformContexts are the build contexts of DET-190 item 2: linux/amd64, linux/arm64,
// linux/386, darwin/arm64 and windows/amd64, with cgo enabled.
func platformContexts() []build.Context {
	targets := [][2]string{{"linux", "amd64"}, {"linux", "arm64"}, {"linux", "386"}, {"darwin", "arm64"}, {"windows", "amd64"}}
	out := make([]build.Context, len(targets))
	for i, t := range targets {
		c := build.Default
		c.GOOS, c.GOARCH, c.CgoEnabled = t[0], t[1], true
		out[i] = c
	}
	return out
}

// platformFile reports DT010 for the .go file name in the absolute directory dir, parsed as f:
// a build constraint line before the package clause, or else a file name that one of the
// platform contexts rejects (DET-190 items 1 and 2).
func (r *pkgRun) platformFile(dir, name string, f *ast.File) error {
	for _, g := range f.Comments {
		if g.Pos() >= f.Package {
			break
		}
		for _, c := range g.List {
			if constraint.IsGoBuild(c.Text) || constraint.IsPlusBuild(c.Text) {
				line := r.c.fset.PositionFor(c.Pos(), false).Line
				r.add(filepath.Join(dir, name), line, 1, RulePlatform, "build constraint in core: "+platformMsg)
				return nil
			}
		}
	}
	for _, ctx := range platformContexts() {
		ok, err := ctx.MatchFile(dir, name)
		if err != nil {
			return err
		}
		if !ok {
			r.add(filepath.Join(dir, name), 0, 0, RulePlatform, fmt.Sprintf("platform-specific file %s: %s", name, platformMsg))
			return nil
		}
	}
	return nil
}

// nonGoExts are the extensions of the non-Go source files of DET-190 item 3.
var nonGoExts = []string{".s", ".S", ".sx", ".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx",
	".m", ".f", ".F", ".for", ".f90", ".syso", ".swig", ".swigcxx"}

// nonGoFile reports DT010 for the file at the absolute path path if it is a non-Go source file
// (DET-190 item 3).
func (r *pkgRun) nonGoFile(path string) {
	if slices.Contains(nonGoExts, filepath.Ext(path)) {
		r.add(path, 0, 0, RulePlatform, fmt.Sprintf("non-Go source file %s: core is plain Go", filepath.Base(path)))
	}
}
