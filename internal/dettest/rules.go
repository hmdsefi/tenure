// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/build/constraint"
	"go/token"
	"go/types"
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

// clockTail ends the DT003 messages about uses of package time (DET-120).
const clockTail = " is not allowed in core: core reads no clock and uses time only for Time and Duration values and their arithmetic"

// clockNames are the package-level objects of package time that core may use (DET-120).
var clockNames = []string{"Duration", "Hour", "Microsecond", "Millisecond", "Minute", "Nanosecond", "Second", "Time"}

// clockMethods are the methods of time.Time and time.Duration that core may use, as
// "<Recv>.<Name>" (DET-120).
var clockMethods = []string{
	"Duration.Abs", "Duration.Microseconds", "Duration.Milliseconds", "Duration.Nanoseconds", "Duration.Round",
	"Duration.String", "Duration.Truncate",
	"Time.Add", "Time.After", "Time.Before", "Time.Compare", "Time.Equal", "Time.IsZero", "Time.Round", "Time.Sub",
	"Time.Unix", "Time.UnixMicro", "Time.UnixMilli", "Time.UnixNano",
}

// fmtNames are the objects of package fmt that core may use (DET-130).
var fmtNames = []string{"Append", "Appendf", "Appendln", "Errorf", "Formatter", "GoStringer", "Sprint", "Sprintf",
	"Sprintln", "State", "Stringer"}

// unstableSorts are the unstable sort functions of DET-170, as "<pkg>.<Name>".
var unstableSorts = []string{"slices.SortFunc", "sort.Slice", "sort.Sort"}

// typeString returns t as types.TypeString writes it with package names as qualifiers
// (DET-030).
func typeString(t types.Type) string {
	return types.TypeString(t, func(p *types.Package) string { return p.Name() })
}

// nodes reports the findings of the rules that read the syntax and the type information of the
// host file f: DT002 (DET-110), DT003 (DET-120), DT004 (DET-130), DT005 (DET-140) and DT008
// (DET-170).
func (r *pkgRun) nodes(f *ast.File, info *types.Info) {
	add := r.addAt
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GoStmt:
			add(n.Pos(), RuleConcurrency, "go statement: core starts no goroutines")
		case *ast.SelectStmt:
			add(n.Pos(), RuleConcurrency, "select statement: core uses no channels")
		case *ast.ChanType:
			add(n.Pos(), RuleConcurrency, "channel type: core uses no channels")
		case *ast.SendStmt:
			add(n.Pos(), RuleConcurrency, "channel send: core uses no channels")
		case *ast.UnaryExpr:
			if n.Op == token.ARROW {
				add(n.Pos(), RuleConcurrency, "channel receive: core uses no channels")
			}
		case *ast.RangeStmt:
			if t := info.TypeOf(n.X); t != nil {
				if _, ok := coreType(t).(*types.Map); ok {
					add(n.For, RuleMapRange, fmt.Sprintf("range over map %s: iteration order is random; "+
						"range over sorted keys or annotate //tenure:maporder <reason>", typeString(t)))
				}
			}
		case *ast.SelectorExpr:
			if sel := info.Selections[n]; sel != nil {
				if recv, ok := timeMember(sel); ok && !slices.Contains(clockMethods, recv+"."+sel.Obj().Name()) {
					add(n.Sel.Pos(), RuleClock, "time."+recv+"."+sel.Obj().Name()+clockTail)
				}
			}
		case *ast.Ident:
			if rule, msg, ok := packageUse(info.Uses[n]); ok {
				add(n.Pos(), rule, msg)
			}
		}
		return true
	})
}

// packageUse returns the rule and message of a use of obj when obj is a package-level object of
// time, fmt, sort or slices that core may not use (DET-120, DET-130, DET-170).
func packageUse(obj types.Object) (Rule, string, bool) {
	if obj == nil || obj.Pkg() == nil || obj.Parent() != obj.Pkg().Scope() {
		return "", "", false
	}
	path, name := obj.Pkg().Path(), obj.Name()
	switch {
	case path == "time" && !slices.Contains(clockNames, name):
		return RuleClock, "time." + name + clockTail, true
	case path == "fmt" && !slices.Contains(fmtNames, name):
		return RuleIO, "fmt." + name + " is not allowed in core: core performs no I/O; return values and errors instead", true
	case (path == "sort" || path == "slices") && slices.Contains(unstableSorts, path+"."+name):
		return RuleSort, path + "." + name + " is not stable: elements that compare equal may come out in any order, " +
			"and the order can change between Go releases; make the comparison a total order and annotate " +
			"//tenure:totalorder <reason>, or use a stable sort", true
	}
	return "", "", false
}

// timeMember returns the name of the type of package time that declares the method or field
// that sel selects, and whether sel selects one.
func timeMember(sel *types.Selection) (string, bool) {
	obj := sel.Obj()
	if obj.Pkg() == nil || obj.Pkg().Path() != "time" {
		return "", false
	}
	var owner types.Type
	if fn, ok := obj.(*types.Func); ok {
		owner = fn.Signature().Recv().Type()
	} else {
		owner = sel.Recv() // the field's struct is at the end of the embedding path
		idx := sel.Index()
		for _, i := range idx[:len(idx)-1] {
			s, ok := deref(owner).Underlying().(*types.Struct)
			if !ok {
				return "", false
			}
			owner = s.Field(i).Type()
		}
	}
	if named, ok := types.Unalias(deref(owner)).(*types.Named); ok {
		return named.Obj().Name(), true
	}
	return typeString(owner), true
}

// deref returns the element type of t if t is a pointer, and t otherwise.
func deref(t types.Type) types.Type {
	if p, ok := t.Underlying().(*types.Pointer); ok {
		return p.Elem()
	}
	return t
}

// coreType returns the core type of t: its underlying type, or, for a type parameter, the
// underlying type that every type in its type set shares; nil when they share none.
func coreType(t types.Type) types.Type {
	switch u := t.Underlying().(type) {
	case *types.Interface: // a type parameter's constraint: its type set is the intersection of its elements
		for i := range u.NumEmbeddeds() {
			if c := coreType(u.EmbeddedType(i)); c != nil {
				return c
			}
		}
		return nil
	case *types.Union:
		var core types.Type
		for i := range u.Len() {
			c := coreType(u.Term(i).Type())
			if c == nil || (core != nil && !types.Identical(c, core)) {
				return nil
			}
			core = c
		}
		return core
	default:
		return u
	}
}
