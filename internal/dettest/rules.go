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

// unstableSorts are the sort functions of DET-170 that are unstable for every element type, as
// "<pkg>.<Name>".
var unstableSorts = []string{"slices.SortFunc", "sort.Float64s", "sort.Slice", "sort.Sort"}

// floatSorts are the generic functions of package slices that DET-170 reports when they are
// instantiated with a floating-point element type.
var floatSorts = []string{"BinarySearch", "Sort", "Sorted"}

// fmtWalked are the functions of package fmt whose operands DET-150 walks; the conversions of
// their arguments to any are left to that walk (DET-120).
var fmtWalked = []string{"Append", "Appendf", "Appendln", "Errorf", "Sprint", "Sprintf", "Sprintln"}

// ioTail ends the DT004 messages (DET-130).
const ioTail = " is not allowed in core: core performs no I/O; return values and errors instead"

// sortMsg returns the DT008 message about the sort function or method name (DET-170).
func sortMsg(name string) string {
	return name + " is not stable: elements that compare equal may come out in any order, " +
		"and the order can change between Go releases; make the comparison a total order and annotate " +
		"//tenure:totalorder <reason>, or use a stable sort"
}

// typeString returns t as types.TypeString writes it with package names as qualifiers
// (DET-030).
func typeString(t types.Type) string {
	return types.TypeString(t, func(p *types.Package) string { return p.Name() })
}

// hostRules runs the rules that need type information on the host files of the package, then
// applies the directives of those files (DET-020, DET-200).
func (r *pkgRun) hostRules(host []*ast.File, info *types.Info) {
	var dirs []*directive
	for _, f := range host {
		r.nodes(f, info)
		dirs = append(dirs, r.directives(f)...)
	}
	r.applyDirectives(dirs)
}

// nodes reports the findings of the rules that read the syntax and the type information of the
// host file f: DT002 (DET-110), DT003 (DET-120), DT004 (DET-130), DT005 (DET-140) and DT008
// (DET-170).
func (r *pkgRun) nodes(f *ast.File, info *types.Info) {
	add := r.addAt
	var stack []ast.Node // n and the nodes that enclose it
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		r.conversions(n, stack, info)
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
				if rule, msg, ok := memberUse(sel); ok {
					add(n.Sel.Pos(), rule, msg)
				}
			}
		case *ast.Ident:
			if rule, msg, ok := packageUse(info.Uses[n]); ok {
				add(n.Pos(), rule, msg)
			}
			if inst, ok := info.Instances[n]; ok {
				r.instance(n, inst, info.Uses[n])
			}
		}
		return true
	})
}

// packageUse returns the rule and message of a use of obj when obj is a package-level object of
// time, fmt, sort or slices that core may not use, or the builtin print or println (DET-120,
// DET-130, DET-170).
func packageUse(obj types.Object) (Rule, string, bool) {
	if b, ok := obj.(*types.Builtin); ok && (b.Name() == "print" || b.Name() == "println") {
		return RuleIO, b.Name() + ioTail, true
	}
	if obj == nil || obj.Pkg() == nil || obj.Parent() != obj.Pkg().Scope() {
		return "", "", false
	}
	path, name := obj.Pkg().Path(), obj.Name()
	switch {
	case path == "time" && !slices.Contains(clockNames, name):
		return RuleClock, "time." + name + clockTail, true
	case path == "fmt" && !slices.Contains(fmtNames, name):
		return RuleIO, "fmt." + name + ioTail, true
	case (path == "sort" || path == "slices") && slices.Contains(unstableSorts, path+"."+name):
		return RuleSort, sortMsg(path + "." + name), true
	}
	return "", "", false
}

// memberUse returns the rule and message of the selection sel when it selects a method or field
// of package time that core may not use (DET-120), or the Sort method of sort.Float64Slice
// (DET-170).
func memberUse(sel *types.Selection) (Rule, string, bool) {
	obj := sel.Obj()
	if obj.Pkg() == nil {
		return "", "", false
	}
	switch obj.Pkg().Path() {
	case "time":
		recv, ok := timeMember(sel)
		if ok && !slices.Contains(clockMethods, recv+"."+obj.Name()) {
			return RuleClock, "time." + recv + "." + obj.Name() + clockTail, true
		}
	case "sort":
		if fn, ok := obj.(*types.Func); ok && recvName(fn) == "Float64Slice" && fn.Name() == "Sort" {
			return RuleSort, sortMsg("sort.Float64Slice.Sort"), true
		}
	}
	return "", "", false
}

// instance reports the type arguments of the instantiation inst, at the instantiated name id
// that denotes obj: floating-point element types of the sorts of DET-170, and type arguments
// with forbidden time methods (DET-120).
func (r *pkgRun) instance(id *ast.Ident, inst types.Instance, obj types.Object) {
	var tparams *types.TypeParamList
	switch obj := obj.(type) {
	case *types.Func:
		tparams = obj.Signature().TypeParams()
		if obj.Pkg() != nil && obj.Pkg().Path() == "slices" && slices.Contains(floatSorts, obj.Name()) && floatArg(inst.TypeArgs) {
			r.addAt(id.Pos(), RuleSort, sortMsg("slices."+obj.Name()))
		}
	case *types.TypeName:
		if named, ok := obj.Type().(*types.Named); ok {
			tparams = named.TypeParams()
		}
	}
	for i := range inst.TypeArgs.Len() {
		arg := inst.TypeArgs.At(i)
		if !forbiddenTime(arg) {
			continue
		}
		constraint := types.Universe.Lookup("any").Type()
		if tparams != nil && i < tparams.Len() {
			constraint = tparams.At(i).Constraint()
		}
		if !allowedVia(arg, constraint) {
			r.addAt(id.Pos(), RuleClock, conversionMsg(arg, constraint))
		}
	}
}

// floatArg reports whether one of the type arguments args has float32 or float64 as its
// underlying type.
func floatArg(args *types.TypeList) bool {
	for i := range args.Len() {
		if b, ok := args.At(i).Underlying().(*types.Basic); ok && (b.Kind() == types.Float32 || b.Kind() == types.Float64) {
			return true
		}
	}
	return false
}

// conversions reports DT003 for the conversions of values with forbidden time methods to
// interface types that the node n makes, explicitly or implicitly; stack holds n and the nodes
// that enclose it (DET-120).
func (r *pkgRun) conversions(n ast.Node, stack []ast.Node, info *types.Info) {
	switch n := n.(type) {
	case *ast.CallExpr:
		if tv, ok := info.Types[n.Fun]; ok && tv.IsType() {
			if len(n.Args) == 1 {
				r.convert(n.Args[0], info.TypeOf(n.Args[0]), info.TypeOf(n))
			}
			return
		}
		if fmtWalkedCall(n, info) {
			return
		}
		sig, ok := typeOf(info, n.Fun).Underlying().(*types.Signature)
		if !ok {
			return
		}
		params := sig.Params()
		for i, v := range values(n.Args, info) {
			var param types.Type
			switch last := params.Len() - 1; {
			case sig.Variadic() && i >= last && !n.Ellipsis.IsValid():
				if s, ok := params.At(last).Type().Underlying().(*types.Slice); ok {
					param = s.Elem()
				}
			case i < params.Len():
				param = params.At(i).Type()
			}
			r.convert(v.expr, v.typ, param)
		}
	case *ast.AssignStmt:
		if n.Tok == token.ASSIGN || n.Tok == token.DEFINE {
			for i, v := range values(n.Rhs, info) {
				if i < len(n.Lhs) {
					r.convert(v.expr, v.typ, info.TypeOf(n.Lhs[i]))
				}
			}
		}
	case *ast.ValueSpec:
		for i, v := range values(n.Values, info) {
			if i < len(n.Names) {
				r.convert(v.expr, v.typ, info.TypeOf(n.Names[i]))
			}
		}
	case *ast.ReturnStmt:
		if sig := enclosingSignature(stack, info); sig != nil {
			for i, v := range values(n.Results, info) {
				if i < sig.Results().Len() {
					r.convert(v.expr, v.typ, sig.Results().At(i).Type())
				}
			}
		}
	case *ast.CompositeLit:
		r.compositeConversions(n, info)
	case *ast.SendStmt:
		if ch, ok := typeOf(info, n.Chan).Underlying().(*types.Chan); ok {
			r.convert(n.Value, info.TypeOf(n.Value), ch.Elem())
		}
	}
}

// compositeConversions reports the conversions of the elements, keys and values of the composite
// literal n to the types of the slots they fill (DET-120).
func (r *pkgRun) compositeConversions(n *ast.CompositeLit, info *types.Info) {
	t := typeOf(info, n)
	for i, elt := range n.Elts {
		kv, _ := elt.(*ast.KeyValueExpr)
		value := elt
		if kv != nil {
			value = kv.Value
		}
		switch u := deref(t).Underlying().(type) {
		case *types.Struct:
			if field := structField(u, i, kv); field != nil {
				r.convert(value, info.TypeOf(value), field.Type())
			}
		case *types.Slice:
			r.convert(value, info.TypeOf(value), u.Elem())
		case *types.Array:
			r.convert(value, info.TypeOf(value), u.Elem())
		case *types.Map:
			if kv != nil {
				r.convert(kv.Key, info.TypeOf(kv.Key), u.Key())
			}
			r.convert(value, info.TypeOf(value), u.Elem())
		}
	}
}

// structField returns the field of s that element i of a struct literal fills: the field
// named by the key of kv, or field i when kv is nil; nil when there is none.
func structField(s *types.Struct, i int, kv *ast.KeyValueExpr) *types.Var {
	if kv == nil {
		if i < s.NumFields() {
			return s.Field(i)
		}
		return nil
	}
	key, ok := kv.Key.(*ast.Ident)
	if !ok {
		return nil
	}
	for j := range s.NumFields() {
		if s.Field(j).Name() == key.Name {
			return s.Field(j)
		}
	}
	return nil
}

// typeOf returns the type of e, or the invalid type when e has none.
func typeOf(info *types.Info, e ast.Expr) types.Type {
	if t := info.TypeOf(e); t != nil {
		return t
	}
	return types.Typ[types.Invalid]
}

// value is one value of a list of expressions: the expression that gives it and its type.
type value struct {
	expr ast.Expr
	typ  types.Type
}

// values returns the values of the expression list exprs: one per expression, or the results of
// a single call that returns several.
func values(exprs []ast.Expr, info *types.Info) []value {
	if len(exprs) == 1 {
		if tuple, ok := info.TypeOf(exprs[0]).(*types.Tuple); ok {
			out := make([]value, tuple.Len())
			for i := range out {
				out[i] = value{exprs[0], tuple.At(i).Type()}
			}
			return out
		}
	}
	out := make([]value, len(exprs))
	for i, e := range exprs {
		out[i] = value{e, info.TypeOf(e)}
	}
	return out
}

// enclosingSignature returns the signature of the innermost function declaration or literal in
// stack, or nil.
func enclosingSignature(stack []ast.Node, info *types.Info) *types.Signature {
	for i := len(stack) - 1; i >= 0; i-- {
		switch fn := stack[i].(type) {
		case *ast.FuncLit:
			sig, _ := info.TypeOf(fn).(*types.Signature)
			return sig
		case *ast.FuncDecl:
			if obj, ok := info.Defs[fn.Name].(*types.Func); ok {
				return obj.Signature()
			}
			return nil
		}
	}
	return nil
}

// fmtWalkedCall reports whether the call n calls one of the fmt functions whose operands DET-150
// walks.
func fmtWalkedCall(n *ast.CallExpr, info *types.Info) bool {
	var id *ast.Ident
	switch fun := ast.Unparen(n.Fun).(type) {
	case *ast.Ident:
		id = fun
	case *ast.SelectorExpr:
		id = fun.Sel
	}
	if id == nil {
		return false
	}
	fn, ok := info.Uses[id].(*types.Func)
	return ok && fn.Pkg() != nil && fn.Pkg().Path() == "fmt" && slices.Contains(fmtWalked, fn.Name())
}

// convert reports DT003 at e when e, a value of type src, is converted to the interface type dst
// and src has forbidden time methods that dst can reach (DET-120).
func (r *pkgRun) convert(e ast.Expr, src, dst types.Type) {
	if src == nil || dst == nil || !isInterface(dst) || !forbiddenTime(src) || allowedVia(src, dst) {
		return
	}
	r.addAt(e.Pos(), RuleClock, conversionMsg(src, dst))
}

// conversionMsg returns the DT003 message about a value of type src used as dst (DET-120).
func conversionMsg(src, dst types.Type) string {
	return typeString(src) + " used as " + typeString(dst) +
		" can reach time methods that read the zone or the clock; pass t.UnixMilli() or a Duration"
}

// isInterface reports whether t is an interface type and not a type parameter.
func isInterface(t types.Type) bool {
	_, param := types.Unalias(t).(*types.TypeParam)
	return !param && types.IsInterface(t)
}

// forbiddenTime reports whether t has forbidden time methods: the method set of t or of *t holds
// a method of package time that DET-120 does not allow. Interfaces and type parameters have none.
func forbiddenTime(t types.Type) bool {
	if types.IsInterface(t) {
		return false
	}
	if _, ok := t.Underlying().(*types.Pointer); !ok {
		t = types.NewPointer(t)
	}
	ms := types.NewMethodSet(t)
	for i := range ms.Len() {
		if fn, ok := ms.At(i).Obj().(*types.Func); ok && forbiddenTimeMethod(fn) {
			return true
		}
	}
	return false
}

// allowedVia reports whether every method that the interface or constraint iface makes callable
// on a value of type t is allowed: iface has at least one method, and none of them resolves to a
// forbidden time method of t (DET-120).
func allowedVia(t, iface types.Type) bool {
	u, ok := iface.Underlying().(*types.Interface)
	if !ok || u.NumMethods() == 0 {
		return false
	}
	for i := range u.NumMethods() {
		m := u.Method(i)
		obj, _, _ := types.LookupFieldOrMethod(t, true, m.Pkg(), m.Name())
		if fn, ok := obj.(*types.Func); ok && forbiddenTimeMethod(fn) {
			return false
		}
	}
	return true
}

// forbiddenTimeMethod reports whether fn is a method of package time that DET-120 does not allow.
func forbiddenTimeMethod(fn *types.Func) bool {
	return fn.Pkg() != nil && fn.Pkg().Path() == "time" && fn.Signature().Recv() != nil &&
		!slices.Contains(clockMethods, recvName(fn)+"."+fn.Name())
}

// recvName returns the name of the type that declares the method fn.
func recvName(fn *types.Func) string {
	recv := fn.Signature().Recv().Type()
	if named, ok := types.Unalias(deref(recv)).(*types.Named); ok {
		return named.Obj().Name()
	}
	return typeString(recv)
}

// timeMember returns the name of the type of package time that declares the method or field
// that sel selects, and whether sel selects one.
func timeMember(sel *types.Selection) (string, bool) {
	obj := sel.Obj()
	if obj.Pkg() == nil || obj.Pkg().Path() != "time" {
		return "", false
	}
	if fn, ok := obj.(*types.Func); ok {
		return recvName(fn), true
	}
	owner := sel.Recv() // the field's struct is at the end of the embedding path
	idx := sel.Index()
	for _, i := range idx[:len(idx)-1] {
		s, ok := deref(owner).Underlying().(*types.Struct)
		if !ok {
			return "", false
		}
		owner = s.Field(i).Type()
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
