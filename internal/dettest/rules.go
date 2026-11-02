// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/build/constraint"
	"go/constant"
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
// host file f: DT002 (DET-110), DT003 (DET-120), DT004 (DET-130), DT005 (DET-140), DT007's math
// functions (DET-160) and DT008 (DET-170), then those of valueRules.
func (r *pkgRun) nodes(f *ast.File, info *types.Info) {
	add := r.addAt
	var stack []ast.Node           // n and the nodes that enclose it
	fmtArgs := map[ast.Expr]bool{} // arguments of the fmt calls DET-150 walks
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		r.conversions(n, stack, fmtArgs, info)
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
	r.valueRules(f, info)
}

// packageUse returns the rule and message of a use of obj when obj is a package-level object of
// time, fmt, math, sort or slices that core may not use, or the builtin print or println (DET-120,
// DET-130, DET-160, DET-170).
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
	case path == "math" && isFunc(obj) && !slices.Contains(mathExact, name):
		return RuleFloat, "math." + name + " may round differently on other platforms; core uses only math's exactly rounded functions", true
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
		if stdContainer(obj) && types.Identical(constraint, types.Universe.Lookup("any").Type()) {
			continue // slices and maps never convert an any-constrained element to an interface
		}
		if u, ok := constraint.Underlying().(*types.Interface); ok && u.IsComparable() || !allowedVia(arg, constraint) {
			r.addAt(id.Pos(), RuleClock, conversionMsg(arg, constraint))
		}
	}
}

// stdContainer reports whether obj is a generic function of package slices or maps.
func stdContainer(obj types.Object) bool {
	fn, ok := obj.(*types.Func)
	return ok && fn.Pkg() != nil && (fn.Pkg().Path() == "slices" || fn.Pkg().Path() == "maps")
}

// floatArg reports whether one of the type arguments args is a floating-point type (floatTerm).
func floatArg(args *types.TypeList) bool {
	for i := range args.Len() {
		if floatTerm(args.At(i)) {
			return true
		}
	}
	return false
}

// floatTerm reports whether t has float32 or float64 as its underlying type or, for a type
// parameter, whether a term of its type set has (DET-170).
func floatTerm(t types.Type) bool {
	switch u := t.Underlying().(type) {
	case *types.Basic:
		return u.Kind() == types.Float32 || u.Kind() == types.Float64
	case *types.Interface:
		for i := range u.NumEmbeddeds() {
			if floatTerm(u.EmbeddedType(i)) {
				return true
			}
		}
	case *types.Union:
		for i := range u.Len() {
			if floatTerm(u.Term(i).Type()) {
				return true
			}
		}
	}
	return false
}

// conversions reports DT003 for the conversions of values with forbidden time methods to
// interface types that the node n makes, explicitly or implicitly; stack holds n and the nodes
// that enclose it, and fmtArgs the arguments of the fmt calls seen so far whose operands DET-150
// walks (DET-120).
func (r *pkgRun) conversions(n ast.Node, stack []ast.Node, fmtArgs map[ast.Expr]bool, info *types.Info) {
	switch n := n.(type) {
	case *ast.CallExpr:
		if tv, ok := info.Types[n.Fun]; ok && tv.IsType() {
			switch {
			case len(n.Args) != 1:
			case !fmtArgs[n]:
				r.convert(n.Args[0], info.TypeOf(n.Args[0]), info.TypeOf(n))
			case isInterface(tv.Type):
				fmtArgs[ast.Unparen(n.Args[0])] = true // DET-150 walks through nested conversions to interfaces
			}
			return
		}
		if calledFmt(n, info) != nil {
			for _, arg := range n.Args {
				fmtArgs[ast.Unparen(arg)] = true
			}
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
	case *ast.RangeStmt:
		if n.Tok == token.ASSIGN {
			key, val := rangeTypes(typeOf(info, n.X))
			if n.Key != nil {
				r.convert(n.Key, key, info.TypeOf(n.Key))
			}
			if n.Value != nil {
				r.convert(n.Value, val, info.TypeOf(n.Value))
			}
		}
	case *ast.IndexExpr:
		if m, ok := coreType(typeOf(info, n.X)).(*types.Map); ok {
			r.convert(n.Index, info.TypeOf(n.Index), m.Key())
		}
	case *ast.SwitchStmt:
		if n.Tag == nil {
			return
		}
		tag := info.TypeOf(n.Tag)
		for _, stmt := range n.Body.List {
			if clause, ok := stmt.(*ast.CaseClause); ok {
				for _, e := range clause.List {
					r.convert(e, info.TypeOf(e), tag)
				}
			}
		}
	}
}

// rangeTypes returns the types of the iteration values a range over a value of type t gives, nil
// for one that it does not give.
func rangeTypes(t types.Type) (key, val types.Type) {
	switch u := coreType(t).(type) {
	case *types.Basic:
		if u.Info()&types.IsString != 0 {
			return types.Typ[types.Int], types.Universe.Lookup("rune").Type()
		}
		return t, nil
	case *types.Array:
		return types.Typ[types.Int], u.Elem()
	case *types.Pointer:
		if a, ok := u.Elem().Underlying().(*types.Array); ok {
			return types.Typ[types.Int], a.Elem()
		}
	case *types.Slice:
		return types.Typ[types.Int], u.Elem()
	case *types.Map:
		return u.Key(), u.Elem()
	case *types.Chan:
		return u.Elem(), nil
	case *types.Signature: // range over func(yield func(K, V) bool)
		if u.Params().Len() == 1 {
			if yield, ok := u.Params().At(0).Type().Underlying().(*types.Signature); ok {
				ps := yield.Params()
				if ps.Len() > 0 {
					key = ps.At(0).Type()
				}
				if ps.Len() > 1 {
					val = ps.At(1).Type()
				}
				return key, val
			}
		}
	}
	return nil, nil
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
// a method of time.Time that DET-120 does not allow. Interfaces and type parameters have none.
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

// forbiddenTimeMethod reports whether fn is a method of time.Time that DET-120 does not allow.
// The other methods of Duration read no zone or clock, so they do not count.
func forbiddenTimeMethod(fn *types.Func) bool {
	return fn.Pkg() != nil && fn.Pkg().Path() == "time" && fn.Signature().Recv() != nil &&
		recvName(fn) == "Time" && !slices.Contains(clockMethods, "Time."+fn.Name())
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

// isFunc reports whether obj is a function.
func isFunc(obj types.Object) bool {
	_, ok := obj.(*types.Func)
	return ok
}

// mathExact are the functions of package math that are exactly rounded on every platform
// (DET-160).
var mathExact = []string{"Abs", "Ceil", "Copysign", "FMA", "Float32bits", "Float32frombits", "Float64bits",
	"Float64frombits", "Floor", "Frexp", "Inf", "IsInf", "IsNaN", "Ldexp", "Max", "Min", "Mod", "Modf", "NaN",
	"Nextafter", "Nextafter32", "Remainder", "Round", "RoundToEven", "Signbit", "Sqrt", "Trunc"}

// fmtFormats are the functions of fmtWalked that take a format (DET-150 item 2).
var fmtFormats = []string{"Appendf", "Errorf", "Sprintf"}

// floatMulMsg is the DT007 message about a floating-point product (DET-160 items 2 and 3).
const floatMulMsg = "floating-point multiplication may be fused with an addition on some platforms; " +
	"convert the product explicitly, for example float64(a*b)"

// valueRules reports the findings of the host file f about values whose behavior depends on
// memory addresses (DT006, DET-150, and DT003 for a formatted time.Time), on the platform's
// floating-point code (DT007, DET-160), and on writes to package-level variables (DT009,
// DET-180).
func (r *pkgRun) valueRules(f *ast.File, info *types.Info) {
	write := func(pos token.Pos, e ast.Expr) {
		if v := packageVar(e, info); v != nil {
			r.addAt(pos, RuleGlobal, fmt.Sprintf("write to package-level variable %s.%s: core keeps no mutable package state",
				v.Pkg().Name(), v.Name()))
		}
	}
	complexLines := map[int]bool{} // lines that have a complex-type finding
	var stack []ast.Node           // the nodes that enclose the visited node
	funcs := 0                     // the function declarations and literals in stack
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			if isFuncNode(stack[len(stack)-1]) {
				funcs--
			}
			stack = stack[:len(stack)-1]
			return true
		}
		r.valueNode(n, stack, funcs > 0, info, write, complexLines)
		stack = append(stack, n)
		if isFuncNode(n) {
			funcs++
		}
		return true
	})
}

// isFuncNode reports whether n is a function declaration or literal.
func isFuncNode(n ast.Node) bool {
	switch n.(type) {
	case *ast.FuncDecl, *ast.FuncLit:
		return true
	}
	return false
}

// valueNode reports the findings of valueRules at the node n, whose ancestors are stack; inFunc
// says whether n is inside a function body, write reports a write to the root of an expression,
// and complexLines holds the lines that already have a complex-type finding.
func (r *pkgRun) valueNode(n ast.Node, stack []ast.Node, inFunc bool, info *types.Info, write func(token.Pos, ast.Expr),
	complexLines map[int]bool) {
	if e, ok := n.(ast.Expr); ok {
		// Inspect visits a line's expressions in source order, so the first one is leftmost.
		if t := info.TypeOf(e); t != nil && isBasic(t, types.IsComplex) {
			if line := r.c.fset.PositionFor(e.Pos(), false).Line; !complexLines[line] {
				complexLines[line] = true
				r.addAt(e.Pos(), RuleFloat, "complex type "+typeString(t)+" in core")
			}
		}
	}
	// Items 3 and 4 of DET-180 hold in package-level declarations too, the only other place where
	// an & or a method call or value can be written; items 1, 2 and 5 need a function body.
	switch n := n.(type) {
	case *ast.CallExpr:
		r.fmtCall(n, info)
		if sel, ok := ast.Unparen(n.Fun).(*ast.SelectorExpr); ok && writesThrough(info.Selections[sel]) {
			write(n.Pos(), sel.X)
		}
		if inFunc && len(n.Args) > 0 && mutatingBuiltin(n.Fun, info) {
			write(n.Pos(), n.Args[0])
		}
	case *ast.SelectorExpr:
		if writesThrough(info.Selections[n]) && !calledFun(n, stack) { // a method value
			write(n.Pos(), n.X)
		}
	case *ast.BinaryExpr:
		switch {
		case n.Op == token.EQL || n.Op == token.NEQ:
			for _, x := range []ast.Expr{n.X, n.Y} {
				if t := info.TypeOf(x); t != nil && containsTime(t) {
					r.addAt(n.Pos(), RulePointer, timeEqualMsg(t))
					break
				}
			}
		case n.Op == token.MUL && isFloatProduct(info.Types[n]) && !floatConversionArg(n, stack, info):
			r.addAt(n.Pos(), RuleFloat, floatMulMsg)
		}
	case *ast.SwitchStmt:
		if n.Tag != nil {
			if t := info.TypeOf(n.Tag); t != nil && containsTime(t) {
				r.addAt(n.Tag.Pos(), RulePointer, timeEqualMsg(t))
			}
		}
	case *ast.MapType:
		if t := info.TypeOf(n.Key); t != nil && containsTime(t) {
			r.addAt(n.Pos(), RulePointer, fmt.Sprintf("map key type %s contains time.Time, whose == compares time zone pointers; "+
				"key by an integer such as UnixNano", typeString(t)))
		}
	case *ast.Ident:
		if info.Uses[n] == types.Universe.Lookup("uintptr") {
			r.addAt(n.Pos(), RulePointer, "uintptr in core: no value may depend on a memory address")
		}
		r.fmtValue(n, stack, info)
	case *ast.AssignStmt:
		if n.Tok == token.MUL_ASSIGN && isFloat(info.TypeOf(n.Lhs[0])) {
			r.addAt(n.Pos(), RuleFloat, floatMulMsg)
		}
		if inFunc && n.Tok != token.DEFINE {
			for _, lhs := range n.Lhs {
				write(n.Pos(), lhs)
			}
		}
	case *ast.RangeStmt:
		if inFunc && n.Tok == token.ASSIGN {
			for _, lhs := range []ast.Expr{n.Key, n.Value} {
				if lhs != nil {
					write(n.Pos(), lhs)
				}
			}
		}
	case *ast.IncDecStmt:
		if inFunc {
			write(n.Pos(), n.X)
		}
	case *ast.UnaryExpr:
		if n.Op == token.AND {
			write(n.Pos(), n.X)
		}
	}
}

// mutatingBuiltin reports whether fun denotes the builtin delete, clear, copy or append, which
// write to their first argument (DET-180 item 5).
func mutatingBuiltin(fun ast.Expr, info *types.Info) bool {
	id, ok := ast.Unparen(fun).(*ast.Ident)
	if !ok {
		return false
	}
	b, ok := info.Uses[id].(*types.Builtin)
	return ok && slices.Contains([]string{"append", "clear", "copy", "delete"}, b.Name())
}

// isBasic reports whether t is a typed type whose underlying type is a basic type with the
// property flag (types.IsFloat: float32 or float64; types.IsComplex: complex64 or complex128).
func isBasic(t types.Type, flag types.BasicInfo) bool {
	if t == nil {
		return false
	}
	b, ok := t.Underlying().(*types.Basic)
	return ok && b.Info()&flag != 0 && b.Info()&types.IsUntyped == 0
}

// isFloat reports whether t is float (DET-160 item 2): its underlying type is float32 or
// float64, or it is a type parameter whose type set has a float32 or float64 term.
func isFloat(t types.Type) bool {
	if isBasic(t, types.IsFloat) {
		return true
	}
	tp, ok := types.Unalias(t).(*types.TypeParam)
	return ok && slices.ContainsFunc(constraintTerms(tp), func(t types.Type) bool { return isBasic(t, types.IsFloat) })
}

// onlyFloat reports whether a conversion to t rounds to float32 or float64 (DET-160 item 2): t's
// underlying type is float32 or float64, or t is a type parameter whose type set has only such
// terms.
func onlyFloat(t types.Type) bool {
	if isBasic(t, types.IsFloat) {
		return true
	}
	tp, ok := types.Unalias(t).(*types.TypeParam)
	if !ok {
		return false
	}
	terms := constraintTerms(tp)
	return len(terms) > 0 && !slices.ContainsFunc(terms, func(t types.Type) bool { return !isBasic(t, types.IsFloat) })
}

// constraintTerms returns the types of the terms of the type set of tp's constraint, through
// unions and embedded interfaces; none for a constraint without terms (any, comparable,
// method-only interfaces).
func constraintTerms(tp *types.TypeParam) []types.Type {
	var out []types.Type
	var walk func(t types.Type)
	walk = func(t types.Type) {
		switch u := t.(type) {
		case *types.Union:
			for i := range u.Len() {
				walk(u.Term(i).Type())
			}
		default:
			iface, ok := t.Underlying().(*types.Interface)
			if !ok {
				out = append(out, t)
				return
			}
			for i := range iface.NumEmbeddeds() {
				walk(iface.EmbeddedType(i))
			}
		}
	}
	walk(tp.Constraint())
	return out
}

// isFloatProduct reports whether a product with the type and value tv is computed at run time
// in a float type (DET-160 item 2). go/types records an untyped constant product such as
// math.Pi * 2 with the type it is converted to, so constants are left out by their value: the
// compiler computes them exactly.
func isFloatProduct(tv types.TypeAndValue) bool {
	return tv.Value == nil && isFloat(tv.Type)
}

// floatConversionArg reports whether the expression n, whose ancestors are stack, is, after
// skipping parentheses, the single argument of a conversion that rounds to float32 or float64
// (DET-160 item 2).
func floatConversionArg(n ast.Node, stack []ast.Node, info *types.Info) bool {
	child := n
	i := len(stack) - 1
	for ; i >= 0; i-- {
		p, ok := stack[i].(*ast.ParenExpr)
		if !ok {
			break
		}
		child = p
	}
	if i < 0 {
		return false
	}
	call, ok := stack[i].(*ast.CallExpr)
	if !ok || len(call.Args) != 1 || call.Args[0] != child {
		return false
	}
	tv := info.Types[call.Fun]
	return tv.IsType() && onlyFloat(tv.Type)
}

// isTime reports whether t is time.Time.
func isTime(t types.Type) bool {
	n, ok := types.Unalias(t).(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == "time" && n.Obj().Name() == "Time"
}

// containsTime reports whether t contains time.Time: it is time.Time, or a struct with a field,
// or an array with an element, whose type contains it (DET-150 item 3). A value cannot contain
// itself, so the walk ends.
func containsTime(t types.Type) bool {
	if isTime(t) {
		return true
	}
	switch u := t.Underlying().(type) {
	case *types.Struct:
		for i := range u.NumFields() {
			if containsTime(u.Field(i).Type()) {
				return true
			}
		}
	case *types.Array:
		return containsTime(u.Elem())
	}
	return false
}

// timeEqualMsg is the DT006 message about comparing values of type t, which contains time.Time,
// with == (DET-150 item 3).
func timeEqualMsg(t types.Type) string {
	return fmt.Sprintf("comparing %s with == compares time zone pointers and monotonic readings; "+
		"use Time.Equal or Time.Compare", typeString(t))
}

// fmtWalkedFunc returns the function of fmtWalked that the identifier id, or the selector whose
// name it is, denotes; nil when it denotes none.
func fmtWalkedFunc(id *ast.Ident, info *types.Info) *types.Func {
	fn, ok := info.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "fmt" || fn.Parent() != fn.Pkg().Scope() ||
		!slices.Contains(fmtWalked, fn.Name()) {
		return nil
	}
	return fn
}

// calledFmt returns the function of fmtWalked that call calls, or nil.
func calledFmt(call *ast.CallExpr, info *types.Info) *types.Func {
	switch fun := ast.Unparen(call.Fun).(type) {
	case *ast.Ident:
		return fmtWalkedFunc(fun, info)
	case *ast.SelectorExpr:
		return fmtWalkedFunc(fun.Sel, info)
	}
	return nil
}

// fmtValue reports a use of a function of fmtWalked, at its identifier id, that is not the
// function of a call: its later calls are hidden from fmtCall (DET-150 item 5).
func (r *pkgRun) fmtValue(id *ast.Ident, stack []ast.Node, info *types.Info) {
	fn := fmtWalkedFunc(id, info)
	if fn == nil {
		return
	}
	var e ast.Expr = id
	anc := stack
	if sel, ok := stack[len(stack)-1].(*ast.SelectorExpr); ok && sel.Sel == id {
		e, anc = sel, stack[:len(stack)-1]
	}
	if !calledFun(e, anc) {
		r.addAt(id.Pos(), RulePointer, "fmt."+fn.Name()+" used as a value: call it directly so its arguments can be checked")
	}
}

// fmtCall reports, for a call of a function of fmtWalked, a non-constant format or a %p verb in
// its format (DET-150 item 2) and each value argument that may print a memory address (DT006) or
// formats a time.Time (DT003) (DET-150 item 1).
func (r *pkgRun) fmtCall(call *ast.CallExpr, info *types.Info) {
	fn := calledFmt(call, info)
	if fn == nil {
		return
	}
	first := fn.Signature().Params().Len() - 1 // index of the variadic parameter
	vals := values(call.Args, info)
	multi := len(vals) != len(call.Args) // the arguments are the results of one call
	if slices.Contains(fmtFormats, fn.Name()) && first-1 < len(vals) {
		format := vals[first-1].expr
		tv := info.Types[format]
		switch {
		case multi || tv.Value == nil || tv.Value.Kind() != constant.String:
			r.addAt(format.Pos(), RulePointer, "non-constant format to fmt."+fn.Name()+" may hold %p; use a constant format")
		default:
			for range pVerbs(constant.StringVal(tv.Value)) {
				r.addAt(format.Pos(), RulePointer, "%p prints a memory address")
			}
		}
	}
	if call.Ellipsis.IsValid() || first > len(vals) {
		return
	}
	for _, v := range vals[first:] {
		at, t := v.expr, v.typ
		if !multi {
			at = interfaceOperand(at, info)
			t = info.TypeOf(at)
		}
		switch (fmtWalk{seen: map[types.Type]bool{}}).rule(t) {
		case RuleClock:
			r.addAt(at.Pos(), RuleClock, fmt.Sprintf("argument of type %s to fmt.%s formats a time.Time in the local time zone; "+
				"pass t.UnixMilli() or a Duration", typeString(t), fn.Name()))
		case RulePointer:
			r.addAt(at.Pos(), RulePointer, fmt.Sprintf("argument of type %s to fmt.%s may print a memory address",
				typeString(t), fn.Name()))
		}
	}
}

// interfaceOperand returns the operand of e when e is, after skipping parentheses, an explicit
// conversion to an interface type, repeatedly; and e otherwise (DET-150 item 1).
func interfaceOperand(e ast.Expr, info *types.Info) ast.Expr {
	for {
		call, ok := ast.Unparen(e).(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return e
		}
		if tv := info.Types[call.Fun]; !tv.IsType() || !isInterface(tv.Type) {
			return e
		}
		e = call.Args[0]
	}
}

// pVerbs returns the number of %p verbs in the fmt format string format (DET-150 item 2).
func pVerbs(format string) int {
	n := 0
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		i++
		if i < len(format) && format[i] == '%' {
			continue
		}
		for i < len(format) && strings.IndexByte("+-# 0123456789.*[]", format[i]) >= 0 {
			i++
		}
		if i < len(format) && format[i] == 'p' {
			n++
		}
	}
	return n
}

// printInterfaces returns the interfaces error and fmt.Stringer, against which the fmt walk tests
// argument types.
func printInterfaces() (errorT, stringer *types.Interface) {
	method := func(name string) *types.Func {
		result := types.NewTuple(types.NewVar(token.NoPos, nil, "", types.Typ[types.String]))
		return types.NewFunc(token.NoPos, nil, name, types.NewSignatureType(nil, nil, nil, nil, result, false))
	}
	errorT = types.NewInterfaceType([]*types.Func{method("Error")}, nil).Complete()
	stringer = types.NewInterfaceType([]*types.Func{method("String")}, nil).Complete()
	return errorT, stringer
}

// errorType and stringerType are the interfaces of printInterfaces.
var errorType, stringerType = printInterfaces()

// fmtWalk walks the type of a value argument of fmt (DET-150 item 1).
type fmtWalk struct {
	seen map[types.Type]bool // named types and type parameters visited
}

// rule returns RuleClock if formatting a value of type t formats a time.Time, RulePointer if
// it may print a memory address, and "" if it does neither. It stops at the first such type.
func (w fmtWalk) rule(t types.Type) Rule {
	switch t := types.Unalias(t).(type) {
	case *types.Named, *types.TypeParam:
		if w.seen[t] {
			return ""
		}
		w.seen[t] = true
	}
	if forbiddenTime(t) {
		return RuleClock
	}
	if types.Implements(t, errorType) || types.Implements(t, stringerType) {
		return ""
	}
	if tp, ok := types.Unalias(t).(*types.TypeParam); ok {
		terms := constraintTerms(tp)
		if len(terms) == 0 {
			return RulePointer
		}
		for _, term := range terms {
			if b, ok := term.Underlying().(*types.Basic); ok && b.Kind() == types.Uintptr {
				continue // core cannot name uintptr (DET-150 item 4), and cmp.Ordered lists it
			}
			if r := w.rule(term); r != "" {
				return r
			}
		}
		return ""
	}
	switch u := t.Underlying().(type) {
	case *types.Pointer, *types.Chan, *types.Signature:
		return RulePointer
	case *types.Basic:
		if u.Kind() == types.Uintptr || u.Kind() == types.UnsafePointer {
			return RulePointer
		}
	case *types.Struct:
		for i := range u.NumFields() {
			if r := w.rule(u.Field(i).Type()); r != "" {
				return r
			}
		}
	case *types.Array:
		return w.rule(u.Elem())
	case *types.Slice:
		return w.rule(u.Elem())
	case *types.Map:
		if r := w.rule(u.Key()); r != "" {
			return r
		}
		return w.rule(u.Elem())
	}
	return ""
}

// writesThrough reports whether calling the method that sel selects, or taking it as a method
// value, may write to the variable at the root of its operand x (DET-180 item 4): the method has
// a pointer receiver, and x is a pointer, or neither x nor an embedded field on the path to the
// method is a pointer (the implicit &; an embedded pointer is passed as it is).
func writesThrough(sel *types.Selection) bool {
	if sel == nil || sel.Kind() != types.MethodVal {
		return false
	}
	fn, ok := sel.Obj().(*types.Func)
	if !ok {
		return false
	}
	recv := fn.Signature().Recv()
	if recv == nil || !isPointer(recv.Type()) {
		return false
	}
	t := sel.Recv()
	if isPointer(t) {
		return true
	}
	idx := sel.Index()
	for _, i := range idx[:len(idx)-1] {
		s, ok := t.Underlying().(*types.Struct)
		if !ok {
			return false
		}
		t = s.Field(i).Type()
		if isPointer(t) {
			return false
		}
	}
	return true
}

// calledFun reports whether the expression n, whose ancestors are stack, is, after skipping
// parentheses, the function of a call.
func calledFun(n ast.Expr, stack []ast.Node) bool {
	i := len(stack) - 1
	for i >= 0 {
		if _, ok := stack[i].(*ast.ParenExpr); !ok {
			break
		}
		i--
	}
	if i < 0 {
		return false
	}
	call, ok := stack[i].(*ast.CallExpr)
	return ok && ast.Unparen(call.Fun) == n
}

// isPointer reports whether t's underlying type is a pointer.
func isPointer(t types.Type) bool {
	_, ok := t.Underlying().(*types.Pointer)
	return ok
}

// packageVar returns the package-level variable at the root of e, or nil if there is none. The
// root is found by stripping parentheses, field and method selections, index and slice
// expressions, type assertions and dereferences; the root of a qualified identifier pkg.V is V
// (DET-180).
func packageVar(e ast.Expr, info *types.Info) *types.Var {
	for {
		switch x := e.(type) {
		case *ast.ParenExpr:
			e = x.X
		case *ast.SelectorExpr:
			if info.Selections[x] == nil {
				e = x.Sel // a qualified identifier
			} else {
				e = x.X
			}
		case *ast.IndexExpr:
			e = x.X
		case *ast.SliceExpr:
			e = x.X
		case *ast.TypeAssertExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		case *ast.Ident:
			v, ok := info.Uses[x].(*types.Var)
			if !ok || v.Name() == "_" || v.Pkg() == nil || v.Parent() != v.Pkg().Scope() {
				return nil
			}
			return v
		default:
			return nil
		}
	}
}
