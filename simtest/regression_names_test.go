// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// regressionFile holds the regression tests.
const regressionFile = "regression_test.go"

// regressionFixture holds a regression test whose name does not match its call.
const regressionFixture = "testdata/regression_names/mismatch.go"

// regressionPrefix starts the name of every regression test.
const regressionPrefix = "TestRegression"

// regressionCall names one regression test's call: the row's test name, the rule and the seed.
type regressionCall struct {
	test string // the test function's name
	row  string // the row's test name, such as "TestFencedStoreOutage"
	rule string // the name of the rule's constant in package world, such as "Observer"
	seed uint64
}

// wantName returns the name the test of c must have: TestRegression, the row's name without
// "Test", the rule, and the first 8 of the 16 hex digits of the seed.
func (c regressionCall) wantName() string {
	return regressionPrefix + strings.TrimPrefix(c.row, "Test") + c.rule + fmt.Sprintf("%016x", c.seed)[:8]
}

// regressionNameErrors parses the Go source src of the file named file and returns a message per
// regression test whose name does not match its call (SIM-630). Every function whose name starts
// with TestRegression must hold exactly one statement, a call
// regression(t, "<row>", world.<Rule>, 0x<16 hex digits>, ...), whose row is in the scenario
// table and runs under the rule, and its name must be the one wantName gives. It also returns
// the calls it read, in file order.
func regressionNameErrors(file string, src []byte) ([]regressionCall, []string) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, []string{err.Error()}
	}
	var calls []regressionCall
	var errs []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, regressionPrefix) {
			continue
		}
		pos := fset.Position(fn.Pos())
		c, why := readRegressionCall(fn)
		if why != "" {
			errs = append(errs, fmt.Sprintf("%s:%d: %s: %s", pos.Filename, pos.Line, fn.Name.Name, why))
			continue
		}
		calls = append(calls, c)
		if want := c.wantName(); fn.Name.Name != want {
			errs = append(errs, fmt.Sprintf("%s:%d: %s runs %s, %s, seed 0x%016x: want the name %s",
				pos.Filename, pos.Line, fn.Name.Name, c.row, c.rule, c.seed, want))
		}
	}
	return calls, errs
}

// readRegressionCall returns the call of the regression test fn, or why fn does not have the
// form of one.
func readRegressionCall(fn *ast.FuncDecl) (regressionCall, string) {
	c := regressionCall{test: fn.Name.Name}
	if fn.Body == nil || len(fn.Body.List) != 1 {
		return c, "the body is not one call of regression"
	}
	stmt, ok := fn.Body.List[0].(*ast.ExprStmt)
	if !ok {
		return c, "the body is not one call of regression"
	}
	call, ok := stmt.X.(*ast.CallExpr)
	if !ok {
		return c, "the body is not one call of regression"
	}
	if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "regression" || len(call.Args) < 4 {
		return c, "the body is not one call of regression with a row, a rule and a seed"
	}
	lit, ok := call.Args[1].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return c, "the row is not a string literal"
	}
	name, err := strconv.Unquote(lit.Value)
	if err != nil {
		return c, "the row is not a string literal"
	}
	c.row = name
	sel, ok := call.Args[2].(*ast.SelectorExpr)
	if x, isIdent := sel.X.(*ast.Ident); !ok || !isIdent || x.Name != "world" {
		return c, "the rule is not world.Server or world.Observer"
	}
	c.rule = sel.Sel.Name
	rule, known := map[string]world.Takeover{"Server": world.Server, "Observer": world.Observer}[c.rule]
	if !known {
		return c, "the rule is not world.Server or world.Observer"
	}
	seed, ok := call.Args[3].(*ast.BasicLit)
	digits, hex := "", false
	if ok && seed.Kind == token.INT {
		digits, hex = strings.CutPrefix(seed.Value, "0x")
	}
	if !hex || len(digits) != 16 {
		return c, "the seed is not 0x and 16 hex digits"
	}
	if c.seed, err = strconv.ParseUint(digits, 16, 64); err != nil {
		return c, "the seed is not 0x and 16 hex digits"
	}
	i := slices.IndexFunc(rows, func(r row) bool { return r.name == c.row })
	if i < 0 {
		return c, fmt.Sprintf("no row named %q", c.row)
	}
	if !slices.Contains(rows[i].takeovers(), rule) {
		return c, fmt.Sprintf("row %s does not run under %v", c.row, rule)
	}
	return c, ""
}

// TestATSIM42RegressionNames checks AT-SIM-42: the name of every regression test in
// regression_test.go matches the row, the rule and the seed of its call, and a fixture whose
// test is named after another seed is reported, with the name it should have. The steps were
// written for this task from the issue and the design note, since the original
// acceptance-test text is lost.
func TestATSIM42RegressionNames(t *testing.T) {
	src, err := os.ReadFile(regressionFile)
	if err != nil {
		t.Fatal(err)
	}
	calls, errs := regressionNameErrors(regressionFile, src)
	for _, e := range errs {
		t.Error(e)
	}
	// The seeds recorded so far, each a test of its own.
	want := []string{
		"TestRegressionFencedStoreOutageObserver80101a0b",
		"TestRegressionFencedStoreOutageObserver0a2a20ac",
		"TestRegressionFencedStoreOutageObserver1733e008",
	}
	var got []string
	for _, c := range calls {
		got = append(got, c.test)
	}
	if !slices.Equal(got, want) {
		t.Errorf("regression tests %v, want %v", got, want)
	}

	src, err = os.ReadFile(regressionFixture)
	if err != nil {
		t.Fatal(err)
	}
	_, errs = regressionNameErrors(regressionFixture, src)
	wantErrs := []string{
		regressionFixture + ":20: TestRegressionFencedStoreOutageObserver80101a0c runs TestFencedStoreOutage, Observer, seed 0x0a2a20ac5ab89fe1: want the name TestRegressionFencedStoreOutageObserver0a2a20ac",
	}
	if !slices.Equal(errs, wantErrs) {
		t.Errorf("fixture:\n got %q\nwant %q", errs, wantErrs)
	}
}

// TestRegressionNameForms checks that regressionNameErrors reports each test that does not have
// the form of a regression test, and passes over other functions.
func TestRegressionNameForms(t *testing.T) {
	src := `package simtest

func TestRegressionA(t *testing.T) {}
func TestRegressionB(t *testing.T) { regression(t, "TestNoSuchRow", world.Server, 0x0000000000000001) }
func TestRegressionC(t *testing.T) { regression(t, "TestSteady", world.Lazy, 0x0000000000000001) }
func TestRegressionD(t *testing.T) { regression(t, "TestSteady", world.Server, 1) }
func TestRegressionE(t *testing.T) { regression(t, name, world.Server, 0x0000000000000001) }
func TestRegressionF(t *testing.T) { run(t, "TestSteady", world.Server, 0x0000000000000001) }
func TestRegressionG(t *testing.T) { regression(t, "TestSteady", world.Server, 0x0000000000000001); t.Log() }
func TestRegressionSteadyServer00000001(t *testing.T) { regression(t, "TestSteady", world.Server, 0x0000000000000001) }
func TestRegressionSteadyObserver00000000(t *testing.T) { regression(t, "TestSteady", world.Observer, 0x0000000000000001) }
func TestOther(t *testing.T) { t.Log() }
func helper() {}
`
	_, errs := regressionNameErrors("x.go", []byte(src))
	want := []string{
		"x.go:3: TestRegressionA: the body is not one call of regression",
		`x.go:4: TestRegressionB: no row named "TestNoSuchRow"`,
		"x.go:5: TestRegressionC: the rule is not world.Server or world.Observer",
		"x.go:6: TestRegressionD: the seed is not 0x and 16 hex digits",
		"x.go:7: TestRegressionE: the row is not a string literal",
		"x.go:8: TestRegressionF: the body is not one call of regression with a row, a rule and a seed",
		"x.go:9: TestRegressionG: the body is not one call of regression",
		"x.go:10: TestRegressionSteadyServer00000001 runs TestSteady, Server, seed 0x0000000000000001: want the name TestRegressionSteadyServer00000000",
	}
	if !slices.Equal(errs, want) {
		t.Errorf("errors:\n%s\nwant:\n%s", strings.Join(errs, "\n"), strings.Join(want, "\n"))
	}
}
