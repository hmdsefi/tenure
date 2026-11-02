// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// placeholder is the version of an in-repo requirement whose module has no tag yet (REP-040).
const placeholder = "v0.0.0-00010101000000-000000000000"

// k8sGoMod is AT-REP-02's go.mod of k8s.
const k8sGoMod = "module github.com/hmdsefi/tenure/k8s\n\ngo 1.26.0\n\n" +
	"require github.com/hmdsefi/tenure " + placeholder + "\n\n" +
	"replace github.com/hmdsefi/tenure => ..\n"

// cmdTenureGoMod is AT-REP-03's go.mod of cmd/tenure.
const cmdTenureGoMod = "module github.com/hmdsefi/tenure/cmd/tenure\n\ngo 1.26.0\n\n" +
	"require (\n\tgithub.com/hmdsefi/tenure v0.3.0\n\tgithub.com/hmdsefi/tenure/k8s v0.3.0\n)\n"

// TestNestedModules checks the nested modules of the repository: REP-030's table, REP-040's go.mod
// rules and REP-050's import rule (REP-060). With no nested module it passes.
func TestNestedModules(t *testing.T) {
	lines, err := checkNestedModules(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lines {
		t.Error(l)
	}
}

// runNestedCheck writes files over a temp tree holding REP-010's go.mod and AT-REP-02's k8s
// module, runs checkNestedModules on it and compares the lines.
func runNestedCheck(t *testing.T, files map[string]string, want []string) {
	t.Helper()
	tree := map[string]string{"go.mod": rootGoMod, "k8s/go.mod": k8sGoMod}
	maps.Copy(tree, files)
	root := t.TempDir()
	writeFiles(t, root, tree)
	got, err := checkNestedModules(root)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("checkNestedModules:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestATREP02NestedModuleTable runs the check on AT-REP-02's temp tree and its variants.
func TestATREP02NestedModuleTable(t *testing.T) {
	const versionLine = "k8s/go.mod: requires github.com/hmdsefi/tenure %s; want the placeholder " +
		placeholder + " or a tag version vX.Y.Z"
	k8s := func(old, replacement string) map[string]string {
		return map[string]string{"k8s/go.mod": strings.Replace(k8sGoMod, old, replacement, 1)}
	}
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{name: "valid"},
		{name: "tag version", files: k8s(placeholder, "v0.1.0")},
		// go mod edit -json prints the canonical form of v0.1, which is v0.1.0.
		{name: "v0.1", files: k8s(placeholder, "v0.1")},
		{
			name:  "pre-release version",
			files: k8s(placeholder, "v0.1.0-rc.1"),
			want:  []string{strings.Replace(versionLine, "%s", "v0.1.0-rc.1", 1)},
		},
		{
			name:  "pseudo-version",
			files: k8s(placeholder, "v0.0.0-20261101000000-0123456789ab"),
			want:  []string{strings.Replace(versionLine, "%s", "v0.0.0-20261101000000-0123456789ab", 1)},
		},
		{
			name:  "missing replace",
			files: k8s("\nreplace github.com/hmdsefi/tenure => ..\n", ""),
			want:  []string{"k8s/go.mod: requires github.com/hmdsefi/tenure without replace github.com/hmdsefi/tenure => .."},
		},
		{
			name:  "wrong replace",
			files: k8s("=> ..", "=> ../.."),
			want:  []string{"k8s/go.mod: replace github.com/hmdsefi/tenure => ../.., want .."},
		},
		{
			name:  "go 1.25",
			files: k8s("go 1.26.0", "go 1.25"),
			want:  []string{"k8s/go.mod: go 1.25, want 1.26.0"},
		},
		{
			name:  "toolchain",
			files: k8s("go 1.26.0\n", "go 1.26.0\n\ntoolchain go1.27.1\n"),
			want:  []string{"k8s/go.mod: unexpected Toolchain directive"},
		},
		{
			name: "etcd requirement",
			files: map[string]string{"k8s/go.mod": k8sGoMod +
				"\nrequire github.com/hmdsefi/tenure/etcd " + placeholder + "\n\n" +
				"replace github.com/hmdsefi/tenure/etcd => ../etcd\n"},
			want: []string{"k8s/go.mod: requires github.com/hmdsefi/tenure/etcd, which k8s may not require"},
		},
		{
			name:  "extra",
			files: map[string]string{"extra/go.mod": "module example.com/extra\n\ngo 1.25\n"},
			want:  []string{"extra/go.mod: extra is not a nested module of the repository layout"},
		},
		{
			name: "metrics/prom",
			files: map[string]string{"metrics/prom/go.mod": "module github.com/hmdsefi/tenure/metrics/prom\n\ngo 1.26.0\n\n" +
				"require github.com/hmdsefi/tenure " + placeholder + "\n\n" +
				"replace github.com/hmdsefi/tenure => ../..\n"},
		},
		{
			name: "examples requires k8s",
			files: map[string]string{"examples/go.mod": "module github.com/hmdsefi/tenure/examples\n\ngo 1.26.0\n\n" +
				"require (\n\tgithub.com/hmdsefi/tenure " + placeholder + "\n\tgithub.com/hmdsefi/tenure/k8s " + placeholder +
				"\n\texample.com/third v1.2.3\n)\n\n" +
				"replace (\n\tgithub.com/hmdsefi/tenure => ..\n\tgithub.com/hmdsefi/tenure/k8s => ../k8s\n)\n"},
		},
		{
			name:  "module path",
			files: k8s("tenure/k8s", "tenure/kubernetes"),
			want:  []string{"k8s/go.mod: module github.com/hmdsefi/tenure/kubernetes, want github.com/hmdsefi/tenure/k8s"},
		},
		{
			name:  "replace with an old version",
			files: k8s("replace github.com/hmdsefi/tenure =>", "replace github.com/hmdsefi/tenure "+placeholder+" =>"),
			want:  []string{"k8s/go.mod: replace github.com/hmdsefi/tenure " + placeholder + " => .., want .."},
		},
		{
			name:  "replace with a new version",
			files: k8s("=> ..", "=> example.com/fork v1.0.0"),
			want:  []string{"k8s/go.mod: replace github.com/hmdsefi/tenure => example.com/fork v1.0.0, want .."},
		},
		{
			name:  "replace of something else",
			files: map[string]string{"k8s/go.mod": k8sGoMod + "\nreplace example.com/third => ../third\n"},
			want:  []string{"k8s/go.mod: replace example.com/third: only in-repo requirements are replaced"},
		},
		{
			name: "every violation in order",
			files: map[string]string{
				"simtest/go.mod": "module example.com/simtest\n\ngo 1.25\n\ntoolchain go1.27.1\n\n" +
					"godebug default=go1.21\n\n" +
					"require (\n\tgithub.com/hmdsefi/tenure v0.1.0-rc.1\n\tgithub.com/hmdsefi/tenure/k8s " + placeholder +
					"\n\tgithub.com/hmdsefi/tenure/bench " + placeholder + "\n\texample.com/third v1.2.3\n)\n\n" +
					"exclude example.com/third v1.2.2\n\nretract v0.0.1\n\ntool example.com/third/cmd/x\n\n" +
					"ignore ./node_modules\n\n" +
					"replace (\n\texample.com/third => ../third\n\tgithub.com/hmdsefi/tenure => ./root\n)\n",
				"testdata/go.mod": "module example.com/fixture\n",
				"zz/go.mod":       "module example.com/zz\n",
			},
			want: []string{
				"simtest/go.mod: module example.com/simtest, want github.com/hmdsefi/tenure/simtest",
				"simtest/go.mod: go 1.25, want 1.26.0",
				"simtest/go.mod: unexpected Toolchain directive",
				"simtest/go.mod: unexpected Exclude directive",
				"simtest/go.mod: unexpected Retract directive",
				"simtest/go.mod: unexpected Tool directive",
				"simtest/go.mod: unexpected GoDebug directive",
				"simtest/go.mod: unexpected Ignore directive",
				"simtest/go.mod: requires github.com/hmdsefi/tenure v0.1.0-rc.1; want the placeholder " + placeholder + " or a tag version vX.Y.Z",
				"simtest/go.mod: replace github.com/hmdsefi/tenure => ./root, want ..",
				"simtest/go.mod: requires github.com/hmdsefi/tenure/k8s without replace github.com/hmdsefi/tenure/k8s => ../k8s",
				"simtest/go.mod: requires github.com/hmdsefi/tenure/bench, which simtest may not require",
				"simtest/go.mod: replace example.com/third: only in-repo requirements are replaced",
				"testdata/go.mod: go.mod under testdata",
				"zz/go.mod: zz is not a nested module of the repository layout",
			},
		},
		{
			name: "skipped directories",
			files: map[string]string{
				".hidden/go.mod": "module example.com/hidden\n",
				"_old/go.mod":    "module example.com/old\n",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runNestedCheck(t, tc.files, tc.want) })
	}
}

// TestATREP03CmdTenure runs the check on AT-REP-03's cmd/tenure module.
func TestATREP03CmdTenure(t *testing.T) {
	tests := []struct {
		name  string
		gomod string
		want  []string
	}{
		{name: "tags without replace", gomod: cmdTenureGoMod},
		{
			name:  "replace",
			gomod: cmdTenureGoMod + "\nreplace github.com/hmdsefi/tenure => ../..\n",
			want:  []string{"cmd/tenure/go.mod: replace github.com/hmdsefi/tenure: cmd/tenure has no replace directives, go install rejects them"},
		},
		{
			name:  "placeholder",
			gomod: strings.Replace(cmdTenureGoMod, "tenure v0.3.0", "tenure "+placeholder, 1),
			want:  []string{"cmd/tenure/go.mod: requires github.com/hmdsefi/tenure at the placeholder version; cmd/tenure requires tags only"},
		},
		{
			name: "placeholder and replace",
			gomod: strings.Replace(cmdTenureGoMod, "tenure v0.3.0", "tenure "+placeholder, 1) +
				"\nreplace github.com/hmdsefi/tenure => ../..\n",
			want: []string{
				"cmd/tenure/go.mod: replace github.com/hmdsefi/tenure: cmd/tenure has no replace directives, go install rejects them",
				"cmd/tenure/go.mod: requires github.com/hmdsefi/tenure at the placeholder version; cmd/tenure requires tags only",
			},
		},
		{
			name: "pseudo-version and placeholder",
			gomod: strings.NewReplacer("tenure v0.3.0", "tenure v0.0.0-20261101000000-0123456789ab",
				"k8s v0.3.0", "k8s "+placeholder).Replace(cmdTenureGoMod),
			want: []string{
				"cmd/tenure/go.mod: requires github.com/hmdsefi/tenure v0.0.0-20261101000000-0123456789ab; want the placeholder " +
					placeholder + " or a tag version vX.Y.Z",
				"cmd/tenure/go.mod: requires github.com/hmdsefi/tenure/k8s at the placeholder version; cmd/tenure requires tags only",
			},
		},
		{
			name: "replace of a requirement not allowed",
			gomod: cmdTenureGoMod + "\nrequire github.com/hmdsefi/tenure/simtest v0.3.0\n\n" +
				"replace github.com/hmdsefi/tenure/simtest => ../../simtest\n",
			want: []string{
				"cmd/tenure/go.mod: requires github.com/hmdsefi/tenure/simtest, which cmd/tenure may not require",
				"cmd/tenure/go.mod: replace github.com/hmdsefi/tenure/simtest: cmd/tenure has no replace directives, go install rejects them",
			},
		},
		{
			name:  "replace of something else",
			gomod: cmdTenureGoMod + "\nreplace example.com/third => ../third\n",
			want:  []string{"cmd/tenure/go.mod: replace example.com/third: cmd/tenure has no replace directives, go install rejects them"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runNestedCheck(t, map[string]string{"cmd/tenure/go.mod": tc.gomod}, tc.want)
		})
	}
}

// TestATREP04NestedImports runs the import check on AT-REP-04's temp tree.
func TestATREP04NestedImports(t *testing.T) {
	const dettestImport = "package p\n\nimport _ \"github.com/hmdsefi/tenure/internal/dettest\"\n"
	simtestGoMod := "module github.com/hmdsefi/tenure/simtest\n\ngo 1.26.0\n\n" +
		"require github.com/hmdsefi/tenure " + placeholder + "\n\n" +
		"replace github.com/hmdsefi/tenure => ..\n"
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name:  "k8s imports internal/dettest",
			files: map[string]string{"k8s/k8s_test.go": dettestImport},
			want:  []string{"k8s/k8s_test.go: imports github.com/hmdsefi/tenure/internal/dettest; nested modules import public packages only"},
		},
		{
			name:  "simtest imports internal/dettest",
			files: map[string]string{"simtest/go.mod": simtestGoMod, "simtest/golden_test.go": dettestImport},
		},
		{
			name:  "go.mod under testdata",
			files: map[string]string{"core/testdata/m/go.mod": "module example.com/m\n"},
			want:  []string{"core/testdata/m/go.mod: go.mod under testdata"},
		},
		{
			name: "walk order",
			files: map[string]string{
				"core/c.go":                 "package core\n\nimport _ \"github.com/hmdsefi/tenure/internal/dettest\"\n",
				"k8s/a.go":                  "package k8s\n\nimport (\n\t_ \"github.com/hmdsefi/tenure/internal\"\n\t_ \"github.com/hmdsefi/tenure/core\"\n\t_ \"github.com/hmdsefi/tenure/internals\"\n\t_ \"github.com/hmdsefi/tenure/internal/x/y\"\n)\n",
				"k8s/sub/b.go":              dettestImport,
				"k8s/testdata/t.go":         dettestImport,
				"k8s/zz/go.mod":             "module example.com/zz\n",
				"k8s/zz/z.go":               dettestImport,
				"simtest/go.mod":            simtestGoMod,
				"simtest/s.go":              "package simtest\n\nimport (\n\t_ \"github.com/hmdsefi/tenure/internal/dettest/sub\"\n\t_ \"github.com/hmdsefi/tenure/internal/machinecases\"\n)\n",
				"simtest/testdata/m/go.mod": "module example.com/m\n",
			},
			want: []string{
				"k8s/a.go: imports github.com/hmdsefi/tenure/internal; nested modules import public packages only",
				"k8s/a.go: imports github.com/hmdsefi/tenure/internal/x/y; nested modules import public packages only",
				"k8s/sub/b.go: imports github.com/hmdsefi/tenure/internal/dettest; nested modules import public packages only",
				"k8s/zz/go.mod: k8s/zz is not a nested module of the repository layout",
				"simtest/s.go: imports github.com/hmdsefi/tenure/internal/machinecases; nested modules import public packages only",
				"simtest/testdata/m/go.mod: go.mod under testdata",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { runNestedCheck(t, tc.files, tc.want) })
	}
}

// TestNestedGoModNullKeys checks that a go mod edit -json key whose value is null counts as absent
// (REP-060): Go 1.26 prints the directives a go.mod does not have as null keys, Go 1.27 omits them.
func TestNestedGoModNullKeys(t *testing.T) {
	const valid = `{
	"Module": {"Path": "github.com/hmdsefi/tenure/k8s"},
	"Go": "1.26.0",
	"Toolchain": null,
	"GoDebug": null,
	"Require": [{"Path": "github.com/hmdsefi/tenure", "Version": "` + placeholder + `"}],
	"Exclude": null,
	"Replace": [{"Old": {"Path": "github.com/hmdsefi/tenure"}, "New": {"Path": ".."}}],
	"Retract": null,
	"Tool": null,
	"Ignore": null
}`
	k8sRow := nestedModules[slices.IndexFunc(nestedModules, func(m nestedModule) bool { return m.dir == "k8s" })]
	got, err := checkNestedGoMod(k8sRow, []byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("checkNestedGoMod with null keys = %q, want no lines", got)
	}

	const nulls = `{"Module": {"Path": "github.com/hmdsefi/tenure/k8s"}, "Go": "1.26.0",
	"Require": null, "Exclude": null, "Replace": null, "Retract": null, "Tool": null, "Ignore": null}`
	got, err = checkNestedGoMod(k8sRow, []byte(nulls))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("checkNestedGoMod with no requirements = %q, want no lines", got)
	}

	const set = `{"Module": {"Path": "github.com/hmdsefi/tenure/k8s"}, "Go": "1.26.0",
	"Ignore": [{"Path": "./x"}], "Exclude": [], "Tool": null}`
	got, err = checkNestedGoMod(k8sRow, []byte(set))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"k8s/go.mod: unexpected Exclude directive", "k8s/go.mod: unexpected Ignore directive"}
	if !slices.Equal(got, want) {
		t.Errorf("checkNestedGoMod = %q, want %q", got, want)
	}
}

// TestNestedModulesSymlinkedRoot runs the check through a symlink to the temp tree: the walk
// resolves the root first, so a bad nested go.mod is still reported.
func TestNestedModulesSymlinkedRoot(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	writeFiles(t, target, map[string]string{
		"go.mod":     rootGoMod,
		"k8s/go.mod": strings.Replace(k8sGoMod, "go 1.26.0", "go 1.25", 1),
	})
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	got, err := checkNestedModules(link)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"k8s/go.mod: go 1.25, want 1.26.0"}
	if !slices.Equal(got, want) {
		t.Errorf("checkNestedModules through a symlink = %q, want %q", got, want)
	}
}

// nestedModule is one row of REP-030's table.
type nestedModule struct {
	dir      string
	requires []string // in-repo modules it may require, as directories; "" is the root
	tagsOnly bool     // cmd/tenure: no replace directives and no placeholder versions (REP-040 item 5)
}

// anyInRepo is what simtest, examples, bench and cmd/tenure may require: the root, the adapters
// and the metrics modules.
var anyInRepo = []string{"", "k8s", "postgres", "etcd", "gossip", "metrics/prom", "metrics/otel"}

// nestedModules is REP-030's table.
var nestedModules = []nestedModule{
	{dir: "k8s", requires: []string{""}},
	{dir: "simtest", requires: anyInRepo},
	{dir: "examples", requires: anyInRepo},
	{dir: "postgres", requires: []string{""}},
	{dir: "etcd", requires: []string{""}},
	{dir: "gossip", requires: []string{""}},
	{dir: "bench", requires: anyInRepo},
	{dir: "cmd/tenure", requires: anyInRepo, tagsOnly: true},
	{dir: "metrics/prom", requires: []string{""}},
	{dir: "metrics/otel", requires: []string{""}},
}

// forbiddenDirectives are the go mod edit -json keys of the directives a nested go.mod must not
// have (REP-040 item 2).
var forbiddenDirectives = []string{"Toolchain", "Exclude", "Retract", "Tool", "GoDebug", "Ignore"}

// tagVersion matches a tag version vX.Y.Z (REP-040 item 3).
var tagVersion = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

// checkNestedModules returns one line per way in which the nested modules below root break
// REP-030, REP-040 and REP-050, in walk order (REP-060). The walk skips directories whose name
// starts with "." or "_". A directory with a go.mod that is not in REP-030's table gets one line,
// and neither its go.mod nor its files are checked further.
func checkNestedModules(root string) ([]string, error) {
	root, err := resolveRoot(root)
	if err != nil {
		return nil, err
	}
	type module struct {
		dir     string // slash-separated, relative to root
		checked bool   // in REP-030's table, so its files are checked
	}
	var (
		lines []string
		stack []module // the nested modules that enclose the current path, innermost last
		fset  = token.NewFileSet()
	)
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		for len(stack) > 0 && !strings.HasPrefix(rel, stack[len(stack)-1].dir+"/") {
			stack = stack[:len(stack)-1]
		}
		inTestdata := slices.Contains(strings.Split(rel, "/"), "testdata")

		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_") {
				return filepath.SkipDir
			}
			switch fi, err := os.Stat(filepath.Join(path, "go.mod")); {
			case errors.Is(err, fs.ErrNotExist) || (err == nil && fi.IsDir()):
				return nil
			case err != nil:
				return err
			}
			if inTestdata {
				lines = append(lines, rel+"/go.mod: go.mod under testdata")
				return nil
			}
			i := slices.IndexFunc(nestedModules, func(m nestedModule) bool { return m.dir == rel })
			if i < 0 {
				lines = append(lines, fmt.Sprintf("%s/go.mod: %s is not a nested module of the repository layout", rel, rel))
				stack = append(stack, module{dir: rel})
				return nil
			}
			out, err := goModEditJSON(path, "go.mod")
			if err != nil {
				return err
			}
			found, err := checkNestedGoMod(nestedModules[i], out)
			if err != nil {
				return err
			}
			lines = append(lines, found...)
			stack = append(stack, module{dir: rel, checked: true})
			return nil
		}

		if len(stack) == 0 || !stack[len(stack)-1].checked || inTestdata || !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		mod := stack[len(stack)-1].dir
		for _, spec := range f.Imports {
			p, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return fmt.Errorf("%s: %w", rel, err)
			}
			if internalImport(mod, p) {
				lines = append(lines, fmt.Sprintf("%s: imports %s; nested modules import public packages only", rel, p))
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("nested modules: walk %s: %w", root, err)
	}
	return lines, nil
}

// internalImport reports whether a file of the nested module in dir may not import path because
// it is github.com/hmdsefi/tenure/internal or below it (REP-050). simtest may import
// internal/dettest and the packages below it.
func internalImport(dir, path string) bool {
	under := func(p string) bool { return path == p || strings.HasPrefix(path, p+"/") }
	if dir == "simtest" && under(tenureModule+"/internal/dettest") {
		return false
	}
	return under(tenureModule + "/internal")
}

// goModReplace is one Replace entry of go mod edit -json.
type goModReplace struct {
	Old, New struct{ Path, Version string }
}

// checkNestedGoMod returns one line per way in which the go.mod of the nested module of row
// breaks REP-040, in REP-060's order. data is the output of go mod edit -json on it; a key whose
// value is null counts as absent.
func checkNestedGoMod(row nestedModule, data []byte) ([]string, error) {
	dir := row.dir
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		return nil, fmt.Errorf("go mod edit -json %s/go.mod: %w", dir, err)
	}
	var mf struct {
		Module  struct{ Path string }
		Go      string
		Require []struct{ Path, Version string }
		Replace []goModReplace
	}
	if err := json.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("go mod edit -json %s/go.mod: %w", dir, err)
	}

	prefix := dir + "/go.mod: "
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, prefix+fmt.Sprintf(format, args...)) }
	if want := tenureModule + "/" + dir; mf.Module.Path != want {
		add("module %s, want %s", mf.Module.Path, want)
	}
	if mf.Go != "1.26.0" {
		add("go %s, want 1.26.0", mf.Go)
	}
	for _, k := range forbiddenDirectives {
		if v, ok := keys[k]; ok && string(v) != "null" {
			add("unexpected %s directive", k)
		}
	}

	inRepo := map[string]bool{} // in-repo requirements, whose replace lines are checked with them
	for _, req := range mf.Require {
		reqDir, ok := inRepoDir(req.Path)
		if !ok {
			continue
		}
		inRepo[req.Path] = true
		allowed := slices.Contains(row.requires, reqDir)
		if !allowed {
			add("requires %s, which %s may not require", req.Path, dir)
		} else if req.Version != placeholder && !tagVersion.MatchString(req.Version) {
			add("requires %s %s; want the placeholder %s or a tag version vX.Y.Z", req.Path, req.Version, placeholder)
		}
		if row.tagsOnly {
			// Every replace is reported in cmd/tenure, a not-allowed requirement's too.
			for _, r := range mf.Replace {
				if r.Old.Path == req.Path {
					add("replace %s: cmd/tenure has no replace directives, go install rejects them", r.Old.Path)
				}
			}
			if allowed && req.Version == placeholder {
				add("requires %s at the placeholder version; cmd/tenure requires tags only", req.Path)
			}
			continue
		}
		if !allowed {
			continue
		}
		rel, err := filepath.Rel(filepath.FromSlash(dir), filepath.FromSlash(cmp.Or(reqDir, ".")))
		if err != nil {
			return nil, err
		}
		rel = filepath.ToSlash(rel)
		replaced := false
		for _, r := range mf.Replace {
			if r.Old.Path != req.Path {
				continue
			}
			replaced = true
			if r.Old.Version != "" || r.New.Version != "" || r.New.Path != rel {
				add("replace %s => %s, want %s", joinVersion(r.Old.Path, r.Old.Version), joinVersion(r.New.Path, r.New.Version), rel)
			}
		}
		if !replaced {
			add("requires %s without replace %s => %s", req.Path, req.Path, rel)
		}
	}
	for _, r := range mf.Replace {
		switch {
		case inRepo[r.Old.Path]:
		case row.tagsOnly:
			add("replace %s: cmd/tenure has no replace directives, go install rejects them", r.Old.Path)
		default:
			add("replace %s: only in-repo requirements are replaced", r.Old.Path)
		}
	}
	return lines, nil
}

// inRepoDir returns the directory, relative to the root, of the in-repo module path names: "" for
// the root module, the rest of the path for a path below it.
func inRepoDir(path string) (string, bool) {
	if path == tenureModule {
		return "", true
	}
	return strings.CutPrefix(path, tenureModule+"/")
}

// joinVersion returns path, followed by a space and version if version is set.
func joinVersion(path, version string) string {
	if version == "" {
		return path
	}
	return path + " " + version
}
