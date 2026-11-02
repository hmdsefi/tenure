// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const tenureModule = "github.com/hmdsefi/tenure"

// rootGoMod is REP-010's text of the root go.mod.
const rootGoMod = "module github.com/hmdsefi/tenure\n\ngo 1.26.0\n"

// licenseHeader is the start of every .go file (REP-110).
const licenseHeader = "// Copyright 2026 Hamed Yousefi\n// SPDX-License-Identifier: MPL-2.0\n\n"

// repoRoot returns the root of the checkout that runs the test.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := FindRoot(".", tenureModule)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// TestRootGoMod checks the root go.mod and go.sum (REP-010, REP-015).
func TestRootGoMod(t *testing.T) {
	lines, err := checkRootGoMod(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lines {
		t.Error(l)
	}
}

// TestATREP01RootGoMod runs the go.mod check on temp copies of REP-010's go.mod (AT-REP-01).
func TestATREP01RootGoMod(t *testing.T) {
	const keyLine = "go.mod: unexpected %s; the root module has no requirements, replacements, exclusions, retractions, tools, toolchain or godebug lines"
	tests := []struct {
		name  string
		gomod string
		gosum bool
		want  []string
	}{
		{name: "unchanged", gomod: rootGoMod},
		{
			name:  "require",
			gomod: rootGoMod + "\nrequire golang.org/x/tools v0.30.0\n",
			want:  []string{fmt.Sprintf(keyLine, "Require")},
		},
		{
			name:  "go 1.27.0",
			gomod: strings.Replace(rootGoMod, "go 1.26.0", "go 1.27.0", 1),
			want:  []string{"go.mod: go 1.27.0, want 1.26.0"},
		},
		{
			name:  "toolchain",
			gomod: rootGoMod + "\ntoolchain go1.27.1\n",
			want:  []string{fmt.Sprintf(keyLine, "Toolchain")},
		},
		{
			name:  "go.sum",
			gomod: rootGoMod,
			gosum: true,
			want:  []string{"go.sum: the root module has no requirements, so it has no go.sum"},
		},
		{
			name:  "module path",
			gomod: strings.Replace(rootGoMod, tenureModule, "example.com/tenure", 1),
			want:  []string{"go.mod: module example.com/tenure, want github.com/hmdsefi/tenure"},
		},
		{
			name: "every violation in order",
			gomod: "module example.com/tenure\n\ngo 1.27.0\n\ntoolchain go1.27.1\n\n" +
				"require golang.org/x/tools v0.30.0\n\nreplace golang.org/x/tools => ../tools\n",
			gosum: true,
			want: []string{
				fmt.Sprintf(keyLine, "Toolchain"),
				fmt.Sprintf(keyLine, "Require"),
				fmt.Sprintf(keyLine, "Replace"),
				"go.mod: module example.com/tenure, want github.com/hmdsefi/tenure",
				"go.mod: go 1.27.0, want 1.26.0",
				"go.sum: the root module has no requirements, so it has no go.sum",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			files := map[string]string{"go.mod": tc.gomod}
			if tc.gosum {
				files["go.sum"] = ""
			}
			writeFiles(t, dir, files)
			got, err := checkRootGoMod(dir)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("checkRootGoMod:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(tc.want, "\n  "))
			}
		})
	}
}

// TestLicenseHeaders checks that every .go file starts with the license header (REP-110).
func TestLicenseHeaders(t *testing.T) {
	paths, err := checkLicenseHeaders(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) > 0 {
		t.Error(licenseHeaderReport(paths))
	}
}

// licenseHeaderReport is TestLicenseHeaders' failure message for paths (REP-110).
func licenseHeaderReport(paths []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d file(s) do not start with the license header:\n", len(paths))
	for _, p := range paths {
		fmt.Fprintf(&b, "  %s\n", p)
	}
	b.WriteString("start each one with these two lines and a blank line:\n" +
		"  // Copyright 2026 Hamed Yousefi\n" +
		"  // SPDX-License-Identifier: MPL-2.0")
	return b.String()
}

// TestATREP05Headers runs the header check on a temp tree (AT-REP-05).
func TestATREP05Headers(t *testing.T) {
	root := t.TempDir()
	const body = "package p\n"
	writeFiles(t, root, map[string]string{
		"a.go":          licenseHeader + body,
		"b.go":          body,
		"c.go":          "// SPDX-License-Identifier: MPL-2.0\n// Copyright 2026 Hamed Yousefi\n\n" + body,
		"d.go":          "// Copyright 2026 Someone Else\n// SPDX-License-Identifier: MPL-2.0\n\n" + body,
		"e.go":          strings.TrimSuffix(licenseHeader, "\n") + body,
		"f.go":          "// Copyright 2025 Hamed Yousefi\n// SPDX-License-Identifier: MPL-2.0\n\n" + body,
		"g.go":          strings.Replace(licenseHeader, "\n", "\r\n", 1) + body,
		"h.go":          "\ufeff" + licenseHeader + body,
		"notes.txt":     "no header\n",
		"testdata/x.go": body,
		".hidden/y.go":  body,
		"_old/z.go":     body,
		"k8s/go.mod":    "module github.com/hmdsefi/tenure/k8s\n\ngo 1.26.0\n",
		"k8s/w.go":      body,
	})
	// A symlink is not a regular file: the walk skips it instead of failing to read it.
	if err := os.Symlink("missing.go", filepath.Join(root, "dangling.go")); err != nil {
		t.Fatal(err)
	}
	got, err := checkLicenseHeaders(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b.go", "c.go", "d.go", "e.go", "g.go", "h.go", "k8s/w.go"}
	if !slices.Equal(got, want) {
		t.Errorf("checkLicenseHeaders = %q, want %q", got, want)
	}

	const wantReport = `7 file(s) do not start with the license header:
  b.go
  c.go
  d.go
  e.go
  g.go
  h.go
  k8s/w.go
start each one with these two lines and a blank line:
  // Copyright 2026 Hamed Yousefi
  // SPDX-License-Identifier: MPL-2.0`
	if r := licenseHeaderReport(got); r != wantReport {
		t.Errorf("licenseHeaderReport:\n%s\nwant:\n%s", r, wantReport)
	}
}

// TestRootImports checks the imports of the root module's packages against REP-080's table.
func TestRootImports(t *testing.T) {
	lines, err := checkRootImports(repoRoot(t), tenureModule)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lines {
		t.Error(l)
	}
}

// TestATREP06RootImports runs the import check on a temp module (AT-REP-06).
func TestATREP06RootImports(t *testing.T) {
	root := t.TempDir()
	writeFiles(t, root, map[string]string{
		"go.mod": rootGoMod,
		"x.go": "package tenure\n\nimport (\n\t\"fmt\"\n\t\"github.com/hmdsefi/tenure/memstore\"\n" +
			"\t\"github.com/hmdsefi/tenure/core\"\n)\n",
		"x_test.go":             "package tenure\n\nimport (\n\t\"testing\"\n\t\"github.com/hmdsefi/tenure/extra\"\n\t\"example.com/x\"\n)\n",
		"memstore/m.go":         "package memstore\n\nimport (\n\t\"testing\"\n\t\"github.com/hmdsefi/tenure\"\n\t\"example.com/a\"\n)\n",
		"storetest/s.go":        "package storetest\n\nimport (\n\t\"testing\"\n\t\"testing/synctest\"\n\t\"github.com/hmdsefi/tenure\"\n)\n",
		"extra/e.go":            "package extra\n",
		"core/c.go":             "package core\n\nimport \"os\"\n",
		"core/sub/c.go":         "package sub\n\nimport \"net\"\n",
		"testdata/t.go":         "package t\n\nimport \"example.com/t\"\n",
		"_old/o.go":             "package old\n\nimport \"example.com/o\"\n",
		".hidden/h.go":          "package hidden\n\nimport \"example.com/h\"\n",
		"k8s/go.mod":            "module github.com/hmdsefi/tenure/k8s\n\ngo 1.26.0\n",
		"k8s/k.go":              "package k8s\n\nimport \"example.com/k\"\n",
		"internal/dettest/d.go": "package dettest\n\nimport (\n\t\"C\"\n\t\"go/parser\"\n)\n",
	})
	got, err := checkRootImports(root, tenureModule)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"extra: package github.com/hmdsefi/tenure/extra has no import rule; add a row to internal/dettest",
		`internal/dettest/d.go: import "C" is not allowed in package github.com/hmdsefi/tenure/internal/dettest`,
		`memstore/m.go: import "example.com/a" is not allowed in package github.com/hmdsefi/tenure/memstore`,
		`memstore/m.go: import "testing" is not allowed in package github.com/hmdsefi/tenure/memstore`,
		`x.go: import "github.com/hmdsefi/tenure/memstore" is not allowed in package github.com/hmdsefi/tenure`,
	}
	if !slices.Equal(got, want) {
		t.Errorf("checkRootImports:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestRepoChecksThroughSymlinkedRoot runs the walks on a root reached through a symlink, as a
// checkout under a symlinked directory is: they must check the files behind the link.
func TestRepoChecksThroughSymlinkedRoot(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target")
	writeFiles(t, target, map[string]string{
		"go.mod": rootGoMod,
		"bad.go": "package tenure\n\nimport \"example.com/x\"\n",
	})
	link := filepath.Join(tmp, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	headers, err := checkLicenseHeaders(link)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"bad.go"}; !slices.Equal(headers, want) {
		t.Errorf("checkLicenseHeaders = %q, want %q", headers, want)
	}
	imports, err := checkRootImports(link, tenureModule)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`bad.go: import "example.com/x" is not allowed in package github.com/hmdsefi/tenure`}
	if !slices.Equal(imports, want) {
		t.Errorf("checkRootImports = %q, want %q", imports, want)
	}
}

// TestRootGoModNullKeys checks that a key printed as null counts as absent: Go 1.26's go mod
// edit -json prints the lists a go.mod does not have that way.
func TestRootGoModNullKeys(t *testing.T) {
	tests := []struct {
		name string
		out  string
		want []string
	}{
		{
			name: "null keys",
			out:  `{"Module":{"Path":"github.com/hmdsefi/tenure"},"Go":"1.26.0","Require":null,"Exclude":null,"Replace":null,"Retract":null,"Tool":null,"Ignore":null}`,
		},
		{
			name: "non-null key",
			out:  `{"Module":{"Path":"github.com/hmdsefi/tenure"},"Go":"1.26.0","Require":[{"Path":"golang.org/x/tools","Version":"v0.30.0"}],"Tool":null}`,
			want: []string{"go.mod: unexpected Require; the root module has no requirements, replacements, exclusions, retractions, tools, toolchain or godebug lines"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rootGoModLines([]byte(tc.out))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("rootGoModLines = %q, want %q", got, tc.want)
			}
		})
	}
}

// goModEditJSON runs `go mod edit -json [args]` in dir and returns its output. GOTOOLCHAIN=local
// keeps go from downloading the toolchain that a newer go or toolchain line names; the rest of
// the environment, GOWORK included, is inherited.
func goModEditJSON(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("go", append([]string{"mod", "edit", "-json"}, args...)...) //nolint:gosec // G204: the go command with the caller's go.mod arguments
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go %s in %s: %w: %s",
			strings.Join(cmd.Args[1:], " "), dir, err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// checkRootGoMod returns one line per way in which the go.mod and go.sum in dir break REP-010,
// in REP-015's order.
func checkRootGoMod(dir string) ([]string, error) {
	out, err := goModEditJSON(dir, "go.mod")
	if err != nil {
		return nil, err
	}
	lines, err := rootGoModLines(out)
	if err != nil {
		return nil, fmt.Errorf("go mod edit -json in %s: %w", dir, err)
	}
	switch _, err := os.Stat(filepath.Join(dir, "go.sum")); {
	case err == nil:
		lines = append(lines, "go.sum: the root module has no requirements, so it has no go.sum")
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	return lines, nil
}

// rootGoModLines returns REP-015's go.mod lines (items 1 to 3) for out, the output of go mod
// edit -json.
func rootGoModLines(out []byte) ([]string, error) {
	// Walk the top-level object as a token stream to keep its keys in printed order.
	dec := json.NewDecoder(bytes.NewReader(out))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if tok != json.Delim('{') {
		return nil, errors.New("output is not a JSON object")
	}
	var keys []string
	values := map[string]json.RawMessage{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("key %v is not a string", tok)
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		// Go 1.26 prints a directive the file does not have as a null key ("Require": null);
		// Go 1.27 omits it. Either way the file has no such line.
		if string(v) == "null" {
			continue
		}
		keys = append(keys, key)
		values[key] = v
	}

	var lines []string
	for _, k := range keys {
		if k != "Module" && k != "Go" {
			lines = append(lines, "go.mod: unexpected "+k+
				"; the root module has no requirements, replacements, exclusions, retractions, tools, toolchain or godebug lines")
		}
	}
	var mod struct{ Path string }
	if v, ok := values["Module"]; ok {
		if err := json.Unmarshal(v, &mod); err != nil {
			return nil, fmt.Errorf("Module: %w", err)
		}
	}
	if mod.Path != tenureModule {
		lines = append(lines, fmt.Sprintf("go.mod: module %s, want %s", mod.Path, tenureModule))
	}
	var goVersion string
	if v, ok := values["Go"]; ok {
		if err := json.Unmarshal(v, &goVersion); err != nil {
			return nil, fmt.Errorf("Go: %w", err)
		}
	}
	if goVersion != "1.26.0" {
		lines = append(lines, fmt.Sprintf("go.mod: go %s, want 1.26.0", goVersion))
	}
	return lines, nil
}

// resolveRoot returns root with its symlinks resolved. filepath.WalkDir does not descend into
// a root that is a symlink, so a walk from an unresolved root would check nothing.
func resolveRoot(root string) (string, error) {
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve root %s: %w", root, err)
	}
	return resolved, nil
}

// skipDir reports whether a walk skips the directory named name below its root: testdata
// directories and directories whose name starts with "." or "_" (REP-110, REP-080).
func skipDir(name string) bool {
	return name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
}

// copyrightLine matches line 1 of the license header (REP-110).
var copyrightLine = regexp.MustCompile(`^// Copyright 20[0-9]{2} Hamed Yousefi$`)

// checkLicenseHeaders returns the slash-separated paths, relative to root and in walk order, of
// the .go files below root that do not start with the license header and a blank line (REP-110).
// Nested modules are walked too.
func checkLicenseHeaders(root string) ([]string, error) {
	root, err := resolveRoot(root)
	if err != nil {
		return nil, err
	}
	var bad []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && skipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		// Symlinks and other non-regular entries are skipped: a link's target is checked where
		// it lives, and a dangling link has no content to check.
		if !d.Type().IsRegular() || !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		data, err := os.ReadFile(path) //nolint:gosec // G304: the walk reads the files it finds below root
		if err != nil {
			return err
		}
		// Three lines and the start of a fourth: line 3 must exist and be empty.
		lines := strings.SplitN(string(data), "\n", 4)
		if len(lines) == 4 && copyrightLine.MatchString(lines[0]) &&
			lines[1] == "// SPDX-License-Identifier: MPL-2.0" && lines[2] == "" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		bad = append(bad, filepath.ToSlash(rel))
		return nil
	})
	return bad, err
}

// importRule is one row of REP-080's table: a root-module package, relative to the module path,
// and what its non-test files may import besides the standard library.
type importRule struct {
	dir     string
	testing bool     // testing and the packages below it
	module  []string // root-module packages, relative to the module path
}

// rootImportRules is REP-080's table. core and the packages below it are DET's (TestCoreImports).
var rootImportRules = []importRule{
	{dir: "", module: []string{"core"}},
	{dir: "memstore", module: []string{"core", ""}},
	{dir: "static", module: []string{"core", ""}},
	{dir: "debughttp", module: []string{"core", ""}},
	{dir: "storetest", testing: true, module: []string{"core", ""}},
	{dir: "membershiptest", testing: true, module: []string{"core", ""}},
	{dir: "internal/dettest"},
	{dir: "internal/machinecases", module: []string{"core"}},
}

// allows reports whether a non-test file of the package of rule r may import path.
func (r importRule) allows(path, modulePath string) bool {
	if path == modulePath {
		return slices.Contains(r.module, "")
	}
	if rel, ok := strings.CutPrefix(path, modulePath+"/"); ok {
		return slices.Contains(r.module, rel)
	}
	first, _, _ := strings.Cut(path, "/")
	switch {
	case strings.Contains(first, "."), path == "C":
		return false
	case first == "testing":
		return r.testing
	default:
		return true
	}
}

// checkRootImports returns one line per import that REP-080's table does not allow in a non-test
// .go file of the module modulePath at root, and one per package directory without a row, sorted
// by file, then import path. It skips testdata, directories whose name starts with "." or "_",
// directories with their own go.mod, and core and the directories below it.
func checkRootImports(root, modulePath string) ([]string, error) {
	type violation struct{ file, path, line string }
	root, err := resolveRoot(root)
	if err != nil {
		return nil, err
	}
	var found []violation
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			rel = ""
		} else {
			if skipDir(d.Name()) || rel == "core" {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir
			} else if !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}

		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		var files []string
		for _, e := range entries {
			name := e.Name()
			if !e.IsDir() && strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
				files = append(files, name)
			}
		}
		if len(files) == 0 {
			return nil
		}
		importPath := modulePath
		if rel != "" {
			importPath += "/" + rel
		}
		i := slices.IndexFunc(rootImportRules, func(r importRule) bool { return r.dir == rel })
		if i < 0 {
			found = append(found, violation{file: rel, line: fmt.Sprintf(
				"%s: package %s has no import rule; add a row to internal/dettest", rel, importPath)})
			return nil
		}
		rule := rootImportRules[i]
		for _, name := range files {
			f, err := parser.ParseFile(fset, filepath.Join(path, name), nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			file := name
			if rel != "" {
				file = rel + "/" + name
			}
			for _, spec := range f.Imports {
				p, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return fmt.Errorf("%s: %w", file, err)
				}
				if !rule.allows(p, modulePath) {
					found = append(found, violation{file: file, path: p, line: fmt.Sprintf(
						"%s: import %q is not allowed in package %s", file, p, importPath)})
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(found, func(a, b violation) int {
		return cmp.Or(cmp.Compare(a.file, b.file), cmp.Compare(a.path, b.path))
	})
	lines := make([]string, len(found))
	for i, v := range found {
		lines[i] = v.line
	}
	return lines, nil
}
