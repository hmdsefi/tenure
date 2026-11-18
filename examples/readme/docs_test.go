// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package readme

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// docFiles are the Markdown files whose Go blocks are compiled here, relative to the repository
// root, with the number of the first block the markers name: the README's first block has
// markers of its own in readme.go.
var docFiles = []struct {
	path  string
	first int
}{
	{"README.md", 2},
	{"docs/routing.md", 1},
	{"examples/orderbook/README.md", 1},
}

// markerRE matches a marker line: "// <file> block <n> begin" or "... end".
var markerRE = regexp.MustCompile(`^\s*// (\S+\.md) block (\d+) (begin|end)$`)

// goBlocks returns the lines of every ```go block of a Markdown file, in order.
func goBlocks(md string) ([][]string, error) {
	var blocks [][]string
	lines := strings.Split(md, "\n")
	for i := 0; i < len(lines); i++ {
		if strings.TrimRight(lines[i], " \t\r") != "```go" {
			continue
		}
		j := i + 1
		for j < len(lines) && !strings.HasPrefix(lines[j], "```") {
			j++
		}
		if j == len(lines) {
			return nil, fmt.Errorf("Go block %d is not closed", len(blocks)+1)
		}
		blocks = append(blocks, lines[i+1:j])
		i = j
	}
	return blocks, nil
}

// markedBlocks returns the marked regions of a Go source, keyed by "<file> block <n>", each with
// the regions' common leading tabs removed.
func markedBlocks(src string) (map[string][]string, error) {
	out := map[string][]string{}
	open := ""
	var body []string
	for i, line := range strings.Split(src, "\n") {
		m := markerRE.FindStringSubmatch(line)
		switch {
		case m == nil:
			if open != "" {
				body = append(body, line)
			}
		case m[3] == "begin":
			if open != "" {
				return nil, fmt.Errorf("line %d: %s begins inside %s", i+1, m[1]+" block "+m[2], open)
			}
			open, body = m[1]+" block "+m[2], nil
		default:
			name := m[1] + " block " + m[2]
			if open != name {
				return nil, fmt.Errorf("line %d: %s ends, but the open region is %q", i+1, name, open)
			}
			if _, dup := out[name]; dup {
				return nil, fmt.Errorf("line %d: %s is marked twice", i+1, name)
			}
			out[name], open = dedent(body), ""
		}
	}
	if open != "" {
		return nil, fmt.Errorf("%s does not end", open)
	}
	return out, nil
}

// dedent removes from every line the leading tabs that all non-empty lines share.
func dedent(lines []string) []string {
	common := -1
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			continue
		}
		n := len(l) - len(strings.TrimLeft(l, "\t"))
		if common < 0 || n < common {
			common = n
		}
	}
	out := make([]string, len(lines))
	for i, l := range lines {
		if len(l) >= common && common > 0 {
			l = l[common:]
		}
		out[i] = l
	}
	return out
}

// checkDocs compares the Go blocks of the documents with the marked regions: every block from
// each file's first has a region with the same lines, and every region has a block.
func checkDocs(docs map[string]string, regions map[string][]string) []error {
	var errs []error
	seen := map[string]bool{}
	for _, f := range docFiles {
		blocks, err := goBlocks(docs[f.path])
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.path, err))
			continue
		}
		for i := f.first - 1; i < len(blocks); i++ {
			name := f.path + " block " + strconv.Itoa(i+1)
			seen[name] = true
			got, ok := regions[name]
			switch {
			case !ok:
				errs = append(errs, fmt.Errorf("%s has no code in examples/readme between // %s begin and end", name, name))
			case strings.Join(got, "\n") != strings.Join(blocks[i], "\n"):
				errs = append(errs, fmt.Errorf("%s differs from the code between its markers in examples/readme; change both together", name))
			}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(regions)) {
		if !seen[name] {
			errs = append(errs, fmt.Errorf("examples/readme marks %s, which is not a Go block of its file", name))
		}
	}
	return errs
}

// readDocs reads the documents and the marked regions of every Go file of this package.
func readDocs(t *testing.T) (map[string]string, map[string][]string) {
	t.Helper()
	docs := map[string]string{}
	for _, f := range docFiles {
		b, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(f.path)))
		if err != nil {
			t.Fatal(err)
		}
		docs[f.path] = string(b)
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	regions := map[string][]string{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		r, err := markedBlocks(string(b))
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		for name, lines := range r {
			if _, dup := regions[name]; dup {
				t.Fatalf("%s: %s is marked in two files", file, name)
			}
			regions[name] = lines
		}
	}
	return docs, regions
}

// TestDocSnippets checks that every Go block of the README after the first, and every Go block
// of docs/routing.md and the orderbook's README, is code that compiles in this package: the code between its markers.
func TestDocSnippets(t *testing.T) {
	docs, regions := readDocs(t)
	for _, err := range checkDocs(docs, regions) {
		t.Error(err)
	}
}

// TestDocSnippetsDetectDrift changes one line of a guide block, drops a block and adds one, and
// expects each to be reported.
func TestDocSnippetsDetectDrift(t *testing.T) {
	docs, regions := readDocs(t)
	md := docs["docs/routing.md"]
	i := strings.Index(md, "```go\n")
	if i < 0 {
		t.Fatal("docs/routing.md has no Go block")
	}
	changed := maps.Clone(docs)

	changed["docs/routing.md"] = md[:i+6] + "// one more line\n" + md[i+6:]
	if errs := checkDocs(changed, regions); len(errs) != 1 || !strings.Contains(errs[0].Error(), "docs/routing.md block 1 differs") {
		t.Errorf("a changed block: %v", errs)
	}
	changed["docs/routing.md"] = md + "\n```go\nx := 1\n```\n"
	if errs := checkDocs(changed, regions); len(errs) != 1 || !strings.Contains(errs[0].Error(), "has no code in examples/readme") {
		t.Errorf("a block without code: %v", errs)
	}
	changed["docs/routing.md"] = strings.Replace(md, "```go\n", "```text\n", 1)
	if errs := checkDocs(changed, regions); len(errs) == 0 {
		t.Error("a dropped block: no error")
	}
}
