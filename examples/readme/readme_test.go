// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package readme

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// driftMessage is REP-165's message for a README whose first Go block differs from readme.go.
const driftMessage = "README.md's first Go block differs from examples/readme/readme.go " +
	"(between // README begin and // README end); change both together"

// snippetFromSource returns the lines of src between the README begin and README end markers,
// with one leading tab removed from every non-empty line.
func snippetFromSource(src string) ([]string, error) {
	var out []string
	in := false
	for _, line := range strings.Split(src, "\n") {
		switch strings.TrimSpace(line) {
		case "// README begin":
			in = true
			continue
		case "// README end":
			if !in {
				return nil, errors.New("readme.go: // README end before // README begin")
			}
			return out, nil
		}
		if in {
			out = append(out, strings.TrimPrefix(line, "\t"))
		}
	}
	if !in {
		return nil, errors.New("readme.go: no // README begin")
	}
	return nil, errors.New("readme.go: no // README end")
}

// firstGoBlock returns the lines of the first ```go block of a Markdown file.
func firstGoBlock(md string) ([]string, error) {
	lines := strings.Split(md, "\n")
	for i, line := range lines {
		if strings.TrimRight(line, " \t\r") != "```go" {
			continue
		}
		for j := i + 1; j < len(lines); j++ {
			if strings.HasPrefix(lines[j], "```") {
				return lines[i+1 : j], nil
			}
		}
		return nil, errors.New("README.md: the first Go block is not closed")
	}
	return nil, errors.New("README.md: no Go block")
}

// checkSnippet compares the first Go block of readme with the marked body of source. It returns
// the drift message when they differ.
func checkSnippet(readme, source string) error {
	want, err := firstGoBlock(readme)
	if err != nil {
		return err
	}
	got, err := snippetFromSource(source)
	if err != nil {
		return err
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		return errors.New(driftMessage)
	}
	return nil
}

// readFiles reads the README and readme.go.
func readFiles(t *testing.T) (readme, source string) {
	t.Helper()
	r, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	s, err := os.ReadFile("readme.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(r), string(s)
}

// TestReadmeSnippet is AT-EXM-24 and AT-REP-11: the README's first Go block is the code between
// the markers of readme.go.
func TestReadmeSnippet(t *testing.T) {
	readme, source := readFiles(t)
	if err := checkSnippet(readme, source); err != nil {
		t.Error(err)
	}
}

// TestReadmeSnippetDetectsDrift changes an in-memory README by one blank line inside the first Go
// block and expects the message of REP-165.
func TestReadmeSnippetDetectsDrift(t *testing.T) {
	readme, source := readFiles(t)
	lines := strings.Split(readme, "\n")
	inserted := false
	for i, line := range lines {
		if line == "```go" {
			// The blank line goes after the first line of the block.
			lines = append(lines[:i+2], append([]string{""}, lines[i+2:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		t.Fatal("README.md has no Go block")
	}
	err := checkSnippet(strings.Join(lines, "\n"), source)
	if err == nil || err.Error() != driftMessage {
		t.Errorf("checkSnippet with a blank line inserted = %v; want %q", err, driftMessage)
	}
}
