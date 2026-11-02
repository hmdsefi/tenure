// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFiles writes each name (slash-separated, relative to root) with its content, creating
// directories as needed.
func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// TestFindRoot checks FindRoot (DET §4): it skips a nested go.mod of another module and stops at
// the first go.mod that declares the module asked for.
func TestFindRoot(t *testing.T) {
	tests := []struct {
		name  string
		outer string // the root go.mod
	}{
		{name: "plain", outer: "module github.com/hmdsefi/tenure\n\ngo 1.26.0\n"},
		{name: "quoted with comment", outer: "module \"github.com/hmdsefi/tenure\" // the root\n\ngo 1.26.0\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFiles(t, root, map[string]string{
				"go.mod":             tc.outer,
				"k8s/go.mod":         "module github.com/hmdsefi/tenure/k8s\n\ngo 1.26.0\n",
				"k8s/sub/deep/x.txt": "",
				"other/go.mod":       "// module github.com/hmdsefi/tenure\nmodule example.com/other\n",
				"other/sub/x.txt":    "",
			})
			for _, start := range []string{"k8s/sub/deep", "k8s", "other/sub", "."} {
				got, err := FindRoot(filepath.Join(root, start), "github.com/hmdsefi/tenure")
				if err != nil {
					t.Fatalf("FindRoot(%s): %v", start, err)
				}
				if got != root {
					t.Errorf("FindRoot(%s) = %s, want %s", start, got, root)
				}
			}
		})
	}

	t.Run("no match", func(t *testing.T) {
		dir := t.TempDir()
		writeFiles(t, dir, map[string]string{"go.mod": "module example.com/other\n\ngo 1.26.0\n"})
		_, err := FindRoot(dir, "github.com/hmdsefi/tenure")
		want := "dettest: no go.mod for module github.com/hmdsefi/tenure above " + dir
		if err == nil || err.Error() != want {
			t.Fatalf("FindRoot error = %v, want %s", err, want)
		}
	})

	t.Run("read error", func(t *testing.T) {
		dir := t.TempDir()
		writeFiles(t, dir, map[string]string{"x.go": "package x\n"})
		file := filepath.Join(dir, "x.go")
		_, err := FindRoot(file, "github.com/hmdsefi/tenure")
		want := "dettest: find root of " + file + ": "
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Fatalf("FindRoot error = %v, want prefix %q", err, want)
		}
	})
}
