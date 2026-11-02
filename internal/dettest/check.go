// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// FindRoot walks up from dir to the first directory whose go.mod declares module modulePath.
func FindRoot(dir, modulePath string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for d := abs; ; d = filepath.Dir(d) {
		data, err := os.ReadFile(filepath.Join(d, "go.mod")) //nolint:gosec // G304: reading go.mod files above dir is the point
		switch {
		case err == nil:
			if goModModule(string(data)) == modulePath {
				return d, nil
			}
		case !errors.Is(err, fs.ErrNotExist):
			return "", err
		}
		if filepath.Dir(d) == d {
			return "", fmt.Errorf("dettest: no go.mod for module %s above %s", modulePath, abs)
		}
	}
}

// goModModule returns the path of the module directive in the go.mod text gomod, without quotes
// or a trailing comment, or "" if there is none.
func goModModule(gomod string) string {
	for line := range strings.SplitSeq(gomod, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module")
		if !ok || rest == "" || (rest[0] != ' ' && rest[0] != '\t') {
			continue
		}
		rest, _, _ = strings.Cut(rest, "//")
		rest = strings.TrimSpace(rest)
		if p, err := strconv.Unquote(rest); err == nil {
			return p
		}
		return rest
	}
	return ""
}
