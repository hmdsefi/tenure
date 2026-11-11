// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// mutants pairs each planted bug's test with the invariant whose violation must fail it
// (SIM-660).
var mutants = []struct {
	test, check string
}{
	{"TestMutantGrantHeld", "tenure/g4"},
	{"TestMutantSkipRaise", "tenure/g3"},
	{"TestMutantLateDeadline", "tenure/g2"},
	{"TestMutantReadConfirm", "tenure/g2"},
}

// TestMutantsCaught checks AT-SIM-37: each planted bug fails the check it breaks. For each
// TestMutant<Name> it runs
//
//	go test -tags tenure_mutants -run '^TestMutant<Name>$' -count=1 .
//
// in a child process, with no FAULTLINE_ variable of this process (and FAULTLINE_ARTIFACTS=off,
// so that the failing seed leaves no files behind), and requires the child to fail with output
// naming the check. The output is matched by substring, as a coverage build adds lines to it.
// The first child builds the test binary with the tag; the others reuse the build cache.
func TestMutantsCaught(t *testing.T) {
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("no go command to build the planted bugs with: %v", err)
	}
	env := append(slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "FAULTLINE_") }),
		"FAULTLINE_ARTIFACTS=off")
	for _, m := range mutants {
		t.Run(m.test, func(t *testing.T) {
			// The command is the go tool, from PATH, on this package, with a test name of the
			// fixed list above: no input of the test's caller reaches it.
			cmd := exec.Command(gobin, "test", "-tags", "tenure_mutants", "-run", "^"+m.test+"$", "-count=1", ".") //nolint:gosec // fixed arguments, see above
			cmd.Env = env
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() == 0 {
				t.Fatalf("%s exited with %v, want a failure; output:\n%s", m.test, err, out.String())
			}
			if want := `invariant "` + m.check + `" violated`; !strings.Contains(out.String(), want) {
				t.Errorf("%s's output does not contain %q:\n%s", m.test, want, out.String())
			}
		})
	}
}
