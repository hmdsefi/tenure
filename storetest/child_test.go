// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest_test

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// child marks a run of the test binary started by runChild. Child-only tests run the suite on
// fixtures that must fail, so they skip unless it is set.
var child = flag.Bool("child", false, "run the child-only tests, which a parent test starts and checks")

// skipUnlessChild skips a child-only test in an ordinary run.
func skipUnlessChild(t *testing.T) {
	t.Helper()
	if !*child {
		t.Skip("child-only test: runs when a parent test passes -child")
	}
}

// childLog is one log line of a child test.
type childLog struct {
	line string // the line as printed
	msg  string // the text after "file.go:N: "
}

// childRun is the parsed -test.v output of a child run.
type childRun struct {
	out     string
	failed  bool                  // the child exited non-zero
	started []string              // tests in the order their "=== RUN" lines appear
	results map[string]string     // test name to PASS, FAIL or SKIP
	logs    map[string][]childLog // test name to its log lines
}

var (
	runLine    = regexp.MustCompile(`^=== (RUN|CONT|NAME|PAUSE)\s+(\S+)`)
	resultLine = regexp.MustCompile(`^\s*--- (PASS|FAIL|SKIP): (\S+) \(`)
	logLine    = regexp.MustCompile(`^\s+\S+\.go:\d+: (.*)$`)
)

// runChild re-executes the test binary with -test.v and -child, running the tests that pattern
// matches, and parses its output. A log line belongs to the test named by the latest "===" line.
func runChild(t *testing.T, pattern string) childRun {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run", pattern, "-test.v", "-test.count=1", "-child") //nolint:gosec // re-executes this test binary
	out, err := cmd.CombinedOutput()
	r := childRun{
		out:     string(out),
		results: make(map[string]string),
		logs:    make(map[string][]childLog),
	}
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		r.failed = true
	case err != nil:
		t.Fatalf("running the child %q: %v\n%s", pattern, err, out)
	}
	var current string
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if m := runLine.FindStringSubmatch(line); m != nil {
			current = m[2]
			if m[1] == "RUN" {
				r.started = append(r.started, current)
			}
			continue
		}
		if m := resultLine.FindStringSubmatch(line); m != nil {
			r.results[m[2]] = m[1]
			continue
		}
		if m := logLine.FindStringSubmatch(line); m != nil && current != "" {
			r.logs[current] = append(r.logs[current], childLog{line: line, msg: m[1]})
		}
	}
	return r
}

// under returns the tests the child started below parent, without the "parent/" prefix, in
// start order.
func (r childRun) under(parent string) []string {
	var names []string
	for _, n := range r.started {
		if rest, ok := strings.CutPrefix(n, parent+"/"); ok {
			names = append(names, rest)
		}
	}
	return names
}
