// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"os"
	"runtime"
	"testing"
)

// The environment variables of REP-400.
const (
	envKubeconfig = "TENURE_TEST_KUBECONFIG"
	envExternal   = "TENURE_TEST_EXTERNAL"
)

// kubeconfig returns the path of the kubeconfig file of the Kubernetes API server the external
// test runs against, from TENURE_TEST_KUBECONFIG (REP-400). It fails the test when
// TENURE_TEST_EXTERNAL is neither empty, 0 nor 1, returns the path when it is set, fails the
// test when it is not set and TENURE_TEST_EXTERNAL=1, and skips the test otherwise.
func kubeconfig(t testing.TB) string {
	t.Helper()
	external := os.Getenv(envExternal)
	if external != "" && external != "0" && external != "1" {
		t.Fatalf("invalid %s=%q: want 0 or 1", envExternal, external)
	}
	if path := os.Getenv(envKubeconfig); path != "" {
		return path
	}
	if external == "1" {
		t.Fatalf("%s is not set and %s=1", envKubeconfig, envExternal)
	}
	t.Skipf("%s is not set; this test needs a Kubernetes API server (see .github/CONTRIBUTING.md)", envKubeconfig)
	return ""
}

// recordingTB is a testing.TB that records how a helper ended. Fatal and Skip end the calling
// goroutine as testing.T's do, so the helper must run on a goroutine of its own.
type recordingTB struct {
	testing.TB
	failed  bool
	skipped bool
	msg     string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatal(args ...any) { r.end(&r.failed, fmt.Sprint(args...)) }

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.end(&r.failed, fmt.Sprintf(format, args...))
}

func (r *recordingTB) FailNow() { r.end(&r.failed, "") }

func (r *recordingTB) Skip(args ...any) { r.end(&r.skipped, fmt.Sprint(args...)) }

func (r *recordingTB) Skipf(format string, args ...any) {
	r.end(&r.skipped, fmt.Sprintf(format, args...))
}

func (r *recordingTB) SkipNow() { r.end(&r.skipped, "") }

func (r *recordingTB) end(flag *bool, msg string) {
	*flag, r.msg = true, msg
	runtime.Goexit()
}

// TestKubeconfigHelper checks REP-400's kubeconfig helper (AT-REP-17): it skips without
// TENURE_TEST_KUBECONFIG, fails without it when TENURE_TEST_EXTERNAL=1, fails on any other
// TENURE_TEST_EXTERNAL than empty, 0 or 1, and returns a set value.
func TestKubeconfigHelper(t *testing.T) {
	const (
		skip    = "TENURE_TEST_KUBECONFIG is not set; this test needs a Kubernetes API server (see .github/CONTRIBUTING.md)"
		missing = "TENURE_TEST_KUBECONFIG is not set and TENURE_TEST_EXTERNAL=1"
		invalid = `invalid TENURE_TEST_EXTERNAL="yes": want 0 or 1`
		path    = "/tmp/kind-kubeconfig"
	)
	tests := []struct {
		name       string
		kubeconfig string
		external   string
		want       string // the returned path, or the message
		failed     bool
		skipped    bool
	}{
		{name: "unset", want: skip, skipped: true},
		{name: "unset, external 0", external: "0", want: skip, skipped: true},
		{name: "unset, external 1", external: "1", want: missing, failed: true},
		{name: "unset, external yes", external: "yes", want: invalid, failed: true},
		{name: "set, external yes", kubeconfig: path, external: "yes", want: invalid, failed: true},
		{name: "set", kubeconfig: path, want: path},
		{name: "set, external 0", kubeconfig: path, external: "0", want: path},
		{name: "set, external 1", kubeconfig: path, external: "1", want: path},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envKubeconfig, tt.kubeconfig)
			t.Setenv(envExternal, tt.external)
			tb := &recordingTB{TB: t}
			var got string
			done := make(chan struct{})
			go func() {
				defer close(done)
				got = kubeconfig(tb)
			}()
			<-done
			if tb.failed != tt.failed || tb.skipped != tt.skipped {
				t.Fatalf("failed, skipped = %v, %v (%q), want %v, %v", tb.failed, tb.skipped, tb.msg, tt.failed, tt.skipped)
			}
			if tt.failed || tt.skipped {
				if tb.msg != tt.want {
					t.Errorf("message = %q, want %q", tb.msg, tt.want)
				}
				return
			}
			if got != tt.want {
				t.Errorf("kubeconfig = %q, want %q", got, tt.want)
			}
		})
	}
}
