// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
)

// TestATK8S30RESTConfig checks that RESTConfig returns a copy with the defaults set, keeps
// values that are set, and returns the errors. The acceptance test's steps were written from
// the issue's text: the original text is unavailable.
func TestATK8S30RESTConfig(t *testing.T) {
	t.Run("defaults on a copy", func(t *testing.T) {
		in := &rest.Config{Host: "https://10.0.0.1:6443", BearerToken: "token"}
		got, err := RESTConfig(in, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got == in {
			t.Fatal("RESTConfig returned its argument, want a copy")
		}
		if got.QPS != 50 || got.Burst != 100 {
			t.Errorf("QPS, Burst = %v, %d; want 50, 100", got.QPS, got.Burst)
		}
		if want := "tenure-k8s " + rest.DefaultKubernetesUserAgent(); got.UserAgent != want {
			t.Errorf("UserAgent = %q, want %q", got.UserAgent, want)
		}
		if !strings.HasPrefix(got.UserAgent, "tenure-k8s ") {
			t.Errorf("UserAgent = %q, want it to start with tenure-k8s", got.UserAgent)
		}
		if got.Host != in.Host || got.BearerToken != in.BearerToken {
			t.Errorf("Host, BearerToken = %q, %q; want %q, %q", got.Host, got.BearerToken, in.Host, in.BearerToken)
		}
		if in.QPS != 0 || in.Burst != 0 || in.UserAgent != "" {
			t.Errorf("RESTConfig changed its argument: QPS %v, Burst %d, UserAgent %q", in.QPS, in.Burst, in.UserAgent)
		}
	})

	t.Run("set values kept", func(t *testing.T) {
		in := &rest.Config{Host: "https://h", QPS: 5, Burst: 7, UserAgent: "mine/1"}
		got, err := RESTConfig(in, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got == in || got.QPS != 5 || got.Burst != 7 || got.UserAgent != "mine/1" {
			t.Errorf("RESTConfig = %p {QPS %v, Burst %d, UserAgent %q}, want a copy of %p with 5, 7, mine/1",
				got, got.QPS, got.Burst, got.UserAgent, in)
		}
		// A negative QPS turns client-go's rate limiter off: it is a setting, not a default.
		in = &rest.Config{Host: "https://h", QPS: -1, Burst: -1}
		if got, err = RESTConfig(in, nil); err != nil {
			t.Fatal(err)
		}
		if got.QPS != -1 || got.Burst != -1 {
			t.Errorf("RESTConfig with QPS -1, Burst -1 = QPS %v, Burst %d; want -1, -1", got.QPS, got.Burst)
		}
	})

	t.Run("error passed through", func(t *testing.T) {
		want := errors.New("unable to load in-cluster configuration")
		got, err := RESTConfig(&rest.Config{Host: "https://h"}, want)
		if got != nil || err != want {
			t.Errorf("RESTConfig(c, err) = %v, %v; want nil, %v", got, err, want)
		}
		if got, err := RESTConfig(nil, want); got != nil || err != want {
			t.Errorf("RESTConfig(nil, err) = %v, %v; want nil, %v", got, err, want)
		}
	})

	t.Run("nil config", func(t *testing.T) {
		got, err := RESTConfig(nil, nil)
		if got != nil || err == nil || err.Error() != "k8s: nil rest.Config" {
			t.Errorf("RESTConfig(nil, nil) = %v, %v; want nil, k8s: nil rest.Config", got, err)
		}
	})
}
