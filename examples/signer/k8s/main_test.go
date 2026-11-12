// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/hmdsefi/tenure/examples/internal/fencedb"
	"github.com/hmdsefi/tenure/examples/internal/signing"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestParseEnv(t *testing.T) {
	good := map[string]string{
		"POD_NAME": "signer-0", "POD_IP": "10.1.2.3", "SLOTS": "3", "NONCESVC_URL": "http://noncesvc:8080",
	}
	cfg, err := parseEnv(envOf(good), "tenure-signer\n")
	if err != nil {
		t.Fatalf("parseEnv: %v", err)
	}
	if cfg.podName != "signer-0" || cfg.slots != 3 || cfg.namespace != "tenure-signer" ||
		cfg.addr() != "10.1.2.3:8080" || cfg.noncesvc != "http://noncesvc:8080" {
		t.Errorf("parseEnv = %+v", cfg)
	}

	without := func(k string) map[string]string {
		m := map[string]string{}
		for key, v := range good {
			if key != k {
				m[key] = v
			}
		}
		return m
	}
	with := func(k, v string) map[string]string {
		m := without(k)
		m[k] = v
		return m
	}
	cases := []struct {
		name string
		env  map[string]string
		ns   string
		want string
	}{
		{"no pod name", without("POD_NAME"), "ns", "POD_NAME"},
		{"no pod ip", without("POD_IP"), "ns", "POD_IP"},
		{"bad pod ip", with("POD_IP", "host"), "ns", "POD_IP"},
		{"no service", without("NONCESVC_URL"), "ns", "NONCESVC_URL"},
		{"no namespace", good, " \n", "namespace"},
		{"bad slots", with("SLOTS", "three"), "ns", "SLOTS"},
		{"negative slots", with("SLOTS", "-1"), "ns", "SLOTS"},
	}
	for _, c := range cases {
		if _, err := parseEnv(envOf(c.env), c.ns); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %v, want one naming %q", c.name, err, c.want)
		}
	}
	if c, err := parseEnv(envOf(without("SLOTS")), "ns"); err != nil || c.slots != 0 {
		t.Errorf("no SLOTS: %+v, %v; want 0 slots", c, err)
	}
}

// TestServe runs the pod against a fake Kubernetes API and a nonce service in process: it
// becomes ready, signs, and stops on its context.
func TestServe(t *testing.T) {
	chain := signing.NewChain()
	svc := httptest.NewServer(signing.NewNoncesvc(signing.NewNonces(fencedb.New()), chain))
	t.Cleanup(svc.Close)

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	cfg := config{podName: "signer-0", podIP: "127.0.0.1", slots: 1, noncesvc: svc.URL, namespace: "tenure-signer"}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, fake.NewClientset(), ln) }()

	get := func(path string) int {
		resp, err := http.Get(base + path)
		if err != nil {
			return 0
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	deadline := time.Now().Add(20 * time.Second)
	for get("/healthz") != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatal("/healthz never answered 200")
		}
		time.Sleep(20 * time.Millisecond)
	}

	for {
		resp, err := http.Post(base+"/sign", "application/json", strings.NewReader(`{"wallet":"w-00","payload":"p"}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/sign never answered 200, last %d", resp.StatusCode)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if rep := chain.Report(); rep.Accepted != 1 {
		t.Errorf("chain report %+v, want one accepted transaction", rep)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not return after its context ended")
	}
}

// TestStopFitsTerminationGrace checks that the drain, the close and the shutdown of the HTTP
// server fit in the terminationGracePeriodSeconds of the signer's manifest, and that the stop
// keeps the 25 s and 3 s that EXM-350 sets.
func TestStopFitsTerminationGrace(t *testing.T) {
	if drainTimeout != 25*time.Second || closeTimeout != 3*time.Second {
		t.Errorf("drain %v and close %v, want 25 s and 3 s", drainTimeout, closeTimeout)
	}
	raw, err := os.ReadFile("deploy/signer.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*terminationGracePeriodSeconds:\s*(\d+)\s*$`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("deploy/signer.yaml has no terminationGracePeriodSeconds")
	}
	grace, _ := strconv.Atoi(string(m[1]))
	if stop := drainTimeout + closeTimeout + shutdownTimeout; stop >= time.Duration(grace)*time.Second {
		t.Errorf("the stop takes up to %v, the pod's grace is %d s", stop, grace)
	}
}
