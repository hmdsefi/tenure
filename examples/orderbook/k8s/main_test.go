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
	"github.com/hmdsefi/tenure/examples/internal/orderbook"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestParseEnv(t *testing.T) {
	good := map[string]string{
		"POD_NAME": "orderbook-0", "POD_IP": "10.1.2.3", "SLOTS": "3", "JOURNAL_URL": "http://journalsvc:8080",
	}
	cfg, err := parseEnv(envOf(good), "tenure-orderbook\n")
	if err != nil {
		t.Fatalf("parseEnv: %v", err)
	}
	if cfg.podName != "orderbook-0" || cfg.slots != 3 || cfg.namespace != "tenure-orderbook" ||
		cfg.addr() != "10.1.2.3:8080" || cfg.journal != "http://journalsvc:8080" {
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
		{"no journal", without("JOURNAL_URL"), "ns", "JOURNAL_URL"},
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

func TestRunWithoutNamespace(t *testing.T) {
	readFile := func(string) ([]byte, error) { return nil, os.ErrNotExist }
	if err := run(t.Context(), envOf(nil), readFile); err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Errorf("run without the namespace file: %v", err)
	}
}

// TestServe runs the pod against a fake Kubernetes API and a journal service in process: it
// becomes ready, takes an order, journals it, and stops on its context.
func TestServe(t *testing.T) {
	svc := httptest.NewServer(orderbook.NewJournalHandler(fencedb.New()))
	t.Cleanup(svc.Close)

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	cfg := config{podName: "orderbook-0", podIP: "127.0.0.1", slots: 1, journal: svc.URL, namespace: "tenure-orderbook"}

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
		resp, err := http.Post(base+orderbook.OrdersPath, "application/json",
			strings.NewReader(`{"id":"o1","market":"mkt-00","side":"buy","price":100,"qty":1}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/orders never answered 200, last %d", resp.StatusCode)
		}
		time.Sleep(50 * time.Millisecond)
	}
	all, err := orderbook.NewHTTPJournal(svc.URL, nil).All(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, lines := range all {
		n += len(lines)
	}
	if n != 1 {
		t.Errorf("the journal has %d lines, want 1", n)
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
// server fit in the terminationGracePeriodSeconds of the orderbook's manifest.
func TestStopFitsTerminationGrace(t *testing.T) {
	raw, err := os.ReadFile("deploy/orderbook.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*terminationGracePeriodSeconds:\s*(\d+)\s*$`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("deploy/orderbook.yaml has no terminationGracePeriodSeconds")
	}
	grace, _ := strconv.Atoi(string(m[1]))
	if stop := drainTimeout + closeTimeout + shutdownTimeout; stop >= time.Duration(grace)*time.Second {
		t.Errorf("the stop takes up to %v, the pod's grace is %d s", stop, grace)
	}
}
