// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
	"github.com/hmdsefi/tenure/examples/internal/orderbook"
	"github.com/hmdsefi/tenure/viewhttp"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestParseEnv(t *testing.T) {
	cfg, err := parseEnv(envOf(map[string]string{"POD_NAME": "client-0"}), "tenure-orderbook\n")
	if err != nil {
		t.Fatalf("parseEnv: %v", err)
	}
	if cfg.podName != "client-0" || cfg.rate != defaultRate || cfg.duration != 0 ||
		cfg.fallback != defaultFallback || cfg.namespace != "tenure-orderbook" ||
		cfg.membership != membershipDirect || cfg.relay != defaultRelay {
		t.Errorf("parseEnv with defaults = %+v", cfg)
	}
	cfg, err = parseEnv(envOf(map[string]string{
		"POD_NAME": "client-0", "RATE": "2.5", "DURATION": "90s", "FALLBACK_URL": "http://ob:8080/",
	}), "ns")
	if err != nil || cfg.rate != 2.5 || cfg.duration != 90*time.Second || cfg.fallback != "http://ob:8080" {
		t.Errorf("parseEnv = %+v, %v", cfg, err)
	}
	cfg, err = parseEnv(envOf(map[string]string{
		"POD_NAME": "client-0", "MEMBERSHIP": "relay", "RELAY_URL": "http://relay:8080/tenure",
	}), "ns")
	if err != nil || cfg.membership != membershipRelay || cfg.relay != "http://relay:8080/tenure" {
		t.Errorf("parseEnv with the relay = %+v, %v", cfg, err)
	}
	// The relay needs no Kubernetes client, so it works outside a cluster too.
	if m, err := membership(cfg); err != nil {
		t.Errorf("membership with the relay: %v", err)
	} else if _, ok := m.(*viewhttp.Membership); !ok {
		t.Errorf("membership with the relay is a %T, want a *viewhttp.Membership", m)
	}
	for _, c := range []struct {
		name string
		env  map[string]string
		ns   string
		want string
	}{
		{"no pod name", map[string]string{}, "ns", "POD_NAME"},
		{"no namespace", map[string]string{"POD_NAME": "p"}, " \n", "namespace"},
		{"bad rate", map[string]string{"POD_NAME": "p", "RATE": "fast"}, "ns", "RATE"},
		{"zero rate", map[string]string{"POD_NAME": "p", "RATE": "0"}, "ns", "RATE"},
		{"bad duration", map[string]string{"POD_NAME": "p", "DURATION": "long"}, "ns", "DURATION"},
		{"negative duration", map[string]string{"POD_NAME": "p", "DURATION": "-1s"}, "ns", "DURATION"},
		{"bad membership", map[string]string{"POD_NAME": "p", "MEMBERSHIP": "gossip"}, "ns", "MEMBERSHIP"},
	} {
		if _, err := parseEnv(envOf(c.env), c.ns); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %v, want one naming %q", c.name, err, c.want)
		}
	}
}

func TestRunWithoutNamespace(t *testing.T) {
	readFile := func(string) ([]byte, error) { return nil, os.ErrNotExist }
	if err := run(t.Context(), envOf(nil), readFile); err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Errorf("run without the namespace file: %v", err)
	}
}

// orderbookCluster starts three orderbook nodes in process over one journal.
func orderbookCluster(t *testing.T, db *fencedb.DB) *demo.Cluster {
	t.Helper()
	books := make([]*orderbook.Orderbook, 3)
	for i := range books {
		books[i] = orderbook.New(orderbook.NewDBJournal(db))
	}
	c, err := demo.Start(t.Context(), demo.Options{
		Nodes:  len(books),
		Spaces: func(i int) []tenure.Space { return []tenure.Space{books[i].Space()} },
		Serve: func(i int, n *tenure.Node) http.Handler {
			return orderbook.NewServer(n, books[i], n.Table().Member.ID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})
	return c
}

// getStats reads GET /stats of base.
func getStats(t *testing.T, base string) (Stats, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/stats", nil)
	if err != nil {
		return Stats{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Stats{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var st Stats
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

// TestServe runs a client against an in-process orderbook: it routes, counts every order, sets
// done when its context ends, serves the final stats while it lingers, and returns.
func TestServe(t *testing.T) {
	db := fencedb.New()
	c := orderbookCluster(t, db)

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	cfg := config{podName: "client-0", rate: 100, fallback: c.URL(0), namespace: "ns"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, cfg, c.Groups[0].Share(), ln, 2*time.Second, tenure.WithRouterTiming(demo.TestTiming()))
	}()

	deadline := time.Now().Add(20 * time.Second)
	for {
		st, err := getStats(t, base)
		if err == nil && st.OK >= 50 {
			if len(st.Routes) != orderbook.Shards {
				t.Errorf("stats have %d routes, want %d", len(st.Routes), orderbook.Shards)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no 50 orders ok: %+v, %v", st, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	var final Stats
	for {
		st, err := getStats(t, base)
		if err == nil && st.Done {
			final = st
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never done: %+v, %v", st, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if final.OK != final.Sent || len(final.Failed) != 0 {
		t.Errorf("final stats %+v: want every order sent ok", final)
	}
	finished := int64(0)
	for _, n := range final.Sends {
		finished += n
	}
	if finished != final.Sent {
		t.Errorf("the sends histogram counts %d orders, %d were sent", finished, final.Sent)
	}
	timed := int64(0)
	for _, n := range final.Latency {
		timed += n
	}
	if timed != final.Sent {
		t.Errorf("the latency histogram counts %d orders, %d were sent", timed, final.Sent)
	}
	if final.LookupNs <= 0 {
		t.Errorf("the stats have no lookup time: %v", final.LookupNs)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after it lingered")
	}

	// The journal holds exactly the orders that got a 200, each once.
	n := 0
	for s := range orderbook.Shards {
		lines, err := orderbook.NewDBJournal(db).Entries(t.Context(), s)
		if err != nil {
			t.Fatal(err)
		}
		n += len(lines)
	}
	if int64(n) != final.OK {
		t.Errorf("the journal has %d orders, the client counted %d ok", n, final.OK)
	}
}

// TestLoadDuration: a load with a duration stops by itself.
func TestLoadDuration(t *testing.T) {
	c := orderbookCluster(t, fencedb.New())
	l := newLoad(&demo.Client{}, c.URL(0)+orderbook.OrdersPath, "p", 50)
	l.duration = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	l.run(ctx)
	st := l.stats()
	if !st.Done || st.Sent == 0 || st.OK != st.Sent || ctx.Err() != nil {
		t.Errorf("stats %+v after a 300 ms load", st)
	}
}

// TestLoadFailures counts orders that fail by their reason, and a 409 after a lost answer as ok.
func TestLoadFailures(t *testing.T) {
	l := newLoad(nil, "", "p", 1)
	l.finish(1, 0, http.StatusConflict, false, time.Millisecond, nil)
	l.finish(2, 1, 0, false, 20*time.Second, context.DeadlineExceeded)
	l.finish(1, 0, 0, false, time.Millisecond, errors.New("boom"))
	l.finish(3, 2, http.StatusConflict, true, 3*time.Millisecond, nil) // a resend after a lost answer
	l.retry503(http.StatusServiceUnavailable)
	l.retry503(http.StatusOK)
	st := l.stats()
	want := map[string]int64{"status 409": 1, "timeout": 1, "error": 1}
	if !reflect.DeepEqual(st.Failed, want) || st.Sends[1] != 2 || st.Hops[1] != 1 || st.Retries503 != 1 || st.OK != 1 || st.Resent409 != 1 || st.Sends[3] != 1 {
		t.Errorf("stats %+v", st)
	}
	wantLatency := map[int]int64{latencyBound(time.Millisecond): 2, latencyBound(3 * time.Millisecond): 1, latencyBound(20 * time.Second): 1}
	if !reflect.DeepEqual(st.Latency, wantLatency) {
		t.Errorf("latency %v, want %v", st.Latency, wantLatency)
	}
}

// TestLatencyBound: a bucket's bound is at least the latency it holds and at most about 5%
// above it, and bounds grow with the latency.
func TestLatencyBound(t *testing.T) {
	if got := latencyBound(0); got != 1 {
		t.Errorf("latencyBound(0) = %d, want 1", got)
	}
	prev := 0
	for d := time.Microsecond; d < time.Minute; d = d*21/20 + time.Microsecond {
		b := latencyBound(d)
		us := float64(d) / float64(time.Microsecond)
		if float64(b) < us || float64(b) > us*1.06+1 {
			t.Fatalf("latencyBound(%v) = %d µs, want in [%.0f, %.0f]", d, b, us, us*1.06+1)
		}
		if b < prev {
			t.Fatalf("latencyBound(%v) = %d, below the bound %d of a shorter latency", d, b, prev)
		}
		prev = b
	}
}

// TestLoadLostAnswer: an order whose answer is lost is sent again, and a 409 for the resend
// counts it ok, as journaled by the lost send.
func TestLoadLostAnswer(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				_ = conn.Close() // the answer is lost
			}
			return
		}
		http.Error(w, "duplicate", http.StatusConflict)
	}))
	t.Cleanup(srv.Close)
	l := newLoad(&demo.Client{HTTP: srv.Client()}, srv.URL+orderbook.OrdersPath, "p", 1)
	l.order(t.Context(), orderbook.Order{ID: "p-0", Market: "mkt-00", Side: orderbook.SideBuy, Price: 1, Qty: 1})
	st := l.stats()
	if st.OK != 1 || st.Resent409 != 1 || len(st.Failed) != 0 || st.Sends[2] != 1 || len(st.Errors) != 1 {
		t.Errorf("stats %+v, want one order ok in 2 sends through a resent 409", st)
	}
}

// readRole returns the rules of the Role in a multi-document YAML file.
func readRole(t *testing.T, path string) []rbacv1.PolicyRule {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	for {
		var r rbacv1.Role
		if err := dec.Decode(&r); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if r.Kind == "Role" {
			return r.Rules
		}
	}
	t.Fatalf("%s has no Role", path)
	return nil
}

// TestReadOnlyRBACCopy checks that the client's Role grants what the k8s module's
// rbac-readonly.yaml grants: kustomize cannot load a file outside its directory, so the deploy
// directory holds a copy.
func TestReadOnlyRBACCopy(t *testing.T) {
	want := readRole(t, "../../../../k8s/rbac-readonly.yaml")
	got := readRole(t, "../deploy/rbac-readonly.yaml")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("deploy/rbac-readonly.yaml grants %+v, k8s/rbac-readonly.yaml %+v", got, want)
	}
}
