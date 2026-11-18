// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/hmdsefi/tenure/k8s"
)

// envLoadResults names the file the load test writes its results to. The load test runs only
// when it is set, as the router-load workflow does, and then needs a cluster as the other
// external tests do.
const envLoadResults = "TENURE_LOAD_RESULTS"

const (
	// loadWindow is how long each configuration is measured, once its clients have converged.
	loadWindow = 60 * time.Second

	// loadRate is the orders per second of all client pods together, whatever their number, so
	// that the orderbook and its journal carry the same load in every configuration.
	loadRate = 80

	// loadSettle is how long the cluster runs with no client pod before the baseline is
	// measured, so that the watches of the last clients are gone.
	loadSettle = 10 * time.Second

	// loadLimit bounds the whole test: seven measured windows and the rollouts between them.
	loadLimit = 45 * time.Minute

	// loadRolloutLimit bounds how long the client pods of one configuration may take to start
	// or to go.
	loadRolloutLimit = 5 * time.Minute
)

// loadRouters are the numbers of client pods the load test runs, in each mode.
var loadRouters = []int{2, 8, 32}

// loadModes are the client's MEMBERSHIP values: a watch-only k8s Adapter in each pod, or the
// orderbook pods' view relay.
var loadModes = []string{"direct", "relay"}

// loadResult is what one configuration measured.
type loadResult struct {
	mode    string // "" for the baseline, with no client pod
	routers int
	elapsed time.Duration
	events  float64 // Lease watch events per second that the API server sent, to every watcher
	w       window
	latency map[int]int64 // the orders by latency bucket, as the clients count them
	lookup  float64       // the mean of the client pods' lookup times, in ns
}

// leaseWatchEvents returns how many watch events of Leases the API server has sent so far, to
// every watcher together, from its apiserver_watch_events_total metric.
func (e *e2e) leaseWatchEvents(ctx context.Context) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	raw, err := e.client.CoreV1().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		return 0, fmt.Errorf("the API server's metrics: %w", err)
	}
	return sumLeaseWatchEvents(raw)
}

// sumLeaseWatchEvents sums the samples of apiserver_watch_events_total for Leases in a
// Prometheus text exposition. Kubernetes labels them by resource ("leases") in recent versions
// and by kind ("Lease") in older ones.
func sumLeaseWatchEvents(raw []byte) (float64, error) {
	const name = "apiserver_watch_events_total{"
	sum, found := 0.0, false
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name) || !strings.Contains(line, `group="coordination.k8s.io"`) {
			continue
		}
		if !strings.Contains(line, `resource="leases"`) && !strings.Contains(line, `kind="Lease"`) {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			return 0, fmt.Errorf("metric line %q: %w", line, err)
		}
		sum, found = sum+v, true
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("no %s sample for coordination.k8s.io leases", strings.TrimSuffix(name, "{"))
	}
	return sum, nil
}

// percentile returns the upper bound, in microseconds, of the latency bucket that holds the
// q-quantile of the orders in hist, 0 for none.
func percentile(hist map[int]int64, q float64) int {
	var total int64
	for _, n := range hist {
		total += n
	}
	if total == 0 {
		return 0
	}
	rank := int64(q * float64(total))
	if rank >= total {
		rank = total - 1
	}
	var seen int64
	for _, bound := range slices.Sorted(maps.Keys(hist)) {
		seen += hist[bound]
		if seen > rank {
			return bound
		}
	}
	return 0
}

// latencyDiff returns the latency buckets of the orders that finished between before and after.
func latencyDiff(before, after map[string]stats) map[int]int64 {
	out := map[int]int64{}
	for pod, a := range after {
		for bound, n := range a.Latency {
			if d := n - before[pod].Latency[bound]; d > 0 {
				out[bound] += d
			}
		}
	}
	return out
}

// configureClients sets the client Deployment to n pods that follow the group in mode and
// together send loadRate orders per second. It stops every client pod first, so that no pod of
// the last configuration runs in the next one, and waits until the new pods are available.
func (e *e2e) configureClients(ctx context.Context, n int, mode string) {
	e.t.Helper()
	rate := strconv.FormatFloat(loadRate/float64(max(n, 1)), 'f', -1, 64)
	e.patchClients(ctx, fmt.Sprintf(`{"spec":{"replicas":0,"template":{"spec":{"containers":[{"name":"client","env":[{"name":"MEMBERSHIP","value":%q},{"name":"RATE","value":%q}]}]}}}}`, mode, rate))
	e.until(ctx, loadRolloutLimit, "the client pods to go", func(ctx context.Context) error {
		list, err := e.client.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{LabelSelector: "app=" + clientApp})
		if err != nil {
			return err
		}
		if len(list.Items) > 0 {
			return fmt.Errorf("%d client pods left", len(list.Items))
		}
		return nil
	})
	if n == 0 {
		return
	}
	e.patchClients(ctx, fmt.Sprintf(`{"spec":{"replicas":%d}}`, n))
	e.until(ctx, loadRolloutLimit, "the rollout of deploy/"+clientDeploy, func(ctx context.Context) error {
		d, err := e.client.AppsV1().Deployments(e2eNamespace).Get(ctx, clientDeploy, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !rolloutDone(d) {
			return fmt.Errorf("status %+v", d.Status)
		}
		return nil
	})
}

// patchClients applies a strategic merge patch to the client Deployment.
func (e *e2e) patchClients(ctx context.Context, patch string) {
	e.t.Helper()
	if _, err := e.client.AppsV1().Deployments(e2eNamespace).Patch(ctx, clientDeploy, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		e.t.Fatalf("patch deploy/%s: %v", clientDeploy, err)
	}
}

// measure runs one window: it reads the Lease watch events and the stats of pods, waits
// loadWindow and reads them again.
func (e *e2e) measure(ctx context.Context, mode string, pods []string) loadResult {
	e.t.Helper()
	before := e.allStats(ctx, pods)
	evBefore, err := e.leaseWatchEvents(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	start := time.Now()
	time.Sleep(loadWindow)
	evAfter, err := e.leaseWatchEvents(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	elapsed := time.Since(start)
	after := e.allStats(ctx, pods)
	res := loadResult{
		mode: mode, routers: len(pods), elapsed: elapsed,
		events:  (evAfter - evBefore) / elapsed.Seconds(),
		w:       diff(before, after),
		latency: latencyDiff(before, after),
	}
	for _, st := range after {
		res.lookup += st.LookupNs / float64(len(after))
	}
	return res
}

// TestATRTR21LoadKubernetes is AT-RTR-21's load test, on a cluster where the manifests in
// deploy-load were applied: five orderbook pods, and a client Deployment that the test scales.
// It measures the Lease watch events that the API server sends per second, to every watcher, with
// no client pod (the members' own watches and the cluster's) and then with R = 2, 8 and 32
// client pods that each embed a Router, once following the group with a watch-only Adapter of
// their own (direct) and once through the orderbook pods' view relay (relay). The clients send
// loadRate orders per second together; each window also reports the orders' p50 and p99
// latency, their sends per order and the clients' mean lookup time. It writes the results as
// Markdown to the file TENURE_LOAD_RESULTS names, and checks only that every configuration
// converged and failed no order: the numbers are published, not gated. The steps were written
// for this test from the text of issue #391.
func TestATRTR21LoadKubernetes(t *testing.T) {
	out := os.Getenv(envLoadResults)
	if out == "" {
		t.Skipf("%s is not set; the load test runs only in the router-load workflow", envLoadResults)
	}
	path := kubeconfig(t)
	config, err := k8s.RESTConfig(clientcmd.BuildConfigFromFlags("", path))
	if err != nil {
		t.Fatalf("kubeconfig %s: %v", path, err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), loadLimit)
	defer cancel()
	e := &e2e{t: t, client: client}
	t.Cleanup(func() {
		// Give the deployment its two direct clients back, as the manifests have them.
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		patch := fmt.Appendf(nil, `{"spec":{"replicas":%d,"template":{"spec":{"containers":[{"name":"client","env":[{"name":"MEMBERSHIP","value":"direct"},{"name":"RATE","value":"40"}]}]}}}}`, clientReplicas)
		if _, err := client.AppsV1().Deployments(e2eNamespace).Patch(cctx, clientDeploy, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil {
			t.Logf("restore deploy/%s: %v", clientDeploy, err)
		}
	})
	e.waitRollout(ctx, orderbookDeploy)
	ob, err := client.AppsV1().Deployments(e2eNamespace).Get(ctx, orderbookDeploy, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	members := int(*ob.Spec.Replicas)
	version, err := client.Discovery().ServerVersion()
	if err != nil {
		t.Fatal(err)
	}

	// The baseline: no client pod.
	e.configureClients(ctx, 0, loadModes[0])
	time.Sleep(loadSettle)
	results := []loadResult{e.measure(ctx, "", nil)}
	t.Logf("baseline: %.1f Lease watch events/s", results[0].events)

	for _, mode := range loadModes {
		for _, n := range loadRouters {
			e.configureClients(ctx, n, mode)
			pods := e.convergedN(ctx, n)
			res := e.measure(ctx, mode, pods)
			results = append(results, res)
			t.Logf("%s, %d routers: %.1f Lease watch events/s; %v; p50 %d µs, p99 %d µs; lookup %.1f ns",
				mode, n, res.events, res.w, percentile(res.latency, 0.5), percentile(res.latency, 0.99), res.lookup)
			if len(res.w.failed) > 0 {
				t.Errorf("%s, %d routers: orders failed: %v", mode, n, res.w.failed)
			}
		}
	}

	report := loadReport(results, members, version.GitVersion)
	t.Log("\n" + report)
	if err := os.WriteFile(out, []byte(report), 0o644); err != nil { //nolint:gosec // a report, not a secret
		t.Fatal(err)
	}
}

// loadReport formats the results as Markdown: the configuration, then one table row per
// configuration. The first result is the baseline.
func loadReport(results []loadResult, members int, version string) string {
	var b strings.Builder
	base := results[0]
	fmt.Fprintf(&b, "Kubernetes %s on kind, %d orderbook pods (M), %d orders/s from all client pods together, %v windows.\n\n",
		version, members, loadRate, loadWindow)
	fmt.Fprintf(&b, "Baseline with no client pod: %.1f Lease watch events/s (the members' own watches and the cluster's).\n\n", base.events)
	b.WriteString("| Membership | R | Lease watch events/s | above baseline | per router | R×M/2 | orders | p50 | p99 | sends per order | one send | lookup |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range results[1:] {
		above := r.events - base.events
		oneSend := 0.0
		perOrder := 0.0
		if r.w.orders > 0 {
			oneSend = 100 * float64(r.w.oneSend) / float64(r.w.orders)
			perOrder = float64(r.w.sends) / float64(r.w.orders)
		}
		fmt.Fprintf(&b, "| %s | %d | %.1f | %.1f | %.2f | %.1f | %d | %s | %s | %.3f | %.2f%% | %.0f ns |\n",
			r.mode, r.routers, r.events, above, above/float64(r.routers), float64(r.routers*members)/2,
			r.w.orders, micros(percentile(r.latency, 0.5)), micros(percentile(r.latency, 0.99)),
			perOrder, oneSend, r.lookup)
	}
	return b.String()
}

// micros formats a duration in microseconds as milliseconds.
func micros(us int) string {
	return strconv.FormatFloat(float64(us)/1000, 'f', 2, 64) + " ms"
}

// TestSumLeaseWatchEvents sums the Lease samples of apiserver_watch_events_total, labelled by
// resource or by kind, and fails when there is none.
func TestSumLeaseWatchEvents(t *testing.T) {
	raw := []byte(`# HELP apiserver_watch_events_total [ALPHA] Number of events sent in watch clients
# TYPE apiserver_watch_events_total counter
apiserver_watch_events_total{group="",resource="pods",version="v1"} 900
apiserver_watch_events_total{group="coordination.k8s.io",resource="leases",version="v1"} 1234
apiserver_watch_events_total{group="coordination.k8s.io",kind="Lease",version="v1"} 6
apiserver_watch_events_total{group="coordination.k8s.io",resource="leasecandidates",version="v1beta1"} 50
apiserver_watch_events_sizes_bucket{group="coordination.k8s.io",resource="leases",version="v1",le="1024"} 77
`)
	got, err := sumLeaseWatchEvents(raw)
	if err != nil || got != 1240 {
		t.Errorf("sumLeaseWatchEvents = %v, %v, want 1240", got, err)
	}
	if _, err := sumLeaseWatchEvents([]byte("apiserver_watch_events_total{group=\"\",resource=\"pods\"} 1\n")); err == nil {
		t.Error("sumLeaseWatchEvents with no Lease sample: no error")
	}
}

// TestPercentile reads the bucket bound that holds a quantile.
func TestPercentile(t *testing.T) {
	hist := map[int]int64{100: 50, 200: 49, 5000: 1}
	for _, c := range []struct {
		q    float64
		want int
	}{{0, 100}, {0.5, 200}, {0.98, 200}, {0.99, 5000}, {1, 5000}} {
		if got := percentile(hist, c.q); got != c.want {
			t.Errorf("percentile(%v) = %d, want %d", c.q, got, c.want)
		}
	}
	if got := percentile(nil, 0.5); got != 0 {
		t.Errorf("percentile of no orders = %d, want 0", got)
	}
}

// TestLoadReport formats a baseline and one configuration.
func TestLoadReport(t *testing.T) {
	results := []loadResult{
		{events: 40},
		{mode: "direct", routers: 8, events: 60, lookup: 41.4,
			w:       window{orders: 100, oneSend: 99, sends: 101},
			latency: map[int]int64{1500: 50, 2500: 49, 9000: 1}},
	}
	got := loadReport(results, 5, "v1.37.0")
	want := "| direct | 8 | 60.0 | 20.0 | 2.50 | 20.0 | 100 | 2.50 ms | 9.00 ms | 1.010 | 99.00% | 41 ns |\n"
	if !strings.HasSuffix(got, want) || !strings.Contains(got, "Baseline with no client pod: 40.0 Lease watch events/s") {
		t.Errorf("loadReport =\n%s\nwant it to end with\n%s", got, want)
	}
}
