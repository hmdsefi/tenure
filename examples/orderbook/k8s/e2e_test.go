// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/hmdsefi/tenure/examples/internal/orderbook"
	"github.com/hmdsefi/tenure/k8s"
)

const (
	e2eNamespace = "tenure-orderbook"
	termKey      = "tenure.term"
	restartKey   = "kubectl.kubernetes.io/restartedAt"

	orderbookDeploy = "orderbook"
	clientDeploy    = "client"
	clientApp       = "client"
	journalsvcApp   = "journalsvc"
	leasePrefix     = "orderbook-markets-"
	remotePort      = "8080"

	clientReplicas = 2

	startupLimit  = 2 * time.Minute  // how long the deployment may take to be ready and converged
	rolloutLimit  = 3 * time.Minute  // how long a rollout may take
	doneLimit     = 40 * time.Second // how long a stopped client may take to say done
	steadyWindow  = 15 * time.Second // the steady-state measurement
	settle        = 10 * time.Second // load after the rollout finished, before the rollout's counts
	quietFor      = 5 * time.Second  // how long the deployment must be quiet for a steady state
	pollPause     = 250 * time.Millisecond
	minOneSend    = 0.99 // the share of steady-state orders that must take one send
	maxRolloutHop = 2    // 1 + the highest hop: at most 3 sends during the rollout
)

// stats is a client pod's GET /stats, as orderbook/k8s/client serves it.
type stats struct {
	Pod        string           `json:"pod"`
	Sent       int64            `json:"sent"`
	OK         int64            `json:"ok"`
	Failed     map[string]int64 `json:"failed"`
	Errors     []string         `json:"errors"`
	Sends      map[int]int64    `json:"sends"`
	Hops       map[int]int64    `json:"hops"`
	Retries503 int64            `json:"retries503"`
	Resent409  int64            `json:"resent409"`
	Done       bool             `json:"done"`
	Routes     []string         `json:"routes"`
	Stale      bool             `json:"stale"`
}

// window is what several clients did between two readings of their stats.
type window struct {
	orders, oneSend, sends, retries503, resent409 int64
	maxHop                                        int
	failed                                        map[string]int64
}

// diff returns what happened between the readings before and after, of the same pods.
func diff(before, after map[string]stats) window {
	w := window{failed: map[string]int64{}}
	for pod, a := range after {
		b := before[pod]
		for n, c := range a.Sends {
			d := c - b.Sends[n]
			w.orders += d
			w.sends += int64(n) * d
			if n == 1 {
				w.oneSend += d
			}
		}
		for h, c := range a.Hops {
			if c > b.Hops[h] && h > w.maxHop {
				w.maxHop = h
			}
		}
		for r, c := range a.Failed {
			if d := c - b.Failed[r]; d > 0 {
				w.failed[r] += d
			}
		}
		w.retries503 += a.Retries503 - b.Retries503
		w.resent409 += a.Resent409 - b.Resent409
	}
	return w
}

func (w window) String() string {
	share := 0.0
	if w.orders > 0 {
		share = float64(w.oneSend) / float64(w.orders)
	}
	return fmt.Sprintf("%d orders, %d with one send (%.2f%%), %d sends, %d 503 retries, %d ok by a 409 after a lost answer, highest hop %d, failed %v",
		w.orders, w.oneSend, 100*share, w.sends, w.retries503, w.resent409, w.maxHop, w.failed)
}

// e2e is the test's handle on the deployment.
type e2e struct {
	t      *testing.T
	client kubernetes.Interface
}

// clientPods returns the names of the client pods that are Ready and not being deleted.
func (e *e2e) clientPods(ctx context.Context) ([]string, error) {
	list, err := e.client.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{LabelSelector: "app=" + clientApp})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, p := range list.Items {
		if p.DeletionTimestamp == nil && podReady(&p) {
			names = append(names, p.Name)
		}
	}
	slices.Sort(names)
	return names, nil
}

func podReady(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// get reads path of pod's port 8080 through the API server's proxy.
func (e *e2e) get(ctx context.Context, pod, path string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return e.client.CoreV1().Pods(e2eNamespace).ProxyGet("http", pod, remotePort, path, nil).DoRaw(ctx)
}

// stats reads the stats of a client pod.
func (e *e2e) stats(ctx context.Context, pod string) (stats, error) {
	raw, err := e.get(ctx, pod, "/stats")
	if err != nil {
		return stats{}, err
	}
	var st stats
	if err := json.Unmarshal(raw, &st); err != nil {
		return stats{}, fmt.Errorf("stats of %s: %w", pod, err)
	}
	return st, nil
}

// allStats reads the stats of every pod in pods.
func (e *e2e) allStats(ctx context.Context, pods []string) map[string]stats {
	e.t.Helper()
	out := make(map[string]stats, len(pods))
	for _, pod := range pods {
		st, err := e.stats(ctx, pod)
		if err != nil {
			e.t.Fatalf("stats of %s: %v", pod, err)
		}
		out[pod] = st
	}
	return out
}

// until polls cond every pollPause until it returns nil, and fails the test with its last
// error when limit passes first.
func (e *e2e) until(ctx context.Context, limit time.Duration, what string, cond func(context.Context) error) {
	e.t.Helper()
	if err := poll(ctx, limit, what, cond); err != nil {
		e.t.Fatal(err)
	}
}

// poll polls cond every pollPause until it returns nil, and returns its last error when limit
// passes first.
func poll(ctx context.Context, limit time.Duration, what string, cond func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	for {
		err := cond(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: not in %v: %w", what, limit, err)
		case <-time.After(pollPause):
		}
	}
}

// holders returns the member that holds each shard's Lease, "" for a shard with none, and each
// shard's term.
func (e *e2e) holders(ctx context.Context) ([]string, []string, error) {
	list, err := e.client.CoordinationV1().Leases(e2eNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, err
	}
	out, terms := make([]string, orderbook.Shards), make([]string, orderbook.Shards)
	for _, l := range list.Items {
		shard, err := strconv.Atoi(strings.TrimPrefix(l.Name, leasePrefix))
		if !strings.HasPrefix(l.Name, leasePrefix) || err != nil || shard < 0 || shard >= orderbook.Shards {
			continue
		}
		if l.Spec.HolderIdentity != nil && l.Annotations[termKey] != "" {
			// The holder is <member ID>/<session>.
			out[shard], _, _ = strings.Cut(*l.Spec.HolderIdentity, "/")
			terms[shard] = l.Annotations[termKey]
		}
	}
	return out, terms, nil
}

// converged waits until the deployment is quiet and the clients' routers agree with it, and
// returns the client pods: there are clientReplicas Ready client pods, the Router of each routes
// every shard to the holder of its Lease, and for quietFor neither a shard's term nor any
// client's 503 retries have changed. The terms settle once the slots have placed the shards;
// the 503 retries once no order is still waiting for a shard that a forming group has not
// started yet.
func (e *e2e) converged(ctx context.Context) []string {
	e.t.Helper()
	var (
		pods    []string
		last    string // the terms and the 503 retries at the last change
		changed time.Time
	)
	e.until(ctx, startupLimit, "the clients' routers to converge", func(ctx context.Context) error {
		var err error
		if pods, err = e.clientPods(ctx); err != nil {
			return err
		}
		if len(pods) != clientReplicas {
			return fmt.Errorf("ready client pods %v", pods)
		}
		holders, terms, err := e.holders(ctx)
		if err != nil {
			return err
		}
		if slices.Contains(holders, "") {
			return fmt.Errorf("lease holders %v", holders)
		}
		now := fmt.Sprint(terms)
		for _, pod := range pods {
			st, err := e.stats(ctx, pod)
			if err != nil {
				return err
			}
			if st.Stale || !slices.Equal(st.Routes, holders) {
				return fmt.Errorf("%s routes %v (stale %v), lease holders %v", pod, st.Routes, st.Stale, holders)
			}
			now += fmt.Sprintf(" %s:%d", pod, st.Retries503)
		}
		if now != last {
			last, changed = now, time.Now()
		}
		if quiet := time.Since(changed); quiet < quietFor {
			return fmt.Errorf("terms or 503 retries changed %v ago: %s", quiet.Round(time.Millisecond), now)
		}
		return nil
	})
	return pods
}

// rolloutDone reports whether d's latest generation is fully rolled out.
func rolloutDone(d *appsv1.Deployment) bool {
	want := int32(1)
	if d.Spec.Replicas != nil {
		want = *d.Spec.Replicas
	}
	s := d.Status
	return s.ObservedGeneration >= d.Generation && s.UpdatedReplicas == want && s.Replicas == want && s.AvailableReplicas == want
}

// waitRollout waits until deploy is fully rolled out.
func (e *e2e) waitRollout(ctx context.Context, deploy string) {
	e.t.Helper()
	e.until(ctx, rolloutLimit, "the rollout of deploy/"+deploy, func(ctx context.Context) error {
		d, err := e.client.AppsV1().Deployments(e2eNamespace).Get(ctx, deploy, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !rolloutDone(d) {
			return fmt.Errorf("status %+v", d.Status)
		}
		return nil
	})
}

// restart restarts the rollout of deploy, as kubectl rollout restart does, and waits until it
// has finished.
func (e *e2e) restart(ctx context.Context, deploy string) {
	e.t.Helper()
	patch := fmt.Appendf(nil, `{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`, restartKey, time.Now().UTC().Format(time.RFC3339Nano))
	if _, err := e.client.AppsV1().Deployments(e2eNamespace).Patch(ctx, deploy, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil {
		e.t.Fatalf("restart deploy/%s: %v", deploy, err)
	}
	e.waitRollout(ctx, deploy)
}

// scale sets the replicas of deploy.
func (e *e2e) scale(ctx context.Context, deploy string, n int) error {
	patch := fmt.Appendf(nil, `{"spec":{"replicas":%d}}`, n)
	_, err := e.client.AppsV1().Deployments(e2eNamespace).Patch(ctx, deploy, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

// finalStats waits until a stopped client pod says done, and returns those stats. It does not
// fail the test, so it can run on a goroutine of its own.
func (e *e2e) finalStats(ctx context.Context, pod string) (stats, error) {
	var final stats
	err := poll(ctx, doneLimit, "the final stats of "+pod, func(ctx context.Context) error {
		st, err := e.stats(ctx, pod)
		if err != nil {
			return err
		}
		if !st.Done {
			return fmt.Errorf("%s: %d sent, %d ok, not done", pod, st.Sent, st.OK)
		}
		final = st
		return nil
	})
	return final, err
}

// journal reads every shard's journal from the journal service through the API server's proxy.
func (e *e2e) journal(ctx context.Context) [][][]byte {
	e.t.Helper()
	var all [][][]byte
	e.until(ctx, time.Minute, "the journal", func(ctx context.Context) error {
		list, err := e.client.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{LabelSelector: "app=" + journalsvcApp})
		if err != nil {
			return err
		}
		if len(list.Items) != 1 {
			return fmt.Errorf("%d journalsvc pods, want 1", len(list.Items))
		}
		raw, err := e.get(ctx, list.Items[0].Name, orderbook.JournalPath)
		if err != nil {
			return err
		}
		var resp struct {
			Shards [][]json.RawMessage `json:"shards"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			return err
		}
		all = make([][][]byte, len(resp.Shards))
		for i, s := range resp.Shards {
			for _, l := range s {
				all[i] = append(all[i], l)
			}
		}
		return nil
	})
	return all
}

// checkJournal checks the journal against the clients' final stats: every shard's journal
// replays to its fills, no order ID is journaled twice, and each client pod's journaled orders,
// <pod>-<k>, are as many as the orders it counted ok. Orders of pods that are not in finals
// (earlier runs on the same deployment) are only checked for replay and uniqueness.
func checkJournal(t *testing.T, all [][][]byte, finals map[string]stats) {
	t.Helper()
	ids := map[string]int{}
	perPod := map[string]int64{}
	lines := 0
	for shard, ls := range all {
		lines += len(ls)
		if err := orderbook.Replay(ls); err != nil {
			t.Errorf("shard %d: the journal does not replay: %v", shard, err)
		}
		for _, l := range ls {
			var en orderbook.Entry
			if err := json.Unmarshal(l, &en); err != nil {
				t.Fatalf("shard %d: journal line %q: %v", shard, l, err)
			}
			ids[en.Order.ID]++
			if i := strings.LastIndexByte(en.Order.ID, '-'); i > 0 {
				perPod[en.Order.ID[:i]]++
			}
		}
	}
	for id, n := range ids {
		if n != 1 {
			t.Errorf("order %s is journaled %d times", id, n)
		}
	}
	for pod, st := range finals {
		if perPod[pod] != st.OK {
			t.Errorf("client %s counted %d orders ok, the journal holds %d of its orders", pod, st.OK, perPod[pod])
		}
	}
	t.Logf("journal: %d orders in %d shards", lines, len(all))
}

// TestOrderbookKubernetes proves AT-RTR-19 on a Kubernetes cluster where the manifests in deploy
// were applied: two client pods, each with a Router on a watch-only Adapter and no proxy, send
// orders at a fixed rate. In steady state at least 99% of the orders take one send; a rollout
// restart of the orderbook (slots on) under that load loses and duplicates no order and takes no
// order to more than 3 sends; deleting each client pod in turn, which the Deployment replaces,
// fails no order. No order ends with a 409 (a client counts a 409 ok only for a resend after a lost
// answer, and the test logs those). The journal check reads the journal service: unique IDs, and
// each client pod's ok count equals its journaled orders. The steps were written for this test from the text of
// issue #385.
func TestOrderbookKubernetes(t *testing.T) {
	path := kubeconfig(t)
	config, err := k8s.RESTConfig(clientcmd.BuildConfigFromFlags("", path))
	if err != nil {
		t.Fatalf("kubeconfig %s: %v", path, err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	e := &e2e{t: t, client: client}
	t.Cleanup(func() {
		// The test ends by scaling the clients to 0; give the deployment its clients back.
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		if err := e.scale(cctx, clientDeploy, clientReplicas); err != nil {
			t.Logf("scale deploy/%s back to %d: %v", clientDeploy, clientReplicas, err)
		}
	})
	e.waitRollout(ctx, clientDeploy)
	finals := map[string]stats{} // the final stats of every client pod of this run

	// Steady state: both clients are ready and route as the Leases say.
	pods := e.converged(ctx)
	before := e.allStats(ctx, pods)
	time.Sleep(steadyWindow)
	steady := diff(before, e.allStats(ctx, pods))
	t.Logf("steady state: %v", steady)
	if steady.orders == 0 {
		t.Fatal("no order finished in the steady state")
	}
	if share := float64(steady.oneSend) / float64(steady.orders); share < minOneSend {
		t.Errorf("%.2f%% of the steady-state orders took one send, want at least %.0f%%", 100*share, 100*minOneSend)
	}
	if len(steady.failed) > 0 {
		t.Errorf("orders failed in the steady state: %v", steady.failed)
	}

	// A rollout restart of the orderbook under load.
	before = e.allStats(ctx, pods)
	e.restart(ctx, orderbookDeploy)
	time.Sleep(settle)
	rollout := diff(before, e.allStats(ctx, pods))
	t.Logf("orderbook rollout: %v", rollout)
	if rollout.maxHop > maxRolloutHop {
		t.Errorf("an order took %d sends during the rollout (highest hop %d), want at most %d",
			rollout.maxHop+1, rollout.maxHop, maxRolloutHop+1)
	}
	if len(rollout.failed) > 0 {
		t.Errorf("orders failed during the rollout: %v", rollout.failed)
	}

	// Delete each client pod in turn under load; the Deployment replaces it.
	for _, pod := range pods {
		if err := client.CoreV1().Pods(e2eNamespace).Delete(ctx, pod, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete client pod %s: %v", pod, err)
		}
		st, err := e.finalStats(ctx, pod)
		if err != nil {
			t.Fatal(err)
		}
		finals[pod] = st
		t.Logf("client %s deleted: %d sent, %d ok, failed %v", pod, finals[pod].Sent, finals[pod].OK, finals[pod].Failed)
		e.waitRollout(ctx, clientDeploy)
	}

	// Stop the clients of the replacement pods, and read their final stats too.
	last := e.converged(ctx)
	if err := e.scale(ctx, clientDeploy, 0); err != nil {
		t.Fatalf("scale deploy/%s to 0: %v", clientDeploy, err)
	}
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		errs []error
	)
	for _, pod := range last {
		wg.Go(func() {
			st, err := e.finalStats(ctx, pod)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			finals[pod] = st
		})
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	for _, pod := range slices.Sorted(maps.Keys(finals)) {
		st := finals[pod]
		if len(st.Failed) > 0 || st.OK != st.Sent {
			t.Errorf("client %s: %d sent, %d ok, failed %v (last errors %q)", pod, st.Sent, st.OK, st.Failed, st.Errors)
		}
	}
	whole := diff(map[string]stats{}, finals)
	t.Logf("all clients: %v", whole)

	checkJournal(t, e.journal(ctx), finals)
}
