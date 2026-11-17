// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/hmdsefi/tenure/examples/internal/orderbook"
	"github.com/hmdsefi/tenure/k8s"
	"github.com/hmdsefi/tenure/routehttp"
)

const (
	e2eNamespace = "tenure-orderbook"
	termKey      = "tenure.term"
	restartKey   = "kubectl.kubernetes.io/restartedAt"

	orderbookApp    = "orderbook"
	orderbookDeploy = "orderbook"
	journalsvcApp   = "journalsvc"
	leasePrefix     = "orderbook-markets-"

	// workers is the number of goroutines of the test's own load, each sending one order at a
	// time through the API server's proxy.
	workers = 4

	steadyBefore = 5 * time.Second  // orders before the restart
	steadyAfter  = 10 * time.Second // orders after the rollout finished
	rolloutLimit = 3 * time.Minute
	orderLimit   = 90 * time.Second // how long one order is retried
	startupLimit = 90 * time.Second // how long the Leases may take to show up
	retryPause   = 200 * time.Millisecond
	orderPause   = 20 * time.Millisecond
	remotePort   = "8080"
)

// cluster is the test's view of the deployment: a client of the API server, and an HTTP client
// that reaches pods through the API server's proxy.
type cluster struct {
	client kubernetes.Interface
	hc     *http.Client

	mu   sync.Mutex
	byIP map[string]string // pod IP to pod name
}

// proxyURL returns the URL of path on port 8080 of pod, through the API server's proxy.
func (c *cluster) proxyURL(pod, path string) string {
	return c.client.CoreV1().RESTClient().Get().Namespace(e2eNamespace).Resource("pods").
		Name(pod + ":" + remotePort).SubResource("proxy").Suffix(path).URL().String()
}

// readyPods returns the names of the Ready pods of app that are not being deleted, and learns
// the IPs of every pod of app.
func (c *cluster) readyPods(ctx context.Context, app string) ([]string, error) {
	list, err := c.client.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{LabelSelector: "app=" + app})
	if err != nil {
		return nil, err
	}
	var names []string
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range list.Items {
		if p.Status.PodIP != "" {
			c.byIP[p.Status.PodIP] = p.Name
		}
		if p.DeletionTimestamp == nil && podReady(&p) {
			names = append(names, p.Name)
		}
	}
	slices.Sort(names)
	return names, nil
}

// podOf returns the pod whose address is addr (IP:8080), listing the pods again when it does
// not know the IP.
func (c *cluster) podOf(ctx context.Context, addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	name, ok := c.byIP[host]
	c.mu.Unlock()
	if ok {
		return name, nil
	}
	if _, err := c.readyPods(ctx, orderbookApp); err != nil {
		return "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if name, ok := c.byIP[host]; ok {
		return name, nil
	}
	return "", fmt.Errorf("no pod has the address %s", addr)
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

// proxyLoad is the test's own load: orders sent through the API server's proxy to the
// orderbook pods. It follows a 421's Tenure-Route as a client of the routing protocol does, and
// counts the sends and the hops of every order (a followed 421, or a pod that could not be
// reached, adds a hop; a 503 retry does not).
type proxyLoad struct {
	c      *cluster
	prefix string // of the order IDs, t<run>-, unique to the run: the journal outlives a run

	ok       atomic.Int64
	sends    atomic.Int64
	maxHops  atomic.Int64
	mu       sync.Mutex
	accepted map[string]bool // IDs that got a 200, or a 409 on a resend after a lost answer
	failures []string
}

// post sends one order until it gets a 200, or a 409 after a send whose answer was lost, or
// until orderLimit passes.
func (l *proxyLoad) post(ctx context.Context, rng *rand.Rand, o orderbook.Order) error {
	body, err := json.Marshal(o)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, orderLimit)
	defer cancel()
	var (
		pod    string
		hop    int64
		resent bool // an earlier send may have been journaled without its answer
		last   error
	)
	for {
		if pod == "" {
			pods, err := l.c.readyPods(ctx, orderbookApp)
			if err == nil && len(pods) == 0 {
				err = errors.New("no Ready orderbook pod")
			}
			if err != nil {
				last = err
				if !pause(ctx, retryPause) {
					return fmt.Errorf("order %s: %w (last: %v)", o.ID, ctx.Err(), last)
				}
				continue
			}
			pod = pods[rng.IntN(len(pods))]
		}
		status, header, answer, err := l.send(ctx, pod, body)
		l.sends.Add(1)
		switch {
		case err == nil && status == http.StatusOK:
			l.done(o.ID, hop)
			return nil
		case err == nil && status == http.StatusConflict && resent:
			l.done(o.ID, hop)
			return nil
		case err == nil && status == http.StatusMisdirectedRequest:
			// A route to a pod the test cannot name yet sends the order to a random pod.
			pod, _ = l.c.podOf(ctx, header.Get(routehttp.RouteHeader))
			hop++
			continue
		case err == nil && status == http.StatusServiceUnavailable && header.Get("Retry-After") != "":
			// The orderbook's own 503: the shard is moving. Wait and send to the same pod.
			last = fmt.Errorf("503: %s", answer)
		default:
			// The proxy could not reach the pod, or the answer was lost: another pod.
			if err == nil {
				err = fmt.Errorf("status %d: %s", status, answer)
			}
			last, pod, resent = err, "", true
			hop++
		}
		if !pause(ctx, retryPause) {
			return fmt.Errorf("order %s: %w (last: %v)", o.ID, ctx.Err(), last)
		}
	}
}

// send posts body to pod once.
func (l *proxyLoad) send(ctx context.Context, pod string, body []byte) (int, http.Header, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.c.proxyURL(pod, orderbook.OrdersPath), bytes.NewReader(body))
	if err != nil {
		return 0, nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := l.c.hc.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode, resp.Header, strings.TrimSpace(string(raw)), err
}

func (l *proxyLoad) done(id string, hop int64) {
	l.ok.Add(1)
	for {
		m := l.maxHops.Load()
		if hop <= m || l.maxHops.CompareAndSwap(m, hop) {
			break
		}
	}
	l.mu.Lock()
	l.accepted[id] = true
	l.mu.Unlock()
}

// run sends the orders of worker w, <prefix><w>-<k>, until stop is closed.
func (l *proxyLoad) run(ctx context.Context, w int, stop <-chan struct{}) {
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(w))) //nolint:gosec // a test load
	for k := 0; ; k++ {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		o := orderbook.RandomOrder(rng, fmt.Sprintf("%s%d-%d", l.prefix, w, k))
		if err := l.post(ctx, rng, o); err != nil {
			l.mu.Lock()
			l.failures = append(l.failures, err.Error())
			l.mu.Unlock()
		}
		if !pause(ctx, orderPause) {
			return
		}
	}
}

// pause waits for d and reports whether ctx is still live.
func pause(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// waitLeases waits until the Leases orderbook-markets-0 to 15 exist with a holder and a term,
// and returns their terms.
func waitLeases(ctx context.Context, client kubernetes.Interface) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, startupLimit)
	defer cancel()
	var lastErr error
	for {
		list, err := client.CoordinationV1().Leases(e2eNamespace).List(ctx, metav1.ListOptions{})
		if err == nil {
			terms := map[string]string{}
			for _, l := range list.Items {
				if strings.HasPrefix(l.Name, leasePrefix) && l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity != "" && l.Annotations[termKey] != "" {
					terms[l.Name] = l.Annotations[termKey]
				}
			}
			if len(terms) == orderbook.Shards {
				return terms, nil
			}
			lastErr = fmt.Errorf("leases with a holder: %v", terms)
		} else {
			lastErr = err
		}
		if !pause(ctx, 500*time.Millisecond) {
			return nil, fmt.Errorf("waiting for the Leases %s0 to %s%d: %w (last: %v)", leasePrefix, leasePrefix, orderbook.Shards-1, ctx.Err(), lastErr)
		}
	}
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

// restart restarts the rollout of deploy, as kubectl rollout restart does, and waits until it
// has finished.
func restart(ctx context.Context, client kubernetes.Interface, deploy string) error {
	patch := fmt.Appendf(nil, `{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`, restartKey, time.Now().UTC().Format(time.RFC3339Nano))
	if _, err := client.AppsV1().Deployments(e2eNamespace).Patch(ctx, deploy, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("restart deploy/%s: %w", deploy, err)
	}
	ctx, cancel := context.WithTimeout(ctx, rolloutLimit)
	defer cancel()
	for {
		d, err := client.AppsV1().Deployments(e2eNamespace).Get(ctx, deploy, metav1.GetOptions{})
		if err == nil && rolloutDone(d) {
			return nil
		}
		if !pause(ctx, 500*time.Millisecond) {
			return fmt.Errorf("the rollout of deploy/%s did not finish in %v (last: %v)", deploy, rolloutLimit, err)
		}
	}
}

// readJournal reads every shard's journal from the journal service through the API server's
// proxy.
func readJournal(ctx context.Context, c *cluster) ([][][]byte, error) {
	var last error
	for range 5 {
		pods, err := c.readyPods(ctx, journalsvcApp)
		if err == nil && len(pods) != 1 {
			err = fmt.Errorf("%d Ready journalsvc pods, want 1", len(pods))
		}
		if err == nil {
			base := strings.TrimSuffix(c.proxyURL(pods[0], "/"), "/")
			var all [][][]byte
			all, err = orderbook.NewHTTPJournal(base, c.hc).All(ctx)
			if err == nil {
				return all, nil
			}
		}
		last = err
		if !pause(ctx, retryPause) {
			break
		}
	}
	return nil, fmt.Errorf("read the journal: %w", last)
}

// checkJournal checks the journal against the orders that were accepted: every shard's journal
// replays to its fills, holds only orders of its own markets, no ID is journaled twice, and the
// orders journaled with prefix are exactly the accepted ones. It returns the number of lines.
func checkJournal(t *testing.T, all [][][]byte, prefix string, accepted map[string]bool) int {
	t.Helper()
	count := map[string]int{}
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
			count[en.Order.ID]++
		}
	}
	for id, n := range count {
		if n != 1 {
			t.Errorf("order %s is journaled %d times", id, n)
		}
		if strings.HasPrefix(id, prefix) && !accepted[id] {
			t.Errorf("order %s is journaled but was never accepted", id)
		}
	}
	for id := range accepted {
		if count[id] != 1 {
			t.Errorf("order %s was accepted and is journaled %d times", id, count[id])
		}
	}
	return lines
}

// TestOrderbookKubernetes runs the orderbook on a Kubernetes cluster where the manifests in
// deploy were applied: it sends orders under load, restarts the rollout of the orderbook (slots
// on) and checks the journal: no accepted order is lost or journaled twice, and every shard's
// journal replays. The steps were written for this test from the text of issue #385.
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
	hc, err := rest.HTTPClientFor(config)
	if err != nil {
		t.Fatalf("HTTP client: %v", err)
	}
	hc.Timeout = 10 * time.Second
	c := &cluster{client: client, hc: hc, byIP: map[string]string{}}

	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()

	terms, err := waitLeases(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("terms before the restart: %v", terms)

	l := &proxyLoad{c: c, prefix: fmt.Sprintf("t%x-", time.Now().UnixMilli()), accepted: map[string]bool{}}
	stop := make(chan struct{})
	var senders sync.WaitGroup
	for w := range workers {
		senders.Go(func() { l.run(ctx, w, stop) })
	}
	stopLoad := sync.OnceFunc(func() { close(stop); senders.Wait() })
	defer func() {
		if t.Failed() {
			cancel() // do not wait out the retries of orders that cannot succeed
		}
		stopLoad()
	}()

	time.Sleep(steadyBefore)
	if n := l.ok.Load(); n == 0 {
		t.Fatalf("no order got a 200 in %v before the restart; failures: %v", steadyBefore, l.failures)
	}
	t.Logf("orders ok before the restart: %d, sends %d", l.ok.Load(), l.sends.Load())

	if err := restart(ctx, client, orderbookDeploy); err != nil {
		t.Fatal(err)
	}
	t.Logf("rollout finished; orders ok so far: %d", l.ok.Load())
	time.Sleep(steadyAfter)
	stopLoad()

	l.mu.Lock()
	failures := slices.Clone(l.failures)
	accepted := l.accepted
	l.mu.Unlock()
	if len(failures) > 0 {
		t.Errorf("%d orders did not end 200, first: %s", len(failures), failures[0])
	}
	ok := l.ok.Load()
	t.Logf("test load: %d orders ok in %d sends; highest hop %d", ok, l.sends.Load(), l.maxHops.Load())

	after, err := waitLeases(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("terms after the rollout: %v", after)

	all, err := readJournal(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	lines := checkJournal(t, all, l.prefix, accepted)
	t.Logf("journal: %d lines in %d shards", lines, len(all))
}
