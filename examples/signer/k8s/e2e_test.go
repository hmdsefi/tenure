// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/signing"
	"github.com/hmdsefi/tenure/k8s"
)

const (
	e2eNamespace = "tenure-signer"
	termKey      = "tenure.term"
	restartKey   = "kubectl.kubernetes.io/restartedAt"

	// wallets is the number of wallets the requests use, w-00 to w-31, and workers the number of
	// goroutines that send them, each over its own share of the wallets one request at a time.
	wallets = 32
	workers = 4

	steadyBefore  = 5 * time.Second  // requests before the restart
	termSettle    = 5 * time.Second  // quiet time that makes the terms stable
	steadyAfter   = 10 * time.Second // requests after the rollout finished
	rolloutLimit  = 3 * time.Minute
	requestLimit  = 90 * time.Second // how long one request is retried
	startupLimit  = 90 * time.Second // how long the Leases may take to show up
	forwardLimit  = 20 * time.Second
	requestPause  = 20 * time.Millisecond
	retryPause    = 200 * time.Millisecond
	remotePort    = "8080"
	signerApp     = "signer"
	noncesvcApp   = "noncesvc"
	signerDeploy  = "signer"
	leasePrefix   = "signer-wallets-"
	shardsOfSpace = signing.Shards
)

// forwarder keeps a local port forwarded to one Ready pod of app. When the forwarded pod is
// replaced, or its stream ends, the next target call forwards to another Ready pod.
type forwarder struct {
	config *rest.Config
	client kubernetes.Interface
	app    string

	mu    sync.Mutex
	gen   int
	pod   string
	avoid string
	base  string
	stop  chan struct{}
}

func newForwarder(t testing.TB, config *rest.Config, client kubernetes.Interface, app string) *forwarder {
	f := &forwarder{config: config, client: client, app: app}
	t.Cleanup(f.close)
	return f
}

// target returns the base URL of the forwarded pod and its generation, which broken takes.
func (f *forwarder) target(ctx context.Context) (string, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stop == nil {
		if err := f.connect(ctx); err != nil {
			return "", 0, err
		}
	}
	return f.base, f.gen, nil
}

// broken closes the forward of generation gen, if it is still the current one, so that the next
// target forwards to another pod.
func (f *forwarder) broken(gen int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if gen == f.gen && f.stop != nil {
		close(f.stop)
		f.stop, f.avoid = nil, f.pod
	}
}

func (f *forwarder) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stop != nil {
		close(f.stop)
		f.stop = nil
	}
}

// readyPods returns the names of the Ready pods of app that are not being deleted.
func (f *forwarder) readyPods(ctx context.Context) ([]string, error) {
	list, err := f.client.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{LabelSelector: "app=" + f.app})
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

// connect forwards a local port to a Ready pod of app, one other than the one that broke last
// when there is another. The caller holds f.mu.
func (f *forwarder) connect(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, forwardLimit)
	defer cancel()
	names, err := f.readyPods(ctx)
	if err != nil {
		return err
	}
	if others := slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == f.avoid }); len(others) > 0 {
		names = others
	}
	if len(names) == 0 {
		return fmt.Errorf("no Ready pod of app=%s", f.app)
	}
	pod := names[0]

	u := f.client.CoreV1().RESTClient().Post().Resource("pods").Namespace(e2eNamespace).Name(pod).SubResource("portforward").URL()
	rt, upgrader, err := spdy.RoundTripperFor(f.config)
	if err != nil {
		return err
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: rt}, http.MethodPost, u)
	stop, ready := make(chan struct{}), make(chan struct{})
	pf, err := portforward.New(dialer, []string{"0:" + remotePort}, stop, ready, io.Discard, io.Discard)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- pf.ForwardPorts() }()
	select {
	case <-ready:
	case err := <-done:
		return fmt.Errorf("forward to %s: %w", pod, err)
	case <-ctx.Done():
		close(stop)
		return fmt.Errorf("forward to %s: %w", pod, ctx.Err())
	}
	ports, err := pf.GetPorts()
	if err != nil || len(ports) != 1 {
		close(stop)
		return fmt.Errorf("forward to %s: ports %v: %v", pod, ports, err)
	}
	f.gen++
	gen := f.gen
	f.pod, f.avoid, f.stop = pod, "", stop
	f.base = fmt.Sprintf("http://127.0.0.1:%d", ports[0].Local)
	go func() {
		err := <-done // the stream ended: the pod is gone or the forward was closed
		_ = err
		f.broken(gen)
	}()
	return nil
}

// load sends signing requests through f, one at a time per worker.
type load struct {
	f  *forwarder
	hc *http.Client

	ok       atomic.Int64
	attempts atomic.Int64 // requests sent, resends included
	mu       sync.Mutex
	failures []string
}

// request signs a payload for wallet and retries until it gets a 200 or requestLimit passes.
// A failed connection means the forwarded pod is gone: it re-forwards.
func (l *load) request(ctx context.Context, wallet string) error {
	body := fmt.Appendf(nil, `{"wallet":%q,"payload":"p"}`, wallet)
	ctx, cancel := context.WithTimeout(ctx, requestLimit)
	defer cancel()
	var last error
	for {
		base, gen, err := l.f.target(ctx)
		if err == nil {
			var res demo.Response
			l.attempts.Add(1)
			res, err = demo.Post(ctx, l.hc, base+"/sign", body)
			switch {
			case err != nil:
				l.f.broken(gen)
			case res.Status == http.StatusOK:
				return nil
			default:
				err = fmt.Errorf("status %d: %s", res.Status, strings.TrimSpace(string(res.Body)))
			}
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("wallet %s: %w (last: %v)", wallet, ctx.Err(), last)
		case <-time.After(retryPause):
		}
	}
}

// run sends the wallets of worker w in turn until stop is closed.
func (l *load) run(ctx context.Context, w int, stop <-chan struct{}) {
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		wallet := fmt.Sprintf("w-%02d", (w+i*workers)%wallets)
		if err := l.request(ctx, wallet); err != nil {
			l.mu.Lock()
			l.failures = append(l.failures, err.Error())
			l.mu.Unlock()
		} else {
			l.ok.Add(1)
		}
		time.Sleep(requestPause)
	}
}

// termWatch counts, for every shard Lease, how many times its tenure.term annotation changed.
type termWatch struct {
	mu      sync.Mutex
	last    map[string]string
	changes map[string]int
	at      time.Time // the last change
}

// quiet reports whether no term changed for d.
func (w *termWatch) quiet(d time.Duration) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return time.Since(w.at) >= d
}

// reset forgets the changes counted so far and takes the current terms as the new baseline.
func (w *termWatch) reset() map[string]string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.changes = map[string]int{}
	return maps.Clone(w.last)
}

func leaseNames() []string {
	var names []string
	for i := range shardsOfSpace {
		names = append(names, fmt.Sprintf("%s%d", leasePrefix, i))
	}
	return names
}

// waitLeases waits until the Leases signer-wallets-0 to 7 exist with a holder and a term, and
// returns their terms and the list's resource version.
func waitLeases(ctx context.Context, client kubernetes.Interface) (map[string]string, string, error) {
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
			if len(terms) == shardsOfSpace {
				return terms, list.ResourceVersion, nil
			}
			lastErr = fmt.Errorf("Leases with a holder: %v", terms)
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return nil, "", fmt.Errorf("waiting for the Leases %s0 to %s%d: %w (last: %v)", leasePrefix, leasePrefix, shardsOfSpace-1, ctx.Err(), lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// watchTerms follows the Leases from resourceVersion, counting the term changes against
// baseline, until ctx ends. It returns when ctx has ended.
func (w *termWatch) watchTerms(ctx context.Context, client kubernetes.Interface, rv string) {
	for ctx.Err() == nil {
		wi, err := client.CoordinationV1().Leases(e2eNamespace).Watch(ctx, metav1.ListOptions{ResourceVersion: rv, AllowWatchBookmarks: true})
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryPause):
			}
			continue
		}
		rv = w.consume(ctx, wi, rv)
		wi.Stop()
	}
}

func (w *termWatch) consume(ctx context.Context, wi watch.Interface, rv string) string {
	for {
		select {
		case <-ctx.Done():
			return rv
		case ev, ok := <-wi.ResultChan():
			if !ok {
				return rv
			}
			l, isLease := ev.Object.(*coordinationv1.Lease)
			if !isLease {
				if ev.Type == watch.Error {
					return "" // too old a version: start from the current state
				}
				continue
			}
			rv = l.ResourceVersion
			if ev.Type == watch.Bookmark || !strings.HasPrefix(l.Name, leasePrefix) {
				continue
			}
			w.mu.Lock()
			if term := l.Annotations[termKey]; term != "" && term != w.last[l.Name] {
				w.last[l.Name] = term
				w.changes[l.Name]++
				w.at = time.Now()
			}
			w.mu.Unlock()
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

// TestSignerKubernetes restarts the rollout of the signer on a Kubernetes cluster where
// EXM-370's commands deployed it, while requests keep coming, and checks that every request ended
// 200, that the chain saw no conflict and no gap, and that the term of each shard's Lease
// changed exactly once: slots make a rolling deploy move each shard once (EXM-380, AT-EXM-15).
func TestSignerKubernetes(t *testing.T) {
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

	baseline, rv, err := waitLeases(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("terms before the restart: %v", baseline)

	tw := &termWatch{last: maps.Clone(baseline), changes: map[string]int{}, at: time.Now()}
	wctx, stopWatch := context.WithCancel(ctx)
	var watching sync.WaitGroup
	watching.Go(func() { tw.watchTerms(wctx, client, rv) })
	defer func() { stopWatch(); watching.Wait() }()

	hc := &http.Client{Timeout: 10 * time.Second}
	l := &load{f: newForwarder(t, config, client, signerApp), hc: hc}
	stop := make(chan struct{})
	var senders sync.WaitGroup
	for w := range workers {
		senders.Go(func() { l.run(ctx, w, stop) })
	}
	stopLoad := sync.OnceFunc(func() { close(stop); senders.Wait() })
	defer func() {
		if t.Failed() {
			cancel() // do not wait out the retries of requests that cannot succeed
		}
		stopLoad()
	}()

	time.Sleep(steadyBefore)
	// The Leases may still be moving while the deployment settles: the baseline is the terms
	// once none has changed for a while, and only the changes after it count.
	for deadline := time.Now().Add(time.Minute); !tw.quiet(termSettle); {
		if time.Now().After(deadline) {
			t.Fatalf("the Lease terms did not settle before the restart")
		}
		time.Sleep(retryPause)
	}
	baseline = tw.reset()
	t.Logf("terms at the restart: %v", baseline)
	if n := l.ok.Load(); n == 0 {
		t.Fatalf("no request got a 200 in %v before the restart; failures: %v", steadyBefore, l.failures)
	}

	patch := fmt.Appendf(nil, `{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`, restartKey, time.Now().UTC().Format(time.RFC3339))
	if _, err := client.AppsV1().Deployments(e2eNamespace).Patch(ctx, signerDeploy, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil {
		t.Fatalf("restart deploy/%s: %v", signerDeploy, err)
	}
	rollCtx, rollCancel := context.WithTimeout(ctx, rolloutLimit)
	defer rollCancel()
	for {
		d, err := client.AppsV1().Deployments(e2eNamespace).Get(rollCtx, signerDeploy, metav1.GetOptions{})
		if err == nil && rolloutDone(d) {
			break
		}
		select {
		case <-rollCtx.Done():
			t.Fatalf("the rollout of deploy/%s did not finish in %v (last: %v)", signerDeploy, rolloutLimit, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Logf("rollout finished; requests ok so far: %d", l.ok.Load())

	time.Sleep(steadyAfter)
	// Pods Ready is not slots moved: count the terms once none has changed for a while.
	for stable := time.Now().Add(termSettle); !tw.quiet(termSettle); {
		if time.Now().After(stable.Add(time.Minute)) {
			t.Fatalf("the Lease terms did not settle")
		}
		time.Sleep(retryPause)
	}
	stopLoad()

	l.mu.Lock()
	failures := slices.Clone(l.failures)
	l.mu.Unlock()
	if len(failures) > 0 {
		t.Errorf("%d requests did not end 200, first: %s", len(failures), failures[0])
	}
	ok := int(l.ok.Load())
	t.Logf("%d requests ended 200", ok)

	nf := newForwarder(t, config, client, noncesvcApp)
	var rep signing.Report
	var repErr error
	for range 5 {
		base, gen, err := nf.target(ctx)
		if err == nil {
			rep, err = signing.NewHTTPChain(base, hc).Report(ctx)
			if err != nil {
				nf.broken(gen)
			}
		}
		if repErr = err; err == nil {
			break
		}
		time.Sleep(retryPause)
	}
	if repErr != nil {
		t.Fatalf("report: %v", repErr)
	}
	t.Logf("chain accepted %d; conflicts %d, gaps %d", rep.Accepted, len(rep.Conflicts), len(rep.Gaps))
	if len(rep.Conflicts) > 0 || len(rep.Gaps) > 0 {
		t.Errorf("the chain saw conflicts %v and gaps %v", rep.Conflicts, rep.Gaps)
	}
	if sent := int(l.attempts.Load()); rep.Accepted > sent {
		t.Errorf("the chain accepted %d transactions, more than the %d requests sent", rep.Accepted, sent)
	}
	if rep.Accepted < ok {
		t.Errorf("the chain accepted %d transactions, fewer than the %d requests that ended 200", rep.Accepted, ok)
	}

	stopWatch()
	watching.Wait()
	tw.mu.Lock()
	defer tw.mu.Unlock()
	final, _, err := waitLeases(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range leaseNames() {
		if final[name] != tw.last[name] {
			t.Errorf("Lease %s: tenure.term is %s but the watch saw %s: a change was missed", name, final[name], tw.last[name])
		}
		if n := tw.changes[name]; n != 1 {
			t.Errorf("Lease %s: tenure.term changed %d times (%s to %s), want once", name, n, baseline[name], tw.last[name])
		}
	}
}
