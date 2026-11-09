// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

// The cluster test harness (K8S-950, K8S §10).
//
// A cluster test starts with newCluster(t), which calls REP-400's kubeconfig helper first, so the
// test skips (or fails, with TENURE_TEST_EXTERNAL=1) when no API server is configured. It gets
// a fresh namespace tenure-<8 hex>, deleted when the test ends, and a raw admin client for the
// test's own reads and writes. Each test uses its own group, group() (g<8 hex>), so a test that
// shares a namespace between subtests still starts from no Leases.
//
//	c := newCluster(t)
//	g := group()
//	a, rec := c.adapter(t, g)               // New(<recorded client>, c.ns, g, WithTiming(T))
//	cl, rec2 := c.client(t)                 // a second recorded client, e.g. for a peer adapter
//	as, _ := c.clientAs(t, "system:serviceaccount:"+c.ns+":tenure") // impersonating, for RBAC
//	l, err := c.leases().Get(ctx, name, metav1.GetOptions{})     // raw, never recorded
//
// Every client from c.client, c.clientAs and c.adapter has its own recorder on
// rest.Config.WrapTransport. It records each request (verb, resource, Lease name, query, decoded
// body, status and times) and holds the four switches of K8S §10:
//
//	rec.fail(m)           matching requests get a 500 from the transport, never reaching the
//	                      server, until the returned func is called; rec.failNext(m, n) fails
//	                      only the next n.
//	rec.stall(m)          matching requests, and the stream of a matching open WATCH, block until
//	                      the returned func is called or their context ends.
//	rec.bump(name, rt)    before the next UPDATE of Lease name is sent, the harness writes that
//	                      Lease through the raw client: it increments the annotation
//	                      tenure.test/bump and sets spec.renewTime to rt unless rt is zero. Its
//	                      resourceVersion changes, so the UPDATE gets 409 Conflict. The
//	                      UPDATE's record holds the Lease as written (bumpedTo); the server
//	                      keeps renewTime to the microsecond.
//	rec.delay(name, d)    requests on Lease name are held d before they are sent, until the
//	                      returned func is called.
//
// A match selects requests: on(verb, name) matches Lease requests by verb (GET, LIST, WATCH,
// CREATE, UPDATE, PATCH, DELETE, DELETECOLLECTION) and name, "" matching any; any func(request)
// bool is a match too. Recorded requests are read with rec.requests(), rec.filter(m),
// rec.count(m), rec.maxInFlight(m) (the most matching requests between entering the transport
// and getting their response, for record concurrency) and rec.waitFor(t, d, m); rec.reset()
// forgets what was recorded. String() of a request is "<VERB> <name> <status>", for example
// "GET g0a1b2c3d-member-pod-0 200".
//
// Every switch is turned off and every stall released when the test that made the recorder
// ends, so a test may leave them on.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// The environment variables of REP-400.
const (
	envKubeconfig = "TENURE_TEST_KUBECONFIG"
	envExternal   = "TENURE_TEST_EXTERNAL"
)

// kubeconfig returns the path of the kubeconfig file of the cluster tests' API server, from
// TENURE_TEST_KUBECONFIG (REP-400). It fails the test when TENURE_TEST_EXTERNAL is neither
// empty, 0 nor 1, returns the path when it is set, fails the test when it is not set and
// TENURE_TEST_EXTERNAL=1, and skips the test otherwise.
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

// randomHex returns 8 random lower-case hex digits.
func randomHex() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b) // never fails
	return hex.EncodeToString(b)
}

// group returns a new group name, g<8 hex>, so every test starts with no Leases of its group.
func group() string { return "g" + randomHex() }

// cluster is one test's part of the API server: a namespace of its own and a raw client.
type cluster struct {
	// ns is the test's namespace, tenure-<8 hex>, deleted when the test ends.
	ns string
	// config is the kubeconfig's configuration, prepared by RESTConfig. Copy it before
	// changing it.
	config *rest.Config
	// raw is an admin client whose requests are not recorded: for a test's own reads and
	// writes, such as deleting a Lease behind the Adapter's back.
	raw kubernetes.Interface
}

// newCluster calls kubeconfig first, so the test skips or fails without an API server, then
// creates the namespace the test works in and deletes it when the test ends.
func newCluster(t testing.TB) *cluster {
	t.Helper()
	path := kubeconfig(t)
	config, err := RESTConfig(clientcmd.BuildConfigFromFlags("", path))
	if err != nil {
		t.Fatalf("kubeconfig %s: %v", path, err)
	}
	raw, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	c := &cluster{ns: "tenure-" + randomHex(), config: config, raw: raw}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: c.ns}}
	if _, err := raw.CoreV1().Namespaces().Create(t.Context(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace %s: %v", c.ns, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := raw.CoreV1().Namespaces().Delete(ctx, c.ns, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete namespace %s: %v", c.ns, err)
		}
	})
	return c
}

// leases returns the raw client's Leases of the test's namespace.
func (c *cluster) leases() coordinationv1client.LeaseInterface {
	return c.raw.CoordinationV1().Leases(c.ns)
}

// client returns a new admin client and the recorder of its requests.
func (c *cluster) client(t testing.TB) (kubernetes.Interface, *recorder) {
	t.Helper()
	return c.newClient(t, rest.CopyConfig(c.config))
}

// clientAs returns a new client that impersonates user and the recorder of its requests. A
// service account's user is system:serviceaccount:<namespace>:<name>.
func (c *cluster) clientAs(t testing.TB, user string) (kubernetes.Interface, *recorder) {
	t.Helper()
	config := rest.CopyConfig(c.config)
	config.Impersonate = rest.ImpersonationConfig{UserName: user}
	return c.newClient(t, config)
}

func (c *cluster) newClient(t testing.TB, config *rest.Config) (kubernetes.Interface, *recorder) {
	t.Helper()
	rec := newRecorder(t, c.raw)
	config.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return &recordingTransport{rec: rec, next: next}
	})
	cl, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return cl, rec
}

// adapter returns an Adapter of group g in the test's namespace with timing T and then opts,
// on a new recorded client, and that client's recorder. It fails the test when New reports a
// configuration error, and, when the test ends, unless the Adapter's informer has stopped
// within 1 s: a test ends every session it opens.
func (c *cluster) adapter(t testing.TB, g string, opts ...Option) (*Adapter, *recorder) {
	t.Helper()
	cl, rec := c.client(t)
	a := New(cl, c.ns, g, append([]Option{WithTiming(T)}, opts...)...)
	if a.err != nil {
		t.Fatalf("New: %v", a.err)
	}
	t.Cleanup(func() { waitStopped(t, a) })
	return a, rec
}

// The verbs of recorded requests: the Kubernetes API verbs, upper-cased.
const (
	verbGet              = "GET"
	verbList             = "LIST"
	verbWatch            = "WATCH"
	verbCreate           = "CREATE"
	verbUpdate           = "UPDATE"
	verbPatch            = "PATCH"
	verbDelete           = "DELETE"
	verbDeleteCollection = "DELETECOLLECTION"
)

// resourceLeases is the resource of Lease requests.
const resourceLeases = "leases"

// bumpAnnotation is the annotation a bump increments, so the bumped Lease changes even when
// no renewTime is given.
const bumpAnnotation = "tenure.test/bump"

// request is one recorded request.
type request struct {
	verb      string     // GET, LIST, WATCH, CREATE, UPDATE, PATCH, DELETE or DELETECOLLECTION
	resource  string     // "leases", "namespaces", ...
	namespace string     // "" for a cluster-scoped resource
	name      string     // the object's name; "" for LIST, WATCH, CREATE and DELETECOLLECTION
	query     url.Values // labelSelector, limit, resourceVersion, watch, ...
	// body is the decoded request body: the *coordinationv1.Lease of a CREATE or UPDATE, the
	// *metav1.DeleteOptions of a DELETE; nil without a body.
	body runtime.Object

	start time.Time // when the request entered the transport
	sent  time.Time // when it went to the server, after any stall or delay; zero if it never did
	done  time.Time // when its response (for a WATCH, the response headers) or error came back
	// status is the HTTP status: 500 for a failed request (injected), 409 for a bumped UPDATE;
	// 0 when no response came back.
	status   int
	err      error // the transport's error, including a context's while stalled or delayed
	injected bool  // the fail switch answered it
	bumped   bool  // a bump changed its Lease just before it was sent
	// bumpedTo is the Lease as the bump wrote it, with the resourceVersion and the
	// microsecond spec.renewTime the server stored; nil when not bumped.
	bumpedTo *coordinationv1.Lease
}

// String returns "<VERB> <name> <status>", leaving out an empty name.
func (r request) String() string {
	if r.name == "" {
		return fmt.Sprintf("%s %d", r.verb, r.status)
	}
	return fmt.Sprintf("%s %s %d", r.verb, r.name, r.status)
}

// lease returns the Lease a CREATE or UPDATE sent, or nil.
func (r request) lease() *coordinationv1.Lease {
	l, _ := r.body.(*coordinationv1.Lease)
	return l
}

// deleteOptions returns the options a DELETE sent, or nil.
func (r request) deleteOptions() *metav1.DeleteOptions {
	o, _ := r.body.(*metav1.DeleteOptions)
	return o
}

// match selects requests.
type match func(r request) bool

// on matches Lease requests with verb and name; an empty verb or name matches any.
func on(verb, name string) match {
	return func(r request) bool {
		return r.resource == resourceLeases && (verb == "" || r.verb == verb) && (name == "" || r.name == name)
	}
}

// rule is one switch that is on: fail, stall or delay.
type rule struct {
	match match
	// fail: the matches still to fail; negative for every match.
	fail int
	// stall: closed on release.
	released chan struct{}
	// delay: how long matching requests wait.
	delay time.Duration
}

// bumpSpec is one pending bump.
type bumpSpec struct {
	renewTime time.Time
}

// recorder records the requests of one client and holds its switches. Its methods are safe
// for concurrent use.
type recorder struct {
	raw kubernetes.Interface

	mu     sync.Mutex
	reqs   []*request
	fails  []*rule
	stalls []*rule
	delays []*rule
	bumps  map[string][]bumpSpec
}

func newRecorder(t testing.TB, raw kubernetes.Interface) *recorder {
	r := &recorder{raw: raw, bumps: map[string][]bumpSpec{}}
	t.Cleanup(r.off)
	return r
}

// off turns every switch off and releases every stall.
func (r *recorder) off() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.stalls {
		close(s.released)
	}
	r.fails, r.stalls, r.delays = nil, nil, nil
	clear(r.bumps)
}

// remove takes rule x out of *rules.
func (r *recorder) remove(rules *[]*rule, x *rule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*rules = slices.DeleteFunc(*rules, func(y *rule) bool { return y == x })
}

// fail answers every request m matches with a 500 Internal Server Error from the transport,
// without sending it, until the returned func is called.
func (r *recorder) fail(m match) (off func()) {
	return r.addFail(m, -1)
}

// failNext answers the next n requests m matches with a 500 Internal Server Error from the
// transport, without sending them.
func (r *recorder) failNext(m match, n int) {
	if n > 0 {
		r.addFail(m, n)
	}
}

func (r *recorder) addFail(m match, n int) func() {
	x := &rule{match: m, fail: n}
	r.mu.Lock()
	r.fails = append(r.fails, x)
	r.mu.Unlock()
	return func() { r.remove(&r.fails, x) }
}

// stall blocks every request m matches before it is sent, and every read of the stream of an
// open WATCH that m matches, until the returned func is called or the request's context ends.
func (r *recorder) stall(m match) (release func()) {
	x := &rule{match: m, released: make(chan struct{})}
	r.mu.Lock()
	r.stalls = append(r.stalls, x)
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if i := slices.Index(r.stalls, x); i >= 0 {
				r.stalls = slices.Delete(r.stalls, i, i+1)
				close(x.released)
			}
		})
	}
}

// delay holds every request on Lease name for d before it is sent, until the returned func is
// called.
func (r *recorder) delay(name string, d time.Duration) (off func()) {
	x := &rule{match: on("", name), delay: d}
	r.mu.Lock()
	r.delays = append(r.delays, x)
	r.mu.Unlock()
	return func() { r.remove(&r.delays, x) }
}

// bump makes the next UPDATE of Lease name conflict: just before it is sent, the harness
// increments the Lease's tenure.test/bump annotation through the raw client and, unless
// renewTime is zero, sets its spec.renewTime to renewTime. Bumps of one name queue up, one per
// UPDATE.
func (r *recorder) bump(name string, renewTime time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bumps[name] = append(r.bumps[name], bumpSpec{renewTime: renewTime})
}

// requests returns a copy of the recorded requests in the order they entered the transport.
func (r *recorder) requests() []request {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]request, len(r.reqs))
	for i, x := range r.reqs {
		out[i] = *x
	}
	return out
}

// filter returns the recorded requests m matches, in order.
func (r *recorder) filter(m match) []request {
	return slices.DeleteFunc(r.requests(), func(x request) bool { return !m(x) })
}

// count returns how many recorded requests m matches.
func (r *recorder) count(m match) int { return len(r.filter(m)) }

// reset forgets the recorded requests. Requests in flight are no longer recorded either.
func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = nil
}

// maxInFlight returns the largest number of requests m matches that were in the transport at
// once, each from entering it (before any stall or delay) to getting its response or error.
func (r *recorder) maxInFlight(m match) int {
	type edge struct {
		at    time.Time
		delta int
	}
	var edges []edge
	for _, x := range r.filter(m) {
		edges = append(edges, edge{x.start, 1})
		if !x.done.IsZero() {
			edges = append(edges, edge{x.done, -1})
		}
	}
	// At equal times an end comes first: requests that only touch were not in flight at once.
	slices.SortFunc(edges, func(a, b edge) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return a.delta - b.delta
	})
	n, most := 0, 0
	for _, e := range edges {
		n += e.delta
		most = max(most, n)
	}
	return most
}

// waitFor waits up to d for a recorded request that m matches and returns the first one. It
// fails the test when none comes.
func (r *recorder) waitFor(t testing.TB, d time.Duration, m match) request {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if got := r.filter(m); len(got) > 0 {
			return got[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no matching request in %v; recorded: %v", d, r.requests())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stalled returns the release channel of the first stall that matches x, or nil.
func (r *recorder) stalled(x request) <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.stalls {
		if s.match(x) {
			return s.released
		}
	}
	return nil
}

// waitStalls blocks while a stall matches x, or until ctx ends or done is closed.
func (r *recorder) waitStalls(ctx context.Context, done <-chan struct{}, x request) error {
	for {
		ch := r.stalled(x)
		if ch == nil {
			return nil
		}
		select {
		case <-ch:
		case <-done:
			return errors.New("harness: body closed while stalled")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// delayOf returns the longest delay that matches x.
func (r *recorder) delayOf(x request) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var d time.Duration
	for _, s := range r.delays {
		if s.match(x) {
			d = max(d, s.delay)
		}
	}
	return d
}

// takeFail reports whether a fail switch answers x, using up one of a failNext's matches.
func (r *recorder) takeFail(x request) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, s := range r.fails {
		if !s.match(x) {
			continue
		}
		if s.fail > 0 {
			s.fail--
			if s.fail == 0 {
				r.fails = slices.Delete(r.fails, i, i+1)
			}
		}
		return true
	}
	return false
}

// takeBump returns the pending bump for x, if x is a Lease UPDATE that has one.
func (r *recorder) takeBump(x request) (bumpSpec, bool) {
	if x.resource != resourceLeases || x.verb != verbUpdate {
		return bumpSpec{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.bumps[x.name]
	if len(q) == 0 {
		return bumpSpec{}, false
	}
	if len(q) == 1 {
		delete(r.bumps, x.name)
	} else {
		r.bumps[x.name] = q[1:]
	}
	return q[0], true
}

// doBump writes Lease name in namespace through the raw client so its resourceVersion changes,
// and returns the Lease as written. A conflict with another writer, such as an Adapter's
// heartbeat, is retried on a fresh read.
func (r *recorder) doBump(ctx context.Context, namespace, name string, b bumpSpec) (*coordinationv1.Lease, error) {
	leases := r.raw.CoordinationV1().Leases(namespace)
	for {
		l, err := leases.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("harness: bump %s: %w", name, err)
		}
		if l.Annotations == nil {
			l.Annotations = map[string]string{}
		}
		n, _ := strconv.Atoi(l.Annotations[bumpAnnotation])
		l.Annotations[bumpAnnotation] = strconv.Itoa(n + 1)
		if !b.renewTime.IsZero() {
			rt := metav1.NewMicroTime(b.renewTime)
			l.Spec.RenewTime = &rt
		}
		l, err = leases.Update(ctx, l, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("harness: bump %s: %w", name, err)
		}
		return l, nil
	}
}

// add records x and returns the record to update.
func (r *recorder) add(x request) *request {
	p := &x
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, p)
	return p
}

// update changes a record under the lock.
func (r *recorder) update(p *request, f func(p *request)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f(p)
}

// snapshot returns a copy of a record.
func (r *recorder) snapshot(p *request) request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return *p
}

// recordingTransport records requests in rec and applies its switches.
type recordingTransport struct {
	rec  *recorder
	next http.RoundTripper
}

// classify returns the request's verb, resource, namespace and name, from its method and path:
// /api/v1/... or /apis/<group>/<version>/..., then namespaces/<ns>/<resource>[/<name>] or
// <resource>[/<name>].
func classify(method string, u *url.URL) request {
	x := request{query: u.Query()}
	segs := strings.Split(strings.Trim(u.Path, "/"), "/")
	switch {
	case len(segs) >= 2 && segs[0] == "api":
		segs = segs[2:]
	case len(segs) >= 3 && segs[0] == "apis":
		segs = segs[3:]
	default:
		segs = nil
	}
	if len(segs) >= 3 && segs[0] == "namespaces" {
		x.namespace, segs = segs[1], segs[2:]
	}
	if len(segs) >= 1 {
		x.resource = segs[0]
	}
	if len(segs) >= 2 {
		x.name = segs[1]
	}
	switch method {
	case http.MethodGet:
		switch {
		case x.name != "":
			x.verb = verbGet
		case x.query.Get("watch") == "true" || x.query.Get("watch") == "1":
			x.verb = verbWatch
		default:
			x.verb = verbList
		}
	case http.MethodPost:
		x.verb = verbCreate
	case http.MethodPut:
		x.verb = verbUpdate
	case http.MethodPatch:
		x.verb = verbPatch
	case http.MethodDelete:
		if x.name != "" {
			x.verb = verbDelete
		} else {
			x.verb = verbDeleteCollection
		}
	default:
		x.verb = method
	}
	return x
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := rt.rec
	x := classify(req.Method, req.URL)
	x.start = time.Now()
	if req.Body != nil && req.Body != http.NoBody {
		b, err := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, err
		}
		if obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(b, nil, nil); err == nil {
			x.body = obj
		}
		req = req.Clone(req.Context())
		req.Body = io.NopCloser(bytes.NewReader(b))
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	}
	p := r.add(x)
	finish := func(status int, err error) {
		r.update(p, func(p *request) { p.done, p.status, p.err = time.Now(), status, err })
	}
	ctx := req.Context()

	if err := r.waitStalls(ctx, nil, x); err != nil {
		finish(0, err)
		return nil, err
	}
	if d := r.delayOf(x); d > 0 {
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			finish(0, ctx.Err())
			return nil, ctx.Err()
		}
	}
	if r.takeFail(x) {
		r.update(p, func(p *request) { p.injected = true })
		finish(http.StatusInternalServerError, nil)
		return failResponse(req), nil
	}
	if b, ok := r.takeBump(x); ok {
		l, err := r.doBump(ctx, x.namespace, x.name, b)
		if err != nil {
			finish(0, err)
			return nil, err
		}
		r.update(p, func(p *request) { p.bumped, p.bumpedTo = true, l })
	}

	r.update(p, func(p *request) { p.sent = time.Now() })
	resp, err := rt.next.RoundTrip(req)
	if err != nil {
		finish(0, err)
		return nil, err
	}
	if x.verb == verbWatch {
		resp.Body = &stallBody{ReadCloser: resp.Body, rec: r, req: r.snapshot(p), ctx: ctx, closed: make(chan struct{})}
	}
	finish(resp.StatusCode, nil)
	return resp, nil
}

// failResponse is the transport's answer to a failed request: a 500 with a Status body, which
// client-go returns as an InternalError and does not retry.
func failResponse(req *http.Request) *http.Response {
	b, _ := json.Marshal(metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure,
		Message:  "harness: injected failure",
		Reason:   metav1.StatusReasonInternalError,
		Code:     http.StatusInternalServerError,
	})
	return &http.Response{
		Status:        "500 Internal Server Error",
		StatusCode:    http.StatusInternalServerError,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": {"application/json"}},
		Body:          io.NopCloser(bytes.NewReader(b)),
		ContentLength: int64(len(b)),
		Request:       req,
	}
}

// stallBody is the stream of a WATCH: while a stall matches the WATCH, what the server sends
// is held back until the stall is released, the request's context ends or the body is closed.
type stallBody struct {
	io.ReadCloser
	rec    *recorder
	req    request
	ctx    context.Context
	once   sync.Once
	closed chan struct{}
}

func (b *stallBody) Read(p []byte) (int, error) {
	if err := b.rec.waitStalls(b.ctx, b.closed, b.req); err != nil {
		return 0, err
	}
	n, err := b.ReadCloser.Read(p)
	if werr := b.rec.waitStalls(b.ctx, b.closed, b.req); werr != nil {
		return 0, werr
	}
	return n, err
}

func (b *stallBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return b.ReadCloser.Close()
}
