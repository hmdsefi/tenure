// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// recordingTB is a testing.TB that records how a helper ended. Fatal and Skip end the
// calling goroutine as testing.T's do, so the helper must run on a goroutine of its own.
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

var (
	namespaceForm = regexp.MustCompile(`^tenure-[0-9a-f]{8}$`)
	groupForm     = regexp.MustCompile(`^g[0-9a-f]{8}$`)
)

// rawLease creates Lease name of group g through the raw client and returns it.
func rawLease(t *testing.T, c *cluster, g, name string) *coordinationv1.Lease {
	t.Helper()
	l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: leaseLabels(g, kindMember, "")}}
	l, err := c.leases().Create(t.Context(), l, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return l
}

// last returns the last recorded request m matches; it fails the test when there is none.
func last(t *testing.T, rec *recorder, m match) request {
	t.Helper()
	got := rec.filter(m)
	if len(got) == 0 {
		t.Fatalf("no matching request; recorded: %v", rec.requests())
	}
	return got[len(got)-1]
}

// TestClusterHarness checks the harness on a cluster: the namespace and group forms, the
// recorder's records and its fail, delay, bump and stall switches, and the namespace's
// deletion when the test ends.
func TestClusterHarness(t *testing.T) {
	kubeconfig(t)
	var c *cluster
	t.Run("cluster", func(t *testing.T) {
		c = newCluster(t)
		ctx := t.Context()
		ns, err := c.raw.CoreV1().Namespaces().Get(ctx, c.ns, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("namespace %s: %v", c.ns, err)
		}
		if !namespaceForm.MatchString(ns.Name) || ns.Status.Phase != corev1.NamespaceActive {
			t.Fatalf("namespace %s is %s, want tenure-<8 hex> and Active", ns.Name, ns.Status.Phase)
		}
		g := group()
		if !groupForm.MatchString(g) || g == group() {
			t.Fatalf("group() = %s, want g<8 hex>, new on every call", g)
		}
		a, _ := c.adapter(t, g)
		if a.namespace != c.ns || a.group != g || a.timing != T || a.CheckTiming(T) != nil {
			t.Fatalf("adapter: namespace %s, group %s, timing %+v, want %s, %s, T", a.namespace, a.group, a.timing, c.ns, g)
		}
		name := memberName(g, "pod-0")
		rawLease(t, c, g, name)

		t.Run("record", func(t *testing.T) { testRecord(t, c, g, name) })
		t.Run("fail", func(t *testing.T) { testFail(t, c, g, name) })
		t.Run("delay", func(t *testing.T) { testDelay(t, c, g, name) })
		t.Run("bump", func(t *testing.T) { testBump(t, c, name) })
		t.Run("stall", func(t *testing.T) { testStall(t, c, g, name) })
	})
	if c == nil {
		return
	}
	ns, err := c.raw.CoreV1().Namespaces().Get(t.Context(), c.ns, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		t.Fatalf("namespace %s after the test: %v", c.ns, err)
	case ns.DeletionTimestamp == nil && ns.Status.Phase != corev1.NamespaceTerminating:
		t.Errorf("namespace %s after the test is %s, want gone or Terminating", c.ns, ns.Status.Phase)
	}
}

// testRecord checks what the recorder records for each verb, and that the raw client is not
// recorded.
func testRecord(t *testing.T, c *cluster, g, name string) {
	ctx := t.Context()
	cl, rec := c.client(t)
	leases := cl.CoordinationV1().Leases(c.ns)

	if _, err := leases.Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("GET: %v", err)
	}
	got := rec.requests()
	if len(got) != 1 || got[0].String() != "GET "+name+" 200" {
		t.Fatalf("recorded %v, want [GET %s 200]", got, name)
	}
	if r := got[0]; r.namespace != c.ns || r.start.IsZero() || r.sent.Before(r.start) || r.done.Before(r.sent) || r.err != nil || r.injected || r.bumped {
		t.Errorf("GET record %+v: want namespace %s, start <= sent <= done, no error", r, c.ns)
	}

	sel := "tenure.group=" + g
	list, err := leases.List(ctx, metav1.ListOptions{LabelSelector: sel, Limit: 1})
	if err != nil {
		t.Fatalf("LIST: %v", err)
	}
	if r := last(t, rec, on(verbList, "")); r.String() != "LIST 200" || r.query.Get("labelSelector") != sel || r.query.Get("limit") != "1" {
		t.Errorf("LIST record %v with query %v, want LIST 200 with labelSelector %s and limit 1", r, r.query, sel)
	}
	w, err := leases.Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
	if err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	w.Stop()
	if r := last(t, rec, on(verbWatch, "")); r.String() != "WATCH 200" || r.query.Get("resourceVersion") != list.ResourceVersion {
		t.Errorf("WATCH record %v with query %v, want WATCH 200 from resourceVersion %s", r, r.query, list.ResourceVersion)
	}

	other := memberName(g, "pod-1")
	l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: other, Labels: leaseLabels(g, kindMember, "")}}
	l, err = leases.Create(ctx, l, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if r := last(t, rec, on(verbCreate, "")); r.String() != "CREATE 201" || r.lease() == nil || r.lease().Name != other {
		t.Errorf("CREATE record %v with body %v, want CREATE 201 sending Lease %s", r, r.body, other)
	}
	base := l.ResourceVersion
	l.Spec.HolderIdentity = new("pod-1/1")
	if l, err = leases.Update(ctx, l, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if r := last(t, rec, on(verbUpdate, other)); r.String() != "UPDATE "+other+" 200" || r.lease() == nil || r.lease().ResourceVersion != base {
		t.Errorf("UPDATE record %v with body %v, want UPDATE 200 sending resourceVersion %s", r, r.body, base)
	}
	pre := metav1.Preconditions{UID: &l.UID, ResourceVersion: &l.ResourceVersion}
	if err := leases.Delete(ctx, other, metav1.DeleteOptions{Preconditions: &pre}); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	r := last(t, rec, on(verbDelete, other))
	if o := r.deleteOptions(); r.String() != "DELETE "+other+" 200" || o == nil || o.Preconditions == nil ||
		o.Preconditions.UID == nil || *o.Preconditions.UID != l.UID || *o.Preconditions.ResourceVersion != l.ResourceVersion {
		t.Errorf("DELETE record %v with body %v, want DELETE 200 sending UID %s and resourceVersion %s", r, r.body, l.UID, l.ResourceVersion)
	}

	var verbs []string
	for _, r := range rec.requests() {
		verbs = append(verbs, r.verb)
	}
	if want := []string{verbGet, verbList, verbWatch, verbCreate, verbUpdate, verbDelete}; fmt.Sprint(verbs) != fmt.Sprint(want) {
		t.Errorf("recorded verbs %v, want %v", verbs, want)
	}
	if n := rec.count(on(verbGet, name)); n != 1 {
		t.Errorf("count(GET %s) = %d, want 1", name, n)
	}
	if _, err := c.leases().Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("raw GET: %v", err)
	}
	if n := len(rec.requests()); n != 6 {
		t.Errorf("%d requests recorded after a raw GET, want 6", n)
	}
	rec.reset()
	if got := rec.requests(); len(got) != 0 {
		t.Errorf("recorded %v after reset, want none", got)
	}

	// An impersonated client has only the user's permissions: none, here.
	user := "system:serviceaccount:" + c.ns + ":nobody"
	as, asRec := c.clientAs(t, user)
	if _, err := as.CoordinationV1().Leases(c.ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsForbidden(err) {
		t.Errorf("GET as %s = %v, want Forbidden", user, err)
	}
	if r := last(t, asRec, on(verbGet, name)); r.String() != "GET "+name+" 403" {
		t.Errorf("GET as %s recorded %v, want GET %s 403", user, r, name)
	}
}

// testFail checks that a failed request gets a 500 without reaching the server.
func testFail(t *testing.T, c *cluster, g, name string) {
	ctx := t.Context()
	cl, rec := c.client(t)
	leases := cl.CoordinationV1().Leases(c.ns)
	other := memberName(g, "pod-2")
	l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: other, Labels: leaseLabels(g, kindMember, "")}}

	off := rec.fail(on(verbCreate, ""))
	if _, err := leases.Create(ctx, l, metav1.CreateOptions{}); !apierrors.IsInternalError(err) {
		t.Fatalf("CREATE with fail on = %v, want an InternalError", err)
	}
	if r := last(t, rec, on(verbCreate, "")); r.String() != "CREATE 500" || !r.injected || !r.sent.IsZero() {
		t.Errorf("failed CREATE record %+v, want CREATE 500, injected, never sent", r)
	}
	if _, err := c.leases().Get(ctx, other, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("raw GET %s after a failed CREATE = %v, want NotFound", other, err)
	}
	if _, err := leases.Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Errorf("GET with fail on CREATE only = %v, want nil", err)
	}
	off()
	if _, err := leases.Create(ctx, l, metav1.CreateOptions{}); err != nil {
		t.Fatalf("CREATE with fail off = %v", err)
	}

	rec.failNext(on(verbGet, name), 1)
	if _, err := leases.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsInternalError(err) {
		t.Errorf("first GET after failNext = %v, want an InternalError", err)
	}
	if _, err := leases.Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Errorf("second GET after failNext(1) = %v, want nil", err)
	}
}

// testDelay checks that a delay holds the requests on its Lease, and only those.
func testDelay(t *testing.T, c *cluster, g, name string) {
	const d = 400 * time.Millisecond
	ctx := t.Context()
	cl, rec := c.client(t)
	leases := cl.CoordinationV1().Leases(c.ns)
	other := memberName(g, "pod-3")
	rawLease(t, c, g, other)

	off := rec.delay(name, d)
	start := time.Now()
	if _, err := leases.Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("delayed GET: %v", err)
	}
	if took := time.Since(start); took < d {
		t.Errorf("delayed GET took %v, want at least %v", took, d)
	}
	if r := last(t, rec, on(verbGet, name)); r.sent.Sub(r.start) < d || r.status != 200 {
		t.Errorf("delayed GET record %v sent %v after it started, want at least %v", r, r.sent.Sub(r.start), d)
	}
	if _, err := leases.Get(ctx, other, metav1.GetOptions{}); err != nil {
		t.Fatalf("GET %s: %v", other, err)
	}
	if r := last(t, rec, on(verbGet, other)); r.sent.Sub(r.start) >= d/4 {
		t.Errorf("GET of another Lease sent %v after it started, want no delay", r.sent.Sub(r.start))
	}

	// Three delayed requests at once are in flight together.
	rec.reset()
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			if _, err := leases.Get(ctx, name, metav1.GetOptions{}); err != nil {
				t.Errorf("concurrent GET: %v", err)
			}
		})
	}
	wg.Wait()
	if n := rec.maxInFlight(on(verbGet, name)); n != 3 {
		t.Errorf("maxInFlight = %d, want 3", n)
	}

	off()
	if _, err := leases.Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("GET with delay off: %v", err)
	}
	if r := last(t, rec, on(verbGet, name)); r.sent.Sub(r.start) >= d/4 {
		t.Errorf("GET with delay off sent %v after it started, want no delay", r.sent.Sub(r.start))
	}
	if n := rec.maxInFlight(on(verbGet, name)); n != 3 {
		t.Errorf("maxInFlight after a lone GET = %d, want 3", n)
	}
}

// testBump checks that a bump makes the next UPDATE of its Lease conflict, and only that one.
func testBump(t *testing.T, c *cluster, name string) {
	ctx := t.Context()
	cl, rec := c.client(t)
	leases := cl.CoordinationV1().Leases(c.ns)

	// update GETs the Lease, sends an UPDATE that changes its leaseDurationSeconds and returns
	// the Lease as it was GOT.
	update := func(want func(error) bool) *coordinationv1.Lease {
		t.Helper()
		l, err := leases.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		changed := l.DeepCopy()
		changed.Spec.LeaseDurationSeconds = new(*l.Spec.LeaseDurationSeconds%100 + 1)
		if _, err := leases.Update(ctx, changed, metav1.UpdateOptions{}); !want(err) {
			t.Fatalf("UPDATE = %v", err)
		}
		return l
	}
	rawGet := func() *coordinationv1.Lease {
		t.Helper()
		l, err := c.leases().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("raw GET: %v", err)
		}
		return l
	}
	ok := func(err error) bool { return err == nil }

	// The Lease has no leaseDurationSeconds yet: give it one.
	l := rawGet()
	l.Spec.LeaseDurationSeconds = new(int32(3))
	if _, err := c.leases().Update(ctx, l, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("raw UPDATE: %v", err)
	}

	renew := time.Date(2026, 11, 2, 12, 0, 0, 123456000, time.UTC)
	rec.bump(name, renew)
	base := update(apierrors.IsConflict)
	after := rawGet()
	if after.ResourceVersion == base.ResourceVersion || after.Spec.RenewTime == nil || !after.Spec.RenewTime.Time.Equal(renew) ||
		after.Annotations[bumpAnnotation] != "1" || *after.Spec.LeaseDurationSeconds != *base.Spec.LeaseDurationSeconds {
		t.Errorf("bumped Lease: resourceVersion %s (was %s), renewTime %v, annotations %v; want a new version, renewTime %v, %s 1",
			after.ResourceVersion, base.ResourceVersion, after.Spec.RenewTime, after.Annotations, renew, bumpAnnotation)
	}
	if r := last(t, rec, on(verbUpdate, name)); r.String() != "UPDATE "+name+" 409" || !r.bumped {
		t.Errorf("bumped UPDATE record %+v, want UPDATE 409, bumped", r)
	}

	update(ok)
	if r := last(t, rec, on(verbUpdate, name)); r.String() != "UPDATE "+name+" 200" || r.bumped {
		t.Errorf("UPDATE after the bump record %+v, want UPDATE 200, not bumped", r)
	}

	// Without a renewTime only the annotation changes.
	rec.bump(name, time.Time{})
	update(apierrors.IsConflict)
	after = rawGet()
	if after.Annotations[bumpAnnotation] != "2" || after.Spec.RenewTime == nil || !after.Spec.RenewTime.Time.Equal(renew) {
		t.Errorf("bumped Lease: renewTime %v, annotations %v; want renewTime %v, %s 2", after.Spec.RenewTime, after.Annotations, renew, bumpAnnotation)
	}
	update(ok)
}

// testStall checks that a stall holds requests until it is released or their context ends,
// holds the stream of an open WATCH, and is released when the test ends.
func testStall(t *testing.T, c *cluster, g, name string) {
	ctx := t.Context()
	cl, rec := c.client(t)
	leases := cl.CoordinationV1().Leases(c.ns)

	release := rec.stall(on(verbGet, name))
	errc := make(chan error, 1)
	go func() {
		_, err := leases.Get(ctx, name, metav1.GetOptions{})
		errc <- err
	}()
	if r := rec.waitFor(t, 5*time.Second, on(verbGet, name)); !r.sent.IsZero() {
		t.Errorf("stalled GET record %+v was sent", r)
	}
	select {
	case err := <-errc:
		t.Fatalf("stalled GET returned %v before the release", err)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	if err := <-errc; err != nil {
		t.Fatalf("released GET: %v", err)
	}
	if r := last(t, rec, on(verbGet, name)); r.String() != "GET "+name+" 200" || r.sent.Sub(r.start) < 300*time.Millisecond {
		t.Errorf("released GET record %v sent %v after it started, want GET 200 after at least 300ms", r, r.sent.Sub(r.start))
	}

	// A stalled request ends with its context.
	release = rec.stall(on(verbGet, name))
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err := leases.Get(short, name, metav1.GetOptions{})
	cancel()
	release()
	if err == nil {
		t.Fatal("stalled GET with an ending context returned nil")
	}
	if r := last(t, rec, on(verbGet, name)); r.err == nil || r.status != 0 || !r.sent.IsZero() {
		t.Errorf("stalled GET record %+v, want the context's error, no status, never sent", r)
	}

	// A stalled WATCH stream holds its events until the release.
	list, err := leases.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("LIST: %v", err)
	}
	w, err := leases.Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
	if err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	defer w.Stop()
	release = rec.stall(on(verbWatch, ""))
	other := memberName(g, "pod-4")
	rawLease(t, c, g, other)
	select {
	case ev := <-w.ResultChan():
		t.Fatalf("stalled WATCH delivered %v %v before the release", ev.Type, ev.Object)
	case <-time.After(500 * time.Millisecond):
	}
	release()
	select {
	case ev := <-w.ResultChan():
		if l, ok := ev.Object.(*coordinationv1.Lease); ev.Type != watch.Added || !ok || l.Name != other {
			t.Errorf("released WATCH delivered %v %v, want ADDED %s", ev.Type, ev.Object, other)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("released WATCH delivered nothing in 5s")
	}

	// A stall left on is released when its test ends.
	errc = make(chan error, 1)
	t.Run("left on", func(t *testing.T) {
		cl, rec := c.client(t)
		rec.stall(on(verbGet, name))
		go func() {
			_, err := cl.CoordinationV1().Leases(c.ns).Get(ctx, name, metav1.GetOptions{})
			errc <- err
		}()
		if r := rec.waitFor(t, 5*time.Second, on(verbGet, name)); r.verb != verbGet {
			t.Errorf("waitFor returned %v", r)
		}
	})
	select {
	case err := <-errc:
		if err != nil {
			t.Errorf("GET stalled by an ended test: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GET stalled by an ended test still blocks after 5s")
	}
}
