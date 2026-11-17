// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package viewhttp_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/membershiptest"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/static"
	"github.com/hmdsefi/tenure/viewhttp"
)

// The tests of NewMembership. Steps of the TestATRTR17 tests were written for them from the
// issue's text.

var _ tenure.Membership = (*viewhttp.Membership)(nil)

// errDown is the error of a request to a stopped endpoint of a fakeNet.
var errDown = errors.New("fakenet: connection refused")

// fakeNet is an http.RoundTripper that serves each host in-process with its handler, in the
// caller's goroutine, so that a long poll blocks only on channels and timers (and
// testing/synctest can wait for it). A stopped host refuses requests, and stopping a host fails
// the requests it is serving.
type fakeNet struct {
	mu    sync.Mutex
	hosts map[string]*fakeHost
}

// fakeHost is one host of a fakeNet.
type fakeHost struct {
	h        http.Handler
	down     bool
	inflight map[*context.CancelFunc]struct{}
	served   int // requests started
}

func newFakeNet() *fakeNet { return &fakeNet{hosts: make(map[string]*fakeHost)} }

// serve starts host with handler h.
func (n *fakeNet) serve(host string, h http.Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.hosts[host] = &fakeHost{h: h, inflight: make(map[*context.CancelFunc]struct{})}
}

// stop stops host: it refuses new requests, and the requests it is serving fail.
func (n *fakeNet) stop(host string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	fh := n.hosts[host]
	fh.down = true
	for c := range fh.inflight {
		(*c)()
	}
}

// busy returns the hosts serving a request now, sorted.
func (n *fakeNet) busy() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []string
	for name, fh := range n.hosts {
		if len(fh.inflight) > 0 {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func (n *fakeNet) RoundTrip(r *http.Request) (*http.Response, error) {
	n.mu.Lock()
	fh := n.hosts[r.URL.Host]
	if fh == nil || fh.down {
		n.mu.Unlock()
		return nil, errDown
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	fh.inflight[&cancel] = struct{}{}
	fh.served++
	n.mu.Unlock()

	w := httptest.NewRecorder()
	fh.h.ServeHTTP(w, r.WithContext(ctx))

	n.mu.Lock()
	delete(fh.inflight, &cancel)
	down := fh.down
	n.mu.Unlock()
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	if down {
		return nil, errDown
	}
	return w.Result(), nil
}

// fastTiming is a valid timing with short periods for the tests.
var fastTiming = core.Timing{
	LeaseDuration: 2 * time.Second,
	RenewDeadline: time.Second,
	RetryPeriod:   50 * time.Millisecond,
	SlotGrace:     2 * time.Second,
}

// newFakeMembership returns a Membership over hosts of n, with a wait of 10s, a backoff from 1s
// to 4s and core.DefaultTiming: polls give up after 20s.
func newFakeMembership(t *testing.T, n *fakeNet, hosts ...string) *viewhttp.Membership {
	t.Helper()
	endpoints := make([]string, len(hosts))
	for i, h := range hosts {
		endpoints[i] = "http://" + h
	}
	m, err := viewhttp.NewMembership(endpoints, viewhttp.WithHTTPClient(&http.Client{Transport: n}),
		viewhttp.WithWait(10*time.Second), viewhttp.WithBackoff(time.Second, 4*time.Second))
	if err != nil {
		t.Fatalf("NewMembership: %v", err)
	}
	return m
}

// recv returns the view ch holds now, failing t when it holds none or is closed.
func recv(t *testing.T, ch <-chan core.View) core.View {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("Watch channel closed, want a view")
		}
		return v
	default:
		t.Fatal("Watch channel empty, want a view")
	}
	return core.View{}
}

// wantEmpty fails t unless ch holds no view and is open.
func wantEmpty(t *testing.T, ch <-chan core.View, why string) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatalf("Watch channel closed %s, want it open", why)
		}
		t.Fatalf("view %d (%s) %s, want none", v.Version, ids(v), why)
	default:
	}
}

// ids returns the IDs of v's members, joined by commas.
func ids(v core.View) string {
	s := make([]string, len(v.Members))
	for i, m := range v.Members {
		s[i] = m.ID
	}
	return strings.Join(s, ",")
}

// other returns the host of a and b that is not h.
func other(h string) string {
	if h == "a" {
		return "b"
	}
	return "a"
}

// TestATRTR17Failover checks that the client follows one of two endpoints, moves to the other
// when it stops, after the backoff, and accepts that endpoint's older view under its own next
// Version; and that it moves back when the first comes back as a new handler and the second
// stops, without delivering a view whose content has not changed.
func TestATRTR17Failover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srcs := map[string]*source{"a": newSource(new(viewOf(1, "m1"))), "b": newSource(new(viewOf(1, "m1")))}
		n := newFakeNet()
		n.serve("a", viewhttp.Handler(srcs["a"]))
		n.serve("b", viewhttp.Handler(srcs["b"]))
		m := newFakeMembership(t, n, "a", "b")
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := m.Watch(ctx)
		if err != nil {
			t.Fatalf("Watch() = %v, want nil", err)
		}
		if v := recv(t, ch); v.Version != 1 || ids(v) != "m1" {
			t.Fatalf("first view %d (%s), want 1 (m1)", v.Version, ids(v))
		}
		synctest.Wait()
		busy := n.busy()
		if len(busy) != 1 {
			t.Fatalf("hosts serving a poll: %v, want one", busy)
		}
		cur := busy[0]

		// The current endpoint's view moves ahead; then it stops, and the other, still at the
		// older view, takes over after the backoff.
		srcs[cur].set(viewOf(9, "m1", "m2"))
		synctest.Wait()
		if v := recv(t, ch); v.Version != 2 || ids(v) != "m1,m2" {
			t.Fatalf("view %d (%s) from %s, want 2 (m1,m2)", v.Version, ids(v), cur)
		}
		stopAt := time.Now()
		n.stop(cur)
		synctest.Wait()
		wantEmpty(t, ch, "before the backoff")
		time.Sleep(time.Second)
		synctest.Wait()
		if v := recv(t, ch); v.Version != 3 || ids(v) != "m1" {
			t.Fatalf("view %d (%s) after failover, want 3 (m1), the older view of %s", v.Version, ids(v), other(cur))
		}
		if d := time.Since(stopAt); d != time.Second {
			t.Errorf("failover took %v, want the 1s backoff", d)
		}
		if got := n.busy(); !slices.Equal(got, []string{other(cur)}) {
			t.Fatalf("hosts serving a poll after failover: %v, want [%s]", got, other(cur))
		}

		// The stopped endpoint comes back as a new handler, at the same view; the other stops.
		n.serve(cur, viewhttp.Handler(srcs[other(cur)]))
		n.stop(other(cur))
		time.Sleep(5 * time.Second)
		synctest.Wait()
		wantEmpty(t, ch, "after moving back to an endpoint with the same content")
		if got := n.busy(); !slices.Equal(got, []string{cur}) {
			t.Fatalf("hosts serving a poll after the second failover: %v, want [%s]", got, cur)
		}
		if m.Stale() {
			t.Error("Stale() = true after the second failover, want false")
		}
		srcs[other(cur)].set(viewOf(2, "m1", "m3"))
		synctest.Wait()
		if v := recv(t, ch); v.Version != 4 || ids(v) != "m1,m3" {
			t.Fatalf("view %d (%s), want 4 (m1,m3)", v.Version, ids(v))
		}

		cancel()
		synctest.Wait()
		if _, ok := <-ch; ok {
			t.Fatal("Watch channel open after its ctx ended, want it closed")
		}
		if got := n.busy(); len(got) != 0 {
			t.Errorf("hosts serving a poll after the last Watch ended: %v, want none", got)
		}
	})
}

// TestATRTR17Stale checks that Stale is true before the first successful poll, false while polls
// succeed, true once no poll has succeeded for the wait plus RenewDeadline, with the last view
// kept and the channel open, and false again after the next successful poll.
func TestATRTR17Stale(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(new(viewOf(1, "m1")))
		n := newFakeNet()
		n.serve("a", viewhttp.Handler(src))
		n.serve("b", viewhttp.Handler(src))
		m := newFakeMembership(t, n, "a", "b")
		if !m.Stale() {
			t.Error("Stale() = false before any poll, want true")
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch, err := m.Watch(ctx)
		if err != nil {
			t.Fatalf("Watch() = %v, want nil", err)
		}
		recv(t, ch)
		if m.Stale() {
			t.Error("Stale() = true after the first view, want false")
		}
		// The second poll waits 10s for a change and gets 304 at 10s: the last success.
		time.Sleep(15 * time.Second)
		n.stop("a")
		n.stop("b")
		time.Sleep(15 * time.Second) // 30s: 20s after the last success
		synctest.Wait()
		if m.Stale() {
			t.Error("Stale() = true 20s after the last successful poll, want false until after it")
		}
		time.Sleep(time.Millisecond)
		if !m.Stale() {
			t.Error("Stale() = false more than 20s after the last successful poll, want true")
		}
		wantEmpty(t, ch, "while stale")

		n.serve("a", viewhttp.Handler(src))
		n.serve("b", viewhttp.Handler(src))
		time.Sleep(4 * time.Second) // at most one backoff, capped at 4s
		synctest.Wait()
		if m.Stale() {
			t.Error("Stale() = true after the endpoints came back, want false")
		}
		wantEmpty(t, ch, "after the endpoints came back with the same view")
	})
}

// TestClientWatchFails checks that Watch fails after the wait plus RenewDeadline when no
// endpoint answers, with an error that wraps the latest failure, and with ctx's error when ctx
// ends first; no goroutine is left either way.
func TestClientWatchFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newFakeNet()
		m := newFakeMembership(t, n, "a", "b")
		start := time.Now()
		ch, err := m.Watch(context.Background())
		if ch != nil || !errors.Is(err, errDown) ||
			!strings.HasPrefix(err.Error(), "viewhttp: Watch: no view within 20s: ") {
			t.Fatalf("Watch() = %v, %v, want an error after 20s wrapping %v", ch, err, errDown)
		}
		if d := time.Since(start); d != 20*time.Second {
			t.Errorf("Watch failed after %v, want 20s", d)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if ch, err := m.Watch(ctx); ch != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Watch(ctx) = %v, %v, want nil, context.DeadlineExceeded", ch, err)
		}
		synctest.Wait()
		if !m.Stale() {
			t.Error("Stale() = false with no successful poll, want true")
		}
	})
}

// TestClientRefuses checks the errors of Join, SetDraining and Leave: ctx's error when ctx is
// done, else one that matches tenure.ErrWatchOnly and names the method.
func TestClientRefuses(t *testing.T) {
	m, err := viewhttp.NewMembership([]string{"http://relay:8080/tenure/"})
	if err != nil {
		t.Fatalf("NewMembership: %v", err)
	}
	ctx := context.Background()
	done, cancel := context.WithCancel(ctx)
	cancel()
	calls := map[string]func(context.Context) error{
		"Join":        func(ctx context.Context) error { return m.Join(ctx, core.Member{ID: "x"}) },
		"SetDraining": func(ctx context.Context) error { return m.SetDraining(ctx, true) },
		"Leave":       m.Leave,
	}
	for name, call := range calls {
		err := call(ctx)
		if want := "viewhttp: " + name + ": tenure: watch only"; err == nil || err.Error() != want ||
			!errors.Is(err, tenure.ErrWatchOnly) {
			t.Errorf("%s = %v, want %q matching ErrWatchOnly", name, err, want)
		}
		if err := call(done); !errors.Is(err, context.Canceled) || errors.Is(err, tenure.ErrWatchOnly) {
			t.Errorf("%s(done ctx) = %v, want context.Canceled", name, err)
		}
	}
}

// TestNewMembershipErrors checks NewMembership's errors.
func TestNewMembershipErrors(t *testing.T) {
	ok := []string{"http://a"}
	bad := core.DefaultTiming
	bad.RetryPeriod = bad.RenewDeadline
	tests := []struct {
		endpoints []string
		opts      []viewhttp.ClientOption
		want      string
	}{
		{nil, nil, "viewhttp: NewMembership: no endpoints"},
		{[]string{"http://a", "relay:80"}, nil, `viewhttp: NewMembership: endpoint "relay:80" is not an http or https URL`},
		{[]string{"ftp://a"}, nil, `viewhttp: NewMembership: endpoint "ftp://a" is not an http or https URL`},
		{[]string{"http://"}, nil, `viewhttp: NewMembership: endpoint "http://" is not an http or https URL`},
		{ok, []viewhttp.ClientOption{viewhttp.WithHTTPClient(nil)}, "viewhttp: NewMembership: nil HTTP client"},
		{ok, []viewhttp.ClientOption{viewhttp.WithWait(0)}, "viewhttp: NewMembership: wait 0s is not positive"},
		{ok, []viewhttp.ClientOption{viewhttp.WithBackoff(0, time.Second)}, "viewhttp: NewMembership: backoff 0s to 1s is not a positive range"},
		{ok, []viewhttp.ClientOption{viewhttp.WithBackoff(2*time.Second, time.Second)}, "viewhttp: NewMembership: backoff 2s to 1s is not a positive range"},
		{ok, []viewhttp.ClientOption{viewhttp.WithTiming(bad)}, "viewhttp: NewMembership: " + bad.Validate().Error()},
	}
	for _, tt := range tests {
		m, err := viewhttp.NewMembership(tt.endpoints, tt.opts...)
		if m != nil || err == nil || err.Error() != tt.want {
			t.Errorf("NewMembership(%q) = %v, %v, want nil, %q", tt.endpoints, m, err, tt.want)
		}
	}
}

// TestClientBadAnswers checks that an answer that is not a valid view, or an error status,
// counts as a failure: the client moves to the next endpoint.
func TestClientBadAnswers(t *testing.T) {
	answers := map[string]http.HandlerFunc{
		"garbage": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{not json")) },
		"invalid": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"version":1,"members":[{"id":""}],"slots":0,"slot_holders":[]}`))
		},
		"bad-time": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"version":1,"members":[{"id":"x","join_at":"yesterday"}]}`))
		},
		"status": func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "busy", http.StatusTooManyRequests) },
		"304":    func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotModified) },
	}
	for name, h := range answers {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				n := newFakeNet()
				n.serve("bad", h)
				m := newFakeMembership(t, n, "bad")
				_, err := m.Watch(context.Background())
				if err == nil || !strings.Contains(err.Error(), "viewhttp: GET http://bad/view: ") {
					t.Errorf("Watch() = %v, want a failure of GET http://bad/view", err)
				}
			})
		})
	}
}

// TestClientRestart checks that the polls stop when the last Watch channel closes and start
// again with the next Watch, whose views go on with the client's Version counter.
func TestClientRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(new(viewOf(4, "m1")))
		n := newFakeNet()
		n.serve("a", viewhttp.Handler(src))
		m := newFakeMembership(t, n, "a")
		ctx, cancel := context.WithCancel(context.Background())
		ch1, err := m.Watch(ctx)
		if err != nil {
			t.Fatalf("Watch() = %v, want nil", err)
		}
		ch2, err := m.Watch(ctx)
		if err != nil {
			t.Fatalf("Watch() = %v, want nil", err)
		}
		if v1, v2 := recv(t, ch1), recv(t, ch2); v1.Version != 1 || v2.Version != 1 {
			t.Fatalf("views %d and %d, want 1 on both", v1.Version, v2.Version)
		}
		cancel()
		synctest.Wait()
		if got := n.busy(); len(got) != 0 {
			t.Fatalf("hosts serving a poll with no Watch open: %v, want none", got)
		}
		src.set(viewOf(5, "m1", "m2"))
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		ch3, err := m.Watch(ctx)
		if err != nil {
			t.Fatalf("Watch() = %v, want nil", err)
		}
		if v := recv(t, ch3); v.Version != 2 || ids(v) != "m1,m2" {
			t.Fatalf("view %d (%s), want 2 (m1,m2)", v.Version, ids(v))
		}
	})
}

// TestATRTR17WatchOnly runs the watch-only conformance cases on a client of a node's handler, over
// real HTTP: the writers are static memberships, and the node, a member of the same group with a
// memstore Store, merges the store's slot data into the views it serves.
func TestATRTR17WatchOnly(t *testing.T) {
	membershiptest.RunWatchOnly(t, func(t *testing.T, ids []string) membershiptest.WatchOnlyFixture {
		g, err := static.New(nil)
		if err != nil {
			t.Fatalf("static.New(nil) = %v", err)
		}
		store := memstore.New(nil)
		writers := make([]tenure.Membership, len(ids))
		for i := range ids {
			writers[i] = g.Share()
		}
		node, err := tenure.Start(context.Background(), tenure.Config{
			Self:       tenure.Member{ID: "relay", Addr: "relay:8080"},
			Membership: g.Share(),
			Store:      store,
			Timing:     fastTiming,
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		t.Cleanup(func() { _ = node.Close(context.Background()) })
		srv := httptest.NewServer(viewhttp.Handler(node))
		t.Cleanup(srv.Close)
		m, err := viewhttp.NewMembership([]string{srv.URL}, viewhttp.WithWait(time.Second),
			viewhttp.WithTiming(fastTiming))
		if err != nil {
			t.Fatalf("NewMembership: %v", err)
		}
		holder := func(id string) string { return id + "/1" }
		return membershiptest.WatchOnlyFixture{
			Writers:   writers,
			WatchOnly: m,
			Settle:    300 * time.Millisecond,
			Slots: &membershiptest.SlotControl{
				SetCount: store.SetSlots,
				Claim: func(ctx context.Context, id string) (int, error) {
					if err := store.KeepAlive(ctx, holder(id), core.DefaultTiming); err != nil {
						return -1, err
					}
					return store.ClaimSlot(ctx, holder(id))
				},
				Release: func(ctx context.Context, id string, slot int) error {
					return store.ReleaseSlot(ctx, holder(id), slot)
				},
			},
		}
	})
}
