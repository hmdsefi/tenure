// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package demo_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/routehttp"
	"github.com/hmdsefi/tenure/static"
)

// routerTo returns a Router whose one member, "a" at addr, owns the one shard of the space
// "orders", once the Router has applied its view.
func routerTo(t *testing.T, addr string) *tenure.Router {
	t.Helper()
	m, err := static.New([]core.Member{{ID: "a", Addr: addr, Slot: -1}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := tenure.NewRouter(m, []tenure.Space{{Name: "orders", Shards: 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if err := r.Ready(testCtx(t, 10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Route("orders", "k"); err != nil || got.Addr != addr {
		t.Fatalf("Route = %+v, %v; want %s", got, err, addr)
	}
	return r
}

// sendLog records what a Client's OnSend reports.
type sendLog struct {
	mu    sync.Mutex
	sends []routehttp.SendInfo
}

func (l *sendLog) add(s routehttp.SendInfo) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sends = append(l.sends, s)
}

func (l *sendLog) all() []routehttp.SendInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]routehttp.SendInfo(nil), l.sends...)
}

// TestClient checks demo.Client: without a Router it is Post, and with one it sends to the
// owner the Router names, follows a 421 and retries a 503 that promises no effect.
func TestClient(t *testing.T) {
	hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	t.Run("without a router it is Post", func(t *testing.T) {
		owner := newScript(t, reply{status: 200, body: "done"})
		other := newScript(t, reply{status: 421, route: host(owner)})
		c := &demo.Client{HTTP: hc}
		res, err := c.Post(testCtx(t, 10*time.Second), other.URL+"/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 200 || string(res.Body) != "done" || res.Sends != 2 || res.Hops != 0 {
			t.Fatalf("Client.Post = %+v, %v; want 200 done after 2 sends, as Post", res, err)
		}
		if n := len(other.requests()) + len(owner.requests()); n != 2 {
			t.Errorf("the members got %d requests, want 2", n)
		}

		// A route loop is ErrRouteLoop after 3 sends, and PostSettled sends again.
		self := newScript(t)
		loop := reply{status: 421, route: host(self)}
		self.setReplies(loop, loop, loop, loop, reply{status: 200, body: "done"})
		res, err = c.Post(testCtx(t, 10*time.Second), self.URL+"/orders", "k", []byte(`{}`))
		if !errors.Is(err, demo.ErrRouteLoop) || res.Sends != 3 {
			t.Fatalf("Client.Post into a loop = %+v, %v; want ErrRouteLoop after 3 sends", res, err)
		}
		res, err = c.PostSettled(testCtx(t, 10*time.Second), self.URL+"/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 200 {
			t.Fatalf("Client.PostSettled = %+v, %v; want 200", res, err)
		}
	})

	t.Run("one send to the owner", func(t *testing.T) {
		owner := newScript(t, reply{status: 200, body: "done"})
		fallback := newScript(t, reply{status: 500})
		var log sendLog
		c := &demo.Client{HTTP: hc, Router: routerTo(t, host(owner)), Space: "orders", OnSend: log.add}
		res, err := c.Post(testCtx(t, 10*time.Second), fallback.URL+"/orders?x=1", "k", []byte(`{"id":"o1"}`))
		if err != nil || res.Status != 200 || string(res.Body) != "done" || res.Sends != 1 || res.Hops != 0 {
			t.Fatalf("Client.Post = %+v, %v; want 200 done after 1 send", res, err)
		}
		if n := len(fallback.requests()); n != 0 {
			t.Errorf("the url's member got %d requests, want 0", n)
		}
		want := sent{"POST", "/orders", "x=1", "application/json", `{"id":"o1"}`}
		if got := owner.requests(); len(got) != 1 || got[0] != want {
			t.Errorf("the owner got %+v, want one %+v", got, want)
		}
		if got := log.all(); len(got) != 1 || got[0].Addr != host(owner) || got[0].Status != 200 || got[0].Shard != 0 {
			t.Errorf("OnSend got %+v, want one 200 from the owner", got)
		}
	})

	t.Run("a 421 is followed", func(t *testing.T) {
		owner := newScript(t, reply{status: 200, body: "done"})
		stale := newScript(t, reply{status: 421, route: host(owner), unprocessed: true})
		fallback := newScript(t, reply{status: 500})
		c := &demo.Client{HTTP: hc, Router: routerTo(t, host(stale)), Space: "orders"}
		res, err := c.Post(testCtx(t, 10*time.Second), fallback.URL+"/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 200 || res.Sends != 2 || res.Hops != 1 {
			t.Fatalf("Client.Post = %+v, %v; want 200 after 2 sends and 1 hop", res, err)
		}
		if len(stale.requests()) != 1 || len(owner.requests()) != 1 || len(fallback.requests()) != 0 {
			t.Errorf("requests: stale member %d, owner %d, url's member %d; want 1, 1, 0",
				len(stale.requests()), len(owner.requests()), len(fallback.requests()))
		}
	})

	t.Run("a 503 that promises no effect is sent again", func(t *testing.T) {
		// Six retries wait at most 300 ms in all with the demo's 50 ms; with Retry-After's 1 s
		// they would take under 750 ms with a probability below 0.03 %.
		busy := reply{status: 503, unprocessed: true}
		owner := newScript(t, busy, busy, busy, busy, busy, busy, reply{status: 200, body: "done"})
		c := &demo.Client{HTTP: hc, Router: routerTo(t, host(owner)), Space: "orders"}
		start := time.Now()
		res, err := c.Post(testCtx(t, 10*time.Second), "http://127.0.0.1:1/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 200 || res.Sends != 7 || res.Hops != 0 {
			t.Fatalf("Client.Post = %+v, %v; want 200 after 7 sends and no hop", res, err)
		}
		if d := time.Since(start); d > 750*time.Millisecond {
			t.Errorf("6 retries took %v; the demo waits at most 50 ms before each", d)
		}
	})

	t.Run("a 503 without the promise is the result", func(t *testing.T) {
		owner := newScript(t, reply{status: 503, body: "maybe"}, reply{status: 200})
		c := &demo.Client{HTTP: hc, Router: routerTo(t, host(owner)), Space: "orders"}
		res, err := c.Post(testCtx(t, 10*time.Second), "http://127.0.0.1:1/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 503 || string(res.Body) != "maybe" || res.Sends != 1 {
			t.Fatalf("Client.Post = %+v, %v; want the 503 after 1 send", res, err)
		}
	})

	t.Run("a spent redirect budget is a route loop", func(t *testing.T) {
		self := newScript(t)
		loop := reply{status: 421, route: host(self), unprocessed: true}
		self.setReplies(loop, loop, loop, loop, reply{status: 200, body: "done"})
		c := &demo.Client{HTTP: hc, Router: routerTo(t, host(self)), Space: "orders"}
		res, err := c.Post(testCtx(t, 10*time.Second), "http://127.0.0.1:1/orders", "k", []byte(`{}`))
		if !errors.Is(err, demo.ErrRouteLoop) || res.Status != 421 || res.Sends != 3 || res.Hops != 2 {
			t.Fatalf("Client.Post = %+v, %v; want ErrRouteLoop after 3 sends and 2 hops", res, err)
		}
		res, err = c.PostSettled(testCtx(t, 10*time.Second), "http://127.0.0.1:1/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 200 || string(res.Body) != "done" || res.Sends != 2 {
			t.Fatalf("Client.PostSettled = %+v, %v; want 200 done, its last Post after 2 sends", res, err)
		}
	})

	t.Run("the url's host takes what the router cannot place", func(t *testing.T) {
		fallback := newScript(t, reply{status: 200, body: "fallback"})
		m, err := static.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := tenure.NewRouter(m, []tenure.Space{{Name: "orders", Shards: 1}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		var log sendLog
		c := &demo.Client{HTTP: hc, Router: r, Space: "orders", OnSend: log.add}
		res, err := c.Post(testCtx(t, 10*time.Second), fallback.URL+"/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 200 || string(res.Body) != "fallback" || res.Sends != 1 {
			t.Fatalf("Client.Post = %+v, %v; want the url's member's answer after 1 send", res, err)
		}
		if got := log.all(); len(got) != 1 || got[0].Shard != -1 || got[0].Addr != host(fallback) {
			t.Errorf("OnSend got %+v, want one send to the fallback", got)
		}

		// A url with another host has that host as its fallback, on the same Client.
		second := newScript(t, reply{status: 200, body: "second"})
		res, err = c.Post(testCtx(t, 10*time.Second), second.URL+"/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 200 || string(res.Body) != "second" || res.Sends != 1 {
			t.Fatalf("Client.Post to a second url = %+v, %v; want that url's member's answer after 1 send", res, err)
		}
		if n := len(fallback.requests()); n != 1 {
			t.Errorf("the first url's member got %d requests, want 1", n)
		}
	})

	t.Run("concurrent posts count their own sends", func(t *testing.T) {
		owner := newScript(t, reply{status: 200, body: "owner"})
		// The Router's member answers /hop with a 421 to owner, which is not in the Router's
		// view: the Router refuses the correction, so every /hop takes 2 sends.
		member := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if r.URL.Path == "/hop" {
				routehttp.Misdirected(w, tenure.Member{ID: "b", Addr: host(owner)})
				return
			}
			_, _ = w.Write([]byte("member"))
		}))
		t.Cleanup(member.Close)
		carrier := &countingTransport{base: hc.Transport}
		c := &demo.Client{
			HTTP:   &http.Client{Transport: carrier},
			Router: routerTo(t, strings.TrimPrefix(member.URL, "http://")),
			Space:  "orders",
		}
		const posts = 64
		ctx := testCtx(t, 10*time.Second)
		errs := make(chan error, posts)
		var wg sync.WaitGroup
		for i := range posts {
			wg.Go(func() {
				path, body, sends, hops := "/direct", "member", 1, 0
				if i%2 == 1 {
					path, body, sends, hops = "/hop", "owner", 2, 1
				}
				res, err := c.Post(ctx, "http://127.0.0.1:1"+path, "k", []byte(`{}`))
				if err != nil || res.Status != 200 || string(res.Body) != body || res.Sends != sends || res.Hops != hops {
					errs <- fmt.Errorf("Client.Post %s = %+v, %v; want %s after %d sends and %d hops", path, res, err, body, sends, hops)
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if got, want := carrier.sends.Load(), int64(posts/2+posts/2*2); got != want {
			t.Errorf("HTTP's transport carried %d sends, want %d", got, want)
		}
	})

	t.Run("HTTP's Timeout bounds a post", func(t *testing.T) {
		release := make(chan struct{})
		slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-release:
			}
		}))
		t.Cleanup(func() {
			close(release)
			slow.Close()
		})
		c := &demo.Client{
			HTTP:   &http.Client{Transport: hc.Transport, Timeout: 100 * time.Millisecond},
			Router: routerTo(t, strings.TrimPrefix(slow.URL, "http://")),
			Space:  "orders",
		}
		start := time.Now()
		if res, err := c.Post(testCtx(t, 5*time.Second), "http://127.0.0.1:1/orders", "k", []byte(`{}`)); err == nil {
			t.Fatalf("Client.Post to a member that never answers = %+v, want an error", res)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Errorf("Client.Post gave up after %v; HTTP's Timeout is 100 ms", d)
		}
	})

	t.Run("bad url", func(t *testing.T) {
		c := &demo.Client{HTTP: hc, Router: routerTo(t, "127.0.0.1:1"), Space: "orders"}
		if _, err := c.Post(context.Background(), "/orders", "k", nil); err == nil {
			t.Error("Client.Post with a relative url succeeded")
		}
	})
}

// countingTransport counts the requests it carries.
type countingTransport struct {
	base  http.RoundTripper
	sends atomic.Int64
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.sends.Add(1)
	return c.base.RoundTrip(r)
}
