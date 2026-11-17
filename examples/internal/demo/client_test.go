// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package demo_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
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
		owner := newScript(t, reply{status: 503, unprocessed: true}, reply{status: 200, body: "done"})
		c := &demo.Client{HTTP: hc, Router: routerTo(t, host(owner)), Space: "orders"}
		start := time.Now()
		res, err := c.Post(testCtx(t, 10*time.Second), "http://127.0.0.1:1/orders", "k", []byte(`{}`))
		if err != nil || res.Status != 200 || res.Sends != 2 || res.Hops != 0 {
			t.Fatalf("Client.Post = %+v, %v; want 200 after 2 sends and no hop", res, err)
		}
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Errorf("the retry took %v; the demo waits at most 50 ms", d)
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
	})

	t.Run("bad url", func(t *testing.T) {
		c := &demo.Client{HTTP: hc, Router: routerTo(t, "127.0.0.1:1"), Space: "orders"}
		if _, err := c.Post(context.Background(), "/orders", "k", nil); err == nil {
			t.Error("Client.Post with a relative url succeeded")
		}
	})
}
