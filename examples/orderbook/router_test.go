// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
	"github.com/hmdsefi/tenure/routehttp"
)

// TestOrderbookRouter checks the one-hop client: once the Router's view agrees with the nodes',
// each order of a demo.Client with a Router takes one send, straight to the member that owns its
// market, and gets a 200 there. The clients are all given n1's URL, which is only their
// Fallback.
func TestOrderbookRouter(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, books := obCluster(t, ctx, fencedb.New(), nil)
	hc := &http.Client{Transport: &http.Transport{}}
	t.Cleanup(hc.CloseIdleConnections)

	r, err := c.Router([]tenure.Space{books[0].space()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()

	// The views converge: every shard has an owner, and the Router names it for every market.
	var own map[int]owner
	pollUntil(t, ctx, "the router to name every shard's owner", func() bool {
		if own = owners(c); own == nil {
			return false
		}
		for i := range marketCount {
			m, err := r.Route(marketsSpace, marketName(i))
			if err != nil || m.ID != own[core.KeyShard(marketName(i), marketShards)].id {
				return false
			}
		}
		return true
	})

	var (
		mu    sync.Mutex
		sends []routehttp.SendInfo
	)
	dc := &demo.Client{HTTP: hc, Router: r, Space: marketsSpace, OnSend: func(s routehttp.SendInfo) {
		mu.Lock()
		sends = append(sends, s)
		mu.Unlock()
	}}
	const orders = 200
	l := &load{dc: dc, urls: []string{c.URL(0)}, clients: 4, orders: orders, seed: 3, prefix: "r"}
	if err := l.run(ctx); err != nil {
		t.Fatalf("orders through the router: %v", err)
	}
	if got := l.accepted.Load(); got != orders {
		t.Fatalf("%d orders accepted, want %d", got, orders)
	}
	if got := l.sends.Load(); got != orders {
		t.Errorf("the orders took %d sends, want one each: %d", got, orders)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sends) != orders {
		t.Errorf("OnSend saw %d sends, want %d", len(sends), orders)
	}
	addrOf := make(map[string]string, len(c.Addrs))
	for i, a := range c.Addrs {
		addrOf[memberID(i)] = a
	}
	away := 0 // sends to a member other than n1, whose URL the clients were given
	for _, s := range sends {
		if s.Status != http.StatusOK || s.Hop != 0 || s.Shard < 0 || s.Addr != addrOf[own[s.Shard].id] {
			t.Errorf("send %+v: want a 200 at hop 0 from the shard's owner %s", s, own[s.Shard].id)
		}
		if s.Addr != c.Addrs[0] {
			away++
		}
	}
	if away == 0 {
		t.Error("every send went to n1: the client did not route")
	}
}
