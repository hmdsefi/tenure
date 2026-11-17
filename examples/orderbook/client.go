// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/orderbook"
)

// load is a set of clients that post orders to the members of a cluster and follow their routes.
type load struct {
	dc      *demo.Client // posts the orders; with a Router, it routes each by its market
	urls    []string     // base URLs of the members the clients start with; client i uses urls[i%len]
	clients int
	orders  int
	seed    uint64
	prefix  string // order IDs are <prefix><client>-<k>; "c" when empty

	accepted atomic.Int64
	fills    atomic.Int64
	sends    atomic.Int64 // the sends of the accepted orders
}

// run posts l.orders orders, split among l.clients clients, and returns when each is accepted,
// a client failed, or ctx ended. Client i makes its orders from rand.New(rand.NewPCG(seed, i)),
// names them <prefix><i>-<k> for k from 0 (prefix "c" unless set), and sends each to its first
// member, or, when l.dc has a Router, to the owner of the order's market. Client.PostSettled
// retries a 503, follows a 421 and sends again after a route loop, so an answer other than 200
// means a bug.
func (l *load) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	for i := range l.clients {
		n := l.orders / l.clients
		if i < l.orders%l.clients {
			n++
		}
		wg.Go(func() {
			if err := l.client(ctx, i, n); err != nil {
				once.Do(func() {
					first = fmt.Errorf("client %d: %w", i, err)
					cancel()
				})
			}
		})
	}
	wg.Wait()
	return first
}

// client posts n orders as client i.
func (l *load) client(ctx context.Context, i, n int) error {
	rng := rand.New(rand.NewPCG(l.seed, uint64(i))) //nolint:gosec // seeded on purpose: the same flags make the same orders
	url := l.urls[i%len(l.urls)] + orderbook.OrdersPath
	prefix := cmp.Or(l.prefix, "c")
	for k := range n {
		o := orderbook.RandomOrder(rng, fmt.Sprintf("%s%d-%d", prefix, i, k))
		body, err := json.Marshal(o)
		if err != nil {
			return err
		}
		res, err := l.dc.PostSettled(ctx, url, o.Market, body)
		if err != nil {
			return fmt.Errorf("order %s: %w", o.ID, err)
		}
		if res.Status != http.StatusOK {
			return fmt.Errorf("order %s: status %d: %s", o.ID, res.Status, res.Body)
		}
		var r orderbook.Result
		if err := json.Unmarshal(res.Body, &r); err != nil {
			return fmt.Errorf("order %s: answer: %w", o.ID, err)
		}
		l.fills.Add(int64(len(r.Fills)))
		l.sends.Add(int64(res.Sends))
		l.accepted.Add(1)
	}
	return nil
}
