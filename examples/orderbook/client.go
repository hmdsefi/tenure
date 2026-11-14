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
)

const (
	// marketCount is the number of markets the clients trade: mkt-00 to mkt-31.
	marketCount = 32

	minPrice, maxPrice = 90, 110
	maxQty             = 10
)

// load is a set of clients that post orders to the members of a cluster and follow their routes.
type load struct {
	hc      *http.Client
	urls    []string // base URLs of the members the clients start with; client i uses urls[i%len]
	clients int
	orders  int
	seed    uint64
	prefix  string // order IDs are <prefix><client>-<k>; "c" when empty

	accepted atomic.Int64
	fills    atomic.Int64
}

// marketName returns the name of market i.
func marketName(i int) string {
	return fmt.Sprintf("mkt-%02d", i)
}

// run posts l.orders orders, split among l.clients clients, and returns when each is accepted,
// a client failed, or ctx ended. Client i makes its orders from rand.New(rand.NewPCG(seed, i)),
// names them <prefix><i>-<k> for k from 0 (prefix "c" unless set), and sends each to its first
// member. demo.PostSettled retries a 503, follows a 421 and sends again after a route loop, so an
// answer other than 200 means a bug.
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
	url := l.urls[i%len(l.urls)] + "/orders"
	prefix := cmp.Or(l.prefix, "c")
	for k := range n {
		o := order{
			ID:     fmt.Sprintf("%s%d-%d", prefix, i, k),
			Market: marketName(rng.IntN(marketCount)),
			Side:   sideBuy,
			Price:  minPrice + rng.Int64N(maxPrice-minPrice+1),
			Qty:    1 + rng.Int64N(maxQty),
		}
		if rng.IntN(2) == 0 {
			o.Side = sideSell
		}
		body, err := json.Marshal(o)
		if err != nil {
			return err
		}
		res, err := demo.PostSettled(ctx, l.hc, url, body)
		if err != nil {
			return fmt.Errorf("order %s: %w", o.ID, err)
		}
		if res.Status != http.StatusOK {
			return fmt.Errorf("order %s: status %d: %s", o.ID, res.Status, res.Body)
		}
		var r result
		if err := json.Unmarshal(res.Body, &r); err != nil {
			return fmt.Errorf("order %s: answer: %w", o.ID, err)
		}
		l.fills.Add(int64(len(r.Fills)))
		l.accepted.Add(1)
	}
	return nil
}
