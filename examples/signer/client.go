// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/signing"
)

// walletCount is the number of wallets the clients sign for: w-00 to w-31.
const walletCount = 32

// load is a set of clients that post sign requests to the members of a cluster and follow their
// routes.
type load struct {
	hc       *http.Client
	urls     []string // base URLs of the members the clients start with; client i uses urls[i%len]
	clients  int
	requests int
	seed     uint64
	prefix   string // payloads are <prefix><client>-<k>; "p" when empty

	signed atomic.Int64

	mu  sync.Mutex
	txs []signing.Tx // the transactions of the 200 answers
}

// walletName returns the name of wallet i.
func walletName(i int) string {
	return fmt.Sprintf("w-%02d", i)
}

// run posts l.requests requests, split among l.clients clients, and returns when each is signed,
// a client failed, or ctx ended. Client i picks its wallets from rand.New(rand.NewPCG(seed, i))
// and sends each request to its first member. demo.Post retries a 503 and follows a 421, so an
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
		n := l.requests / l.clients
		if i < l.requests%l.clients {
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

// client posts n requests as client i.
func (l *load) client(ctx context.Context, i, n int) error {
	rng := rand.New(rand.NewPCG(l.seed, uint64(i))) //nolint:gosec // seeded on purpose: the same flags make the same requests
	url := l.urls[i%len(l.urls)] + "/sign"
	prefix := l.prefix
	if prefix == "" {
		prefix = "p"
	}
	for k := range n {
		req := signing.SignRequest{Wallet: walletName(rng.IntN(walletCount)), Payload: fmt.Sprintf("%s%d-%d", prefix, i, k)}
		body, err := json.Marshal(req)
		if err != nil {
			return err
		}
		res, err := demo.Post(ctx, l.hc, url, body)
		if err != nil {
			return fmt.Errorf("request %s: %w", req.Payload, err)
		}
		if res.Status != http.StatusOK {
			return fmt.Errorf("request %s: status %d: %s", req.Payload, res.Status, res.Body)
		}
		var tx signing.Tx
		if err := json.Unmarshal(res.Body, &tx); err != nil {
			return fmt.Errorf("request %s: answer: %w", req.Payload, err)
		}
		l.record(tx)
	}
	return nil
}

// record keeps a signed transaction.
func (l *load) record(tx signing.Tx) {
	l.mu.Lock()
	l.txs = append(l.txs, tx)
	l.mu.Unlock()
	l.signed.Add(1)
}
