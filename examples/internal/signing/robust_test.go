// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package signing

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

func TestSignHandlerBodyLimit(t *testing.T) {
	r := startRig(t, false)
	body := `{"wallet":"w-00","payload":"` + strings.Repeat("x", maxBody) + `"}`
	if a := r.post(t, 0, body, nil); a.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("a body over the limit: %d, want 413: %.100s", a.status, a.body)
	}
	if got := r.nonces.reserves.Load(); got != 0 {
		t.Errorf("%d reservations for an oversized body", got)
	}
}

// TestSignHandlerProxyTargetFromClient checks that a client cannot name where the server proxies:
// the internal target header is overwritten with the shard owner's address, and an owner ignores
// it.
func TestSignHandlerProxyTargetFromClient(t *testing.T) {
	var hits atomic.Int64
	victim := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer victim.Close()
	target := strings.TrimPrefix(victim.URL, "http://")

	r := startRig(t, true)
	o := r.owner(t)
	hdr := map[string]string{proxyTarget: target}
	if a := r.postUntil(t, other(o), signBody("p"), hdr); a.status != http.StatusOK {
		t.Fatalf("through the proxy with a forged target: %d: %s", a.status, a.body)
	}
	if a := r.postUntil(t, o, signBody("p"), hdr); a.status != http.StatusOK {
		t.Fatalf("at the owner with a forged target: %d: %s", a.status, a.body)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("the server proxied to a client-chosen host %d times", got)
	}
}

// TestSignHandlerConcurrentWallet sends many requests for one wallet at once: the owner hands out
// each nonce once and broadcasts them in order, so the chain sees no conflict and no gap.
func TestSignHandlerConcurrentWallet(t *testing.T) {
	r := startRig(t, false)
	o := r.owner(t)
	r.postUntil(t, o, signBody("warm"), nil)

	const n = 64
	var wg sync.WaitGroup
	statuses := make([]int, n)
	for i := range n {
		wg.Go(func() {
			req, err := http.NewRequestWithContext(r.ctx, http.MethodPost, r.c.URL(o)+"/sign",
				strings.NewReader(signBody(strconv.Itoa(i))))
			if err != nil {
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			statuses[i] = resp.StatusCode
		})
	}
	wg.Wait()
	for i, s := range statuses {
		if s != http.StatusOK {
			t.Fatalf("request %d answered %d", i, s)
		}
	}
	if rep := r.chain.Report(); rep.Accepted != n+1 || len(rep.Conflicts)+len(rep.Gaps) != 0 {
		t.Fatalf("Report = accepted %d, %d conflicts, %d gaps; want %d accepted and none rejected",
			rep.Accepted, len(rep.Conflicts), len(rep.Gaps), n+1)
	}
}

// slowChain delays every broadcast, so that unserialised requests overtake each other, and
// records whether the context of the last broadcast was done.
type slowChain struct {
	*FakeChain
	done atomic.Bool
	n    atomic.Int64
}

func (c *slowChain) Broadcast(ctx context.Context, tx Tx) error {
	c.done.Store(ctx.Err() != nil)
	if c.n.Add(1)%2 == 1 {
		time.Sleep(2 * time.Millisecond)
	}
	return c.FakeChain.Broadcast(ctx, tx)
}

// published runs the shard at term in the background, waits until it is published and returns
// the function that ends Run.
func published(t *testing.T, s *Signer, term core.Term) (stop func()) {
	const shard = 2
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, &tenure.Shard{Space: SpaceName, ID: shard, Term: term}) }()
	deadline := time.Now().Add(5 * time.Second)
	for s.shard(shard, term) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("shard %d at term %d never published", shard, term)
		}
		time.Sleep(time.Millisecond)
	}
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run = %v", err)
		}
	}
}

func TestSignerRun(t *testing.T) {
	ctx := context.Background()
	const shard = 2
	newSigner := func(c Chain) (*Signer, *MemNonces) {
		nonces := NewNonces(fencedb.New())
		s := New(nonces, c)
		s.settle = 20 * time.Millisecond
		return s, nonces
	}

	t.Run("settles before it serves and unpublishes at the end", func(t *testing.T) {
		s, _ := newSigner(NewChain())
		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- s.Run(rctx, &tenure.Shard{ID: shard, Term: 3}) }()
		if _, err := s.sign(ctx, shard, 3, wallet, "p"); !errors.Is(err, errNotPublished) {
			t.Fatalf("sign during the settle = %v, want errNotPublished", err)
		}
		for s.shard(shard, 3) == nil {
			time.Sleep(time.Millisecond)
		}
		if tx, err := s.sign(ctx, shard, 3, wallet, "p"); err != nil || tx.Nonce != 0 {
			t.Fatalf("sign = %+v, %v", tx, err)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run = %v", err)
		}
		if _, err := s.sign(ctx, shard, 3, wallet, "p"); !errors.Is(err, errNotPublished) {
			t.Fatalf("sign after Run ended = %v, want errNotPublished", err)
		}
	})

	t.Run("a stale raise ends Run", func(t *testing.T) {
		s, nonces := newSigner(NewChain())
		if err := nonces.Raise(ctx, shard, 9); err != nil {
			t.Fatal(err)
		}
		if err := s.Run(ctx, &tenure.Shard{ID: shard, Term: 5}); !errors.Is(err, ErrStale) {
			t.Fatalf("Run at a stale term = %v, want ErrStale", err)
		}
		if s.shard(shard, 5) != nil {
			t.Fatal("a stale shard was published")
		}
	})

	t.Run("a new term on the same node", func(t *testing.T) {
		chain := NewChain()
		s, _ := newSigner(chain)
		stopOld := published(t, s, 5)
		if _, err := s.sign(ctx, shard, 5, wallet, "a"); err != nil {
			t.Fatal(err)
		}
		// The new term raises the fence while the old Run still holds its shard.
		stopNew := published(t, s, 6)
		defer stopNew()
		if _, err := s.sign(ctx, shard, 5, wallet, "b"); !errors.Is(err, ErrStale) {
			t.Fatalf("sign at the old term = %v, want ErrStale", err)
		}
		if _, err := s.sign(ctx, shard, 5, wallet, "b"); !errors.Is(err, errRefusing) {
			t.Fatalf("second sign at the old term = %v, want errRefusing", err)
		}
		if tx, err := s.sign(ctx, shard, 6, wallet, "c"); err != nil || tx.Nonce != 1 {
			t.Fatalf("sign at the new term = %+v, %v; want nonce 1", tx, err)
		}
		if _, err := s.sign(ctx, shard, 7, wallet, "d"); !errors.Is(err, errNotPublished) {
			t.Fatalf("sign at an unpublished term = %v, want errNotPublished", err)
		}
		stopOld()
		if rep := chain.Report(); rep.Accepted != 2 || len(rep.Conflicts)+len(rep.Gaps) != 0 {
			t.Fatalf("Report = %+v", rep)
		}
	})

	t.Run("one shard serialises a wallet", func(t *testing.T) {
		chain := &slowChain{FakeChain: NewChain()}
		s, _ := newSigner(chain)
		defer published(t, s, 1)()
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() { _, _ = s.sign(ctx, shard, 1, wallet, "p") })
		}
		wg.Wait()
		if rep := chain.Report(); rep.Accepted != 16 || len(rep.Conflicts)+len(rep.Gaps) != 0 {
			t.Fatalf("Report = accepted %d, %d conflicts, %d gaps", rep.Accepted, len(rep.Conflicts), len(rep.Gaps))
		}
	})

	t.Run("a broadcast outlives the request", func(t *testing.T) {
		chain := &slowChain{FakeChain: NewChain()}
		s, _ := newSigner(chain)
		defer published(t, s, 1)()
		gone, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.sign(gone, shard, 1, wallet, "p"); err != nil {
			t.Fatalf("sign with a cancelled context = %v", err)
		}
		if chain.done.Load() {
			t.Fatal("the broadcast ran on the cancelled context of the request")
		}
	})
}
