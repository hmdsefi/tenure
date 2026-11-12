// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package signing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

const wallet = "w-00"

// countNonces counts the calls of Reserve that reach the wrapped Nonces.
type countNonces struct {
	Nonces
	reserves atomic.Int64
}

func (c *countNonces) Reserve(ctx context.Context, shard int, term core.Term, w string) (uint64, error) {
	c.reserves.Add(1)
	return c.Nonces.Reserve(ctx, shard, term, w)
}

// hopLog records the Tenure-Hops header of every request a node's server received.
type hopLog struct {
	mu   sync.Mutex
	hops [][]string
}

func (l *hopLog) wrap(i int, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		for len(l.hops) <= i {
			l.hops = append(l.hops, nil)
		}
		l.hops[i] = append(l.hops[i], r.Header.Get(HopsHeader))
		l.mu.Unlock()
		h.ServeHTTP(w, r)
	})
}

func (l *hopLog) of(i int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if i >= len(l.hops) {
		return nil
	}
	return append([]string(nil), l.hops[i]...)
}

// rig is a demo cluster of signers over one nonce service and one chain.
type rig struct {
	c      *demo.Cluster
	db     *fencedb.DB
	nonces *countNonces
	chain  *FakeChain
	hops   *hopLog
	ctx    context.Context
}

// startRig starts three signer nodes, with forwarding when forward is set.
func startRig(t *testing.T, forward bool) *rig {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	t.Cleanup(cancel)
	r := &rig{db: fencedb.New(), chain: NewChain(), hops: new(hopLog), ctx: ctx}
	r.nonces = &countNonces{Nonces: NewNonces(r.db)}
	signers := make([]*Signer, 3)
	for i := range signers {
		signers[i] = New(r.nonces, r.chain)
	}
	c, err := demo.Start(ctx, demo.Options{
		Spaces: func(i int) []tenure.Space { return []tenure.Space{signers[i].Space()} },
		Serve: func(i int, n *tenure.Node) http.Handler {
			return r.hops.wrap(i, signers[i].Handler(n, "n"+strconv.Itoa(i+1), forward))
		},
	})
	if err != nil {
		t.Fatalf("start the cluster: %v", err)
	}
	r.c = c
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := c.Close(cctx); err != nil {
			t.Errorf("close the cluster: %v", err)
		}
	})
	return r
}

// owner waits until exactly one node serves the wallet and returns its index.
func (r *rig) owner(t *testing.T) int {
	t.Helper()
	for {
		owners := -1
		count := 0
		for i, n := range r.c.Nodes {
			if _, err := n.Local(SpaceName, wallet); err == nil {
				owners, count = i, count+1
			}
		}
		if count == 1 {
			return owners
		}
		select {
		case <-r.ctx.Done():
			t.Fatalf("no single owner of %s", wallet)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// other returns the index of a node that is not i.
func other(i int) int { return (i + 1) % 3 }

// answer is one HTTP answer.
type answer struct {
	status int
	header http.Header
	body   []byte
}

// post sends one request to node i, once, with the extra headers.
func (r *rig) post(t *testing.T, i int, body string, hdr map[string]string) answer {
	t.Helper()
	req, err := http.NewRequestWithContext(r.ctx, http.MethodPost, r.c.URL(i)+"/sign", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post to node %d: %v", i, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return answer{resp.StatusCode, resp.Header, b}
}

// postUntil sends to node i until the status is not 503, which a shard answers while it settles.
func (r *rig) postUntil(t *testing.T, i int, body string, hdr map[string]string) answer {
	t.Helper()
	for {
		a := r.post(t, i, body, hdr)
		if a.status != http.StatusServiceUnavailable {
			return a
		}
		select {
		case <-r.ctx.Done():
			t.Fatalf("node %d answered 503 until the deadline: %s", i, a.body)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func signBody(payload string) string {
	b, _ := json.Marshal(SignRequest{Wallet: wallet, Payload: payload})
	return string(b)
}

func decodeTx(t *testing.T, a answer) Tx {
	t.Helper()
	var tx Tx
	if err := json.Unmarshal(a.body, &tx); err != nil {
		t.Fatalf("answer %d %q is not a Tx: %v", a.status, a.body, err)
	}
	return tx
}

// TestATEXM11Signer covers AT-EXM-11; its steps were written for this change from the issue's
// text.
func TestATEXM11Signer(t *testing.T) {
	ctx := context.Background()
	const shard = 3

	t.Run("nonces", func(t *testing.T) {
		db := fencedb.New()
		n := NewNonces(db)
		if err := n.Raise(ctx, shard, 1); err != nil {
			t.Fatal(err)
		}
		for want := range uint64(2) {
			got, err := n.Reserve(ctx, shard, 1, wallet)
			if err != nil || got != want {
				t.Fatalf("Reserve = %d, %v; want %d", got, err, want)
			}
		}
		// A new owner raises the fence; the old one can no longer reserve, and the next nonce
		// stays 2.
		if err := n.Raise(ctx, shard, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := n.Reserve(ctx, shard, 1, wallet); !errors.Is(err, ErrStale) {
			t.Fatalf("Reserve at the stale term = %v, want ErrStale", err)
		}
		if err := n.Raise(ctx, shard, 1); !errors.Is(err, ErrStale) {
			t.Fatalf("Raise below the fence = %v, want ErrStale", err)
		}
		if got, err := n.Reserve(ctx, shard, 2, wallet); err != nil || got != 2 {
			t.Fatalf("Reserve at the new term = %d, %v; want 2", got, err)
		}
		// Another wallet starts at 0.
		if got, err := n.Reserve(ctx, shard, 2, "w-01"); err != nil || got != 0 {
			t.Fatalf("Reserve of another wallet = %d, %v; want 0", got, err)
		}
	})

	t.Run("chain", func(t *testing.T) {
		c := NewChain()
		tx := func(nonce uint64) Tx { return Tx{Wallet: wallet, Nonce: nonce, Payload: "p"} }
		for _, nonce := range []uint64{0, 1} {
			if err := c.Broadcast(ctx, tx(nonce)); err != nil {
				t.Fatalf("Broadcast(%d) = %v", nonce, err)
			}
		}
		// 2 is next: 0 is a conflict, 5 a gap, and neither moves the next nonce.
		if err := c.Broadcast(ctx, tx(0)); !errors.Is(err, ErrConflict) {
			t.Fatalf("Broadcast(0) = %v, want ErrConflict", err)
		}
		if err := c.Broadcast(ctx, tx(5)); !errors.Is(err, ErrGap) {
			t.Fatalf("Broadcast(5) = %v, want ErrGap", err)
		}
		if err := c.Broadcast(ctx, tx(2)); err != nil {
			t.Fatalf("Broadcast(2) = %v", err)
		}
		rep := c.Report()
		if rep.Accepted != 3 || len(rep.Conflicts) != 1 || rep.Conflicts[0].Nonce != 0 ||
			len(rep.Gaps) != 1 || rep.Gaps[0].Nonce != 5 {
			t.Fatalf("Report = %+v", rep)
		}
		// The report is a copy.
		rep.Conflicts[0].Nonce = 99
		if got := c.Report().Conflicts[0].Nonce; got != 0 {
			t.Fatalf("Report shares its slices: conflict nonce %d", got)
		}
	})

	t.Run("421 without forwarding", func(t *testing.T) {
		r := startRig(t, false)
		o := r.owner(t)
		np := other(o)

		a := r.post(t, np, signBody("x"), nil)
		if a.status != http.StatusMisdirectedRequest {
			t.Fatalf("non-owner answered %d, want 421: %s", a.status, a.body)
		}
		if got := a.header.Get(RouteHeader); got != r.c.Addrs[o] {
			t.Fatalf("%s = %q, want the owner's address %q", RouteHeader, got, r.c.Addrs[o])
		}

		// demo.Post follows the route and gets the first nonce, then the second.
		for want := range uint64(2) {
			res, err := demo.Post(r.ctx, nil, r.c.URL(np)+"/sign", []byte(signBody("x")))
			if err != nil || res.Status != http.StatusOK {
				t.Fatalf("demo.Post = %d, %v: %s", res.Status, err, res.Body)
			}
			tx := decodeTx(t, answer{body: res.Body})
			if tx.Nonce != want || tx.Wallet != wallet || tx.Sig != Signature(wallet, want, "x") {
				t.Fatalf("tx = %+v, want nonce %d signed", tx, want)
			}
		}
		if rep := r.chain.Report(); rep.Accepted != 2 || len(rep.Conflicts)+len(rep.Gaps) != 0 {
			t.Fatalf("Report = %+v", rep)
		}
	})

	t.Run("forwarding", func(t *testing.T) {
		r := startRig(t, true)
		o := r.owner(t)
		np := other(o)

		for want := range uint64(2) {
			a := r.postUntil(t, np, signBody("y"), nil)
			if a.status != http.StatusOK {
				t.Fatalf("through the proxy: %d: %s", a.status, a.body)
			}
			if tx := decodeTx(t, a); tx.Nonce != want {
				t.Fatalf("nonce %d, want %d", tx.Nonce, want)
			}
		}
		// The owner saw the request after one hop, and the client's own request carried none.
		if got := r.hops.of(o); len(got) == 0 || got[0] != "1" {
			t.Fatalf("hops seen by the owner = %q, want it to start with 1", got)
		}
		if got := r.hops.of(np); len(got) == 0 || got[0] != "" {
			t.Fatalf("hops seen by the entry node = %q, want it to start with none", got)
		}
		if rep := r.chain.Report(); rep.Accepted != 2 {
			t.Fatalf("Report = %+v", rep)
		}
	})
}

func TestSignHandlerBadRequests(t *testing.T) {
	r := startRig(t, true)
	for name, tc := range map[string]struct {
		body string
		hdr  map[string]string
	}{
		"not JSON":        {body: "{"},
		"empty":           {body: ""},
		"two objects":     {body: signBody("p") + signBody("q")},
		"unknown field":   {body: `{"wallet":"w-00","payload":"p","extra":1}`},
		"empty wallet":    {body: `{"wallet":"","payload":"p"}`},
		"no wallet":       {body: `{"payload":"p"}`},
		"wallet a number": {body: `{"wallet":7,"payload":"p"}`},
		"bad hops":        {body: signBody("p"), hdr: map[string]string{HopsHeader: "x"}},
		"negative hops":   {body: signBody("p"), hdr: map[string]string{HopsHeader: "-1"}},
	} {
		t.Run(name, func(t *testing.T) {
			for i := range r.c.Nodes {
				if a := r.post(t, i, tc.body, tc.hdr); a.status != http.StatusBadRequest {
					t.Errorf("node %d answered %d, want 400: %s", i, a.status, a.body)
				}
			}
		})
	}
	if got := r.nonces.reserves.Load(); got != 0 {
		t.Errorf("%d reservations for bad requests", got)
	}
}

func TestSignHandlerHopLimit(t *testing.T) {
	r := startRig(t, true)
	o := r.owner(t)
	np := other(o)
	r.postUntil(t, o, signBody("warm"), nil) // the owner serves; the chain has nonce 0

	// A request that arrives after 2 hops and is not local ends here.
	a := r.post(t, np, signBody("p"), map[string]string{HopsHeader: "2"})
	if a.status != http.StatusLoopDetected {
		t.Fatalf("after 2 hops: %d, want 508: %s", a.status, a.body)
	}
	// After 1 hop it is sent on once more, with 2, and the owner serves it.
	a = r.post(t, np, signBody("p"), map[string]string{HopsHeader: "1"})
	if a.status != http.StatusOK {
		t.Fatalf("after 1 hop: %d, want 200: %s", a.status, a.body)
	}
	if got := r.hops.of(o); got[len(got)-1] != "2" {
		t.Fatalf("hops seen by the owner = %q, want the last to be 2", got)
	}
	// The owner serves a request that came with 2 hops, since it is local.
	if a := r.post(t, o, signBody("p"), map[string]string{HopsHeader: "2"}); a.status != http.StatusOK {
		t.Fatalf("owner after 2 hops: %d: %s", a.status, a.body)
	}
	if rep := r.chain.Report(); rep.Accepted != 3 || len(rep.Conflicts)+len(rep.Gaps) != 0 {
		t.Fatalf("Report = %+v", rep)
	}
}

func TestSignHandlerStaleTerm(t *testing.T) {
	r := startRig(t, false)
	o := r.owner(t)
	if a := r.postUntil(t, o, signBody("p"), nil); a.status != http.StatusOK {
		t.Fatalf("first request: %d: %s", a.status, a.body)
	}
	sh, err := r.c.Nodes[o].Local(SpaceName, wallet)
	if err != nil {
		t.Fatal(err)
	}

	// Another owner raises the fence while this one still believes it owns the shard.
	if err := r.db.Raise(SpaceName, sh.ID, sh.Term+1); err != nil {
		t.Fatal(err)
	}
	before := r.nonces.reserves.Load()
	a := r.post(t, o, signBody("p"), nil)
	if a.status != http.StatusServiceUnavailable || a.header.Get("Retry-After") != "1" {
		t.Fatalf("stale reservation: %d, Retry-After %q; want 503 and 1: %s", a.status, a.header.Get("Retry-After"), a.body)
	}
	if got := r.nonces.reserves.Load() - before; got != 1 {
		t.Fatalf("the stale request made %d reservations, want 1", got)
	}

	// The shard now refuses the rest of its term without asking the nonce service.
	for range 3 {
		if a := r.post(t, o, signBody("p"), nil); a.status != http.StatusServiceUnavailable {
			t.Fatalf("after the stale reservation: %d, want 503: %s", a.status, a.body)
		}
	}
	if got := r.nonces.reserves.Load() - before; got != 1 {
		t.Fatalf("%d reservations after the shard turned stale, want 1 in all", got)
	}
	if rep := r.chain.Report(); rep.Accepted != 1 || len(rep.Conflicts)+len(rep.Gaps) != 0 {
		t.Fatalf("Report = %+v, want the one transaction of the first request", rep)
	}
}

// brokenChain rejects every transaction with err.
type brokenChain struct{ err error }

func (c brokenChain) Broadcast(context.Context, Tx) error { return c.err }

func TestSignHandlerChainRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	s := New(NewNonces(fencedb.New()), brokenChain{ErrConflict})
	c, err := demo.Start(ctx, demo.Options{
		Nodes:  1,
		Spaces: func(int) []tenure.Space { return []tenure.Space{s.Space()} },
		Serve:  func(_ int, n *tenure.Node) http.Handler { return s.Handler(n, "n1", false) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = c.Close(cctx)
	})
	res, err := demo.Post(ctx, nil, c.URL(0)+"/sign", []byte(signBody("p")))
	if err != nil || res.Status != http.StatusInternalServerError {
		t.Fatalf("answer %d, %v; want 500: %s", res.Status, err, res.Body)
	}
}

func TestSignature(t *testing.T) {
	a := Signature("w", 1, "p")
	if len(a) != 64 || a != Signature("w", 1, "p") {
		t.Fatalf("Signature = %q", a)
	}
	for _, b := range []string{Signature("v", 1, "p"), Signature("w", 2, "p"), Signature("w", 1, "q")} {
		if b == a {
			t.Fatal("two different transactions share a signature")
		}
	}
}
