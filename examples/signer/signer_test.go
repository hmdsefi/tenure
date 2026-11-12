// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
	"github.com/hmdsefi/tenure/examples/internal/signing"
)

// call is one Raise or Reserve of a node's Nonces, with its result.
type call struct {
	node  int
	raise bool // a Raise; otherwise a Reserve
	shard int
	term  core.Term
	err   error
}

// recorder keeps the calls of every node's recording Nonces in the order the in-memory Nonces
// served them.
type recorder struct {
	mu    sync.Mutex
	calls []call
}

// recording is a signing.Nonces that passes every call to inner and records it. Its calls hold
// the recorder's lock, one at a time across all nodes, so the record is in the order the
// underlying fence saw them.
type recording struct {
	node  int
	inner signing.Nonces
	rec   *recorder
}

func (r *recording) Raise(ctx context.Context, shard int, term core.Term) error {
	r.rec.mu.Lock()
	defer r.rec.mu.Unlock()
	err := r.inner.Raise(ctx, shard, term)
	r.rec.calls = append(r.rec.calls, call{node: r.node, raise: true, shard: shard, term: term, err: err})
	return err
}

func (r *recording) Reserve(ctx context.Context, shard int, term core.Term, wallet string) (uint64, error) {
	r.rec.mu.Lock()
	defer r.rec.mu.Unlock()
	nonce, err := r.inner.Reserve(ctx, shard, term, wallet)
	r.rec.calls = append(r.rec.calls, call{node: r.node, shard: shard, term: term, err: err})
	return nonce, err
}

// snapshot returns a copy of the recorded calls.
func (rc *recorder) snapshot() []call {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return slices.Clone(rc.calls)
}

// staleFrom counts the reservations of node n that the fence rejected.
func (rc *recorder) staleFrom(n int) int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	count := 0
	for _, c := range rc.calls {
		if c.node == n && !c.raise && errors.Is(c.err, signing.ErrStale) {
			count++
		}
	}
	return count
}

// signerCluster starts a cluster of three nodes over db whose nodes reserve through one
// recording wrapper each, and closes it when the test is over. It returns the cluster, the
// chain and the recorder.
func signerCluster(t *testing.T, ctx context.Context, db *fencedb.DB) (*demo.Cluster, *signing.FakeChain, *recorder) {
	t.Helper()
	rec := new(recorder)
	chain := signing.NewChain()
	nonces := make([]signing.Nonces, nodeCount)
	for i := range nonces {
		nonces[i] = &recording{node: i, inner: signing.NewNonces(db), rec: rec}
	}
	c, err := startCluster(ctx, nonces, chain)
	if err != nil {
		t.Fatalf("start the cluster: %v", err)
	}
	t.Cleanup(func() {
		for _, cut := range c.Cuts {
			cut.Heal()
		}
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := c.Close(cctx); err != nil {
			t.Errorf("close the cluster: %v", err)
		}
	})
	return c, chain, rec
}

// answer is one HTTP answer of a node.
type answer struct {
	status int
	header http.Header
	body   []byte
}

// postSign sends one sign request to url, once: it follows no route and retries nothing.
func postSign(ctx context.Context, hc *http.Client, url string, req signing.SignRequest) (answer, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return answer{}, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/sign", bytes.NewReader(body))
	if err != nil {
		return answer{}, err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(r)
	if err != nil {
		return answer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return answer{resp.StatusCode, resp.Header, b}, err
}

// TestSigner is the property the example exists to show. Three nodes serve 8 shards of wallets
// over HTTP; clients send sign requests; n3 is drained; n1 is cut from the store, marked as
// draining in the group so that n2 takes its shards, and keeps running because the space is
// fenced; requests keep going to n1 directly until the fence has rejected one of its nonce
// reservations; then everything heals. The chain must see no conflict and no gap, every wallet's
// signed nonces must be 0 to k-1, every reservation below a raised fence must have failed, and
// the drained node must own nothing. The steps are the four assertions of EXM-340 (AT-EXM-13),
// written from the issue's text.
func TestSigner(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()

	c, chain, rec := signerCluster(t, ctx, fencedb.New())
	hc := &http.Client{Transport: &http.Transport{}}
	t.Cleanup(hc.CloseIdleConnections)

	if _, err := waitOwners(ctx, c, nil); err != nil {
		t.Fatalf("wait for the owners of the shards: %v", err)
	}
	const cut = 0 // n1 is cut, n3 is drained
	stay := []string{c.URL(0), c.URL(1)}

	var loads []*load

	// Phase 1: requests through n1 and n2 while the three nodes serve.
	one := &load{hc: hc, urls: stay, clients: 4, requests: 80, seed: 1, prefix: "a"}
	loads = append(loads, one)
	if err := one.run(ctx); err != nil {
		t.Fatalf("first requests: %v", err)
	}

	// Phase 2: drain n3 and wait for its shards to move.
	if err := c.Nodes[drained].Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if _, err := waitOwners(ctx, c, func(m map[int]owner) bool { return !ownedBy(m, memberID(drained)) }); err != nil {
		t.Fatalf("wait for the shards of the drained node to move: %v", err)
	}
	two := &load{hc: hc, urls: stay, clients: 4, requests: 80, seed: 2, prefix: "b"}
	loads = append(loads, two)
	if err := two.run(ctx); err != nil {
		t.Fatalf("requests after the drain: %v", err)
	}

	// Phase 3: cut n1, and mark it draining in the group: with static membership a cut node keeps
	// its placement, so no other node would ever take its shards. Then keep sending n1 requests
	// directly until the fence rejected one of its reservations, while other clients go on
	// through n2.
	c.Cuts[cut].Cut()
	if err := c.Groups[cut].SetDraining(ctx, true); err != nil {
		t.Fatalf("mark the cut node as draining in the group: %v", err)
	}
	three := &load{hc: hc, urls: stay[1:], clients: 2, requests: 60, seed: 3, prefix: "c"}
	loads = append(loads, three)
	threeDone := make(chan error, 1)
	go func() { threeDone <- three.run(ctx) }()

	staleCtx, staleCancel := context.WithTimeout(ctx, 25*time.Second)
	defer staleCancel()
	direct := &load{} // the transactions n1 signed when asked directly
	loads = append(loads, direct)
	refused := 0 // direct requests that did not get a 200
	for i := 0; rec.staleFrom(cut) == 0; i++ {
		select {
		case <-staleCtx.Done():
			t.Fatal("the fence rejected no reservation of the cut node")
		case <-time.After(5 * time.Millisecond):
		}
		wallet := walletName(i % walletCount)
		if _, err := c.Nodes[cut].Local(signing.SpaceName, wallet); err != nil {
			continue // n1 does not believe it owns the shard
		}
		a, err := postSign(staleCtx, hc, c.URL(cut), signing.SignRequest{Wallet: wallet, Payload: fmt.Sprintf("x-%d", i)})
		if err != nil {
			t.Fatalf("post to the cut node: %v", err)
		}
		switch a.status {
		case http.StatusOK:
			var tx signing.Tx
			if err := json.Unmarshal(a.body, &tx); err != nil {
				t.Fatalf("answer of the cut node: %v", err)
			}
			direct.record(tx)
		case http.StatusServiceUnavailable:
			if a.header.Get("Retry-After") != "1" {
				t.Errorf("503 without Retry-After: 1: %v", a.header)
			}
			refused++
		case http.StatusMisdirectedRequest:
			refused++
		default:
			t.Fatalf("post to the cut node: status %d: %s", a.status, a.body)
		}
	}

	// Phase 4: heal and let the clients finish.
	c.Cuts[cut].Heal()
	if err := <-threeDone; err != nil {
		t.Fatalf("requests during the cut: %v", err)
	}

	// Assertion 4: the drained node owns nothing, and no Local of it names it as owner.
	for _, sp := range c.Nodes[drained].Table().Spaces {
		for _, row := range sp.Rows {
			if row.State == "owned" || row.State == "degraded" || row.Running {
				t.Errorf("after its drain %s still runs shard %d: state %q, running %v",
					memberID(drained), row.Shard, row.State, row.Running)
			}
		}
	}
	for i := range walletCount {
		if _, err := c.Nodes[drained].Local(signing.SpaceName, walletName(i)); !errors.Is(err, tenure.ErrNotOwner) {
			t.Errorf("Local(%s) on the drained node = %v, want ErrNotOwner", walletName(i), err)
		}
	}

	// Stop the nodes so that nothing moves while the record is read; close the client's
	// connections first, as a server's Shutdown waits for a connection that was never used.
	hc.CloseIdleConnections()
	cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
	defer ccancel()
	if err := c.Close(cctx); err != nil {
		t.Fatalf("close the cluster: %v", err)
	}

	// Assertion 1: the chain saw no conflict and no gap, and accepted exactly the transactions
	// that got a 200.
	var txs []signing.Tx
	for _, l := range loads {
		l.mu.Lock()
		txs = append(txs, l.txs...)
		l.mu.Unlock()
	}
	rep := chain.Report()
	if len(rep.Conflicts) != 0 || len(rep.Gaps) != 0 {
		t.Errorf("the chain saw %d conflicts and %d gaps: %+v", len(rep.Conflicts), len(rep.Gaps), rep)
	}
	if rep.Accepted != len(txs) {
		t.Errorf("the chain accepted %d transactions, and %d requests got a 200", rep.Accepted, len(txs))
	}

	// Assertion 2: each wallet's signed nonces are exactly 0 to k-1.
	nonces := make(map[string][]uint64)
	for _, tx := range txs {
		nonces[tx.Wallet] = append(nonces[tx.Wallet], tx.Nonce)
		if tx.Sig != signing.Signature(tx.Wallet, tx.Nonce, tx.Payload) {
			t.Errorf("transaction %s/%d has a wrong signature", tx.Wallet, tx.Nonce)
		}
	}
	for wallet, ns := range nonces {
		slices.Sort(ns)
		for want, got := range ns {
			if uint64(want) != got {
				t.Errorf("wallet %s: signed nonces %v are not 0 to %d", wallet, ns, len(ns)-1)
				break
			}
		}
	}

	// Assertion 3: every reservation made after a raise of the same shard to a higher term failed
	// with ErrStale, and the cut node made at least one.
	calls := rec.snapshot()
	staleAfterRaise, staleFromCut := 0, 0
	for i, r := range calls {
		if r.raise {
			continue
		}
		raisedAbove := slices.ContainsFunc(calls[:i], func(p call) bool {
			return p.raise && p.err == nil && p.shard == r.shard && p.term > r.term
		})
		switch {
		case raisedAbove && !errors.Is(r.err, signing.ErrStale):
			t.Errorf("node %s reserved on shard %d at term %d after the fence was raised above it: %v",
				memberID(r.node), r.shard, r.term, r.err)
		case raisedAbove:
			staleAfterRaise++
			if r.node == cut {
				staleFromCut++
			}
		case r.err != nil:
			t.Errorf("node %s failed to reserve on shard %d at term %d before any higher raise: %v",
				memberID(r.node), r.shard, r.term, r.err)
		}
	}
	if staleFromCut == 0 {
		t.Errorf("no reservation of %s was rejected by the fence (%d rejected in all)", memberID(cut), staleAfterRaise)
	}
	t.Logf("%d requests signed, %d direct requests to %s refused, %d reservations rejected by the fence (%d from %s)",
		len(txs), refused, memberID(cut), staleAfterRaise, staleFromCut, memberID(cut))
}

// TestCommand runs the program in process: two tables and the report, every request signed. It
// matches the output by substring, as the program may be built with coverage.
func TestCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := execute(ctx, []string{"-requests", "60", "-seed", "7"}, &out); err != nil {
		t.Fatalf("execute: %v\n%s", err, out.String())
	}
	s := out.String()
	for _, want := range []string{"before the drain of n3:", "after the drain of n3:", "shard  owner  term",
		"requests signed: 60\n", "accepted: 60\n", "conflicts: 0\n", "gaps: 0\n"} {
		if !strings.Contains(s, want) {
			t.Errorf("output lacks %q:\n%s", want, s)
		}
	}
	if n := strings.Count(s, "shard  owner  term"); n != 2 {
		t.Errorf("output has %d tables, want 2", n)
	}

	if err := execute(ctx, []string{"-requests", "0"}, io.Discard); err == nil {
		t.Error("execute with -requests 0 succeeded")
	}
}

// TestCommandShort runs the program with so few requests that the clients may end before the
// program has seen half of them signed: it must still drain, print both tables and the report.
func TestCommandShort(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	for i := range 5 {
		var out bytes.Buffer
		if err := execute(ctx, []string{"-requests", "1"}, &out); err != nil {
			t.Fatalf("run %d: execute: %v\n%s", i, err, out.String())
		}
		if s := out.String(); strings.Count(s, "shard  owner  term") != 2 || !strings.Contains(s, "requests signed: 1\n") ||
			!strings.Contains(s, "conflicts: 0\n") {
			t.Fatalf("run %d: output lacks two tables or the report:\n%s", i, s)
		}
	}
}
