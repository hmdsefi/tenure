// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command orderbook runs a sharded order book that a node can lose without losing an order.
//
//	go run ./orderbook [-orders 2000] [-clients 4] [-seed 1]
//
// It starts three tenure nodes in one process, n1 to n3, each with an HTTP listener, and an
// in-memory fake downstream that stands in for a database. The markets space has 16 shards and
// is fenced: a shard's owner matches the orders of its markets, mkt-00 to mkt-31, and journals
// each order with its fills in the downstream under the shard's term. A new owner raises the
// shard's fence in the downstream first and rebuilds the books from the journal, so a stale
// owner that has not noticed it lost the shard can no longer commit.
//
// Clients send orders to n1 and n2 with POST /orders, following the Tenure-Route header of a
// 421 answer to the member that owns the shard and retrying a 503. After half of the orders are
// accepted the program drains n3, so that its shards move to n1 and n2. It prints the ownership
// table before and after the drain, and then
//
//	orders accepted: <n>
//	fills: <n>
//	shards moved: <n>
//
// The flags are the number of orders (-orders, default 2000), the number of clients that send
// them (-clients, default 4) and the seed of their random orders (-seed, default 1). The program
// exits 0 when every order has been accepted.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"text/tabwriter"
	"time"

	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

const (
	nodeCount = 3

	// drained is the index of the node the program drains.
	drained = nodeCount - 1

	// setupTimeout bounds the start of the cluster and the wait for every shard's owner.
	setupTimeout = 30 * time.Second

	// drainTimeout bounds the drain of a node and the wait for its shards to move.
	drainTimeout = 30 * time.Second
)

func main() {
	os.Exit(exitCode())
}

// exitCode runs the program and returns its exit code.
func exitCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "orderbook:", err)
		return 1
	}
	return 0
}

// execute is main with its context, arguments and output.
func execute(ctx context.Context, args []string, out io.Writer) (err error) {
	fs := flag.NewFlagSet("orderbook", flag.ContinueOnError)
	orders := fs.Int("orders", 2000, "number of orders to send")
	clients := fs.Int("clients", 4, "number of clients that send them")
	seed := fs.Uint64("seed", 1, "seed of the clients' random orders")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *orders < 1 || *clients < 1 {
		return fmt.Errorf("-orders and -clients must be at least 1, got %d and %d", *orders, *clients)
	}

	books := newBooks(fencedb.New(), nodeCount)
	c, err := startCluster(ctx, books)
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), setupTimeout)
		defer cancel()
		if cerr := c.Close(cctx); cerr != nil && err == nil {
			err = cerr
		}
	}()

	wctx, cancel := context.WithTimeout(ctx, setupTimeout)
	before, err := waitOwners(wctx, c, nil)
	cancel()
	if err != nil {
		return fmt.Errorf("wait for the shards: %w", err)
	}

	hc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 8}}
	defer hc.CloseIdleConnections()
	l := &load{
		hc:      hc,
		urls:    []string{c.URL(0), c.URL(1)}, // the nodes that stay
		clients: *clients,
		orders:  *orders,
		seed:    *seed,
	}
	// The clients stop when execute returns, whatever the reason.
	lctx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	loadDone := make(chan error, 1)
	go func() { loadDone <- l.run(lctx) }()

	// The drain comes after half of the orders are accepted. When the clients end first, with
	// every order accepted, the drain comes at once; a client that failed ends the program.
	half := int64((*orders + 1) / 2)
	finished := false
wait:
	for l.accepted.Load() < half {
		select {
		case err := <-loadDone:
			if err != nil {
				return err
			}
			finished = true
			break wait
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}

	before, err = ownersNow(c, before)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "before the drain of %s:\n", memberID(drained))
	printOwners(out, before)

	dctx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()
	if err := c.Nodes[drained].Drain(dctx); err != nil {
		return fmt.Errorf("drain %s: %w", memberID(drained), err)
	}
	after, err := waitOwners(dctx, c, func(m map[int]owner) bool { return !ownedBy(m, memberID(drained)) })
	if err != nil {
		return fmt.Errorf("wait for the shards of %s to move: %w", memberID(drained), err)
	}
	if !finished {
		if err := <-loadDone; err != nil {
			return err
		}
	}

	fmt.Fprintf(out, "\nafter the drain of %s:\n", memberID(drained))
	printOwners(out, after)
	moved := 0
	for shard, o := range before {
		if after[shard].id != o.id {
			moved++
		}
	}
	fmt.Fprintf(out, "\norders accepted: %d\nfills: %d\nshards moved: %d\n", l.accepted.Load(), l.fills.Load(), moved)
	return nil
}

// memberID returns the ID of node i.
func memberID(i int) string {
	return fmt.Sprintf("n%d", i+1)
}

// owner is the node that runs a shard and the term it runs it at.
type owner struct {
	id   string
	term uint64
}

// owners reads who runs each shard from the nodes' own tables. It returns nil when some shard
// has no owner now.
func owners(c *demo.Cluster) map[int]owner {
	m := make(map[int]owner, marketShards)
	for _, n := range c.Nodes {
		t := n.Table()
		for _, sp := range t.Spaces {
			if sp.Name != marketsSpace {
				continue
			}
			for _, row := range sp.Rows {
				if row.State == "owned" {
					m[row.Shard] = owner{id: t.Member.ID, term: uint64(row.Term)}
				}
			}
		}
	}
	if len(m) != marketShards {
		return nil
	}
	return m
}

// ownersNow is owners with a fallback: when a shard has no owner at this instant it returns the
// last complete answer.
func ownersNow(c *demo.Cluster, last map[int]owner) (map[int]owner, error) {
	if m := owners(c); m != nil {
		return m, nil
	}
	if last == nil {
		return nil, fmt.Errorf("some shard has no owner")
	}
	return last, nil
}

// waitOwners polls until every shard has an owner and ok, when not nil, holds for them.
func waitOwners(ctx context.Context, c *demo.Cluster, ok func(map[int]owner) bool) (map[int]owner, error) {
	for {
		if m := owners(c); m != nil && (ok == nil || ok(m)) {
			return m, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// ownedBy reports whether node id owns a shard in m.
func ownedBy(m map[int]owner, id string) bool {
	for _, o := range m {
		if o.id == id {
			return true
		}
	}
	return false
}

// printOwners writes the ownership table: one line for each shard.
func printOwners(out io.Writer, m map[int]owner) {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "shard\towner\tterm")
	for shard := range marketShards {
		o := m[shard]
		fmt.Fprintf(tw, "%d\t%s\t%d\n", shard, o.id, o.term)
	}
	_ = tw.Flush()
}
