// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package demo runs several tenure nodes in one process, for the examples and their tests.
//
// The nodes share one in-memory store and one static group, and each reaches the store through
// its own CutStore, so a test can cut one node off while the others carry on. Every node has
// its own HTTP listener, whose address is the node's member address: a route that names a member
// names a socket a client can reach. Post is the client that follows those routes.
package demo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/static"
)

// DefaultNodes is the number of nodes Start runs when Options.Nodes is 0.
const DefaultNodes = 3

// TestTiming returns the timing the demos and the example tests run with: a lease of 1 second,
// a renew deadline of 600 ms, a retry period of 100 ms and a slot grace of 1 second. A member
// that stops renewing loses its shards after about a second, so a test sees a takeover quickly.
func TestTiming() core.Timing {
	return core.Timing{
		LeaseDuration: time.Second,
		RenewDeadline: 600 * time.Millisecond,
		RetryPeriod:   100 * time.Millisecond,
		SlotGrace:     time.Second,
	}
}

// Options configures Start. The zero value runs three nodes with TestTiming, no spaces and no
// handlers.
type Options struct {
	// Nodes is the number of nodes, DefaultNodes when 0.
	Nodes int

	// Timing is the session timing of every node, TestTiming() when it is the zero value.
	Timing core.Timing

	// Spaces returns the spaces of node i, counted from 0. nil means no spaces.
	Spaces func(i int) []tenure.Space

	// Serve returns the HTTP handler of node i, counted from 0, which Start calls once the node
	// has started. nil, or a nil handler, answers every request with 404. Requests that reach
	// a listener before its server runs wait in the listener's backlog.
	Serve func(i int, n *tenure.Node) http.Handler
}

// Cluster is a running demo group, as Start returns it. Index i of the slices is node n<i+1>.
type Cluster struct {
	// Nodes are the running nodes.
	Nodes []*tenure.Node

	// Cuts are the stores the nodes use; each wraps Store.
	Cuts []*CutStore

	// Store is the one store shared by all nodes, reachable without a cut.
	Store *memstore.Store

	// Addrs are the listener addresses, "127.0.0.1:port": the member addresses.
	Addrs []string

	listeners []net.Listener
	servers   []*http.Server

	closeOnce sync.Once
	closeErr  error
}

// URL returns the base URL of node i, "http://" and its address.
func (c *Cluster) URL(i int) string {
	return "http://" + c.Addrs[i]
}

// Start starts the nodes of a demo group and their servers. It opens every listener first, on
// 127.0.0.1 with a port the system picks, so that every member's address is known when the group
// is formed; member IDs are n1, n2 and so on, and one memstore and one static group serve all of
// them. It then starts the nodes with their spaces, calls Options.Serve for each, and serves
// each listener. When any step fails, Start tears down what it built and returns the error.
// ctx bounds Start only; the nodes run until Close.
func Start(ctx context.Context, opts Options) (*Cluster, error) {
	if opts.Nodes < 0 {
		return nil, fmt.Errorf("demo: %d nodes", opts.Nodes)
	}
	if opts.Nodes == 0 {
		opts.Nodes = DefaultNodes
	}
	if opts.Timing == (core.Timing{}) {
		opts.Timing = TestTiming()
	}
	c := &Cluster{Store: memstore.New(nil)}

	members := make([]core.Member, opts.Nodes)
	var lc net.ListenConfig
	for i := range members {
		ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
		if err != nil {
			c.abort(ctx)
			return nil, fmt.Errorf("demo: listen for node %d: %w", i+1, err)
		}
		c.listeners = append(c.listeners, ln)
		c.Addrs = append(c.Addrs, ln.Addr().String())
		members[i] = core.Member{ID: fmt.Sprintf("n%d", i+1), Addr: ln.Addr().String(), Slot: -1}
	}

	group, err := static.New(members)
	if err != nil {
		c.abort(ctx)
		return nil, fmt.Errorf("demo: group: %w", err)
	}
	for i, self := range members {
		cut := NewCutStore(c.Store)
		cfg := tenure.Config{
			Self:       self,
			Membership: group.Share(),
			Store:      cut,
			Timing:     opts.Timing,
		}
		if opts.Spaces != nil {
			cfg.Spaces = opts.Spaces(i)
		}
		n, err := tenure.Start(ctx, cfg)
		if err != nil {
			c.abort(ctx)
			return nil, fmt.Errorf("demo: start %s: %w", self.ID, err)
		}
		c.Cuts = append(c.Cuts, cut)
		c.Nodes = append(c.Nodes, n)
	}

	for i, n := range c.Nodes {
		var h http.Handler
		if opts.Serve != nil {
			h = opts.Serve(i, n)
		}
		if h == nil {
			h = http.NotFoundHandler()
		}
		srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
		c.servers = append(c.servers, srv)
		go func() {
			// Serve returns http.ErrServerClosed after Shutdown or Close; any other error
			// means the listener broke, and the clients see the failure.
			_ = srv.Serve(c.listeners[i])
		}()
	}
	return c, nil
}

// abort tears down a half-built cluster. It cannot use the caller's ctx, which may be done.
func (c *Cluster) abort(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_ = c.Close(ctx)
}

// Close stops the servers, closes every node and closes the listeners. It returns the first
// error. The nodes close concurrently, each under ctx: a node whose store is cut cannot release
// its shards, so Heal it first or give ctx a deadline. Close is safe to call again and returns
// the first call's result.
func (c *Cluster) Close(ctx context.Context) error {
	c.closeOnce.Do(func() { c.closeErr = c.close(ctx) })
	return c.closeErr
}

func (c *Cluster) close(ctx context.Context) error {
	var errs []error
	for _, srv := range c.servers {
		if err := srv.Shutdown(ctx); err != nil {
			errs = append(errs, err)
			_ = srv.Close() // Shutdown gave up on a request in flight: cut it
		}
	}
	nodeErrs := make([]error, len(c.Nodes))
	var wg sync.WaitGroup
	for i, n := range c.Nodes {
		wg.Go(func() { nodeErrs[i] = n.Close(ctx) })
	}
	wg.Wait()
	errs = append(errs, nodeErrs...)
	for _, ln := range c.listeners {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
