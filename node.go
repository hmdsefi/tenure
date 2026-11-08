// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// errStoreNotSupported is Start's error for a Config with a Store, until the node runs one.
var errStoreNotSupported = errors.New("tenure: Start: a Config with a Store is not supported yet")

// incarnationBytes is the number of random bytes that name one session of a member.
const incarnationBytes = 16

// Node is a running member of a group, as Start returns it. Its methods are safe for concurrent
// use.
type Node struct {
	cfg    Config    // checkConfig's result: defaults applied, Spaces and Self.Meta copied
	holder string    // Self.ID + "/" + the incarnation in lowercase hex
	epoch  time.Time // the node's monotonic origin, taken after the incarnation is drawn
	obs    observer

	// ctx is the internal context, context.WithCancel(context.WithoutCancel(Start's ctx)):
	// every goroutine and every call after Start runs under it, and Close cancels it.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // every goroutine of the node

	view  atomic.Pointer[core.View] // the published merged view; nil before the first
	first chan struct{}             // closed when the first view is published
	ended chan struct{}             // closed when the watcher returns

	mu        sync.Mutex
	left      bool          // leave has been called: the watcher calls Watch no more
	watchCall chan struct{} // open while the watcher's Watch call is in flight, else nil
	drainDone chan struct{} // nil before the first Drain or Close; closed when the drain completes
	closing   bool          // Close has been called
	closed    bool          // Close has returned
}

// Start starts a node for cfg and returns it once the member has joined the group and the node
// holds a valid view of it. It fails with an error that starts with "tenure: invalid config: "
// when cfg is not valid, and otherwise with the step that failed, such as "tenure: join: " and the
// error of Join. A failed Start leaves the group again if it joined, and leaves no goroutine
// behind.
//
// ctx bounds Start only: the node keeps running after ctx ends, until Close. Start does not yet
// run a store: it supports only a Config without a Store, whose node joins the group and follows
// its views, and returns an error for any other.
func Start(ctx context.Context, cfg Config) (*Node, error) {
	cfg, err := checkConfig(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Store != nil {
		return nil, errStoreNotSupported
	}

	var inc [incarnationBytes]byte
	if _, err := io.ReadFull(cfg.Rand, inc[:]); err != nil {
		return nil, fmt.Errorf("tenure: draw incarnation: %w", err)
	}
	n := &Node{
		cfg:    cfg,
		holder: cfg.Self.ID + "/" + hex.EncodeToString(inc[:]),
		epoch:  time.Now(),
		obs:    nopObserver{},
		first:  make(chan struct{}),
		ended:  make(chan struct{}),
	}
	n.obs.start(n.holder)

	if err := cfg.Membership.Join(ctx, cfg.Self); err != nil {
		err = fmt.Errorf("tenure: join: %w", err)
		n.obs.startFailed(ctx, err)
		return nil, err
	}
	n.ctx, n.cancel = context.WithCancel(context.WithoutCancel(ctx))
	ch, err := cfg.Membership.Watch(n.ctx)
	switch {
	case err != nil:
		return nil, n.failStart(ctx, fmt.Errorf("tenure: watch: %w", err))
	case ch == nil:
		return nil, n.failStart(ctx, errors.New("tenure: watch: nil channel"))
	}
	n.wg.Go(func() { n.watchViews(ch) })

	select {
	case <-n.first:
	case <-n.ended:
		return nil, n.failStart(ctx, fmt.Errorf("tenure: watch: %w", ErrClosed))
	case <-ctx.Done():
		return nil, n.failStart(ctx, fmt.Errorf("tenure: first view: %w", context.Cause(ctx)))
	}
	return n, nil
}

// failStart runs RUN-070's cleanup after Join succeeded: Leave with a fresh context bounded by
// RetryPeriod, then cancel the internal context and wait for every goroutine. It returns err.
func (n *Node) failStart(ctx context.Context, err error) error {
	n.leave(context.WithoutCancel(ctx))
	n.cancel()
	n.wg.Wait()
	n.obs.startFailed(ctx, err)
	return err
}

// Drain hands the member's work over: it marks the member as draining in the group and returns
// nil once the drain has completed. The first call of Drain or Close starts the drain; later and
// concurrent calls wait for the same drain. When ctx ends first, Drain returns ctx's error and
// the drain goes on. After Close, Drain returns ErrClosed.
func (n *Node) Drain(ctx context.Context) error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ErrClosed
	}
	if n.drainDone == nil {
		// Under n.mu, so that Close cannot wait for the goroutines before this one is added.
		n.drainDone = make(chan struct{})
		n.obs.draining()
		n.wg.Go(n.drain)
	}
	done := n.drainDone
	n.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
	case <-n.ctx.Done():
	}
	select {
	case <-done: // a drain that completed at the same instant wins
		return nil
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrClosed // Close stopped the node before the drain completed
}

// drain is the drain goroutine (RUN-500 without a store): it calls SetDraining(true) with a
// context bounded by RetryPeriod, and again RetryPeriod after each failed attempt started, until
// one succeeds; the drain then completes. It ends early when the internal context is done.
func (n *Node) drain() {
	period := n.cfg.Timing.RetryPeriod
	for {
		start := time.Now()
		ctx, cancel := context.WithTimeout(n.ctx, period)
		err := n.cfg.Membership.SetDraining(ctx, true)
		cancel()
		if err == nil {
			break
		}
		if n.ctx.Err() != nil {
			return
		}
		n.obs.membershipFailed("SetDraining", err)
		t := time.NewTimer(time.Until(start.Add(period)))
		select {
		case <-t.C:
		case <-n.ctx.Done():
			t.Stop()
			return
		}
	}
	n.obs.drained()
	close(n.drainDone)
}

// Close drains the node as Drain does, leaves the group, and stops every goroutine of the node.
// It returns nil once all of that is done; later calls return ErrClosed. When ctx ends before
// the drain completes, Close does not leave the group: it stops the node and returns an error
// that wraps ctx's error.
func (n *Node) Close(ctx context.Context) error {
	n.mu.Lock()
	if n.closing {
		n.mu.Unlock()
		return ErrClosed
	}
	n.closing = true
	n.mu.Unlock()

	reason := "drained"
	err := n.Drain(ctx)
	if err == nil {
		n.leave(ctx)
	} else {
		reason, err = "deadline", fmt.Errorf("tenure: close: %w", err)
	}
	n.cancel()
	n.wg.Wait()

	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	n.obs.closed(ctx, reason)
	return err
}
