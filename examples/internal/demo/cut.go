// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package demo

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// ErrCut means a call reached a CutStore while it was cut and its context ended before the store
// was healed. The error also matches the context's error.
var ErrCut = errors.New("demo: store cut")

// CutStore wraps a tenure.Store, and tenure.Watcher when the wrapped store is one, so that a
// demo or a test can cut one node off from the store while the node keeps running: a network
// partition between a node and its store.
//
// While the store is cut, every call blocks until its context ends and then returns an error
// that matches ErrCut and the context's error; a call that was blocked when Heal came goes on
// and returns the wrapped store's answer. The channel of WatchHolders stays open but receives
// nothing while the store is cut: writes that happen meanwhile are kept in order and arrive
// after Heal. A call never blocks past its context. Without a cut the calls go straight to the
// wrapped store.
//
// A CutStore is safe for concurrent use.
type CutStore struct {
	s tenure.Store

	mu   sync.Mutex
	gate chan struct{} // non-nil while cut; closed by Heal
}

// NewCutStore returns a CutStore over s, which starts healed.
func NewCutStore(s tenure.Store) *CutStore {
	return &CutStore{s: s}
}

// Cut cuts the store: from now on its calls block as CutStore describes. Cutting a cut store
// does nothing.
func (c *CutStore) Cut() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gate == nil {
		c.gate = make(chan struct{})
	}
}

// Heal ends a cut: the calls that wait go on, and later calls reach the wrapped store. Healing
// a store that is not cut does nothing.
func (c *CutStore) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gate != nil {
		close(c.gate)
		c.gate = nil
	}
}

// current returns the channel that Heal will close, or nil when the store is not cut.
func (c *CutStore) current() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gate
}

// pass waits while the store is cut. It returns nil when the call may reach the wrapped store.
func (c *CutStore) pass(ctx context.Context) error {
	for {
		g := c.current()
		if g == nil {
			return nil
		}
		select {
		case <-g:
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrCut, ctx.Err())
		}
	}
}

// KeepAlive calls the wrapped store's KeepAlive unless the store is cut.
func (c *CutStore) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	if err := c.pass(ctx); err != nil {
		return err
	}
	return c.s.KeepAlive(ctx, holder, timing)
}

// Acquire calls the wrapped store's Acquire unless the store is cut.
func (c *CutStore) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	if err := c.pass(ctx); err != nil {
		return 0, err
	}
	return c.s.Acquire(ctx, holder, space, shard)
}

// Release calls the wrapped store's Release unless the store is cut.
func (c *CutStore) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	if err := c.pass(ctx); err != nil {
		return err
	}
	return c.s.Release(ctx, holder, space, shard, term)
}

// Confirm calls the wrapped store's Confirm unless the store is cut.
func (c *CutStore) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	if err := c.pass(ctx); err != nil {
		return tenure.Holding{}, err
	}
	return c.s.Confirm(ctx, holder, space, shard, term)
}

// CloseSession calls the wrapped store's CloseSession unless the store is cut.
func (c *CutStore) CloseSession(ctx context.Context, holder string) error {
	if err := c.pass(ctx); err != nil {
		return err
	}
	return c.s.CloseSession(ctx, holder)
}

// Holdings calls the wrapped store's Holdings unless the store is cut.
func (c *CutStore) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	if err := c.pass(ctx); err != nil {
		return nil, err
	}
	return c.s.Holdings(ctx, space)
}

// WatchHolders opens a watch on the wrapped store, which must be a tenure.Watcher, and returns a
// channel that carries what the wrapped store's channel receives, in order, except while the
// store is cut: then the values wait, however many come, and the channel stays open. The
// channel is closed when ctx ends, and after the wrapped store's channel closed and every value
// it carried was delivered. Opening the watch is a call like the others and waits while cut.
func (c *CutStore) WatchHolders(ctx context.Context) (<-chan tenure.Holding, error) {
	w, ok := c.s.(tenure.Watcher)
	if !ok {
		return nil, errors.New("demo: the wrapped store is not a tenure.Watcher")
	}
	if err := c.pass(ctx); err != nil {
		return nil, err
	}
	src, err := w.WatchHolders(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan tenure.Holding)
	go c.forward(ctx, src, out)
	return out, nil
}

// forward moves values from src to out, holding them back while the store is cut.
func (c *CutStore) forward(ctx context.Context, src <-chan tenure.Holding, out chan<- tenure.Holding) {
	defer close(out)
	var queue []tenure.Holding
	for {
		gate := c.current()
		var (
			send chan<- tenure.Holding
			head tenure.Holding
		)
		if len(queue) > 0 && gate == nil {
			send, head = out, queue[0]
		}
		if src == nil && len(queue) == 0 {
			return
		}
		select {
		case h, ok := <-src: // a nil src blocks for good
			if !ok {
				src = nil
				continue
			}
			queue = append(queue, h)
		case send <- head: // a nil send blocks for good
			queue = queue[1:]
		case <-gate: // Heal: look at the state again; a nil gate blocks for good
		case <-ctx.Done():
			return
		}
	}
}
