// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package demo

import (
	"context"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Membership wraps m so that the node whose Membership it is falls out of step with the group
// while c is cut: the node's Watch keeps its channel open but delivers no view, as an isolated
// node would see none, and delivers the newest one after Heal. Join, SetDraining and Leave go
// straight to m. Cutting a node's store and its membership together is the partition that
// leaves the other members to evict it while it goes on believing it owns its shards.
func (c *CutStore) Membership(m tenure.Membership) tenure.Membership {
	return &cutMembership{Membership: m, cut: c}
}

// cutMembership is the Membership that CutStore.Membership returns.
type cutMembership struct {
	tenure.Membership
	cut *CutStore
}

// Propagation returns the wrapped Membership's, 0 when it does not tell.
func (m *cutMembership) Propagation() time.Duration {
	if pe, ok := m.Membership.(tenure.PropagationEstimator); ok {
		return pe.Propagation()
	}
	return 0
}

// Watch opens a watch on the wrapped Membership and returns a channel that carries its views
// except while the store is cut: then the newest view waits, and the channel stays open. The
// channel is closed when ctx ends, and after the wrapped channel closed and its last view was
// delivered.
func (m *cutMembership) Watch(ctx context.Context) (<-chan core.View, error) {
	src, err := m.Membership.Watch(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan core.View)
	go m.cut.forwardViews(ctx, src, out)
	return out, nil
}

// forwardViews moves views from src to out, holding the newest back while the store is cut.
func (c *CutStore) forwardViews(ctx context.Context, src <-chan core.View, out chan<- core.View) {
	defer close(out)
	var pending *core.View
	for {
		gate := c.current()
		var (
			send chan<- core.View
			head core.View
		)
		if pending != nil && gate == nil {
			send, head = out, *pending
		}
		if src == nil && pending == nil {
			return
		}
		select {
		case v, ok := <-src: // a nil src blocks for good
			if !ok {
				src = nil
				continue
			}
			pending = &v
		case send <- head: // a nil send blocks for good
			pending = nil
		case <-gate: // Heal: look at the state again; a nil gate blocks for good
		case <-ctx.Done():
			return
		}
	}
}
