// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// errWatchClosed is the error the watcher reports when a Watch channel closes before Leave.
var errWatchClosed = errors.New("tenure: watch channel closed")

// mergeView returns v with the slot data merged in (RUN-220): Slots is count, SlotHolders is
// holders cut or padded with "" to count entries, and each member's Slot is the lowest slot whose
// holder's member ID equals the member's ID, else -1. Version is v's. The members slice is a copy,
// never shared with v; the members' Meta maps are shared.
func mergeView(v core.View, count int, holders []string) core.View {
	out := core.View{Version: v.Version, Slots: count}
	slotOf := make(map[string]int)
	if count > 0 {
		out.SlotHolders = make([]string, count)
		copy(out.SlotHolders, holders)
		for s, h := range out.SlotHolders {
			if h == "" {
				continue
			}
			if id := holderID(h); slotOf[id] == 0 {
				slotOf[id] = s + 1 // 1 + the lowest slot, so that 0 means none
			}
		}
	}
	if v.Members != nil {
		out.Members = make([]core.Member, len(v.Members))
		for i, m := range v.Members {
			m.Slot = slotOf[m.ID] - 1
			out.Members[i] = m
		}
	}
	return out
}

// holderID returns the member ID part of a holder string: the text before the first "/", or the
// whole string when it has none.
func holderID(holder string) string {
	id, _, _ := strings.Cut(holder, "/")
	return id
}

// watchViews is the watcher goroutine (RUN-200). It follows ch, the channel of Start's Watch, and
// after a closed channel or a failed Watch calls Watch again RetryPeriod later. It ends when Watch
// returns ErrClosed, when the internal context is done, and once leave has been called: the
// channel Leave closes is not reported and no Watch follows it. Each view it reads is merged and
// offered; ended is closed when it returns.
func (n *Node) watchViews(ch <-chan core.View) {
	defer close(n.ended)
	for {
		if ch != nil && !n.follow(ch) {
			return
		}
		if !n.pause() {
			return
		}
		var err error
		ch, err = n.rewatch()
		switch {
		case ch != nil:
		case err == nil, errors.Is(err, ErrClosed), n.ctx.Err() != nil:
			return // leave was called, or the membership or the node is closed
		default:
			n.obs.membershipFailed("Watch", err)
		}
	}
}

// follow offers each view of ch until ch is closed, and reports whether the watcher should call
// Watch again: false when the internal context is done or leave has been called.
func (n *Node) follow(ch <-chan core.View) bool {
	for {
		select {
		case v, ok := <-ch:
			if ok {
				n.offer(v)
				continue
			}
			if n.leaving() || n.ctx.Err() != nil {
				return false
			}
			n.obs.membershipFailed("Watch", errWatchClosed)
			return true
		case <-n.ctx.Done():
			return false
		}
	}
}

// pause waits RetryPeriod and reports whether the internal context is still not done.
func (n *Node) pause() bool {
	t := time.NewTimer(n.cfg.Timing.RetryPeriod)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-n.ctx.Done():
		return false
	}
}

// rewatch calls Watch on the internal context, unless leave has been called: then it returns
// nil, nil. While the call is in flight, n.watchCall is open, so that leave can wait for it and no
// Watch starts once Leave has been called.
func (n *Node) rewatch() (<-chan core.View, error) {
	n.mu.Lock()
	if n.left {
		n.mu.Unlock()
		return nil, nil
	}
	call := make(chan struct{})
	n.watchCall = call
	n.mu.Unlock()

	ch, err := n.cfg.Membership.Watch(n.ctx)

	n.mu.Lock()
	n.watchCall = nil
	n.mu.Unlock()
	close(call)
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, errors.New("tenure: Watch returned a nil channel")
	}
	return ch, nil
}

// offer takes one view from the membership. It merges the node's slot data into it (none yet) and
// drops it when the merged view is not valid or its Version is not above that of the last view
// offer took. Without a store it publishes the merged view; with a store it hands the view to the
// loop, which steps it and then publishes it. The first view it takes lets Start go on.
func (n *Node) offer(v core.View) {
	merged := mergeView(v, 0, nil)
	if merged.Validate() != nil {
		return
	}
	if n.offered && merged.Version <= n.offeredVersion {
		return
	}
	first := !n.offered
	n.offered, n.offeredVersion = true, merged.Version
	if n.cfg.Store == nil {
		n.publish(merged)
	} else {
		n.inbox.postView(merged)
	}
	if first {
		close(n.first)
	}
}

// publish makes v the node's published view and reports it.
func (n *Node) publish(v core.View) {
	n.view.Store(&v)
	n.obs.view(v)
}

// leaving reports whether leave has been called.
func (n *Node) leaving() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.left
}

// leave calls Leave with ctx bounded by RetryPeriod (RUN-070). It first marks the node as left and
// waits, within that bound, for a Watch call in flight, so that no Watch starts after Leave. A
// failed Leave is reported and otherwise ignored.
func (n *Node) leave(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, n.cfg.Timing.RetryPeriod)
	defer cancel()
	n.mu.Lock()
	n.left = true
	call := n.watchCall
	n.mu.Unlock()
	if call != nil {
		select {
		case <-call:
		case <-ctx.Done():
		}
	}
	if err := n.cfg.Membership.Leave(ctx); err != nil {
		n.obs.membershipFailed("Leave", err)
	}
}
