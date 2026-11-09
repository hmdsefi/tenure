// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"log/slog"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// Test-only exports for the tests of package tenure_test.

// CheckConfig is checkConfig: it applies the defaults to a Config and returns the first failure
// of its checks.
var CheckConfig = checkConfig

// Holder returns the node's holder string: Self.ID, "/" and the incarnation in lowercase hex. It
// takes the lock the loop holds while it carries a step out, so a step hook must not call it.
func (n *Node) Holder() string {
	n.stepMu.Lock()
	defer n.stepMu.Unlock()
	return n.holder
}

// Epoch returns the node's monotonic origin.
func (n *Node) Epoch() time.Time { return n.epoch }

// PublishedView returns the node's published merged view, and false before the first.
func (n *Node) PublishedView() (core.View, bool) {
	v := n.view.Load()
	if v == nil {
		return core.View{}, false
	}
	return *v, true
}

// StepRecord is what one step of a node's loop did.
type StepRecord = stepRecord

// WithStepHook returns a copy of ctx that makes Start, given it, call f after each step of the
// node's loop, on the loop goroutine. f must not block for long: the loop waits for it.
func WithStepHook(ctx context.Context, f func(StepRecord)) context.Context {
	return context.WithValue(ctx, stepHookKey{}, f)
}

// CallStamp returns the executor's start stamp of the store call made with ctx: lane calls,
// Holdings and Slots reads carry one. It returns 0 for any other context.
func CallStamp(ctx context.Context) uint64 {
	s, _ := ctx.Value(callStampKey{}).(uint64)
	return s
}

// SessionDeadline returns the session deadline the loop keeps for unfenced shards, as a reading
// since the epoch.
func (n *Node) SessionDeadline() time.Duration { return time.Duration(n.deadline.Load()) }

// PublishedShard returns the published Shard of shard id of space, nil when none is published.
func (n *Node) PublishedShard(space string, id int) *Shard {
	return n.shards[space].local[id].Load()
}

// NewNodeForTest returns an inert node that was never started: RecentEvents returns events as
// they are given, the newest 256 of them, and Table shows a node of member "test" with no
// spaces, view or session. No other method works on it.
func NewNodeForTest(events ...Event) *Node {
	cfg := Config{Self: Member{ID: "test"}}
	n := &Node{cfg: cfg, epoch: time.Now(), events: newEventLog(cfg.Self.ID, nil, nil)}
	n.tab = newTableState(cfg, n.holder, n.epoch)
	l := n.events
	l.seq = uint64(len(events))
	for i, e := range events {
		l.ring[i%ringSize] = e // recent reads event number i+1 at index i % ringSize
	}
	return n
}

// EventAttrsForTest is eventAttrs: the attributes of e's log record after its kind, in order, or
// nil for a value that is not a kind.
func EventAttrsForTest(e Event) []slog.Attr { return eventAttrs(e) }
