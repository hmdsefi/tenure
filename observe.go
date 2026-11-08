// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"

	"github.com/hmdsefi/tenure/core"
)

// observer is the seam through which the node reports what happens to it. Start installs
// nopObserver; plan 1.6 implements events and Table behind it. The node calls it at these points:
//
//   - start, once Start has drawn the incarnation and before it calls Join, with the holder;
//   - startFailed, last, when Start fails after start, with Start's ctx and the error it returns;
//   - view, for each merged view the node publishes;
//   - membershipFailed, for each failed call of the membership after Join that the node retries
//     or gives up: "Watch" (also a channel that closed before Leave), "SetDraining" and "Leave";
//   - draining, once, when the first Drain or Close starts the drain;
//   - drained, once, when the drain completes;
//   - closed, last, when Close finishes, with Close's ctx and the reason: "drained" after a
//     completed drain, "deadline" when ctx ended first.
//
// start, startFailed, closed and membershipFailed("Leave") are called from the goroutine of Start
// or Close, draining from the first Drain or Close while it holds n.mu, and the rest from the
// node's watcher and drain goroutines. No call may block, or call back into the node.
type observer interface {
	start(holder string)
	startFailed(ctx context.Context, err error)
	view(v core.View)
	membershipFailed(op string, err error)
	draining()
	drained()
	closed(ctx context.Context, reason string)
}

// nopObserver is the observer that does nothing.
type nopObserver struct{}

var _ observer = nopObserver{}

func (nopObserver) start(string)                       {}
func (nopObserver) startFailed(context.Context, error) {}
func (nopObserver) view(core.View)                     {}
func (nopObserver) membershipFailed(string, error)     {}
func (nopObserver) draining()                          {}
func (nopObserver) drained()                           {}
func (nopObserver) closed(context.Context, string)     {}
