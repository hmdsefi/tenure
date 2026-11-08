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
//   - view, for each merged view the node publishes: without a store when the watcher takes it,
//     with a store after the loop has stepped it;
//   - step, for each step of the loop, under n.stepMu, after the step's actions were executed
//     and its view published;
//   - callFailed, for each failed store call other than Acquire and KeepAlive, on the call's
//     lane (KeepAlive failures reach step as SessionFailed with Err), and for a failed
//     CloseSession of Start's cleanup;
//   - membershipFailed, for each failed call of the membership after Join that the node retries
//     or gives up: "Watch" (also a channel that closed before Leave), "SetDraining" and "Leave";
//   - draining, once, when the first Drain or Close starts the drain;
//   - drained, once, when the drain completes;
//   - closed, last, when Close finishes, with Close's ctx and the reason: "drained" after a
//     completed drain, "deadline" when ctx ended first.
//
// start, startFailed, closed, membershipFailed("Leave") and the callFailed of Start's cleanup are
// called from the goroutine of Start or Close, draining from the first Drain or Close while it
// holds n.mu, step and a store's view from the loop goroutine, the other callFailed calls from
// the executor's lanes, and the rest from the node's watcher and drain goroutines. No call may
// block, or call back into the node.
type observer interface {
	start(holder string)
	startFailed(ctx context.Context, err error)
	view(v core.View)
	step(rec stepRecord)
	callFailed(r callResult)
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
func (nopObserver) step(stepRecord)                    {}
func (nopObserver) callFailed(callResult)              {}
func (nopObserver) membershipFailed(string, error)     {}
func (nopObserver) draining()                          {}
func (nopObserver) drained()                           {}
func (nopObserver) closed(context.Context, string)     {}
