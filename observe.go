// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"time"

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
//     lane (KeepAlive failures reach step as SessionFailed with Err), for a failed CloseSession
//     of Start's cleanup, for each failed Holdings call of a resync (op "Holdings" and the space),
//     and for each failed WatchHolders call or closed store watch (op "WatchHolders", with
//     errStoreWatchEnded for a closed channel);
//   - runReturned, for each return of a Run that its supervisor restarts (RUN-330): the Run's
//     Shard and error, the count of consecutive returns and the backoff before the restart;
//   - slotCountMismatch, in Start, when the store's slot count is positive and differs from a
//     positive Config.Slots, with both counts; the node keeps the stored count;
//   - slotsFailed, for each failed Slots call of the slot refresher, which keeps the slot data
//     it has and reads again later;
//   - membershipFailed, for each failed call of the membership after Join that the node retries
//     or gives up: "Watch" (also a channel that closed before Leave), "SetDraining" and "Leave";
//   - draining, once, when the first Drain or Close starts the drain;
//   - drained, once, when the drain completes;
//   - shardClosed, for each shard that a Close whose ctx ended first stops with reason "closed"
//     (RUN-530), under n.stepMu, right after the stop and before its Run is canceled;
//   - closed, once, with Close's ctx and the reason: "drained" last, when Close finishes after a
//     completed drain; "deadline" when ctx ended first, once every shard is stopped and before
//     the node's goroutines are stopped (RUN-530).
//
// start, startFailed, slotCountMismatch, shardClosed, closed, membershipFailed("Leave") and the
// callFailed of Start's cleanup are called from the goroutine of Start or Close, slotsFailed
// from the slot refresher's goroutine, draining from the first Drain or Close while it holds
// n.mu, step and a store's view from the loop goroutine, the callFailed calls of Holdings and the
// store watch from the resync and store-watch goroutines, the other callFailed calls from
// the executor's lanes, runReturned from the shard's supervisor goroutine, and the rest from the
// node's watcher and drain goroutines. No call may block, or call back into the node.
type observer interface {
	start(holder string)
	startFailed(ctx context.Context, err error)
	view(v core.View)
	step(rec stepRecord)
	callFailed(r callResult)
	runReturned(sh *Shard, err error, count int, backoff time.Duration)
	slotCountMismatch(configured, stored int)
	slotsFailed(err error)
	membershipFailed(op string, err error)
	draining()
	drained()
	shardClosed(sh *Shard)
	closed(ctx context.Context, reason string)
}

// nopObserver is the observer that does nothing.
type nopObserver struct{}

var _ observer = nopObserver{}

func (nopObserver) start(string)                                  {}
func (nopObserver) startFailed(context.Context, error)            {}
func (nopObserver) view(core.View)                                {}
func (nopObserver) step(stepRecord)                               {}
func (nopObserver) callFailed(callResult)                         {}
func (nopObserver) runReturned(*Shard, error, int, time.Duration) {}
func (nopObserver) slotCountMismatch(int, int)                    {}
func (nopObserver) slotsFailed(error)                             {}
func (nopObserver) membershipFailed(string, error)                {}
func (nopObserver) draining()                                     {}
func (nopObserver) drained()                                      {}
func (nopObserver) shardClosed(*Shard)                            {}
func (nopObserver) closed(context.Context, string)                {}
