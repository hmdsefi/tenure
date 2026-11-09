// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// The observer methods of the run, store and membership failures (RUN-720): each turns one
// failure that a goroutine of the node sees into its event, with OBS-150's fields. They are called
// where observer's doc says; the event log drops any event emitted after node.closed.

// runReturned emits run.returned for a return of sh's Run that its supervisor restarts: Count is
// the count of consecutive returns, Duration the backoff before the restart and Err Run's error.
func (o *logObserver) runReturned(sh *Shard, err error, count int, backoff time.Duration) {
	e := shardEvent(EventRunReturned, sh.Space, sh.ID, sh.Term)
	e.Count, e.Duration, e.Err = count, backoff, err
	o.n.events.emit(e)
}

// callFailed emits store.failed for a failed store call, unless storeFailed says the failure has
// no event of its own.
func (o *logObserver) callFailed(r callResult) {
	if e, ok := storeFailed(r); ok {
		o.n.events.emit(e)
	}
}

// slotsFailed emits store.failed for a failed Slots call of the slot refresher.
func (o *logObserver) slotsFailed(err error) {
	o.n.events.emit(Event{Kind: EventStoreFailed, Reason: "Slots", Shard: -1, Slot: -1, Err: err})
}

// membershipFailed emits membership.failed for a failed call op of the membership. The watcher
// reports no closed channel once Leave has been called, so the channel Leave closes emits nothing.
func (o *logObserver) membershipFailed(op string, err error) {
	o.n.events.emit(Event{Kind: EventMembershipFailed, Reason: op, Shard: -1, Slot: -1, Err: err})
}

// storeFailed returns the store.failed event of r: Reason is the Store method, Space the space of
// a Release, Confirm or Holdings call, Shard the shard of a Release or Confirm call, and Slot the
// slot of a ReleaseSlot call. It reports false for a failure that emits no store.failed: an
// Acquire, which reaches shard.acquire_failed, a KeepAlive, which reaches session.renew_failed,
// and a ClaimSlot that finds no free slot, which is not a failure.
func storeFailed(r callResult) (Event, bool) {
	e := Event{Kind: EventStoreFailed, Shard: -1, Slot: -1, Err: r.err}
	switch a := r.act.(type) {
	case nil:
		e.Reason, e.Space = r.op, r.space
	case core.Release:
		e.Reason, e.Space, e.Shard = "Release", a.Space, a.Shard
	case core.Confirm:
		e.Reason, e.Space, e.Shard = "Confirm", a.Space, a.Shard
	case core.ReleaseSlot:
		e.Reason, e.Slot = "ReleaseSlot", a.Slot
	case core.ClaimSlot:
		if errors.Is(r.err, ErrNoSlot) {
			return Event{}, false
		}
		e.Reason = "ClaimSlot"
	case core.CloseSession:
		e.Reason = "CloseSession"
	default: // Acquire and KeepAlive
		return Event{}, false
	}
	return e, true
}
