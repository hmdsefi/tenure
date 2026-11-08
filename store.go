// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"fmt"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// Store keeps the shard records of every space and the sessions of their holders. A record names
// the holder of one shard, or no holder, and the shard's latest term. A Store is safe for
// concurrent use, also when several clients share one backend.
//
// A holder string names one session. Its member ID is the text before the first "/", or the whole
// string when it has none. A session is live from its first successful KeepAlive until it ends
// for good: when it expires, LeaseDuration of its own Timing after its last renewal; when a
// session of the same member ID opens; or when CloseSession names it. An ended session's records
// stay as they are, and any live holder may take them at once.
//
// The store decides expiry on a clock of its own, or lets the acquirer decide it: then an
// acquirer may take a record once it has observed the holder's session unchanged for that
// LeaseDuration on its own monotonic clock. A renewal that lands while such a takeover is in
// flight can keep the session live although the shard is taken; Confirm shows such a takeover.
//
// Every method here and in SlotStore and Watcher returns ctx.Err() itself, and changes nothing,
// when ctx is already done at the call. It then checks its arguments in parameter order, with
// errors of the store's own; then, if it needs the live session of holder, returns
// ErrSessionLost when that session is not live; and only then returns a *HeldError or ErrNoSlot.
// The argument checks reject at least an empty holder or space, a holder or space that contains a
// zero byte, a space that contains "/", a negative shard or slot, a term of 0 and a negative slot
// count; a store may reject more, such as names its backend cannot hold. A call that fails an
// argument check, or returns ErrSessionLost, a *HeldError or ErrNoSlot, changes nothing. Any other
// error, such as a failed request or a context that ends during the call, leaves the outcome
// unknown: the call may have taken effect.
type Store interface {
	// KeepAlive opens a session for a holder that has never had one, or renews the live session
	// of holder and stores timing as its new Timing. It returns ErrSessionLost when the session
	// of holder has ended, also when the renewal arrives at or after its expiry, and when
	// CloseSession named holder before it ever had a session. A renewal never grants a term.
	KeepAlive(ctx context.Context, holder string, timing core.Timing) error

	// Acquire grants the shard to holder and returns the new term, when the record is absent,
	// has no holder, names a holder whose session is not live, or names holder itself. The new
	// term is core.NextTerm of the record's term (0 when there is no record) and the current wall
	// time, so it is larger than every earlier term of the shard unless the store lost the
	// record. Otherwise Acquire returns 0 and a *HeldError that names the record's holder, with
	// the RetryPeriod of holder's own Timing as RetryAfter. A grant is atomic with the read it
	// rests on, so of racing calls by different holders at most one is granted a term. Acquire
	// needs the live session of holder; otherwise it returns ErrSessionLost.
	Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error)

	// Release clears the record's holder when the record names holder at term, and keeps the
	// term. Any other record, or none, is left as it is, and Release still returns nil. It does
	// not look at the session of holder.
	Release(ctx context.Context, holder, space string, shard int, term core.Term) error

	// Confirm returns the shard's record, which shows every write committed before the call and
	// every grant to another holder that did not see the latest successful renewal of holder: no
	// such grant commits after Confirm returns. A store that decides expiry on its own clock gets
	// this from a read; one that lets acquirers decide it may need a write, which makes such a
	// grant still in flight fail. Either way Confirm changes neither the holder nor the term of
	// the record. An absent record reads as Holding{Space: space, Shard: shard}. term is the term
	// the caller holds the shard at; Confirm does not compare it with the record. Confirm needs
	// the live session of holder; otherwise it returns ErrSessionLost.
	Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (Holding, error)

	// CloseSession ends the session of holder for good, also for a holder that is unknown or
	// already ended, and clears the holder of every shard record and slot record that names it,
	// keeping the terms. It never returns ErrSessionLost.
	CloseSession(ctx context.Context, holder string) error

	// Holdings returns every record of space, also those with no holder, sorted by shard. It does
	// not look at sessions: Holder is the holder a record names, whether or not its session is
	// live. The slice is the caller's own.
	Holdings(ctx context.Context, space string) ([]Holding, error)
}

// SlotStore is implemented by a Store that keeps slots. A slot record names the holder of one
// slot; the holder counts only while its session is live. The slot count belongs to the whole
// group.
type SlotStore interface {
	// ClaimSlot returns the lowest slot that holder holds below the count, without writing. When
	// holder holds no slot below the count but one at or above it, or no slot below the count is
	// free, it returns -1 and ErrNoSlot. Otherwise it claims the lowest free slot: one with no
	// record, no holder, or a holder whose session is not live. ClaimSlot needs the live session
	// of holder; otherwise it returns ErrSessionLost.
	ClaimSlot(ctx context.Context, holder string) (int, error)

	// ReleaseSlot clears the slot's holder when the slot record names holder. Any other record,
	// or none, is left as it is, and ReleaseSlot still returns nil. It does not look at the
	// session of holder.
	ReleaseSlot(ctx context.Context, holder string, slot int) error

	// Slots returns the slot count and one entry per slot below it: the holder of the slot while
	// its session is live, else "". When the count is 0 it returns 0, nil, nil.
	Slots(ctx context.Context) (count int, holders []string, err error)

	// SetSlots sets the slot count. It changes no slot record, so the holder of a slot at or above
	// the new count keeps it until the slot is released or the holder's session is closed.
	SetSlots(ctx context.Context, count int) error
}

// Watcher is implemented by a Store that reports changes to its shard records.
type Watcher interface {
	// WatchHolders returns a channel that receives, in commit order, every write to a shard record
	// of any space committed after WatchHolders returns, each as the record after the write. A
	// deletion arrives as a Holding with only Space, Shard and Version set, Version being the
	// deletion's. When the store cannot deliver every such write, it closes the channel instead.
	// The channel is closed once ctx is done.
	WatchHolders(ctx context.Context) (<-chan Holding, error)
}

// Holding is a shard record as a Store reports it.
type Holding struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Holder is the holder the record names; "" means the record has no holder.
	Holder string

	// Term is the record's latest term. Clearing the holder keeps it.
	Term core.Term

	// Version orders the record's writes. Each write to a shard record, such as a grant, a clear
	// or a deletion, takes a version larger than that of every earlier write to any shard record
	// of the store. 0 means there is no record.
	Version uint64
}

// HeldError is the error Acquire returns when another holder has the shard.
type HeldError struct {
	// Holder is the holder the record names.
	Holder string

	// RetryAfter is how long the acquirer should wait before it tries again.
	RetryAfter time.Duration
}

// Error returns "tenure: held by <Holder>, retry after <RetryAfter>", with RetryAfter in
// time.Duration's String form.
func (e *HeldError) Error() string {
	return fmt.Sprintf("tenure: held by %s, retry after %v", e.Holder, e.RetryAfter)
}
