// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// Shard is one ownership of a shard by this member: the node hands it to a space's Run when the
// member takes the shard, and Run checks it before every side effect. A Shard stays valid until
// the member stops owning the shard under its Term; a later ownership of the same shard gets a
// new Shard.
//
// A Shard is safe for concurrent use, and a copy of a Shard checks the same ownership. A Shard
// that the node did not make, such as the zero Shard, is never owned.
type Shard struct {
	// Space is the name of the shard's space.
	Space string

	// ID is the shard's number in its space, from 0 to the space's Shards minus 1.
	ID int

	// Term is the term of this ownership. It grows with every acquisition of the shard, so a
	// downstream fence that keeps the highest term seen rejects the writes of an older owner.
	Term core.Term

	st *shardState
}

// shardState is the part of a Shard the node changes while Run reads it.
type shardState struct {
	// err is the stop error, nil while the shard is owned. The first stop stores it.
	err atomic.Pointer[stopError]

	// fenced is the space's Fenced. A shard of an unfenced space is owned only before the
	// session deadline (RUN-310).
	fenced bool

	// epoch and deadline give the session deadline of an unfenced shard: deadline holds the
	// node's session deadline in nanoseconds since epoch, shared by every shard of the node.
	epoch    time.Time
	deadline *atomic.Int64

	// expired is the error Check returns once the deadline has passed, built in advance so
	// that Check never allocates.
	expired *stopError
}

// reasonExpired is the reason of a stop at the session deadline: core.StopExpired's name.
const reasonExpired = "expired"

// newShard returns an owned Shard. For an unfenced space, deadline holds the node's session
// deadline in nanoseconds since epoch, and the Shard is owned only while time.Since(epoch) is
// below it; a fenced Shard ignores epoch and deadline, which may be nil.
func newShard(space string, id int, term core.Term, fenced bool, epoch time.Time, deadline *atomic.Int64) *Shard {
	return &Shard{
		Space: space,
		ID:    id,
		Term:  term,
		st: &shardState{
			fenced:   fenced,
			epoch:    epoch,
			deadline: deadline,
			expired:  newStopError(space, id, term, reasonExpired),
		},
	}
}

// Check returns nil while this member owns the shard. Otherwise it returns an error that
// matches ErrNotOwner and says why, for example "tenure: not owner of orders/3 (term 42):
// moved". Once the shard has stopped, every call returns the same error, which is also the cause
// of Run's context. In a space without fencing the shard is owned only until the session
// deadline: from that instant Check returns an error ending in "expired", even before the node
// stops the shard. A Shard the node did not make returns ErrNotOwner. Check does not allocate.
func (s *Shard) Check() error {
	if s == nil || s.st == nil {
		return ErrNotOwner
	}
	if err := s.st.err.Load(); err != nil {
		return err
	}
	if !s.st.fenced && time.Since(s.st.epoch) >= time.Duration(s.st.deadline.Load()) {
		return s.st.expired
	}
	return nil
}

// Valid reports whether this member owns the shard: it is Check() == nil. Valid does not
// allocate.
func (s *Shard) Valid() bool {
	return s.Check() == nil
}

// stop ends the ownership with reason, a core.StopReason's name or "closed", and returns the
// stop error. The first stop wins: later calls change nothing and return the first call's error
// (RUN-320). The caller cancels Run's context with that error only after stop has returned, so a
// Run that sees its context done already gets the error from Check.
func (s *Shard) stop(reason string) error {
	if err := s.st.err.Load(); err != nil {
		return err
	}
	err := s.st.expired
	if reason != reasonExpired {
		err = newStopError(s.Space, s.ID, s.Term, reason)
	}
	if !s.st.err.CompareAndSwap(nil, err) {
		return s.st.err.Load()
	}
	return err
}

// stopError is the error of a Shard that is no longer owned.
type stopError struct {
	reason string // a core.StopReason's name or "closed"
	msg    string
}

// newStopError builds the error and its text once, so that neither Check nor Error allocates.
func newStopError(space string, shard int, term core.Term, reason string) *stopError {
	return &stopError{
		reason: reason,
		msg: "tenure: not owner of " + space + "/" + strconv.Itoa(shard) +
			" (term " + strconv.FormatUint(uint64(term), 10) + "): " + reason,
	}
}

func (e *stopError) Error() string { return e.msg }

// Is reports whether target is ErrNotOwner.
func (e *stopError) Is(target error) bool { return target == ErrNotOwner }

// supervise runs one shard's Run until ctx is done; the caller runs it in its own goroutine.
//
// It first waits for prev, which the previous supervisor of the same shard closes when it has
// ended, across machines and incarnations, so that at most one Run of a shard runs at a time
// (RUN-300). It waits for prev even when ctx is done, since stopped must not report the shard
// stopped while an older Run of it may still run.
//
// Each time run returns while ctx is not done, supervise restarts it after a backoff (RUN-330):
// with c the count of consecutive returns, c is first reset to 0 when the call lasted at least
// LeaseDuration, then incremented, and the backoff is min(RetryPeriod·2^(c−1), LeaseDuration).
// onReturn gets run's error, c and the backoff before the wait.
//
// A return after sh has stopped is no restart, even while ctx is not done yet: the node stops
// the shard before it cancels ctx (RUN-320), so a run that sees Check fail can return between
// the two. supervise then waits for ctx without calling onReturn.
//
// Once ctx is done, before a call, after one or during the backoff, supervise calls stopped
// exactly once and returns. A panic in run is not recovered: it crashes the process, and
// stopped is not called.
func supervise(
	ctx context.Context,
	sh *Shard,
	run func(context.Context, *Shard) error,
	prev <-chan struct{},
	t core.Timing,
	onReturn func(err error, count int, backoff time.Duration),
	stopped func(),
) {
	if prev != nil {
		<-prev
	}
	count := 0
	for ctx.Err() == nil {
		start := time.Now()
		err := run(ctx, sh)
		if ctx.Err() != nil {
			break
		}
		if sh.st.err.Load() != nil {
			<-ctx.Done()
			break
		}
		if time.Since(start) >= t.LeaseDuration {
			count = 0
		}
		count++
		backoff := restartBackoff(t, count)
		onReturn(err, count, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
	}
	stopped()
}

// restartBackoff returns min(RetryPeriod·2^(count−1), LeaseDuration) for count ≥ 1 without
// overflowing: the doubling stops once LeaseDuration is reached.
func restartBackoff(t core.Timing, count int) time.Duration {
	b := t.RetryPeriod
	for i := 1; i < count && b < t.LeaseDuration; i++ {
		b *= 2
	}
	return min(b, t.LeaseDuration)
}
