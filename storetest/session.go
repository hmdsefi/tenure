// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest

import "time"

// The Session cases check how sessions open, renew, expire, are replaced and close, and that a
// session's end frees its records at once while keeping their terms.

// sessionRenewalKeepsRecords: renewing two sessions for twice the lease keeps both their
// records, with their terms, against a third holder.
func sessionRenewalKeepsRecords(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	t2 := e.acq(h2, "b", 1)

	e.elapse(2*e.f.Timing.LeaseDuration, h1, h2)

	e.rec(h1, "a", 0, h1, t1)
	e.rec(h2, "b", 1, h2, t2)
	e.conf(h1, "a", 0, t1, h1, t1)
	e.conf(h2, "b", 1, t2, h2, t2)
	e.open(h3)
	e.held(h3, "a", 0, h1)
	e.held(h3, "b", 1, h2)
}

// sessionExpiryBoundary: a session not renewed still holds its record 1ms before its lease
// ends and loses it when the lease ends.
func sessionExpiryBoundary(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.acq(h1, "a", 0)
	e.held(h2, "a", 0, h1)

	e.elapse(e.f.Timing.LeaseDuration-time.Millisecond, h2)
	e.held(h2, "a", 0, h1)

	e.elapse(time.Millisecond, h2)
	t2 := e.acq(h2, "a", 0)
	e.rec(h1, "a", 0, h2, t2)
}

// sessionExpired: once a session expires, another holder takes its shard and the expired
// holder can acquire nothing.
func sessionExpired(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.acq(h1, "a", 0)
	e.held(h2, "a", 0, h1)

	e.expire(h2)
	t2 := e.acq(h2, "a", 0)
	e.lostAcq(h1, "a", 1)
	e.rec(h1, "a", 0, h2, t2)
}

// sessionLateRenewal: a renewal after expiry is refused, and the session stays lost.
func sessionLateRenewal(e *env) {
	h1 := holder(1, 1)
	e.open(h1)
	e.acq(h1, "a", 0)

	e.expire()
	e.lost(h1)
	e.elapse(e.f.Timing.RetryPeriod)
	e.lost(h1)
	e.lostAcq(h1, "b", 1)
}

// sessionReplace: a new incarnation of a member ends the old one's session at once, so another
// holder takes its shard without waiting.
func sessionReplace(e *env) {
	old, next, h2, h3 := holder(1, 1), holder(1, 2), holder(2, 1), holder(3, 1)
	e.open(old)
	e.acq(old, "a", 0)
	e.open(h3)
	e.held(h3, "a", 0, old)

	e.open(next)
	t3 := e.acq(h3, "a", 0)
	e.rec(h2, "a", 0, h3, t3)
}

// sessionReplacedForGood: a replaced incarnation can neither renew nor acquire, while the new
// one can.
func sessionReplacedForGood(e *env) {
	old, next := holder(1, 1), holder(1, 2)
	e.open(old)
	e.acq(old, "a", 0)

	e.open(next)
	e.lost(old)
	e.lostAcq(old, "a", 1)
	e.open(next)
	e.acq(next, "a", 0)
	e.lost(old)
}

// sessionClose: CloseSession clears the holder's records, keeping their terms, frees them at once
// and ends the session for good.
func sessionClose(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	t0 := e.acq(h1, "a", 0)
	t1 := e.acq(h1, "b", 3)
	r0 := e.rec(h1, "a", 0, h1, t0)
	r1 := e.rec(h1, "b", 3, h1, t1)

	e.closeSession(h1)
	e.ver(r0, e.rec(h1, "a", 0, "", t0))
	e.ver(r1, e.rec(h1, "b", 3, "", t1))
	e.lost(h1)
	e.lostAcq(h1, "a", 1)

	e.open(h2)
	e.rec(h2, "a", 0, "", t0)
	e.acq(h2, "a", 0)
}

// sessionCloseUnknown: CloseSession of a holder that never had a session succeeds and keeps the
// holder from ever opening one.
func sessionCloseUnknown(e *env) {
	h1 := holder(1, 1)
	e.closeSession(h1)
	e.lost(h1)
	e.lostAcq(h1, "a", 0)

	e.elapse(e.f.Timing.RetryPeriod)
	e.closeSession(h1)
	e.lost(h1)
}

// sessionHolderLease: expiry follows the holder's own LeaseDuration, so a holder whose lease is
// twice the acquirer's still holds its record after the acquirer's lease.
func sessionHolderLease(e *env) {
	h1, h3 := holder(1, 1), holder(3, 1)
	long := e.f.Timing
	long.LeaseDuration *= 2
	long.SlotGrace = max(long.SlotGrace, long.LeaseDuration)
	e.openWith(h1, long)
	e.acq(h1, "a", 0)
	e.open(h3)
	e.held(h3, "a", 0, h1)

	e.elapse(e.f.Timing.LeaseDuration, h3)
	e.held(h3, "a", 0, h1)

	e.elapse(e.f.Timing.LeaseDuration, h3)
	e.acq(h3, "a", 0)
}

// sessionRetryAfter: a refused Acquire asks the acquirer to retry after the RetryPeriod of its
// own latest Timing.
func sessionRetryAfter(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.acq(h1, "a", 0)

	half := e.f.Timing
	half.RetryPeriod /= 2
	e.openWith(h3, half)
	e.held(h3, "a", 0, h1)

	e.open(h2)
	e.held(h2, "a", 0, h1)

	quarter := e.f.Timing
	quarter.RetryPeriod /= 4
	e.openWith(h3, quarter)
	e.held(h3, "a", 0, h1)
}
