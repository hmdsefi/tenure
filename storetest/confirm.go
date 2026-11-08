// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest

import "github.com/hmdsefi/tenure"

// The Confirm cases check that Confirm shows the shard's current record to any caller with a
// live session, whatever term it passes, also after a takeover; that it changes neither holder
// nor term; and that it needs the caller's live session. The cases never compare the versions
// Confirm returns, since a store may write to confirm.

// confirmHeld: the holder's Confirm shows its own record, also after renewals, and leaves the
// shard held.
func confirmHeld(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)

	e.conf(h1, "a", 0, t1, h1, t1)
	e.rec(h2, "a", 0, h1, t1)
	e.held(h2, "a", 0, h1)

	e.elapse(2*e.f.Timing.RetryPeriod, h1, h2)
	e.conf(h1, "a", 0, t1, h1, t1)
	e.rec(h2, "a", 0, h1, t1)
	e.held(h2, "a", 0, h1)
}

// confirmOther: Confirm by a live holder that does not hold the shard shows the record as it
// is: the holder's, a cleared one, or, for a shard with no record, Holding{Space, Shard}.
func confirmOther(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	t1 := e.acq(h1, "a", 0)

	e.conf(h2, "a", 0, t1, h1, t1)
	e.conf(h3, "a", 0, 1, h1, t1)
	e.rec(h2, "a", 0, h1, t1)
	e.held(h2, "a", 0, h1)

	e.release(h1, "a", 0, t1)
	e.conf(h2, "a", 0, t1, "", t1)
	e.conf(h3, "a", 0, t1, "", t1)

	want := tenure.Holding{Space: "a", Shard: 5}
	got, err := e.client(h2).Confirm(e.ctx, h2, "a", 5, t1)
	if err != nil || got != want {
		e.t.Fatalf("Confirm(%q, %q, %d, %d) = %s, %s, want %s, nil", h2, "a", 5, t1, fmtHolding(got),
			errText(err), fmtHolding(want))
	}
	e.rec(h1, "a", 5, "", 0)
}

// confirmWrongTerm: Confirm does not compare its term with the record's: at a term above, below
// or older than the record's it shows the record, and the record stays as it is.
func confirmWrongTerm(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)

	e.conf(h1, "a", 0, t1+1, h1, t1)
	e.conf(h1, "a", 0, 1, h1, t1)
	e.rec(h2, "a", 0, h1, t1)

	t2 := e.acq(h1, "a", 0)
	e.conf(h1, "a", 0, t1, h1, t2)
	e.rec(h2, "a", 0, h1, t2)
	e.held(h2, "a", 0, h1)
}

// confirmAfterTakeover: once another holder has taken the shard over, Confirm shows the new
// holder and term: to the old holder after it released the shard, and to a new incarnation of an
// old holder whose session expired, while the expired one gets tenure.ErrSessionLost.
func confirmAfterTakeover(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	t1 := e.acq(h1, "a", 0)
	e.release(h1, "a", 0, t1)
	e.rec(h2, "a", 0, "", t1)
	t2 := e.acq(h2, "a", 0)
	e.conf(h1, "a", 0, t1, h2, t2)

	t3 := e.acq(h3, "b", 1)
	e.held(h1, "b", 1, h3)
	e.expire(h1, h2)
	t4 := e.acq(h1, "b", 1)
	e.lostConf(h3, "b", 1, t3)
	next := holder(3, 2)
	e.open(next)
	e.conf(next, "b", 1, t3, h1, t4)
	e.conf(h2, "b", 1, t3, h1, t4)
}

// confirmNoSession: Confirm by a holder with no live session (never opened, closed, replaced,
// or the record's own holder once replaced) returns tenure.ErrSessionLost and leaves the record
// as it is.
func confirmNoSession(e *env) {
	h1, h2, h3, h5 := holder(1, 1), holder(2, 1), holder(3, 1), holder(5, 1)
	e.open(h1)
	t1 := e.acq(h1, "a", 0)
	r := e.rec(h1, "a", 0, h1, t1)

	e.lostConf(h5, "a", 0, t1)
	e.open(h3)
	e.closeSession(h3)
	e.lostConf(h3, "a", 0, t1)
	e.open(h2)
	e.open(holder(2, 2))
	e.lostConf(h2, "a", 0, t1)
	e.same(r, e.rec(h1, "a", 0, h1, t1))

	e.open(holder(1, 2))
	e.lostConf(h1, "a", 0, t1)
	e.same(r, e.rec(h3, "a", 0, h1, t1))
}
