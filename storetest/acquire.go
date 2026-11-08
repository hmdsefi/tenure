// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest

import (
	"fmt"
	"math"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// The Acquire cases check when a shard is granted and when it is refused. The Term cases check
// the terms grants carry: core.NextTerm of the record's term and the store's wall time, so they
// grow with every grant of a shard, follow the wall clock when it is ahead and the previous term
// when it is behind, and start again from the wall clock once the record is lost.

// acquireFresh: a shard with no record is granted at a non-zero term, and the record then names
// the holder at that term, with a version, through either client.
func acquireFresh(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.rec(h1, "a", 0, "", 0)

	t1 := e.acq(h1, "a", 0)
	r := e.rec(h2, "a", 0, h1, t1)
	if r.Version == 0 {
		e.t.Fatalf("Holdings(%q) = a record %s, nil, want a record of shard 0 with Version above 0, nil",
			"a", fmtHolding(r))
	}
	e.rec(h1, "a", 0, h1, t1)
	e.conf(h1, "a", 0, t1, h1, t1)
}

// acquireHeld: a shard held by a live holder is refused to another with a *tenure.HeldError
// that names the holder and reads "tenure: held by <holder>, retry after <RetryPeriod>", and the
// refusal leaves the record as it is.
func acquireHeld(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	t1 := e.acq(h1, "a", 0)
	r := e.rec(h2, "a", 0, h1, t1)

	e.held(h2, "a", 0, h1)
	e.held(h3, "a", 0, h1)
	want := fmt.Sprintf("tenure: held by %s, retry after %v", h1, e.timingOf(h2).RetryPeriod)
	term, err := e.client(h2).Acquire(e.ctx, h2, "a", 0)
	if term != 0 || err == nil || err.Error() != want {
		e.t.Fatalf("Acquire(%q, %q, %d) = %d, %s, want 0, %s", h2, "a", 0, term, errText(err), want)
	}
	e.same(r, e.rec(h1, "a", 0, h1, t1))
	e.conf(h1, "a", 0, t1, h1, t1)
}

// acquireReacquire: the holder of a shard is granted it again at a larger term, and the record
// moves to that term with a larger version.
func acquireReacquire(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	r1 := e.rec(h2, "a", 0, h1, t1)

	t2 := e.acq(h1, "a", 0)
	r2 := e.rec(h2, "a", 0, h1, t2)
	e.ver(r1, r2)
	t3 := e.acq(h1, "a", 0)
	e.ver(r2, e.rec(h2, "a", 0, h1, t3))
	e.held(h2, "a", 0, h1)
}

// acquireAfterRelease: a released shard is granted at once to another holder, and then back to
// the first, each time at a larger term.
func acquireAfterRelease(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	e.release(h1, "a", 0, t1)

	e.rec(h2, "a", 0, "", t1)
	t2 := e.acq(h2, "a", 0)
	e.rec(h1, "a", 0, h2, t2)
	e.held(h1, "a", 0, h2)
	e.release(h2, "a", 0, t2)

	e.rec(h1, "a", 0, "", t2)
	t3 := e.acq(h1, "a", 0)
	e.rec(h2, "a", 0, h1, t3)
}

// acquireIndependent: records are kept per space and shard: a holder of one shard keeps no other
// holder from the same shard number in another space or another shard of the same space, and
// releasing one leaves the others as they are.
func acquireIndependent(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	ta0 := e.acq(h1, "a", 0)
	tb0 := e.acq(h2, "b", 0)
	ta1 := e.acq(h3, "a", 1)
	tc0 := e.acq(h1, "c", 0)

	e.rec(h2, "a", 0, h1, ta0)
	rb0 := e.rec(h1, "b", 0, h2, tb0)
	ra1 := e.rec(h2, "a", 1, h3, ta1)
	rc0 := e.rec(h2, "c", 0, h1, tc0)
	e.held(h2, "a", 0, h1)
	e.held(h1, "b", 0, h2)
	e.held(h1, "a", 1, h3)

	e.release(h1, "a", 0, ta0)
	e.rec(h2, "a", 0, "", ta0)
	e.same(rb0, e.rec(h1, "b", 0, h2, tb0))
	e.same(ra1, e.rec(h2, "a", 1, h3, ta1))
	e.same(rc0, e.rec(h2, "c", 0, h1, tc0))
	e.acq(h2, "a", 0)
}

// termWallAhead: with the wall clock ahead of a shard's term, a grant takes the wall time in
// Unix milliseconds as its term.
func termWallAhead(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)

	w := wallOf(t1).Add(time.Hour)
	at := e.setWall(w)
	lo, hi := e.wallTerms(t1, w, at)
	t2 := e.acqIn(h1, "a", 0, lo, hi)
	e.rec(h2, "a", 0, h1, t2)
	e.release(h1, "a", 0, t2)

	e.rec(h2, "a", 0, "", t2)
	w = wallOf(t2).Add(24 * time.Hour)
	at = e.setWall(w)
	lo, hi = e.wallTerms(t2, w, at)
	t3 := e.acqIn(h2, "a", 0, lo, hi)
	e.rec(h1, "a", 0, h2, t3)
}

// termWallBehind: with the wall clock behind a shard's term, also before 1970, a grant takes the
// previous term plus 1, so terms still grow.
func termWallBehind(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)

	e.setWall(wallOf(t1).Add(-time.Hour))
	e.acqIn(h1, "a", 0, t1+1, t1+1)
	e.acqIn(h1, "a", 0, t1+2, t1+2)
	e.release(h1, "a", 0, t1+2)
	e.rec(h2, "a", 0, "", t1+2)
	e.acqIn(h2, "a", 0, t1+3, t1+3)
	e.rec(h1, "a", 0, h2, t1+3)

	e.setWall(time.Unix(-3600, 0))
	e.acqIn(h2, "a", 0, t1+4, t1+4)
	e.rec(h1, "a", 0, h2, t1+4)
}

// termLostRecord: once a shard's record is lost, it reads as absent, and the next grant takes a
// new record at a term from the wall clock, above the lost term since the wall clock has moved
// on, with a version above the lost record's.
func termLostRecord(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	r1 := e.rec(h2, "a", 0, h1, t1)

	e.f.DeleteRecord("a", 0)
	e.rec(h1, "a", 0, "", 0)
	e.rec(h2, "a", 0, "", 0)
	e.conf(h1, "a", 0, t1, "", 0)
	e.elapse(e.f.Timing.RetryPeriod, h1, h2)

	t2 := e.acq(h2, "a", 0)
	e.ver(r1, e.rec(h1, "a", 0, h2, t2))
	e.release(h1, "a", 0, t1)
	e.rec(h1, "a", 0, h2, t2)
	e.held(h1, "a", 0, h2)
}

// termLostRecordWallBehind: once a shard's record is lost, the next grant starts again from the
// wall clock alone, so with the wall clock behind the lost term the new term is below it; later
// grants grow from there.
func termLostRecordWallBehind(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "b", 3)

	w := wallOf(t1).Add(-time.Hour)
	at := e.setWall(w)
	e.f.DeleteRecord("b", 3)
	e.rec(h1, "b", 3, "", 0)
	e.rec(h2, "b", 3, "", 0)
	lo, hi := e.wallTerms(0, w, at)
	t2 := e.acqIn(h2, "b", 3, lo, hi)
	e.rec(h1, "b", 3, h2, t2)

	t3 := e.acq(h2, "b", 3)
	e.rec(h1, "b", 3, h2, t3)
}

// termSequence: a shard passes through every kind of grant (a first grant, a re-grant, after a
// release, after CloseSession, after expiry and after a replaced session) and each grant's term
// is above all earlier ones. When the fixture can set the wall clock, the sequence goes on with
// the wall clock an hour behind, and the terms still grow.
func termSequence(e *env) {
	h1, h2, h3, h5 := holder(1, 1), holder(2, 1), holder(3, 1), holder(5, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	e.open(h5)
	e.acq(h1, "a", 0)
	t := e.acq(h1, "a", 0)
	e.release(h1, "a", 0, t)
	e.rec(h2, "a", 0, "", t)
	t = e.acq(h2, "a", 0)
	e.rec(h1, "a", 0, h2, t)

	e.closeSession(h2)
	e.rec(h3, "a", 0, "", t)
	t = e.acq(h3, "a", 0)
	e.rec(h1, "a", 0, h3, t)

	e.held(h5, "a", 0, h3)
	e.expire(h1, h5)
	t = e.acq(h5, "a", 0)
	e.rec(h1, "a", 0, h5, t)

	e.open(holder(5, 2))
	t = e.acq(h1, "a", 0)
	e.rec(h1, "a", 0, h1, t)

	if e.f.SetWall == nil {
		return
	}
	e.setWall(wallOf(t).Add(-time.Hour))
	t = e.acq(h1, "a", 0)
	e.release(h1, "a", 0, t)
	h4 := holder(4, 1)
	e.open(h4)
	e.rec(h4, "a", 0, "", t)
	t = e.acq(h4, "a", 0)
	e.rec(h1, "a", 0, h4, t)
}

// setWall sets the store's wall clock to w and returns the real time just before, from which the
// store's wall clock runs on in real time.
func (e *env) setWall(w time.Time) time.Time {
	e.t.Helper()
	at := time.Now()
	e.f.SetWall(w)
	return at
}

// wallTerms returns the bounds of the term a grant over prev gets after the store's wall clock
// was set to w at real time at. In virtual time the wall clock stays at w, so the term is exact;
// in real time it has run on since at.
func (e *env) wallTerms(prev core.Term, w, at time.Time) (lo, hi core.Term) {
	lo = core.NextTerm(prev, w)
	if e.f.Advance != nil {
		return lo, lo
	}
	return lo, core.NextTerm(prev, w.Add(time.Since(at)))
}

// wallOf returns the wall time whose Unix milliseconds are t.
func wallOf(t core.Term) time.Time {
	return time.UnixMilli(int64(min(t, math.MaxInt64)))
}
