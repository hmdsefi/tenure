// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/hmdsefi/tenure"
)

// The Release cases check that Release is a compare-and-swap on holder and term: it clears the
// record that names the caller at the caller's term, whatever the caller's session, keeps the
// term, and leaves every other record, or none, as it is. CloseSession/Stale checks CloseSession
// of a session that has already ended, and the Holdings cases check how records are listed and
// that their versions grow store-wide.

// releaseMatch: Release by the holder at its term clears the record, keeps the term, takes a
// larger version, and frees the shard at once.
func releaseMatch(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	r := e.rec(h2, "a", 0, h1, t1)

	e.release(h1, "a", 0, t1)
	e.ver(r, e.rec(h2, "a", 0, "", t1))
	e.rec(h1, "a", 0, "", t1)
	t2 := e.acq(h2, "a", 0)
	e.rec(h1, "a", 0, h2, t2)
}

// releaseWrongTerm: Release by the holder at any term but the record's returns nil and leaves
// the record as it is, also at the holder's own earlier term.
func releaseWrongTerm(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	r := e.rec(h2, "a", 0, h1, t1)

	e.release(h1, "a", 0, t1+1)
	if t1 > 1 {
		e.release(h1, "a", 0, t1-1)
	}
	e.same(r, e.rec(h2, "a", 0, h1, t1))
	e.held(h2, "a", 0, h1)

	t2 := e.acq(h1, "a", 0)
	r = e.rec(h2, "a", 0, h1, t2)
	e.release(h1, "a", 0, t1)
	e.same(r, e.rec(h2, "a", 0, h1, t2))
	e.held(h2, "a", 0, h1)
}

// releaseWrongHolder: Release at the record's term by any holder but the record's returns nil
// and leaves the record as it is, also by another incarnation of the holder's member.
func releaseWrongHolder(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	r := e.rec(h2, "a", 0, h1, t1)

	e.release(h2, "a", 0, t1)
	e.release(h3, "a", 0, t1)
	e.release(holder(1, 2), "a", 0, t1)
	e.same(r, e.rec(h2, "a", 0, h1, t1))
	e.held(h2, "a", 0, h1)
}

// releaseLate: a Release that comes after another holder took the shard over returns nil and
// leaves the new holder's record as it is.
func releaseLate(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	e.rec(h2, "a", 0, h1, t1)
	e.held(h2, "a", 0, h1)

	e.expire(h2)
	t2 := e.acq(h2, "a", 0)
	r := e.rec(h1, "a", 0, h2, t2)
	e.release(h1, "a", 0, t1)
	e.same(r, e.rec(h1, "a", 0, h2, t2))
	h3 := holder(3, 1)
	e.open(h3)
	e.held(h3, "a", 0, h2)
}

// releaseAbsent: Release of a shard with no record returns nil and writes no record, also in a
// space with other records and, when the fixture can lose records, after the record was lost.
func releaseAbsent(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.release(h1, "a", 0, 1)
	e.rec(h2, "a", 0, "", 0)

	t1 := e.acq(h1, "a", 1)
	r := e.rec(h2, "a", 1, h1, t1)
	e.release(h1, "a", 0, t1)
	e.release(h1, "c", 1, t1)
	e.rec(h2, "a", 0, "", 0)
	e.rec(h2, "c", 1, "", 0)
	e.same(r, e.rec(h2, "a", 1, h1, t1))

	if e.f.DeleteRecord == nil {
		return
	}
	t2 := e.acq(h1, "a", 2)
	e.f.DeleteRecord("a", 2)
	e.rec(h2, "a", 2, "", 0)
	e.rec(h1, "a", 2, "", 0)
	e.release(h1, "a", 2, t2)
	e.rec(h2, "a", 2, "", 0)
}

// releaseTwice: a second Release of a record already released returns nil and leaves it as it
// is; so does a Release at the earlier term once the holder has the shard again.
func releaseTwice(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t1 := e.acq(h1, "a", 0)
	e.release(h1, "a", 0, t1)
	r := e.rec(h2, "a", 0, "", t1)

	e.release(h1, "a", 0, t1)
	e.same(r, e.rec(h2, "a", 0, "", t1))

	t2 := e.acq(h1, "a", 0)
	r = e.rec(h2, "a", 0, h1, t2)
	e.release(h1, "a", 0, t1)
	e.same(r, e.rec(h2, "a", 0, h1, t2))
}

// releaseEndedSession: Release does not look at the caller's session, so a holder whose session
// was replaced or has expired still clears its own record at its own term, keeping the term.
func releaseEndedSession(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	t0 := e.acq(h1, "a", 0)
	t1 := e.acq(h1, "a", 1)
	t2 := e.acq(h3, "a", 2)

	e.open(holder(1, 2))
	r := e.rec(h2, "a", 0, h1, t0)
	e.release(h1, "a", 0, t0)
	e.ver(r, e.rec(h2, "a", 0, "", t0))
	e.rec(h2, "a", 1, h1, t1)

	e.expire(h2)
	r = e.rec(h2, "a", 2, h3, t2)
	e.release(h3, "a", 2, t2)
	e.ver(r, e.rec(h2, "a", 2, "", t2))
	e.acq(h2, "a", 2)
}

// closeSessionStale: CloseSession of a session that has already ended clears the records that
// still name it, keeping their terms, and leaves the records other holders took over as they
// are. When the store keeps slots, the same holds for slots: the ended holder's slot, claimed
// by another holder, stays with that holder, and a live holder's slot is untouched.
func closeSessionStale(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	slots := e.slotStores()
	e.open(h1)
	e.open(h2)
	e.open(h3)
	t0 := e.acq(h1, "a", 0)
	ta1 := e.acq(h1, "a", 1)
	e.held(h3, "a", 1, h1)
	if slots {
		e.setSlots(2)
		e.claim(h1, 0)
		e.slots(h2, h1, "")
		e.claim(h2, 1)
		e.slots(h3, h1, h2)
		e.noSlot(h3)
	}
	e.rec(h2, "a", 1, h1, ta1)

	e.expire(h2, h3)
	t1 := e.acq(h3, "a", 1)
	if slots {
		e.claim(h3, 0)
		e.slots(h2, h3, h2)
		e.slots(h1, h3, h2)
	}
	r0 := e.rec(h2, "a", 0, h1, t0)
	r1 := e.rec(h2, "a", 1, h3, t1)

	e.closeSession(h1)
	c0 := e.rec(h2, "a", 0, "", t0)
	e.ver(r0, c0)
	r0 = c0
	e.same(r1, e.rec(h2, "a", 1, h3, t1))
	if slots {
		e.slots(h2, h3, h2)
	}
	e.lost(h1)

	e.closeSession(h1)
	e.same(r0, e.rec(h2, "a", 0, "", t0))
	e.same(r1, e.rec(h2, "a", 1, h3, t1))
	if slots {
		e.slots(h2, h3, h2)
	}
}

// holdingsOrder: Holdings lists every record of the space, also those with no holder, sorted by
// shard whatever the order of the grants, and no record of another space; the slice is the
// caller's own.
func holdingsOrder(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	t7 := e.acq(h1, "a", 7)
	t2 := e.acq(h2, "a", 2)
	t11 := e.acq(h1, "a", 11)
	t0 := e.acq(h2, "a", 0)
	t5 := e.acq(h1, "a", 5)
	tb := e.acq(h2, "b", 3)
	e.release(h2, "a", 2, t2)

	want := []tenure.Holding{
		{Space: "a", Shard: 0, Holder: h2, Term: t0},
		{Space: "a", Shard: 2, Holder: "", Term: t2},
		{Space: "a", Shard: 5, Holder: h1, Term: t5},
		{Space: "a", Shard: 7, Holder: h1, Term: t7},
		{Space: "a", Shard: 11, Holder: h1, Term: t11},
	}
	e.holdings(h1, "a", want)
	hs := e.holdings(h2, "a", want)
	e.holdings(h1, "b", []tenure.Holding{{Space: "b", Shard: 3, Holder: h2, Term: tb}})
	e.holdings(h2, "c", nil)

	hs[0].Holder, hs[1].Term = h1, 0
	slices.Reverse(hs)
	e.holdings(h2, "a", want)
}

// holdingsVersion: every grant and clear takes a version above that of every earlier write to
// any record of the store, across shards and spaces, and a refused Acquire or a Release that
// matches nothing leaves every version as it is. When the fixture can lose records, a grant after
// a lost record, of that shard or another, still takes a larger version.
func holdingsVersion(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	var top uint64
	grew := func(got tenure.Holding) tenure.Holding {
		e.t.Helper()
		e.ver(tenure.Holding{Version: top}, got)
		top = got.Version
		return got
	}

	ta0 := e.acq(h1, "a", 0)
	grew(e.rec(h2, "a", 0, h1, ta0))
	tb4 := e.acq(h2, "b", 4)
	rb4 := grew(e.rec(h1, "b", 4, h2, tb4))
	ta3 := e.acq(h1, "a", 3)
	grew(e.rec(h2, "a", 3, h1, ta3))
	ta0 = e.acq(h1, "a", 0)
	ra0 := grew(e.rec(h2, "a", 0, h1, ta0))
	e.release(h1, "a", 3, ta3)
	ra3 := grew(e.rec(h2, "a", 3, "", ta3))

	e.held(h2, "a", 0, h1)
	e.release(h2, "a", 0, ta0)
	e.release(h1, "b", 4, tb4)
	e.release(h1, "a", 3, ta3)
	e.same(ra0, e.rec(h2, "a", 0, h1, ta0))
	e.same(ra3, e.rec(h2, "a", 3, "", ta3))
	e.same(rb4, e.rec(h1, "b", 4, h2, tb4))

	e.closeSession(h2)
	grew(e.rec(h1, "b", 4, "", tb4))
	ta3 = e.acq(h1, "a", 3)
	grew(e.rec(h1, "a", 3, h1, ta3))

	if e.f.DeleteRecord == nil {
		return
	}
	e.f.DeleteRecord("a", 0)
	e.rec(h1, "a", 0, "", 0)
	ta0 = e.acqIn(h1, "a", 0, 1, exactly(math.MaxUint64))
	grew(e.rec(h1, "a", 0, h1, ta0))
	e.f.DeleteRecord("a", 3)
	tb4 = e.acq(h1, "b", 4)
	grew(e.rec(h1, "b", 4, h1, tb4))
}

// holdings checks, through via's client, that Holdings of space lists exactly the want records,
// in order, and returns the list. Versions are not compared. The read is synced.
func (e *env) holdings(via, space string, want []tenure.Holding) []tenure.Holding {
	e.t.Helper()
	var got []tenure.Holding
	e.sync(func() string {
		var err error
		got, err = e.client(via).Holdings(e.ctx, space)
		same := err == nil && len(got) == len(want)
		for i := 0; same && i < len(got); i++ {
			g := got[i]
			g.Version = 0
			same = g == want[i]
		}
		if !same {
			return fmt.Sprintf("Holdings(%q) = %s, %s, want %s, nil", space, fmtHoldings(got, fmtHolding),
				errText(err), fmtHoldings(want, fmtWant))
		}
		return ""
	})
	return got
}

// fmtHoldings formats a list of records with f.
func fmtHoldings(hs []tenure.Holding, f func(tenure.Holding) string) string {
	parts := make([]string, len(hs))
	for i, h := range hs {
		parts[i] = f(h)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// slotStores reports whether the store, and the peer when there is one, implement
// tenure.SlotStore.
func (e *env) slotStores() bool {
	_, ok := e.f.Store.(tenure.SlotStore)
	if e.peer != nil {
		_, peerOK := e.peer.(tenure.SlotStore)
		ok = ok && peerOK
	}
	return ok
}

// setSlots checks that SetSlots(count) through Store returns nil.
func (e *env) setSlots(count int) {
	e.t.Helper()
	ss, _ := e.f.Store.(tenure.SlotStore)
	if err := ss.SetSlots(e.ctx, count); err != nil {
		e.t.Fatalf("SetSlots(%d) = %s, want nil", count, errText(err))
	}
}

// claim checks that h's ClaimSlot returns slot.
func (e *env) claim(h string, slot int) {
	e.t.Helper()
	got, err := e.slotClient(h).ClaimSlot(e.ctx, h)
	if err != nil || got != slot {
		e.t.Fatalf("ClaimSlot(%q) = %d, %s, want %d, nil", h, got, errText(err), slot)
	}
}

// noSlot checks that h's ClaimSlot returns -1 and tenure.ErrNoSlot.
func (e *env) noSlot(h string) {
	e.t.Helper()
	got, err := e.slotClient(h).ClaimSlot(e.ctx, h)
	if got != -1 || !errors.Is(err, tenure.ErrNoSlot) {
		e.t.Fatalf("ClaimSlot(%q) = %d, %s, want -1, %s", h, got, errText(err), tenure.ErrNoSlot)
	}
}

// slots checks, through via's client, that Slots returns one entry per holder in want, the
// count being len(want). The read is synced.
func (e *env) slots(via string, want ...string) {
	e.t.Helper()
	e.sync(func() string {
		count, holders, err := e.slotClient(via).Slots(e.ctx)
		if err != nil || count != len(want) || !slices.Equal(holders, want) {
			return fmt.Sprintf("Slots() = %d, %q, %s, want %d, %q, nil", count, holders, errText(err),
				len(want), want)
		}
		return ""
	})
}

// slotClient returns the client h calls through as a tenure.SlotStore. Call it only when
// slotStores reports true.
func (e *env) slotClient(h string) tenure.SlotStore {
	ss, _ := e.client(h).(tenure.SlotStore)
	return ss
}
