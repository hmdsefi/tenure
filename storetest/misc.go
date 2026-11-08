// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest

import (
	"context"
	"errors"
	"fmt"

	"github.com/hmdsefi/tenure"
)

// Context/Canceled checks that every method returns the context's error when its context is
// already done, and Args/Invalid that every method rejects the arguments the contract rules out
// before it looks at sessions or records. Neither kind of call changes anything.

// contextCanceled: every method of Store, and of tenure.SlotStore and tenure.Watcher when the
// store implements them, called through each client with a canceled context, returns an error
// that is context.Canceled and changes nothing: records, sessions, slots and watches are as
// before.
func contextCanceled(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	slots, watches := e.slotStores(), e.watchStores()
	e.open(h1)
	e.open(h2)
	t0 := e.acq(h1, "a", 0)
	if slots {
		e.setSlots(2)
		e.claim(h1, 0)
	}
	r0 := e.rec(h2, "a", 0, h1, t0)
	var w *watch
	if watches {
		w = e.watch(e.ctx)
	}

	done, cancel := context.WithCancel(e.ctx)
	cancel()
	tm := e.f.Timing
	check := func(call, got string, err error) {
		e.t.Helper()
		if !errors.Is(err, context.Canceled) {
			e.t.Fatalf("%s = %s%s, want %s", call, got, errText(err), context.Canceled)
		}
	}
	const ctx = "a canceled context"
	for _, c := range e.clients() {
		check(fmt.Sprintf("KeepAlive(%s, %q, %v)", ctx, h3, tm), "", c.KeepAlive(done, h3, tm))
		check(fmt.Sprintf("KeepAlive(%s, %q, %v)", ctx, h1, tm), "", c.KeepAlive(done, h1, tm))
		for _, shard := range []int{1, 0} {
			term, err := c.Acquire(done, h1, "a", shard)
			check(fmt.Sprintf("Acquire(%s, %q, %q, %d)", ctx, h1, "a", shard), fmt.Sprintf("%d, ", term), err)
		}
		check(fmt.Sprintf("Release(%s, %q, %q, %d, %d)", ctx, h1, "a", 0, t0), "", c.Release(done, h1, "a", 0, t0))
		got, err := c.Confirm(done, h1, "a", 0, t0)
		check(fmt.Sprintf("Confirm(%s, %q, %q, %d, %d)", ctx, h1, "a", 0, t0), fmtHolding(got)+", ", err)
		check(fmt.Sprintf("CloseSession(%s, %q)", ctx, h1), "", c.CloseSession(done, h1))
		hs, err := c.Holdings(done, "a")
		check(fmt.Sprintf("Holdings(%s, %q)", ctx, "a"), fmtHoldings(hs, fmtHolding)+", ", err)
		if ss, ok := c.(tenure.SlotStore); slots && ok {
			slot, err := ss.ClaimSlot(done, h2)
			check(fmt.Sprintf("ClaimSlot(%s, %q)", ctx, h2), fmt.Sprintf("%d, ", slot), err)
			check(fmt.Sprintf("ReleaseSlot(%s, %q, %d)", ctx, h1, 0), "", ss.ReleaseSlot(done, h1, 0))
			count, holders, err := ss.Slots(done)
			check(fmt.Sprintf("Slots(%s)", ctx), fmt.Sprintf("%d, %q, ", count, holders), err)
			check(fmt.Sprintf("SetSlots(%s, %d)", ctx, 1), "", ss.SetSlots(done, 1))
		}
		if wt, ok := c.(tenure.Watcher); watches && ok {
			_, err := wt.WatchHolders(done)
			check(fmt.Sprintf("WatchHolders(%s)", ctx), "", err)
		}
	}

	e.same(r0, e.rec(h2, "a", 0, h1, t0))
	e.rec(h2, "a", 1, "", 0)
	e.held(h2, "a", 0, h1)
	e.conf(h1, "a", 0, t0, h1, t0)
	e.open(h1)
	e.lostAcq(h3, "a", 2)
	if slots {
		e.slots(h2, h1, "")
		e.claim(h2, 1)
		e.claim(h1, 0)
	}
	if watches {
		t := e.acq(h2, "b", 0)
		w.next(tenure.Holding{Space: "b", Shard: 0, Holder: h2, Term: t})
	}
}

// argsInvalid: every method, called through each client with an argument the contract rules
// out (an empty holder, a holder with a zero byte, an empty space, a space with '/' or a zero
// byte, a negative shard, term 0, a negative slot or a negative count), returns an error that is
// none of tenure.ErrSessionLost, tenure.ErrNoSlot and *tenure.HeldError, also for a holder with
// no session, and changes nothing.
func argsInvalid(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	slots, watches := e.slotStores(), e.watchStores()
	e.open(h1)
	e.open(h2)
	t0 := e.acq(h1, "a", 0)
	if slots {
		e.setSlots(2)
		e.claim(h1, 0)
	}
	r0 := e.rec(h2, "a", 0, h1, t0)
	var w *watch
	if watches {
		w = e.watch(e.ctx)
	}

	tm := e.f.Timing
	badHolders := []string{"", h1[:2] + "\x00" + h1[2:]}
	badSpaces := []string{"", "a/0", "a\x00"}
	// h1 has a live session and the record of shard 0; h3 has no session.
	for _, c := range e.clients() {
		ss, _ := c.(tenure.SlotStore)
		for _, h := range badHolders {
			e.invalid(fmt.Sprintf("KeepAlive(%q, %v)", h, tm), "", c.KeepAlive(e.ctx, h, tm))
			term, err := c.Acquire(e.ctx, h, "a", 1)
			e.invalid(fmt.Sprintf("Acquire(%q, %q, %d)", h, "a", 1), fmt.Sprintf("%d, ", term), err)
			e.invalid(fmt.Sprintf("Release(%q, %q, %d, %d)", h, "a", 0, t0), "", c.Release(e.ctx, h, "a", 0, t0))
			got, err := c.Confirm(e.ctx, h, "a", 0, t0)
			e.invalid(fmt.Sprintf("Confirm(%q, %q, %d, %d)", h, "a", 0, t0), fmtHolding(got)+", ", err)
			e.invalid(fmt.Sprintf("CloseSession(%q)", h), "", c.CloseSession(e.ctx, h))
			if slots {
				slot, err := ss.ClaimSlot(e.ctx, h)
				e.invalid(fmt.Sprintf("ClaimSlot(%q)", h), fmt.Sprintf("%d, ", slot), err)
				e.invalid(fmt.Sprintf("ReleaseSlot(%q, %d)", h, 1), "", ss.ReleaseSlot(e.ctx, h, 1))
			}
		}
		for _, sp := range badSpaces {
			for _, h := range []string{h1, h3} {
				term, err := c.Acquire(e.ctx, h, sp, 1)
				e.invalid(fmt.Sprintf("Acquire(%q, %q, %d)", h, sp, 1), fmt.Sprintf("%d, ", term), err)
				got, err := c.Confirm(e.ctx, h, sp, 0, t0)
				e.invalid(fmt.Sprintf("Confirm(%q, %q, %d, %d)", h, sp, 0, t0), fmtHolding(got)+", ", err)
			}
			e.invalid(fmt.Sprintf("Release(%q, %q, %d, %d)", h1, sp, 0, t0), "", c.Release(e.ctx, h1, sp, 0, t0))
			hs, err := c.Holdings(e.ctx, sp)
			e.invalid(fmt.Sprintf("Holdings(%q)", sp), fmtHoldings(hs, fmtHolding)+", ", err)
		}
		for _, h := range []string{h1, h3} {
			term, err := c.Acquire(e.ctx, h, "a", -1)
			e.invalid(fmt.Sprintf("Acquire(%q, %q, %d)", h, "a", -1), fmt.Sprintf("%d, ", term), err)
			got, err := c.Confirm(e.ctx, h, "a", -1, t0)
			e.invalid(fmt.Sprintf("Confirm(%q, %q, %d, %d)", h, "a", -1, t0), fmtHolding(got)+", ", err)
			got, err = c.Confirm(e.ctx, h, "a", 0, 0)
			e.invalid(fmt.Sprintf("Confirm(%q, %q, %d, %d)", h, "a", 0, 0), fmtHolding(got)+", ", err)
		}
		e.invalid(fmt.Sprintf("Release(%q, %q, %d, %d)", h1, "a", -1, t0), "", c.Release(e.ctx, h1, "a", -1, t0))
		e.invalid(fmt.Sprintf("Release(%q, %q, %d, %d)", h1, "a", 0, 0), "", c.Release(e.ctx, h1, "a", 0, 0))
		if slots {
			e.invalid(fmt.Sprintf("ReleaseSlot(%q, %d)", h1, -1), "", ss.ReleaseSlot(e.ctx, h1, -1))
			e.invalid(fmt.Sprintf("SetSlots(%d)", -1), "", ss.SetSlots(e.ctx, -1))
		}
	}

	e.same(r0, e.rec(h2, "a", 0, h1, t0))
	e.rec(h2, "a", 1, "", 0)
	e.held(h2, "a", 0, h1)
	e.conf(h1, "a", 0, t0, h1, t0)
	e.open(h1)
	e.lostAcq(h3, "a", 2)
	if slots {
		e.slots(h2, h1, "")
		e.claim(h2, 1)
		e.claim(h1, 0)
	}
	if watches {
		t := e.acq(h2, "b", 0)
		w.next(tenure.Holding{Space: "b", Shard: 0, Holder: h2, Term: t})
	}
}

// clients returns Store and, when the fixture has one, the peer.
func (e *env) clients() []tenure.Store {
	if e.peer == nil {
		return []tenure.Store{e.f.Store}
	}
	return []tenure.Store{e.f.Store, e.peer}
}

// invalid checks that call, rejected for one of its arguments, returned an error that is none of
// tenure.ErrSessionLost, tenure.ErrNoSlot and *tenure.HeldError. got is the call's other results,
// each followed by ", ", for the failure message.
func (e *env) invalid(call, got string, err error) {
	e.t.Helper()
	var he *tenure.HeldError
	if err == nil || errors.Is(err, tenure.ErrSessionLost) || errors.Is(err, tenure.ErrNoSlot) ||
		errors.As(err, &he) {
		e.t.Fatalf("%s = %s%s, want an error for the invalid argument", call, got, errText(err))
	}
}
