// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest

import (
	"errors"
	"fmt"

	"github.com/hmdsefi/tenure"
)

// The Slots cases check tenure.SlotStore: ClaimSlot claims the lowest free slot below the count
// or returns the caller's own, a slot is free once its holder's session has ended, ReleaseSlot is
// a compare-and-swap on the holder, and SetSlots changes the count without touching slot
// records, so a slot at or above the count stays its holder's.

// slotsClaim: ClaimSlot claims the lowest free slot below the count, also when a slot below it
// was freed after higher ones were claimed.
func slotsClaim(e *env) {
	h1, h2, h3, h4 := holder(1, 1), holder(2, 1), holder(3, 1), holder(4, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	e.open(h4)
	e.setSlots(4)
	e.slots(h1, "", "", "", "")
	e.claim(h1, 0)
	e.slots(h2, h1, "", "", "")
	e.claim(h2, 1)
	e.slots(h3, h1, h2, "", "")
	e.claim(h3, 2)

	e.releaseSlot(h1, 0)
	e.slots(h4, "", h2, h3, "")
	e.claim(h4, 0)
	e.slots(h1, h4, h2, h3, "")
}

// slotsOwn: ClaimSlot by the holder of a slot below the count returns that slot and changes
// nothing.
func slotsOwn(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.setSlots(3)
	e.claim(h1, 0)
	e.slots(h2, h1, "", "")
	e.claim(h2, 1)
	e.claim(h2, 1)
	e.slots(h2, h1, h2, "")
	e.claim(h1, 0)
	e.claim(h1, 0)
	e.slots(h2, h1, h2, "")
	e.slots(h1, h1, h2, "")
}

// slotsFull: with every slot below the count held, ClaimSlot returns -1 and tenure.ErrNoSlot and
// changes nothing.
func slotsFull(e *env) {
	h1, h2, h3, h4 := holder(1, 1), holder(2, 1), holder(3, 1), holder(4, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	e.open(h4)
	e.setSlots(2)
	e.claim(h1, 0)
	e.slots(h2, h1, "")
	e.claim(h2, 1)
	e.slots(h4, h1, h2)
	e.noSlot(h4)
	e.claim(h2, 1)
	e.slots(h3, h1, h2)
	e.noSlot(h3)
	e.noSlot(h3)
	e.claim(h1, 0)
	e.slots(h4, h1, h2)
	e.slots(h3, h1, h2)
}

// slotsNoSession: ClaimSlot by a holder with no live session (never opened, closed, or replaced)
// returns tenure.ErrSessionLost and claims nothing.
func slotsNoSession(e *env) {
	h1, h2, h3, h5 := holder(1, 1), holder(2, 1), holder(3, 1), holder(5, 1)
	e.open(h2)
	e.open(h3)
	e.open(h5)
	e.closeSession(h3)
	e.open(holder(5, 2))
	e.setSlots(2)
	e.slots(h2, "", "")
	e.claim(h2, 0)
	e.slots(h1, h2, "")
	e.lostSlot(h1)
	e.lostSlot(h3)
	e.lostSlot(h5)
	e.slots(h2, h2, "")
	e.slots(h1, h2, "")
}

// slotsExpired: once the session of a slot's holder expires, Slots shows the slot as free and
// another holder claims it, while the expired holder can claim nothing.
func slotsExpired(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.setSlots(1)
	e.claim(h1, 0)
	e.slots(h2, h1)
	e.noSlot(h2)

	e.expire(h2)
	e.slots(h2, "")
	e.claim(h2, 0)
	e.slots(h1, h2)
	e.lostSlot(h1)
}

// slotsReplaced: a new incarnation of a slot's holder frees the slot at once, for another holder
// to claim; the replaced incarnation can claim nothing.
func slotsReplaced(e *env) {
	h1, h2, next := holder(1, 1), holder(2, 1), holder(1, 2)
	e.open(h1)
	e.open(h2)
	e.setSlots(1)
	e.claim(h1, 0)
	e.slots(h2, h1)
	e.noSlot(h2)

	e.open(next)
	e.slots(h2, "")
	e.claim(h2, 0)
	e.lostSlot(h1)
	e.slots(next, h2)
	e.noSlot(next)
}

// slotsRelease: ReleaseSlot by a slot's holder frees the slot at once, for another holder or the
// same one to claim.
func slotsRelease(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.setSlots(2)
	e.claim(h1, 0)
	e.releaseSlot(h1, 0)
	e.slots(h2, "", "")
	e.claim(h2, 0)
	e.slots(h1, h2, "")
	e.claim(h1, 1)
	e.releaseSlot(h1, 1)
	e.slots(h2, h2, "")
	e.releaseSlot(h2, 0)
	e.slots(h1, "", "")
	e.claim(h1, 0)
}

// slotsReleaseOther: ReleaseSlot of another holder's slot, also by another incarnation of the
// holder's member, or of a free slot returns nil and changes nothing.
func slotsReleaseOther(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	e.setSlots(3)
	e.claim(h1, 0)
	e.slots(h2, h1, "", "")
	e.releaseSlot(h2, 0)
	e.releaseSlot(h2, 1)
	e.releaseSlot(h3, 0)
	e.releaseSlot(holder(1, 2), 0)
	e.releaseSlot(h1, 2)
	e.slots(h3, h1, "", "")
	e.claim(h1, 0)
	e.claim(h3, 1)
	e.slots(h2, h1, h3, "")
	e.claim(h2, 2)
}

// slotsCount: after SetSlots, Slots returns that count and one entry per slot below it, raised
// or lowered.
func slotsCount(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.setSlots(3)
	e.slots(h1, "", "", "")
	e.slots(h2, "", "", "")
	e.claim(h1, 0)
	e.setSlots(5)
	e.slots(h2, h1, "", "", "", "")
	e.claim(h2, 1)
	e.setSlots(8)
	e.slots(h1, h1, h2, "", "", "", "", "", "")
	e.setSlots(2)
	e.slots(h2, h1, h2)
	e.slots(h1, h1, h2)
}

// slotsZero: with a count of 0, Slots returns 0, nil, nil and ClaimSlot returns -1 and
// tenure.ErrNoSlot, also for the holder of a slot the count no longer covers.
func slotsZero(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	e.setSlots(0)
	e.noSlots(h1)
	e.noSlots(h2)
	e.noSlot(h1)
	e.noSlot(h2)

	e.setSlots(1)
	e.claim(h1, 0)
	e.setSlots(0)
	e.noSlots(h2)
	e.noSlot(h2)
	e.noSlots(h1)
	e.noSlot(h1)
}

// slotsRetired: lowering the count below a held slot retires it: Slots omits it, and its holder's
// ClaimSlot returns -1 and tenure.ErrNoSlot, also when a slot below the count is free, which
// another holder then claims.
func slotsRetired(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	e.setSlots(3)
	e.claim(h1, 0)
	e.slots(h2, h1, "", "")
	e.claim(h2, 1)
	e.slots(h3, h1, h2, "")
	e.claim(h3, 2)
	e.releaseSlot(h1, 0)

	e.setSlots(2)
	e.slots(h2, "", h2)
	e.claim(h2, 1)
	e.slots(h3, "", h2)
	e.noSlot(h3)

	e.setSlots(1)
	e.slots(h2, "")
	e.noSlot(h2)
	e.slots(h3, "")
	e.noSlot(h3)
	e.claim(h1, 0)
	e.slots(h2, h1)
}

// slotsGrow: raising the count back over a retired slot returns it to its holder: Slots shows it
// again and ClaimSlot returns it as the holder's own.
func slotsGrow(e *env) {
	h1, h2, h3 := holder(1, 1), holder(2, 1), holder(3, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	e.setSlots(3)
	e.claim(h1, 0)
	e.slots(h2, h1, "", "")
	e.claim(h2, 1)
	e.slots(h3, h1, h2, "")
	e.claim(h3, 2)

	e.setSlots(1)
	e.slots(h2, h1)
	e.noSlot(h2)
	e.noSlot(h3)

	e.setSlots(3)
	e.slots(h2, h1, h2, h3)
	e.claim(h2, 1)
	e.claim(h3, 2)
	e.claim(h1, 0)
	e.slots(h2, h1, h2, h3)
}

// slotsCloseSession: CloseSession frees every slot of the holder, a retired one too, for other
// holders to claim, and the closed holder can claim nothing.
func slotsCloseSession(e *env) {
	h1, h2, h3, h4, h5 := holder(1, 1), holder(2, 1), holder(3, 1), holder(4, 1), holder(5, 1)
	e.open(h1)
	e.open(h2)
	e.open(h3)
	e.open(h4)
	e.open(h5)
	e.setSlots(3)
	e.slots(h2, "", "", "")
	e.claim(h2, 0)
	e.slots(h4, h2, "", "")
	e.claim(h4, 1)
	e.slots(h1, h2, h4, "")
	e.claim(h1, 2)

	e.setSlots(2)
	e.closeSession(h1)
	e.lostSlot(h1)
	e.setSlots(3)
	e.slots(h3, h2, h4, "")
	e.claim(h3, 2)

	e.closeSession(h2)
	e.slots(h5, "", h4, h3)
	e.claim(h5, 0)
	e.slots(h4, h5, h4, h3)
	e.lostSlot(h2)
}

// lostSlot checks that h's ClaimSlot returns tenure.ErrSessionLost.
func (e *env) lostSlot(h string) {
	e.t.Helper()
	got, err := e.slotClient(h).ClaimSlot(e.ctx, h)
	if !errors.Is(err, tenure.ErrSessionLost) {
		e.t.Fatalf("ClaimSlot(%q) = %d, %s, want %s", h, got, errText(err), tenure.ErrSessionLost)
	}
}

// releaseSlot checks that h's ReleaseSlot of slot returns nil.
func (e *env) releaseSlot(h string, slot int) {
	e.t.Helper()
	if err := e.slotClient(h).ReleaseSlot(e.ctx, h, slot); err != nil {
		e.t.Fatalf("ReleaseSlot(%q, %d) = %s, want nil", h, slot, errText(err))
	}
}

// noSlots checks, through via's client, that Slots returns 0, nil, nil. The read is synced.
func (e *env) noSlots(via string) {
	e.t.Helper()
	e.sync(func() string {
		count, holders, err := e.slotClient(via).Slots(e.ctx)
		if err != nil || count != 0 || holders != nil {
			got := "nil"
			if holders != nil {
				got = fmt.Sprintf("%q", holders)
			}
			return fmt.Sprintf("Slots() = %d, %s, %s, want 0, nil, nil", count, got, errText(err))
		}
		return ""
	})
}
