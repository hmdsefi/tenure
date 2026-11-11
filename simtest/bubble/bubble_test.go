// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bubble

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// The tests of the real runtime (SIM-700): three real nodes on one memstore, each in its own
// synctest bubble, checking invariants only (SIM-710): g1, g3, g5 with no delay and no drift,
// and no overlap of two holders' passing Checks of an unfenced shard. Each test ends with every
// shard owned by its assignee.

// settle is how long after the last disturbance a test runs before it checks that the group
// converged and g5 held: 3·LeaseDuration, which is longer than the bound of g5.
const settle = 3 * leaseDuration

// disturbAt is when each test disturbs the group: well after it converged.
const disturbAt = 10 * time.Second

// TestBubbleConverge starts the group and checks that it converges within 3·LeaseDuration.
func TestBubbleConverge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		h.startAll()
		h.sleepUntil(settle)
		if err := h.converged(); err != nil {
			t.Error(err)
		}
		h.finish(0)
	})
}

// TestBubbleCrash crashes m2 at 10 s and restarts it at 20 s: m1 and m3 take its shards over
// while it is down.
func TestBubbleCrash(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		h.startAll()
		m2 := h.member("m2")

		h.sleepUntil(disturbAt)
		h.mustConverge("before the crash")
		if m2.owned() == 0 {
			t.Fatal("m2 owns no shard before the crash")
		}
		crashAt := h.elapsed()
		m2.crash()

		// While m2 is down, m1 and m3 take its shards over within the bound.
		h.sleepUntil(2 * disturbAt)
		if err := h.converged(); err != nil {
			t.Errorf("while m2 is down: %v", err)
		}
		if err := h.liveness(crashAt, h.bound()); err != nil {
			t.Errorf("while m2 is down: %v", err)
		}
		m2.heal()
		if err := m2.start(context.Background()); err != nil {
			t.Fatalf("restart m2: %v", err)
		}
		r := h.elapsed()

		h.sleepUntil(r + settle)
		if err := h.converged(); err != nil {
			t.Error(err)
		}
		h.finish(r)
	})
}

// TestBubbleStorePartition cuts m3 off from the store at 10 s for 2·LeaseDuration and heals it:
// the store ends m3's session, and m3 comes back under a new holder.
func TestBubbleStorePartition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		h.startAll()
		m3 := h.member("m3")

		h.sleepUntil(disturbAt)
		h.mustConverge("before the cut")
		if m3.owned() == 0 {
			t.Fatal("m3 owns no shard before the cut")
		}
		before := m3.inc.node.Table().Holder
		m3.cut()
		h.sleepUntil(disturbAt + 2*leaseDuration)
		m3.heal()
		r := h.elapsed()

		h.sleepUntil(r + settle)
		if err := h.converged(); err != nil {
			t.Error(err)
		}
		// The cut outlasted the lease, so the store ended m3's session and m3 came back under a
		// new holder.
		if after := m3.inc.node.Table().Holder; after == before {
			t.Errorf("m3's holder is still %s after a cut of 2·LeaseDuration", after)
		}
		h.finish(r)
	})
}

// TestBubbleDrain drains m1 at 10 s and closes it: its shards move to m2 and m3.
func TestBubbleDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		h.startAll()
		m1 := h.member("m1")

		h.sleepUntil(disturbAt)
		h.mustConverge("before the drain")
		if m1.owned() == 0 {
			t.Fatal("m1 owns no shard before the drain")
		}
		if err := m1.drain(); err != nil {
			t.Fatalf("drain m1: %v", err)
		}
		r := h.elapsed()

		h.sleepUntil(r + settle)
		if err := h.converged(); err != nil {
			t.Error(err)
		}
		h.finish(r)
	})
}

// TestBubbleSlotRollout runs the group with 3 slots and replaces each member once, 5 s apart:
// the old incarnation drains and closes, and a new one starts and claims the slot it left.
func TestBubbleSlotRollout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{slots: 3})
		h.startAll()
		h.sleepUntil(disturbAt)
		h.mustConverge("before the rollout")

		var r time.Duration
		for i, m := range h.members {
			h.sleepUntil(disturbAt + time.Duration(i)*5*time.Second)
			old := m.inc.node.Table()
			slot := old.Slot
			if slot < 0 {
				t.Fatalf("%s holds no slot before its replacement", m.id)
			}
			if err := m.drain(); err != nil {
				t.Fatalf("drain %s: %v", m.id, err)
			}
			if err := m.start(context.Background()); err != nil {
				t.Fatalf("restart %s: %v", m.id, err)
			}
			r = h.elapsed()
			h.sleepUntil(r + 2*retryPeriod)
			if got := m.inc.node.Table(); got.Slot != slot || got.Holder == old.Holder {
				t.Errorf("%s's new incarnation %s holds slot %d, want a new holder (not %s) in its old slot %d",
					m.id, got.Holder, got.Slot, old.Holder, slot)
			}
		}

		h.sleepUntil(r + settle)
		if err := h.converged(); err != nil {
			t.Error(err)
		}
		h.finish(r)
	})
}
