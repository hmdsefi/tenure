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

// TestBubbleCrash crashes m2 at 10 s and restarts it at 20 s.
func TestBubbleCrash(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		h.startAll()
		m2 := h.member("m2")

		h.sleepUntil(disturbAt)
		m2.crash()
		h.sleepUntil(2 * disturbAt)
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

// TestBubbleStorePartition cuts m3 off from the store at 10 s for 2·LeaseDuration and heals it.
func TestBubbleStorePartition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		h.startAll()
		m3 := h.member("m3")

		h.sleepUntil(disturbAt)
		m3.cut()
		h.sleepUntil(disturbAt + 2*leaseDuration)
		m3.heal()
		r := h.elapsed()

		h.sleepUntil(r + settle)
		if err := h.converged(); err != nil {
			t.Error(err)
		}
		h.finish(r)
	})
}

// TestBubbleDrain drains m1 at 10 s and closes it: its shards move to m2 and m3.
func TestBubbleDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		h.startAll()

		h.sleepUntil(disturbAt)
		if err := h.member("m1").drain(); err != nil {
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
		if err := h.converged(); err != nil {
			t.Fatalf("before the rollout: %v", err)
		}

		var r time.Duration
		for i, m := range h.members {
			h.sleepUntil(disturbAt + time.Duration(i)*5*time.Second)
			slot := m.inc.node.Table().Slot
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
			if got := m.inc.node.Table().Slot; got != slot {
				t.Errorf("%s's new incarnation holds slot %d, want its old slot %d", m.id, got, slot)
			}
		}

		h.sleepUntil(r + settle)
		if err := h.converged(); err != nil {
			t.Error(err)
		}
		h.finish(r)
	})
}
