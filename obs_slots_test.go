// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of the slot events of running nodes: slot.claimed, slot.released with its reasons,
// slot.standby, slot.count_changed and slot.count_mismatch (OBS-130). They use OBS §10's harness
// (obs_harness_test.go). The steps of the tests carrying an AT-OBS number were written from the
// issue's text.

// slotKinds are the slot kinds.
var slotKinds = []tenure.EventKind{
	tenure.EventSlotClaimed, tenure.EventSlotReleased, tenure.EventSlotStandby,
	tenure.EventSlotCountChanged, tenure.EventSlotCountMismatch,
}

// withSlots sets Config.Slots to k.
func withSlots(k int) func(*tenure.Config) {
	return func(cfg *tenure.Config) { cfg.Slots = k }
}

// slotEv returns the slot event of kind with slot and reason; Shard is -1, as for every event that
// names no shard.
func slotEv(kind tenure.EventKind, slot int, reason string) tenure.Event {
	return tenure.Event{Kind: kind, Shard: -1, Slot: slot, Reason: reason}
}

// countChanged returns slot.count_changed with count k.
func countChanged(k int) tenure.Event {
	return tenure.Event{Kind: tenure.EventSlotCountChanged, Shard: -1, Slot: -1, Count: k}
}

// standby is slot.standby.
var standby = slotEv(tenure.EventSlotStandby, -1, "")

// lastIndex returns the index of the last event of kind, or -1 when there is none.
func lastIndex(events []tenure.Event, kind tenure.EventKind) int {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind == kind {
			return i
		}
	}
	return -1
}

// wantSlotEvents fails t unless the slot events of n are want, in order.
func wantSlotEvents(t *testing.T, n *obsNode, events []tenure.Event, want ...tenure.Event) {
	t.Helper()
	if got := bareAll(ofKinds(events, slotKinds...)); !reflect.DeepEqual(got, want) {
		t.Errorf("%s: slot events =\n%+v\nwant\n%+v", n.ID, got, want)
	}
}

// wantCountAfterView fails t unless each slot.count_changed of events comes right after a
// view.changed.
func wantCountAfterView(t *testing.T, id string, events []tenure.Event) {
	t.Helper()
	for i, e := range events {
		if e.Kind == tenure.EventSlotCountChanged && (i == 0 || events[i-1].Kind != tenure.EventViewChanged) {
			t.Errorf("%s: slot.count_changed at %d does not follow a view.changed: %+v", id, i, events)
		}
	}
}

// TestATOBS10Slots checks three members with Config.Slots 2: slot.count_changed{Count: 2} on every
// node, right after its view.changed; slot.claimed{Slot: 0} on a and slot.claimed{Slot: 1} on b;
// one slot.standby on c, however many of its ClaimSlot calls find no slot. When a closes,
// slot.released{Slot: 0, Reason: "draining"} comes after a's shards are released, and c then
// claims slot 0.
func TestATOBS10Slots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		h := newObsHarness(t)
		a := h.Start("a", withSlots(2))
		synctest.Wait()
		b := h.Start("b", withSlots(2))
		synctest.Wait()
		c := h.Start("c", withSlots(2))
		time.Sleep(time.Minute)

		if got := len(c.FS.Calls("ClaimSlot")); got < 2 {
			t.Errorf("c: %d ClaimSlot calls in a minute, want several", got)
		}
		cBefore := c.RecentEvents()
		wantSlotEvents(t, c, cBefore, countChanged(2), standby)

		a.MustClose()
		time.Sleep(time.Minute)

		ae, be, ce := a.Events.Events(), b.RecentEvents(), c.RecentEvents()
		wantSlotEvents(t, a, ae, countChanged(2), slotEv(tenure.EventSlotClaimed, 0, ""),
			slotEv(tenure.EventSlotReleased, 0, "draining"))
		wantSlotEvents(t, b, be, countChanged(2), slotEv(tenure.EventSlotClaimed, 1, ""))
		wantSlotEvents(t, c, ce, countChanged(2), standby, slotEv(tenure.EventSlotClaimed, 0, ""))
		for _, n := range []struct {
			id     string
			events []tenure.Event
		}{{"a", ae}, {"b", be}, {"c", ce}} {
			wantCountAfterView(t, n.id, n.events)
		}

		rel := indexOf(ae, tenure.EventSlotReleased, -1)
		if last := lastIndex(ae, tenure.EventShardReleased); last < 0 || rel < last {
			t.Errorf("a: slot.released at %d, last shard.released at %d: want it after a's shards are released", rel, last)
		}
		if closed := indexOf(ae, tenure.EventSessionClosed, -1); rel < 0 || closed < rel {
			t.Errorf("a: slot.released at %d, session.closed at %d: want the slot released first", rel, closed)
		}
		if _, holders, err := h.ms.Slots(context.Background()); err != nil || len(holders) != 2 || holders[0] != c.Holder {
			t.Errorf("stored slot holders %q (err %v), want c's holder on slot 0", holders, err)
		}

		b.MustClose()
		c.MustClose()
		wantDelivery(t, a)
		wantDelivery(t, b)
		wantDelivery(t, c)
		check()
	})
}

// TestATOBS11SlotCountMismatch checks Start over a store that holds 3 slots: with Config.Slots 2,
// slot.count_mismatch{Count: 3, Configured: 2} comes before session.opened, and its log record is
// a WARN with the keys stored and configured; with Config.Slots 0 or 3 there is none.
func TestATOBS11SlotCountMismatch(t *testing.T) {
	for _, configured := range []int{2, 0, 3} {
		t.Run("Slots"+strconv.Itoa(configured), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				h := newObsHarness(t)
				if err := h.ms.SetSlots(context.Background(), 3); err != nil {
					t.Fatal(err)
				}
				a := h.Start("a", withSlots(configured))
				synctest.Wait()
				events := a.RecentEvents()
				mismatches := ofKinds(events, tenure.EventSlotCountMismatch)

				if configured != 2 {
					if len(mismatches) != 0 {
						t.Errorf("slot.count_mismatch events %+v, want none", mismatches)
					}
				} else {
					want := tenure.Event{Kind: tenure.EventSlotCountMismatch, Shard: -1, Slot: -1, Count: 3, Configured: 2}
					if got := bareAll(mismatches); !reflect.DeepEqual(got, []tenure.Event{want}) {
						t.Errorf("slot.count_mismatch events %+v, want [%+v]", got, want)
					}
					m, opened := indexOf(events, tenure.EventSlotCountMismatch, -1), indexOf(events, tenure.EventSessionOpened, -1)
					if m < 0 || opened < m {
						t.Errorf("slot.count_mismatch at %d, session.opened at %d: want the mismatch first", m, opened)
					}
					wantRec := `{"level":"WARN","msg":"tenure: slot count mismatch","member":"a","holder":"` + obsHolderA +
						`","stored":3,"configured":2}`
					if lines := a.Log.Lines(); m < 0 || m >= len(lines) || lines[m] != wantRec {
						t.Errorf("log records = %q, want %s at %d", lines, wantRec, m)
					}
				}
				if got := ofKinds(events, tenure.EventSlotCountChanged); len(got) != 1 || got[0].Count != 3 {
					t.Errorf("slot.count_changed events %+v, want one with Count 3: the node keeps the stored count", got)
				}

				a.MustClose()
				wantDelivery(t, a)
				check()
			})
		})
	}
}

// TestATOBS12RetiredAndLostSlots checks slot.released's reasons other than "draining": "retired"
// when SetSlots(1) retires the slot that b holds; and "lost", with shard.lost for the shards of the
// slot, within 2 s of another holder taking a's slot 0.
func TestATOBS12RetiredAndLostSlots(t *testing.T) {
	t.Run("Retired", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			h := newObsHarness(t)
			a := h.Start("a", withSlots(2))
			synctest.Wait()
			b := h.Start("b", withSlots(2))
			time.Sleep(time.Minute)

			if err := a.SetSlots(context.Background(), 1); err != nil {
				t.Fatalf("SetSlots(1) = %v, want nil", err)
			}
			time.Sleep(time.Minute)

			ae, be := a.RecentEvents(), b.RecentEvents()
			wantSlotEvents(t, a, ae, countChanged(2), slotEv(tenure.EventSlotClaimed, 0, ""), countChanged(1))
			wantSlotEvents(t, b, be, countChanged(2), slotEv(tenure.EventSlotClaimed, 1, ""), countChanged(1),
				slotEv(tenure.EventSlotReleased, 1, "retired"), standby)
			wantCountAfterView(t, "a", ae)
			wantCountAfterView(t, "b", be)
			if rs := b.FS.Calls("ReleaseSlot"); len(rs) == 0 || rs[0].Args[1] != 1 || rs[0].Err != nil {
				t.Errorf("b: ReleaseSlot calls = %v, want ReleaseSlot(1) first", rs)
			}

			b.MustClose()
			a.MustClose()
			wantDelivery(t, a)
			wantDelivery(t, b)
			check()
		})
	})

	t.Run("Lost", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			h := newObsHarness(t)
			// One slot: slot 0 has both shards.
			a := h.Start("a", withSlots(1))
			synctest.Wait()
			if got := running(a.Runs); !slices.Equal(got, []int{0, 1}) {
				t.Fatalf("a runs %v, want both shards", got)
			}
			terms := make(map[int]core.Term)
			for shard := range 2 {
				terms[shard] = a.Runs.Of(obsSpace, shard)[0].Shard.Term
			}

			// Another holder takes slot 0: a's slot reads show it, and a's claims find no slot.
			other := "x/" + strings.Repeat("99", 16)
			a.FS.Answer("Slots", func(context.Context, []any) ([]any, error) {
				return []any{1, []string{other}}, nil
			})
			a.FS.Answer("ClaimSlot", func(context.Context, []any) ([]any, error) {
				return []any{-1}, tenure.ErrNoSlot
			})
			t0 := time.Now()
			time.Sleep(2 * time.Second)
			synctest.Wait()

			// The step that loses the slot sends a ClaimSlot at once, which finds no slot.
			events := a.RecentEvents()
			wantSlotEvents(t, a, events, countChanged(1), slotEv(tenure.EventSlotClaimed, 0, ""),
				slotEv(tenure.EventSlotReleased, 0, "lost"), standby)
			rel := indexOf(events, tenure.EventSlotReleased, -1)
			if rel >= 0 && events[rel].Time.Sub(t0) > 2*time.Second {
				t.Errorf("slot.released at T0+%v, want within 2s", events[rel].Time.Sub(t0))
			}
			for shard := range 2 {
				i := indexOf(events, tenure.EventShardLost, shard)
				if i < 0 {
					t.Errorf("no shard.lost for s/%d in %+v", shard, events)
					continue
				}
				if want := shardEv(tenure.EventShardLost, shard, terms[shard], ""); bare(events[i]) != want {
					t.Errorf("shard.lost = %+v, want %+v", bare(events[i]), want)
				}
				if d := events[i].Time.Sub(t0); d > 2*time.Second {
					t.Errorf("shard.lost for s/%d at T0+%v, want within 2s", shard, d)
				}
				if i > rel {
					t.Errorf("shard.lost for s/%d at %d after slot.released at %d: want the step's actions first", shard, i, rel)
				}
			}

			// a waits for a slot as a standby: its later claims emit nothing more, and its runs
			// return and their shards are released.
			time.Sleep(10 * time.Second)
			if n := len(a.FS.Calls("ClaimSlot")); n < 3 {
				t.Errorf("a: %d ClaimSlot calls, want a claim at Start and several after the loss", n)
			}
			events = a.RecentEvents()
			wantSlotEvents(t, a, events, countChanged(1), slotEv(tenure.EventSlotClaimed, 0, ""),
				slotEv(tenure.EventSlotReleased, 0, "lost"), standby)
			for shard := range 2 {
				released := shardEv(tenure.EventShardReleased, shard, terms[shard], "lost")
				if i := indexOf(events, tenure.EventShardReleased, shard); i < 0 || bare(events[i]) != released {
					t.Errorf("no shard.released{Reason: lost} for s/%d in %+v", shard, events)
				}
			}

			a.MustClose()
			wantDelivery(t, a)
			check()
		})
	})
}
