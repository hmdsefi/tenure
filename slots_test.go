// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of slots: seeding in Start, slot data merged into the views, the slot refresher,
// slot actions and Route over slot keys. Steps carrying an AT-RUN number were written from the
// issue's text.

// holder02 is the holder of member n2 whose incarnation is the bytes 0 to 15.
const holder02 = "n2/000102030405060708090a0b0c0d0e0f"

// withoutRefreshes returns calls, in Seq order, without the Slots calls of the slot refresher:
// those that start after the first KeepAlive, which opens the session in Start.
func withoutRefreshes(calls []call) []call {
	calls = callsInOrder(calls)
	ka := slices.IndexFunc(calls, func(c call) bool { return c.Method == "KeepAlive" })
	if ka < 0 {
		return calls
	}
	out := slices.Clone(calls[:ka+1])
	for _, c := range calls[ka+1:] {
		if c.Method != "Slots" {
			out = append(out, c)
		}
	}
	return out
}

// slotConfig returns r's Config of n1 with Config.Slots slots.
func (r shardRig) slotConfig(slots int) tenure.Config {
	cfg := r.config()
	cfg.Slots = slots
	return cfg
}

// slotKeys returns the placement keys slot-0 … slot-(k-1).
func slotKeys(k int) []string {
	keys := make([]string, k)
	for i := range keys {
		keys[i] = "slot-" + strconv.Itoa(i)
	}
	return keys
}

// slotOwner returns the slot each shard of orders is placed on over k slots, by core.Balanced.
func slotOwner(k int) map[int]int {
	out := make(map[int]int)
	for key, shards := range bal("orders", 8, slotKeys(k)...) {
		s, _ := strconv.Atoi(strings.TrimPrefix(key, "slot-"))
		for _, shard := range shards {
			out[shard] = s
		}
	}
	return out
}

// shardsOf returns the shards of orders placed on slot s over k slots, ascending.
func shardsOf(k, s int) []int {
	return bal("orders", 8, slotKeys(k)...)["slot-"+strconv.Itoa(s)]
}

// slotted returns "<id>:<slot>" for each member of v that holds a slot, in ID order.
func slotted(v core.View) []string {
	var out []string
	for _, m := range v.Members {
		if m.Slot >= 0 {
			out = append(out, m.ID+":"+strconv.Itoa(m.Slot))
		}
	}
	return out
}

// wantSlotRoute fails t unless n routes the key of shard to member id with Slot slot and with
// addrOf(id).
func wantSlotRoute(t *testing.T, n *tenure.Node, shard int, id string, slot int) {
	t.Helper()
	m, err := n.Route("orders", keyOf(shard))
	if err != nil || m.ID != id || m.Addr != addrOf(id) || m.Slot != slot {
		t.Errorf("Route(orders/%d) = %+v, %v, want %s (%s) with Slot %d", shard, m, err, id, addrOf(id), slot)
	}
}

// wantNoMember fails t unless Route of shard's key fails with "tenure: no members:
// orders/<shard>: <detail>", matching ErrNoMembers, and Local names no route.
func wantNoMember(t *testing.T, n *tenure.Node, shard int, detail string) {
	t.Helper()
	key := keyOf(shard)
	want := fmt.Sprintf("tenure: no members: orders/%d: %s", shard, detail)
	m, err := n.Route("orders", key)
	if err == nil || err.Error() != want || !errors.Is(err, tenure.ErrNoMembers) || m.ID != "" {
		t.Errorf("Route(orders/%d) = %+v, %v, want %q matching ErrNoMembers", shard, m, err, want)
	}
	sh, err := n.Local("orders", key)
	if sh != nil {
		t.Errorf("Local(orders/%d) = %v, want nil", shard, sh)
	}
	wantNotOwner(t, err, "orders", shard, "")
}

// wantSlots fails t unless the memstore's slots are count and holders.
func wantSlots(t *testing.T, r shardRig, count int, holders []string) {
	t.Helper()
	c, h, err := r.ms.Slots(context.Background())
	if err != nil || c != count || !slices.Equal(h, holders) {
		t.Errorf("memstore Slots() = %d, %q, %v, want %d, %q", c, h, err, count, holders)
	}
}

// running returns the shards of orders whose latest Run in l has not returned, ascending.
func running(l *runLog) []int {
	var ids []int
	for _, c := range l.Calls() {
		if !c.Done {
			ids = append(ids, c.Shard.ID)
		}
	}
	slices.Sort(ids)
	return ids
}

// TestATRUN07SeedSlots checks Start's slot count (RUN-230): (a) a count of 0 in the store is
// seeded with SetSlots(Config.Slots) and read again before KeepAlive; (b) a positive stored count
// is kept, also when Config.Slots differs, and Route works over its slots; (c) the member claims
// a slot and runs that slot's shards, and the shards of the vacant slots once SlotGrace has
// passed.
func TestATRUN07SeedSlots(t *testing.T) {
	t.Run("a: seeded", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			n := start(t, context.Background(), r.slotConfig(3))

			calls := withoutRefreshes(r.fs.Calls("Slots", "SetSlots", "KeepAlive"))
			if got, want := callMethods(calls), []string{"Slots", "SetSlots", "Slots", "KeepAlive"}; !slices.Equal(got, want) {
				t.Errorf("store calls of Start = %v, want %v", got, want)
			}
			if ss := r.fs.Calls("SetSlots"); len(ss) != 1 || ss[0].Args[0] != 3 || ss[0].Err != nil {
				t.Errorf("SetSlots calls = %v, want SetSlots(3) once", ss)
			}
			if sl := r.fs.Calls("Slots"); len(sl) < 2 || resultAt[int](sl[0].Result, 0) != 0 || resultAt[int](sl[1].Result, 0) != 3 {
				t.Errorf("Slots calls = %v, want a count of 0 and then 3", sl)
			}
			if v := published(t, n); v.Slots != 3 || len(v.SlotHolders) != 3 {
				t.Errorf("published view has %d slots (%q), want 3", v.Slots, v.SlotHolders)
			}
			closeNode(t, n)
			wantSlots(t, r, 3, []string{"", "", ""})
			check()
		})
	})

	for _, configured := range []int{3, 2, 0} {
		t.Run("b: stored count kept, Config.Slots "+strconv.Itoa(configured), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				r := newShardRig(t)
				if err := r.ms.SetSlots(context.Background(), 3); err != nil {
					t.Fatal(err)
				}
				n := start(t, context.Background(), r.slotConfig(configured))
				synctest.Wait()

				calls := withoutRefreshes(r.fs.Calls("Slots", "SetSlots", "KeepAlive"))
				if got, want := callMethods(calls), []string{"Slots", "KeepAlive"}; !slices.Equal(got, want) {
					t.Errorf("store calls of Start = %v, want %v", got, want)
				}
				if ss := r.fs.Calls("SetSlots"); len(ss) != 0 {
					t.Errorf("SetSlots calls = %v, want none", ss)
				}
				v := published(t, n)
				if v.Slots != 3 || !slices.Equal(v.SlotHolders, []string{holder01, "", ""}) {
					t.Errorf("published view has slots %d %q, want 3 with n1 on slot 0", v.Slots, v.SlotHolders)
				}
				for _, s := range allShards {
					if slot := slotOwner(3)[s]; slot == 0 {
						wantSlotRoute(t, n, s, "n1", 0)
					} else {
						wantNoMember(t, n, s, "slot-"+strconv.Itoa(slot)+" is vacant")
					}
				}
				closeNode(t, n)
				wantSlots(t, r, 3, []string{"", "", ""})
				check()
			})
		})
	}

	t.Run("c: slots used", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			t0 := time.Now()
			n := start(t, context.Background(), r.slotConfig(3))
			synctest.Wait()

			cs := r.fs.Calls("ClaimSlot")
			if len(cs) != 1 || !cs[0].Start.Equal(t0) || resultAt[int](cs[0].Result, 0) != 0 || cs[0].Err != nil {
				t.Errorf("ClaimSlot calls = %v, want one at T0 that returns slot 0", cs)
			}
			if got, want := slotted(published(t, n)), []string{"n1:0"}; !slices.Equal(got, want) {
				t.Errorf("members with a slot = %v, want %v", got, want)
			}
			mine := shardsOf(3, 0)
			if got := running(r.runs); !slices.Equal(got, mine) {
				t.Errorf("runs %v, want the shards of slot-0 %v", got, mine)
			}

			sleepUntil(t0.Add(tenure.DefaultSlotGrace - time.Millisecond))
			if got := running(r.runs); !slices.Equal(got, mine) {
				t.Errorf("runs %v just before SlotGrace, want %v", got, mine)
			}
			sleepUntil(t0.Add(tenure.DefaultSlotGrace))
			synctest.Wait()
			if got := running(r.runs); !slices.Equal(got, allShards) {
				t.Errorf("runs %v at SlotGrace, want every shard", got)
			}
			for _, a := range r.fs.Calls("Acquire") {
				want := t0.Add(tenure.DefaultSlotGrace)
				if slices.Contains(mine, argAt[int](a.Args, 2)) {
					want = t0
				}
				if !a.Start.Equal(want) || a.Err != nil {
					t.Errorf("%v at T0+%v, want T0+%v", a, a.Start.Sub(t0), want.Sub(t0))
				}
			}
			for _, s := range allShards {
				wantSlotRoute(t, n, s, "n1", 0)
			}
			closeNode(t, n)
			check()
		})
	})
}

// TestATRUN21MergedSlots checks that the slot data is merged into every view: with n2 holding
// the only slot, n1's published views, the views its machine steps and its node resolver's
// members show [n2] with Slot 0, and Route sends every shard to n2.
func TestATRUN21MergedSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		runs2 := &runLog{}
		cfg2 := nodeConfig("n2", r.g.Absent("n2"), newRecStore(r.ms), orders(runs2.Run))
		cfg2.Slots = 1
		n2 := start(t, context.Background(), cfg2)
		synctest.Wait()

		var log stepLog
		n1 := start(t, log.ctx(context.Background()), r.slotConfig(1))
		res := n1.Resolver("")
		synctest.Wait()

		if got, want := slotted(published(t, n1)), []string{"n2:0"}; !slices.Equal(got, want) {
			t.Errorf("n1's published view: members with a slot = %v, want %v", got, want)
		}
		if v := published(t, n1); !slices.Equal(v.SlotHolders, []string{holder02}) || !slices.Equal(memberIDs(v), []string{"n1", "n2"}) {
			t.Errorf("n1's published view = %+v, want n1 and n2 with slot 0 held by %s", v, holder02)
		}
		recs, _ := log.records()
		views := 0
		for _, rec := range recs {
			vc, ok := rec.Input.(core.ViewChanged)
			if !ok {
				continue
			}
			views++
			if got, want := slotted(vc.View), []string{"n2:0"}; !slices.Equal(got, want) || vc.View.Slots != 1 {
				t.Errorf("n1 stepped a view with %d slots, members with a slot %v, want 1 slot and %v", vc.View.Slots, got, want)
			}
		}
		if views == 0 {
			t.Error("n1 stepped no view")
		}
		var withSlot []string
		for _, m := range res.Members() {
			if m.Slot >= 0 {
				withSlot = append(withSlot, m.ID+":"+strconv.Itoa(m.Slot))
			}
		}
		if want := []string{"n2:0"}; !slices.Equal(withSlot, want) {
			t.Errorf("n1's resolver: members with a slot = %v, want %v", withSlot, want)
		}
		for _, s := range allShards {
			wantSlotRoute(t, n1, s, "n2", 0)
			wantSlotRoute(t, n2, s, "n2", 0)
		}
		if cs := r.fs.Calls("ClaimSlot"); len(cs) == 0 || !errors.Is(cs[0].Err, tenure.ErrNoSlot) {
			t.Errorf("n1's ClaimSlot calls = %v, want ErrNoSlot", cs)
		}
		if got := running(r.runs); len(got) != 0 {
			t.Errorf("n1 runs %v, want none: it holds no slot", got)
		}
		closeNode(t, n1)
		closeNode(t, n2)
		check()
	})
}

// TestATRUN22SlotCountChange changes the stored slot count behind the node's back: the
// refresher's next read, RetryPeriod after the previous one, brings the new count into a view
// the machine steps, whose Version grows while the membership's does not, and Route follows it.
func TestATRUN22SlotCountChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		var log stepLog
		t0 := time.Now()
		n := start(t, log.ctx(context.Background()), r.slotConfig(1))
		synctest.Wait()
		member := r.g.View().Version
		v0 := published(t, n)
		if v0.Slots != 1 || !slices.Equal(v0.SlotHolders, []string{holder01}) {
			t.Fatalf("published view has slots %d %q, want n1 on the only slot", v0.Slots, v0.SlotHolders)
		}

		set := func(at time.Duration, count int) {
			sleepUntil(t0.Add(at))
			if err := r.ms.SetSlots(context.Background(), count); err != nil {
				t.Fatal(err)
			}
		}
		wantView := func(slots int, version uint64) {
			t.Helper()
			if v := published(t, n); v.Slots != slots || v.Version != version {
				t.Errorf("at T0+%v: published view has %d slots, Version %d; want %d, %d",
					time.Since(t0), v.Slots, v.Version, slots, version)
			}
		}

		set(time.Second, 2)
		sleepUntil(t0.Add(2*time.Second - time.Millisecond))
		wantView(1, v0.Version)
		sleepUntil(t0.Add(2 * time.Second))
		synctest.Wait()
		wantView(2, v0.Version+1)
		for _, s := range allShards {
			if slot := slotOwner(2)[s]; slot == 0 {
				wantSlotRoute(t, n, s, "n1", 0)
			} else {
				wantNoMember(t, n, s, "slot-1 is vacant")
			}
		}

		set(3*time.Second, 1)
		sleepUntil(t0.Add(4 * time.Second))
		synctest.Wait()
		wantView(1, v0.Version+2)
		for _, s := range allShards {
			wantSlotRoute(t, n, s, "n1", 0)
		}
		if got := r.g.View().Version; got != member {
			t.Errorf("membership Version %d, want %d unchanged", got, member)
		}

		recs, _ := log.records()
		var counts []string
		for _, rec := range recs {
			if vc, ok := rec.Input.(core.ViewChanged); ok && rec.Now > 0 {
				counts = append(counts, fmt.Sprintf("%v:%d", rec.Now, vc.View.Slots))
			}
		}
		if want := []string{"2s:2", "4s:1"}; !slices.Equal(counts, want) {
			t.Errorf("views stepped after T0 (reading:slots) = %v, want %v", counts, want)
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN27SlotRefresh checks the refresher around slot results (RUN-210, MCH O8): a failed
// claim is followed by a Slots read at once; a read that started before the claim result and
// returns after it is dropped and read again; and from the claim result on, Route never calls
// slot-0 vacant and no view the machine steps shows it vacant.
func TestATRUN27SlotRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		boom := errors.New("boom")
		r.fs.Fail("ClaimSlot", boom)
		r.fs.Delay("ClaimSlot", 1500*time.Millisecond)

		var (
			node    atomic.Pointer[tenure.Node]
			mu      sync.Mutex
			claimed bool // a SlotClaimed has been stepped
			held    bool // and no SlotReleased since
			bad     []string
		)
		hook := tenure.WithStepHook(context.Background(), func(rec tenure.StepRecord) {
			mu.Lock()
			defer mu.Unlock()
			switch rec.Input.(type) {
			case core.SlotClaimed:
				claimed, held = true, true
			case core.SlotReleased: // Close's drain gives the slot back
				held = false
			}
			if !held {
				return
			}
			if vc, ok := rec.Input.(core.ViewChanged); ok && vc.View.SlotHolders[0] != holder01 {
				bad = append(bad, fmt.Sprintf("view at %v with slot 0 held by %q", rec.Now, vc.View.SlotHolders[0]))
			}
			n := node.Load()
			if n == nil {
				return
			}
			for _, s := range allShards {
				if _, err := n.Route("orders", keyOf(s)); err != nil && strings.Contains(err.Error(), "slot-0 is vacant") {
					bad = append(bad, fmt.Sprintf("Route(orders/%d) after the step at %v: %v", s, rec.Now, err))
				}
			}
		})
		t0 := time.Now()
		n := start(t, hook, r.slotConfig(1))
		node.Store(n)

		// The first claim goes out at T0 and fails at T0+1.5s; the next goes out at T0+2s.
		sleepUntil(t0.Add(time.Second))
		r.fs.Fail("ClaimSlot", nil)
		r.fs.Delay("ClaimSlot", 0)
		r.fs.Answer("Slots", func(context.Context, []any) ([]any, error) {
			return []any{1, []string{""}}, nil // the slot as it was before the claim
		})
		release := r.fs.Block("Slots")

		sleepUntil(t0.Add(2 * time.Second))
		synctest.Wait()
		cs := r.fs.Calls("ClaimSlot")
		if len(cs) != 2 || !errors.Is(cs[0].Err, boom) || cs[0].End.Sub(t0) != 1500*time.Millisecond ||
			cs[1].Start.Sub(t0) != 2*time.Second || cs[1].Err != nil || resultAt[int](cs[1].Result, 0) != 0 {
			t.Fatalf("ClaimSlot calls = %v, want a failure ending at T0+1.5s and slot 0 at T0+2s", cs)
		}
		sl := r.fs.Calls("Slots")
		after := slices.IndexFunc(sl, func(c call) bool { return c.Seq > cs[0].EndSeq })
		if after < 0 || sl[after].Start.Sub(t0) != 1500*time.Millisecond {
			t.Errorf("Slots calls at %v, want one at T0+1.5s right after the failed claim", startsAt(sl, t0))
		}
		for _, s := range allShards {
			wantSlotRoute(t, n, s, "n1", 0)
		}

		// The read held since T0+1.5s returns the old slot at T0+3s: it is dropped, and the read
		// made again at once takes 0.5 s, so a view from the old read would be stepped first.
		sleepUntil(t0.Add(3 * time.Second))
		r.fs.Answer("Slots", nil)
		r.fs.Delay("Slots", 500*time.Millisecond)
		release()
		synctest.Wait()
		if v := published(t, n); !slices.Equal(v.SlotHolders, []string{""}) {
			t.Errorf("published view at T0+3s has slot holders %q, want the view before the claim", v.SlotHolders)
		}
		sleepUntil(t0.Add(3500 * time.Millisecond))
		r.fs.Delay("Slots", 0)
		synctest.Wait()
		sl = r.fs.Calls("Slots")
		if got, want := startsAt(sl, t0), secs(0, 0, 1.5, 3); !slices.Equal(got, want) {
			t.Errorf("Slots calls at %v, want %v: Start's two, one after the failed claim, one again at once", got, want)
		} else if sl[2].End.Sub(t0) != 3*time.Second || sl[3].Seq < sl[2].EndSeq || sl[3].End.Sub(t0) != 3500*time.Millisecond {
			t.Errorf("Slots calls %v, %v: want the second to start once the first returned at T0+3s", sl[2], sl[3])
		}
		if v := published(t, n); !slices.Equal(v.SlotHolders, []string{holder01}) {
			t.Errorf("published view has slot holders %q, want %s", v.SlotHolders, holder01)
		}
		for _, s := range allShards {
			wantSlotRoute(t, n, s, "n1", 0)
		}

		sleepUntil(t0.Add(5*time.Second - time.Millisecond))
		if got := len(r.fs.Calls("Slots")); got != 4 {
			t.Errorf("%d Slots calls before T0+5s, want 4", got)
		}
		sleepUntil(t0.Add(5 * time.Second))
		synctest.Wait()
		if sl := r.fs.Calls("Slots"); len(sl) != 5 || sl[4].Start.Sub(t0) != 5*time.Second {
			t.Errorf("Slots calls at %v, want the next RetryPeriod after T0+3s", startsAt(sl, t0))
		}
		closeNode(t, n)
		mu.Lock()
		if !claimed || len(bad) > 0 {
			t.Errorf("claim stepped %t; after it: %q", claimed, bad)
		}
		mu.Unlock()
		check()
	})
}

// TestATRUN52NoMembersSlots checks Route's no-member errors with slots: a vacant slot, a slot
// whose holder's member is not in the view, and no placement keys once the only slot has been
// vacant for SlotGrace.
func TestATRUN52NoMembersSlots(t *testing.T) {
	t.Run("vacant", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			n := start(t, context.Background(), r.slotConfig(2))
			synctest.Wait()
			for _, s := range allShards {
				if slotOwner(2)[s] == 0 {
					wantSlotRoute(t, n, s, "n1", 0)
				} else {
					wantNoMember(t, n, s, "slot-1 is vacant")
				}
			}
			closeNode(t, n)
			check()
		})
	})

	t.Run("holder not in the view", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			ctx := context.Background()
			if err := r.ms.SetSlots(ctx, 2); err != nil {
				t.Fatal(err)
			}
			out := newOutside(t, r.ms, core.DefaultTiming)
			if s, err := r.ms.ClaimSlot(ctx, outsideHolder); s != 0 || err != nil {
				t.Fatalf("ClaimSlot(%s) = %d, %v, want 0", outsideHolder, s, err)
			}
			n := start(t, ctx, r.slotConfig(2))
			synctest.Wait()
			if v := published(t, n); !slices.Equal(v.SlotHolders, []string{outsideHolder, holder01}) {
				t.Errorf("published view has slot holders %q, want x/1 and n1", v.SlotHolders)
			}
			for _, s := range allShards {
				if slotOwner(2)[s] == 1 {
					wantSlotRoute(t, n, s, "n1", 1)
				} else {
					wantNoMember(t, n, s, "x is not in the view")
				}
			}
			closeNode(t, n)
			out.Stop()
			check()
		})
	})

	t.Run("no placement keys", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			r.fs.Fail("ClaimSlot", tenure.ErrNoSlot) // the member stays a standby
			t0 := time.Now()
			n := start(t, context.Background(), r.slotConfig(1))
			sleepUntil(t0.Add(tenure.DefaultSlotGrace - time.Millisecond))
			for _, s := range allShards {
				wantNoMember(t, n, s, "slot-0 is vacant")
			}
			sleepUntil(t0.Add(tenure.DefaultSlotGrace))
			synctest.Wait()
			for _, s := range allShards {
				wantNoMember(t, n, s, "no placement keys")
			}
			if got := running(r.runs); len(got) != 0 {
				t.Errorf("runs %v, want none", got)
			}
			closeNode(t, n)
			check()
		})
	})
}

// TestATRUN54RouteBySlot runs n1 and n2 on two slots: each claims one, both route every key to
// the member whose holder holds the key's slot, and each runs exactly its slot's shards.
func TestATRUN54RouteBySlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		cfg1 := r.slotConfig(2)
		t0 := time.Now()
		n1 := start(t, context.Background(), cfg1)
		sleepUntil(t0.Add(time.Second))
		runs2 := &runLog{}
		fs2 := newRecStore(r.ms)
		cfg2 := nodeConfig("n2", r.g.Absent("n2"), fs2, orders(runs2.Run))
		cfg2.Slots = 2
		n2 := start(t, context.Background(), cfg2)
		synctest.Wait()

		if cs := fs2.Calls("ClaimSlot"); len(cs) != 1 || cs[0].Start.Sub(t0) != time.Second || resultAt[int](cs[0].Result, 0) != 1 {
			t.Errorf("n2's ClaimSlot calls = %v, want slot 1 at T0+1s", cs)
		}
		if ss := fs2.Calls("SetSlots"); len(ss) != 0 {
			t.Errorf("n2's SetSlots calls = %v, want none: n1 seeded the count", ss)
		}
		// n1 learns of n2's claim from its next slot read, RetryPeriod after its claim's at T0.
		for _, s := range shardsOf(2, 1) {
			wantNoMember(t, n1, s, "slot-1 is vacant")
		}
		sleepUntil(t0.Add(2 * time.Second))
		synctest.Wait()
		ids := map[int]string{0: "n1", 1: "n2"}
		for _, n := range []*tenure.Node{n1, n2} {
			if got, want := slotted(published(t, n)), []string{"n1:0", "n2:1"}; !slices.Equal(got, want) {
				t.Errorf("members with a slot = %v, want %v", got, want)
			}
			for i := range 100 {
				key := "key-" + strconv.Itoa(i)
				slot := slotOwner(2)[core.KeyShard(key, 8)]
				m, err := n.Route("orders", key)
				if err != nil || m.ID != ids[slot] || m.Slot != slot {
					t.Errorf("Route(%q) = %+v, %v, want %s on slot %d", key, m, err, ids[slot], slot)
				}
			}
		}
		if got, want := running(r.runs), shardsOf(2, 0); !slices.Equal(got, want) {
			t.Errorf("n1 runs %v, want slot-0's %v", got, want)
		}
		if got, want := running(runs2), shardsOf(2, 1); !slices.Equal(got, want) {
			t.Errorf("n2 runs %v, want slot-1's %v", got, want)
		}
		for _, s := range allShards {
			owner, other, runs := n1, n2, r.runs
			if slotOwner(2)[s] == 1 {
				owner, other, runs = n2, n1, runs2
			}
			if sh, err := owner.Local("orders", keyOf(s)); err != nil || sh != lastOf(runs.Of("orders", s)).Shard {
				t.Errorf("owner's Local(orders/%d) = %v, %v, want the Shard of its Run", s, sh, err)
			}
			_, err := other.Local("orders", keyOf(s))
			wantNotOwner(t, err, "orders", s, ids[slotOwner(2)[s]])
			key := keyOf(s)
			for _, n := range []*tenure.Node{n1, n2} {
				if got := testing.AllocsPerRun(100, func() { _, err = n.Route("orders", key) }); got != 0 || err != nil {
					t.Errorf("Route(orders/%d) by slot allocates %v times (%v), want 0", s, got, err)
				}
			}
		}
		closeNode(t, n1)
		closeNode(t, n2)
		check()
	})
}

// TestATRUN70DrainWithSlot drains a member that holds a slot: it releases every shard, then
// ReleaseSlot(0) once the last Release has returned, then CloseSession once ReleaseSlot has.
func TestATRUN70DrainWithSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		t0 := time.Now()
		n := start(t, context.Background(), r.slotConfig(1))
		synctest.Wait()
		if got := running(r.runs); !slices.Equal(got, allShards) {
			t.Fatalf("runs %v, want every shard on slot 0", got)
		}

		sleepUntil(t0.Add(time.Second))
		r.fs.Delay("Release", 500*time.Millisecond)
		r.fs.Delay("ReleaseSlot", 500*time.Millisecond)
		if err := n.Drain(context.Background()); err != nil {
			t.Fatalf("Drain() = %v, want nil", err)
		}
		if d := time.Since(t0); d != 2*time.Second {
			t.Errorf("Drain returned at T0+%v, want T0+2s", d)
		}

		rel := r.fs.Calls("Release")
		rs := r.fs.Calls("ReleaseSlot")
		cl := r.fs.Calls("CloseSession")
		if len(rel) != 8 || len(rs) != 1 || len(cl) != 1 {
			t.Fatalf("Release %v, ReleaseSlot %v, CloseSession %v; want 8, 1, 1", rel, rs, cl)
		}
		var last call
		for _, c := range rel {
			if c.Start.Sub(t0) != time.Second || c.Err != nil {
				t.Errorf("%v at T0+%v, want T0+1s", c, c.Start.Sub(t0))
			}
			if c.EndSeq > last.EndSeq {
				last = c
			}
		}
		if rs[0].Args[0] != holder01 || rs[0].Args[1] != 0 || rs[0].Err != nil ||
			rs[0].Seq < last.EndSeq || rs[0].Start.Sub(t0) != 1500*time.Millisecond {
			t.Errorf("%v at T0+%v (seq %d), want ReleaseSlot(%s, 0) at T0+1.5s after %v (end seq %d)",
				rs[0], rs[0].Start.Sub(t0), rs[0].Seq, holder01, last, last.EndSeq)
		}
		if cl[0].Seq < rs[0].EndSeq || cl[0].Start.Sub(t0) != 2*time.Second || cl[0].Err != nil {
			t.Errorf("%v at T0+%v, want CloseSession at T0+2s after %v", cl[0], cl[0].Start.Sub(t0), rs[0])
		}
		wantSlots(t, r, 1, []string{""})
		closeNode(t, n)
		check()
	})
}
