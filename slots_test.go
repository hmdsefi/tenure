// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"cmp"
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
	"github.com/hmdsefi/tenure/memstore"
)

// The tests of slots: seeding in Start, slot data merged into the views, the slot refresher,
// slot actions and Route over slot keys. Steps carrying an AT-RUN number were written from the
// issue's text.

// holderN2 is the holder of member n2 whose incarnation is the bytes 0 to 15.
const holderN2 = "n2/000102030405060708090a0b0c0d0e0f"

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
		if v := published(t, n1); !slices.Equal(v.SlotHolders, []string{holderN2}) || !slices.Equal(memberIDs(v), []string{"n1", "n2"}) {
			t.Errorf("n1's published view = %+v, want n1 and n2 with slot 0 held by %s", v, holderN2)
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

// wideOrders returns orders with 12 shards, whose ShardOf maps the key "<s>" to shard s.
func wideOrders(run func(context.Context, *tenure.Shard) error) tenure.Space {
	return tenure.Space{
		Name:   "orders",
		Shards: 12,
		ShardOf: func(key string) int {
			s, err := strconv.Atoi(key)
			if err != nil {
				return -1
			}
			return s
		},
		Run: run,
	}
}

// slotNode is one member of a test that runs several nodes on one memstore and one group.
type slotNode struct {
	id   string
	fs   *recStore
	runs *runLog
	log  *stepLog
	n    *tenure.Node
}

// startSlotNode starts member id on ms and g with Config.Slots slots and the space that space
// returns for its runLog, recording its store calls, its runs and its steps.
func startSlotNode(t *testing.T, ms *memstore.Store, g *group, id string, slots int,
	space func(func(context.Context, *tenure.Shard) error) tenure.Space,
) *slotNode {
	t.Helper()
	sn := &slotNode{id: id, fs: newRecStore(ms), runs: &runLog{}, log: &stepLog{}}
	cfg := nodeConfig(id, g.Absent(id), sn.fs, space(sn.runs.Run))
	cfg.Slots = slots
	sn.n = start(t, sn.log.ctx(context.Background()), cfg)
	return sn
}

// holderOf returns the holder of member id whose incarnation is the bytes 0 to 15.
func holderOf(id string) string { return id + strings.TrimPrefix(holder01, "n1") }

// heldSlot returns the slot that sn's machine holds after its latest step, -1 for none.
func (sn *slotNode) heldSlot() int {
	recs, _ := sn.log.records()
	if len(recs) == 0 {
		return -1
	}
	return recs[len(recs)-1].After.Slot
}

// claims returns the result of each ClaimSlot call of sn, in order: the slot, or the error.
func (sn *slotNode) claims() []string {
	var out []string
	for _, c := range sn.fs.Calls("ClaimSlot") {
		if c.Err != nil {
			out = append(out, c.Err.Error())
			continue
		}
		out = append(out, strconv.Itoa(resultAt[int](c.Result, 0)))
	}
	return out
}

// slotShards returns, for each of k slots, the shards of orders with shards shards that
// core.Balanced places on it, ascending.
func slotShards(shards, k int) [][]int {
	b := bal("orders", shards, slotKeys(k)...)
	out := make([][]int, k)
	for s := range out {
		out[s] = b["slot-"+strconv.Itoa(s)]
	}
	return out
}

// slotIndex returns the slot of k that core.Balanced places shard of orders with shards shards
// on.
func slotIndex(shard, shards, k int) int {
	for s, ids := range slotShards(shards, k) {
		if slices.Contains(ids, shard) {
			return s
		}
	}
	return -1
}

// runHistory returns, for each shard of orders, the ID of the node of each Run call of the shard
// in start order, and fails t when a Run of a shard, on any node, started before the one before
// it had returned.
func runHistory(t *testing.T, nodes ...*slotNode) map[int][]string {
	t.Helper()
	type run struct {
		id string
		c  runCall
	}
	byShard := make(map[int][]run)
	for _, sn := range nodes {
		for _, c := range sn.runs.Calls() {
			byShard[c.Shard.ID] = append(byShard[c.Shard.ID], run{sn.id, c})
		}
	}
	out := make(map[int][]string)
	for shard, runs := range byShard {
		slices.SortFunc(runs, func(a, b run) int { return cmp.Compare(a.c.Seq, b.c.Seq) })
		for i, r := range runs {
			if prev := runs[max(i-1, 0)]; i > 0 && (!prev.c.Done || r.c.Seq < prev.c.EndSeq) {
				t.Errorf("orders/%d: %s's %v started before %s's %v returned", shard, r.id, r.c, prev.id, prev.c)
			}
			out[shard] = append(out[shard], r.id)
		}
	}
	return out
}

// wantStoredSlots fails t unless ms holds one slot per entry of holders, held by that holder.
func wantStoredSlots(t *testing.T, ms *memstore.Store, holders []string) {
	t.Helper()
	c, h, err := ms.Slots(context.Background())
	if err != nil || c != len(holders) || !slices.Equal(h, holders) {
		t.Errorf("memstore Slots() = %d, %q, %v, want %d, %q", c, h, err, len(holders), holders)
	}
}

// TestATRUN23RetireSlot runs n1, n2 and n3 on slots 0, 1 and 2 over 12 shards and lowers the
// count to 2 through n1 (RUN-240, RUN-250): when SetSlots returns, n1 routes every key over
// slot-0 and slot-1; within 30 s, n3, whose slot was retired, runs nothing and holds no slot, and
// n1 and n2 run 6 shards each; no shard ever has two Runs at once.
func TestATRUN23RetireSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ms := memstore.New(nil)
		g := newGroup(t)
		t0 := time.Now()
		var nodes []*slotNode
		for i, id := range []string{"n1", "n2", "n3"} {
			sleepUntil(t0.Add(time.Duration(i) * time.Second))
			nodes = append(nodes, startSlotNode(t, ms, g, id, 3, wideOrders))
		}
		n1, n3 := nodes[0], nodes[2]
		sleepUntil(t0.Add(10 * time.Second))
		synctest.Wait()
		for s, sn := range nodes {
			if got, want := running(sn.runs), slotShards(12, 3)[s]; !slices.Equal(got, want) || sn.heldSlot() != s {
				t.Fatalf("%s holds slot %d and runs %v, want slot %d and %v", sn.id, sn.heldSlot(), got, s, want)
			}
		}

		t1 := time.Now()
		if err := n1.n.SetSlots(context.Background(), 2); err != nil {
			t.Fatalf("SetSlots(2) = %v, want nil", err)
		}
		if d := time.Since(t1); d != 0 {
			t.Errorf("SetSlots(2) took %v, want 0: no call is delayed", d)
		}
		if v := published(t, n1.n); v.Slots != 2 {
			t.Errorf("n1's published view has %d slots when SetSlots returns, want 2", v.Slots)
		}
		ids := []string{"n1", "n2"}
		for slot, shards := range slotShards(12, 2) {
			if len(shards) != 6 {
				t.Errorf("slot-%d has shards %v, want 6", slot, shards)
			}
			for _, s := range shards {
				m, err := n1.n.Route("orders", strconv.Itoa(s))
				if err != nil || m.ID != ids[slot] || m.Slot != slot {
					t.Errorf("Route(orders, %d) when SetSlots returns = %+v, %v, want %s on slot %d", s, m, err, ids[slot], slot)
				}
			}
		}
		holders := []string{holderOf("n1"), holderOf("n2")}
		wantStoredSlots(t, ms, holders)

		sleepUntil(t1.Add(30 * time.Second))
		synctest.Wait()
		for s, sn := range nodes[:2] {
			if got, want := running(sn.runs), slotShards(12, 2)[s]; !slices.Equal(got, want) || len(got) != 6 || sn.heldSlot() != s {
				t.Errorf("%s holds slot %d and runs %v 30 s after SetSlots, want slot %d and its 6 shards %v",
					sn.id, sn.heldSlot(), got, s, want)
			}
		}
		if got := running(n3.runs); len(got) != 0 {
			t.Errorf("n3 runs %v 30 s after SetSlots, want nothing", got)
		}
		if s := n3.heldSlot(); s != -1 {
			t.Errorf("n3's machine holds slot %d 30 s after SetSlots, want none", s)
		}
		if rs := n3.fs.Calls("ReleaseSlot"); len(rs) != 1 || rs[0].Args[1] != 2 || rs[0].Err != nil {
			t.Errorf("n3's ReleaseSlot calls = %v, want slot 2 released once", rs)
		}
		for _, sn := range nodes {
			if got, want := slotted(published(t, sn.n)), []string{"n1:0", "n2:1"}; !slices.Equal(got, want) {
				t.Errorf("%s's view: members with a slot = %v, want %v", sn.id, got, want)
			}
		}
		wantStoredSlots(t, ms, holders)
		// Each shard of slot 2 moved once, to the member of its slot over two slots; no other
		// shard moved.
		for shard, ran := range runHistory(t, nodes...) {
			want := []string{nodes[slotIndex(shard, 12, 3)].id}
			if to := ids[slotIndex(shard, 12, 2)]; to != want[0] {
				want = append(want, to)
			}
			if !slices.Equal(ran, want) {
				t.Errorf("orders/%d ran on %v, want %v", shard, ran, want)
			}
		}
		for _, sn := range nodes {
			closeNode(t, sn.n)
		}
		check()
	})
}

// TestATRUN24SetSlotsErrors checks SetSlots's checks in order (RUN-240): ErrClosed, ErrDraining,
// a negative count, a store that is not a SlotStore; then the errors of the write and of the read
// after it, and of a ctx that ends first. A SetSlots that succeeds returns once the node steps
// and routes by the new count.
func TestATRUN24SetSlotsErrors(t *testing.T) {
	ctx := context.Background()
	wantText := func(t *testing.T, what string, err error, want string) {
		t.Helper()
		if err == nil || err.Error() != want {
			t.Errorf("%s = %v, want %q", what, err, want)
		}
	}

	t.Run("no SlotStore", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			cfg := r.config()
			cfg.Store = r.fs.StoreOnly()
			n := start(t, ctx, cfg)
			solo := start(t, ctx, tenure.Config{
				Self: tenure.Member{ID: "n2", Addr: "a2"}, Membership: r.g.Absent("n2"), Rand: countingRand(),
			})
			wantText(t, "SetSlots(-1)", n.SetSlots(ctx, -1), "tenure: SetSlots: negative count -1")
			const noSlots = "tenure: SetSlots: store does not implement SlotStore"
			wantText(t, "SetSlots(2) over a plain Store", n.SetSlots(ctx, 2), noSlots)
			wantText(t, "SetSlots(2) without a Store", solo.SetSlots(ctx, 2), noSlots)
			wantText(t, "SetSlots(-3) without a Store", solo.SetSlots(ctx, -3), "tenure: SetSlots: negative count -3")
			closeNode(t, n)
			closeNode(t, solo)
			check()
		})
	})

	t.Run("store errors", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			n := start(t, ctx, r.slotConfig(1))
			synctest.Wait()
			boom := errors.New("boom")

			r.fs.Fail("SetSlots", boom)
			wantErrText(t, "SetSlots(2) with a failing write", n.SetSlots(ctx, 2), "tenure: SetSlots: boom", boom)
			wantSlots(t, r, 1, []string{holder01})
			r.fs.Fail("SetSlots", nil)

			r.fs.Fail("Slots", boom)
			at := time.Now()
			wantErrText(t, "SetSlots(2) with a failing read", n.SetSlots(ctx, 2), "tenure: SetSlots: read slots: boom", boom)
			ss, sl := r.fs.Calls("SetSlots"), r.fs.Calls("Slots")
			if last := sl[len(sl)-1]; !last.Start.Equal(at) || last.Seq < ss[len(ss)-1].EndSeq {
				t.Errorf("last Slots call %v at T+%v, want one at once after %v", last, last.Start.Sub(at), ss[len(ss)-1])
			}
			wantSlots(t, r, 2, []string{holder01, ""}) // written before the read failed
			if v := published(t, n); v.Slots != 1 {
				t.Errorf("published view has %d slots after the failed read, want 1", v.Slots)
			}
			r.fs.Fail("Slots", nil)

			at = time.Now()
			if err := n.SetSlots(ctx, 3); err != nil || time.Since(at) != 0 {
				t.Errorf("SetSlots(3) = %v after %v, want nil at once", err, time.Since(at))
			}
			if v := published(t, n); v.Slots != 3 {
				t.Errorf("published view has %d slots when SetSlots(3) returns, want 3", v.Slots)
			}
			wantSlotRoute(t, n, slotShards(8, 3)[0][0], "n1", 0)
			wantNoMember(t, n, slotShards(8, 3)[2][0], "slot-2 is vacant")

			release := r.fs.Block("Slots")
			cctx, cancel := context.WithTimeout(ctx, time.Second)
			at = time.Now()
			err := n.SetSlots(cctx, 2)
			cancel()
			wantErrText(t, "SetSlots(2) with a held read", err, "tenure: SetSlots: context deadline exceeded", context.DeadlineExceeded)
			if d := time.Since(at); d != time.Second {
				t.Errorf("SetSlots(2) with a held read returned after %v, want 1s", d)
			}
			release()
			wantSlots(t, r, 2, []string{holder01, ""})
			closeNode(t, n)
			check()
		})
	})

	t.Run("after a new incarnation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			cfg := r.slotConfig(1)
			cfg.Rand = recoveryRand()
			t0 := time.Now()
			n := start(t, ctx, cfg)
			sleepUntil(t0.Add(9 * time.Second))
			loseSession(r.fs)
			sleepUntil(t0.Add(20 * time.Second))
			synctest.Wait()
			if got := n.Holder(); got != holder02 {
				t.Fatalf("Holder() = %q, want %q: a new incarnation", got, holder02)
			}
			// The loop steps views with Versions raised once per new incarnation; SetSlots still
			// waits for the view that holds its read.
			at := time.Now()
			if err := n.SetSlots(ctx, 2); err != nil || time.Since(at) != 0 {
				t.Errorf("SetSlots(2) = %v after %v, want nil at once", err, time.Since(at))
			}
			if v := published(t, n); v.Slots != 2 {
				t.Errorf("published view has %d slots when SetSlots(2) returns, want 2", v.Slots)
			}
			wantNoMember(t, n, slotShards(8, 2)[1][0], "slot-1 is vacant")
			closeNode(t, n)
			check()
		})
	})

	t.Run("draining and closed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			n := start(t, ctx, r.slotConfig(1))
			synctest.Wait()
			if err := n.Drain(ctx); err != nil {
				t.Fatalf("Drain() = %v, want nil", err)
			}
			for _, count := range []int{2, -1} {
				if err := n.SetSlots(ctx, count); err != tenure.ErrDraining {
					t.Errorf("SetSlots(%d) after Drain = %v, want ErrDraining", count, err)
				}
			}
			closeNode(t, n)
			for _, count := range []int{2, -1} {
				if err := n.SetSlots(ctx, count); err != tenure.ErrClosed {
					t.Errorf("SetSlots(%d) after Close = %v, want ErrClosed", count, err)
				}
			}
			if ss := r.fs.Calls("SetSlots"); len(ss) != 1 {
				t.Errorf("SetSlots calls = %v, want only Start's seeding", ss)
			}
			wantSlots(t, r, 1, []string{""})
			check()
		})
	})

	t.Run("closed while waiting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			n := start(t, ctx, r.slotConfig(1))
			synctest.Wait()
			release := r.fs.Block("Slots")
			set := make(chan error, 1)
			go func() { set <- n.SetSlots(ctx, 2) }()
			synctest.Wait()
			closed := make(chan error, 1)
			go func() { closed <- n.Close(ctx) }()
			synctest.Wait()
			select {
			case err := <-set:
				if err != tenure.ErrClosed {
					t.Errorf("SetSlots(2) waiting at Close = %v, want ErrClosed", err)
				}
			default:
				t.Error("SetSlots(2) waiting at Close has not returned once Close stopped the node")
			}
			release() // Close waits for the refresher's read, which ignores its ctx
			if err := <-closed; err != nil {
				t.Errorf("Close() = %v, want nil", err)
			}
			wantSlots(t, r, 2, []string{"", ""})
			check()
		})
	})
}

// TestATRUN25Rollout replaces both pods of a group with two slots, one at a time, as a rolling
// deploy does: the new pod starts as a standby and runs nothing, the old pod drains, and the
// standby claims the freed slot and runs the same shards. Each shard moves once per pod
// replaced, from the old pod to its replacement, and never has two Runs at once.
func TestATRUN25Rollout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ms := memstore.New(nil)
		g := newGroup(t)
		t0 := time.Now()
		n1 := startSlotNode(t, ms, g, "n1", 2, orders)
		sleepUntil(t0.Add(time.Second))
		n2 := startSlotNode(t, ms, g, "n2", 2, orders)
		sleepUntil(t0.Add(5 * time.Second))
		synctest.Wait()
		for s, sn := range []*slotNode{n1, n2} {
			if got, want := running(sn.runs), shardsOf(2, s); !slices.Equal(got, want) || sn.heldSlot() != s {
				t.Fatalf("%s holds slot %d and runs %v, want slot %d and %v", sn.id, sn.heldSlot(), got, s, want)
			}
		}

		// replace starts id as the replacement of old, which holds slot, and drains old once the
		// new pod waits as a standby.
		replace := func(old *slotNode, id string, slot int) *slotNode {
			t.Helper()
			sn := startSlotNode(t, ms, g, id, 2, orders)
			time.Sleep(5 * time.Second)
			synctest.Wait()
			if got := running(sn.runs); len(got) != 0 || sn.heldSlot() != -1 {
				t.Errorf("standby %s holds slot %d and runs %v, want none", id, sn.heldSlot(), got)
			}
			for _, c := range sn.claims() {
				if c != tenure.ErrNoSlot.Error() {
					t.Errorf("standby %s's claims %v, want only ErrNoSlot", id, sn.claims())
					break
				}
			}
			closeNode(t, old.n)
			time.Sleep(10 * time.Second)
			synctest.Wait()
			if got, want := running(sn.runs), shardsOf(2, slot); !slices.Equal(got, want) || sn.heldSlot() != slot {
				t.Errorf("%s holds slot %d and runs %v, want slot %d and %v", id, sn.heldSlot(), got, slot, want)
			}
			return sn
		}
		n3 := replace(n1, "n3", 0)
		if got, want := running(n2.runs), shardsOf(2, 1); !slices.Equal(got, want) {
			t.Errorf("n2 runs %v once n1 is replaced, want %v", got, want)
		}
		for _, s := range shardsOf(2, 0) {
			wantSlotRoute(t, n2.n, s, "n3", 0)
		}
		n4 := replace(n2, "n4", 1)
		for _, n := range []*tenure.Node{n3.n, n4.n} {
			for _, s := range allShards {
				wantSlotRoute(t, n, s, []string{"n3", "n4"}[slotOwner(2)[s]], slotOwner(2)[s])
			}
		}

		for shard, ran := range runHistory(t, n1, n2, n3, n4) {
			want := [][]string{{"n1", "n3"}, {"n2", "n4"}}[slotOwner(2)[shard]]
			if !slices.Equal(ran, want) {
				t.Errorf("orders/%d ran on %v, want %v: one move per pod replaced", shard, ran, want)
			}
		}
		closeNode(t, n3.n)
		closeNode(t, n4.n)
		check()
	})
}

// TestATRUN26CrashedSlotHolder crashes n1, the holder of slot 0 of two, with standby n3 waiting:
// n1 neither releases its shards nor closes its session, so its slot stays taken until the store
// expires the session, LeaseDuration after its last renewal. n3 claims the slot at its first claim
// after that and runs slot 0's shards; n2's shards never move, and no shard has two Runs at once.
func TestATRUN26CrashedSlotHolder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ms := memstore.New(nil)
		g := newGroup(t)
		t0 := time.Now()
		var nodes []*slotNode
		for i, id := range []string{"n1", "n2", "n3"} {
			sleepUntil(t0.Add(time.Duration(i) * time.Second))
			nodes = append(nodes, startSlotNode(t, ms, g, id, 2, orders))
		}
		n1, n2, n3 := nodes[0], nodes[1], nodes[2]
		sleepUntil(t0.Add(5 * time.Second))
		synctest.Wait()
		for s, sn := range nodes {
			want := []int(nil)
			if s < 2 {
				want = shardsOf(2, s)
			} else {
				s = -1
			}
			if got := running(sn.runs); !slices.Equal(got, want) || sn.heldSlot() != s {
				t.Fatalf("%s holds slot %d and runs %v, want slot %d and %v", sn.id, sn.heldSlot(), got, s, want)
			}
		}

		// The crash: a Close whose ctx is done already, which neither drains nor closes the
		// session.
		dead, cancel := context.WithCancel(context.Background())
		cancel()
		if err := n1.n.Close(dead); !errors.Is(err, context.Canceled) {
			t.Fatalf("Close(canceled ctx) = %v, want context.Canceled", err)
		}
		if cs := n1.fs.Calls("Release", "ReleaseSlot", "CloseSession"); len(cs) != 0 {
			t.Fatalf("n1's calls %v, want none: it crashed", cs)
		}
		ka := n1.fs.Calls("KeepAlive")
		expiry := ka[len(ka)-1].End.Add(core.DefaultTiming.LeaseDuration)

		sleepUntil(expiry.Add(-time.Millisecond))
		synctest.Wait()
		if got := running(n3.runs); len(got) != 0 || n3.heldSlot() != -1 {
			t.Errorf("n3 holds slot %d and runs %v before n1's session expired, want none", n3.heldSlot(), got)
		}
		wantStoredSlots(t, ms, []string{holder01, holderOf("n2")})

		sleepUntil(expiry.Add(10 * time.Second))
		synctest.Wait()
		cs := n3.fs.Calls("ClaimSlot")
		won := slices.IndexFunc(cs, func(c call) bool { return c.Err == nil })
		switch {
		case won < 0:
			t.Fatalf("n3's claims %v, want slot 0 after n1's session expired", n3.claims())
		case resultAt[int](cs[won].Result, 0) != 0 || cs[won].Start.Before(expiry) ||
			!cs[won].Start.Before(expiry.Add(core.DefaultTiming.RetryPeriod)):
			t.Errorf("n3's winning claim %v at expiry%+v, want slot 0 within RetryPeriod after n1's session expired",
				cs[won], cs[won].Start.Sub(expiry))
		}
		for _, c := range cs[:max(won, 0)] {
			if !errors.Is(c.Err, tenure.ErrNoSlot) {
				t.Errorf("n3's %v before the winning claim, want ErrNoSlot", c)
			}
		}
		if got, want := running(n3.runs), shardsOf(2, 0); !slices.Equal(got, want) || n3.heldSlot() != 0 {
			t.Errorf("n3 holds slot %d and runs %v, want slot 0 and %v", n3.heldSlot(), got, want)
		}
		for _, a := range n3.fs.Calls("Acquire") {
			if a.Start.Before(cs[won].End) {
				t.Errorf("n3's %v before its claim of slot 0 returned", a)
			}
		}
		for _, s := range allShards {
			wantSlotRoute(t, n2.n, s, []string{"n3", "n2"}[slotOwner(2)[s]], slotOwner(2)[s])
		}
		wantStoredSlots(t, ms, []string{holderOf("n3"), holderOf("n2")})
		for shard, ran := range runHistory(t, nodes...) {
			want := [][]string{{"n1", "n3"}, {"n2"}}[slotOwner(2)[shard]]
			if !slices.Equal(ran, want) {
				t.Errorf("orders/%d ran on %v, want %v", shard, ran, want)
			}
		}
		closeNode(t, n2.n)
		closeNode(t, n3.n)
		check()
	})
}

// TestSlotsAfterRecovery loses the session of a member that holds the only slot at T0+10s. The
// new incarnation starts with no slot and a view that shows slot 0 held by the old holder. It
// claims slot 0 (the memstore ended the old session when the new one opened), holds it, and runs
// every shard again. A Slots read follows the new claim at once, the Versions of the stepped
// views keep rising across the incarnations, and no view the new machine steps after its
// SlotClaimed shows slot 0 vacant or held by another holder.
func TestSlotsAfterRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		cfg := r.slotConfig(1)
		cfg.Rand = recoveryRand()

		var (
			mu       sync.Mutex
			versions []uint64
			claimed  bool // the new machine has stepped its SlotClaimed
			released bool // and a SlotReleased since
			bad      []string
		)
		hook := tenure.WithStepHook(context.Background(), func(rec tenure.StepRecord) {
			mu.Lock()
			defer mu.Unlock()
			switch in := rec.Input.(type) {
			case core.SlotClaimed:
				claimed = claimed || rec.Gen > 1
			case core.SlotReleased:
				released = claimed
			case core.ViewChanged:
				versions = append(versions, in.View.Version)
				if claimed && !released && in.View.SlotHolders[0] != holder02 {
					bad = append(bad, fmt.Sprintf("view at %v with slot 0 held by %q", rec.Now, in.View.SlotHolders[0]))
				}
			}
		})
		t0 := time.Now()
		n := start(t, hook, cfg)
		synctest.Wait()
		if got := slotted(published(t, n)); !slices.Equal(got, []string{"n1:0"}) {
			t.Fatalf("members with a slot = %v, want n1 on slot 0", got)
		}
		sleepUntil(t0.Add(9 * time.Second))
		loseSession(r.fs)
		sleepUntil(t0.Add(40 * time.Second))
		synctest.Wait()

		calls := r.fs.Calls()
		wantLossAt(t, calls, t0.Add(10*time.Second))
		var won call
		for _, c := range callsOf(calls, holder02) {
			if c.Method == "ClaimSlot" && c.Err == nil {
				won = c
				break
			}
		}
		if won.Method == "" || resultAt[int](won.Result, 0) != 0 || !won.Start.Equal(t0.Add(10*time.Second)) {
			t.Fatalf("calls of %s = %v, want a ClaimSlot at T0+10s that returns slot 0", holder02, callsOf(calls, holder02))
		}
		if v := published(t, n); !slices.Equal(v.SlotHolders, []string{holder02}) {
			t.Errorf("published view has slot holders %q, want %s", v.SlotHolders, holder02)
		}
		for _, s := range allShards {
			wantSlotRoute(t, n, s, "n1", 0)
		}
		if got := running(r.runs); !slices.Equal(got, allShards) {
			t.Errorf("runs %v, want every shard again under %s", got, holder02)
		}
		after := slices.IndexFunc(r.fs.Calls("Slots"), func(c call) bool { return c.Seq > won.EndSeq })
		if after < 0 || !r.fs.Calls("Slots")[after].Start.Equal(won.End) {
			t.Errorf("Slots calls at %v, want one right after the new claim at T0+%v",
				startsAt(r.fs.Calls("Slots"), t0), won.End.Sub(t0))
		}
		closeNode(t, n)
		mu.Lock()
		defer mu.Unlock()
		for i := 1; i < len(versions); i++ {
			if versions[i] <= versions[i-1] {
				t.Errorf("stepped view Versions %v, want each above the one before", versions)
				break
			}
		}
		if !claimed || len(bad) > 0 {
			t.Errorf("new claim stepped %t; after it: %q", claimed, bad)
		}
		check()
	})
}
