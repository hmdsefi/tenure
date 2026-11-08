// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// The tests of Route and Local: keys routed by the published snapshot, local shards served while
// owned. Steps carrying an AT-RUN number were written from the issue's text.

// keyOf returns the first key "k0", "k1", … whose shard of orders by core.KeyShard is shard.
func keyOf(shard int) string {
	for i := 0; ; i++ {
		if k := "k" + strconv.Itoa(i); core.KeyShard(k, 8) == shard {
			return k
		}
	}
}

// ownerOf turns bal's result into the owner ID of each shard.
func ownerOf(b map[string][]int) map[int]string {
	out := make(map[int]string)
	for id, shards := range b {
		for _, s := range shards {
			out[s] = id
		}
	}
	return out
}

// addrOf returns the address the route tests give member id: "a" and the ID's digits, a1 for n1.
func addrOf(id string) string { return "a" + id[1:] }

// wantRoute fails t unless n routes key in space to the member id with addrOf(id), and the
// member is the one n's published view holds.
func wantRoute(t *testing.T, n *tenure.Node, space, key, id string) {
	t.Helper()
	m, err := n.Route(space, key)
	if err != nil || m.ID != id || m.Addr != addrOf(id) {
		t.Errorf("Route(%q, %q) = %+v, %v, want %s (%s)", space, key, m, err, id, addrOf(id))
		return
	}
	v := published(t, n)
	i := slices.IndexFunc(v.Members, func(vm core.Member) bool { return vm.ID == id })
	if i < 0 || !reflect.DeepEqual(m, v.Members[i]) {
		t.Errorf("Route(%q, %q) = %+v, want the view's member %s", space, key, m, id)
	}
}

// wantNotOwner fails t unless err is a *NotOwnerError for shard of space naming the member id
// with addrOf(id), or no route when id is "", that matches ErrNotOwner.
func wantNotOwner(t *testing.T, err error, space string, shard int, id string) {
	t.Helper()
	var no *tenure.NotOwnerError
	if !errors.As(err, &no) || !errors.Is(err, tenure.ErrNotOwner) {
		t.Errorf("error = %v, want a *NotOwnerError matching ErrNotOwner", err)
		return
	}
	want := fmt.Sprintf("tenure: not owner of %s/%d: no route", space, shard)
	if id != "" {
		want = fmt.Sprintf("tenure: not owner of %s/%d: route to %s (%s)", space, shard, id, addrOf(id))
	}
	if no.Space != space || no.Shard != shard || no.Route.ID != id || err.Error() != want {
		t.Errorf("error = %q (%+v), want %q", err, no, want)
	}
}

// newPairRig returns a shardRig whose group lists n2 with address a2, a member that runs no node.
func newPairRig(t *testing.T) shardRig {
	t.Helper()
	r := newShardRig(t)
	r.g = newGroup(t, tenure.Member{ID: "n2", Addr: "a2"})
	r.sm = r.g.Absent("n1")
	return r
}

// nodeConfig returns the Config of member id with Addr addrOf(id) over m and st, with space and
// the counting Rand.
func nodeConfig(id string, m tenure.Membership, st tenure.Store, space tenure.Space) tenure.Config {
	return tenure.Config{
		Self:       tenure.Member{ID: id, Addr: addrOf(id)},
		Membership: m,
		Store:      st,
		Spaces:     []tenure.Space{space},
		Rand:       countingRand(),
	}
}

// TestATRUN50RouteAgrees starts n1 and n2 on one memstore and one group: both route each of 1000
// keys to the member core.Balanced gives the key's shard.
func TestATRUN50RouteAgrees(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ms := memstore.New(nil)
		g := newGroup(t)
		runs := &runLog{}
		var nodes []*tenure.Node
		for _, id := range []string{"n1", "n2"} {
			cfg := nodeConfig(id, g.Absent(id), newRecStore(ms), orders(runs.Run))
			nodes = append(nodes, start(t, context.Background(), cfg))
		}
		synctest.Wait()

		owner := ownerOf(bal("orders", 8, "n1", "n2"))
		for i := range 1000 {
			key := "key-" + strconv.Itoa(i)
			for _, n := range nodes {
				wantRoute(t, n, "orders", key, owner[core.KeyShard(key, 8)])
			}
		}
		for _, n := range nodes {
			closeNode(t, n)
		}
		check()
	})
}

// TestATRUN51ShardOfAndUnknownSpace checks Route's and Local's second and third steps: an
// unknown space, a ShardOf result outside the space's shards, and a ShardOf that picks the shard,
// next to a space whose nil ShardOf means core.KeyShard.
func TestATRUN51ShardOfAndUnknownSpace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ms := memstore.New(nil)
		g := newGroup(t, tenure.Member{ID: "n2", Addr: "a2"})
		runs := &runLog{}
		bench := tenure.Space{Name: "bench", Shards: 4, Run: runs.Run, ShardOf: func(key string) int {
			s, err := strconv.Atoi(key)
			if err != nil {
				return -1
			}
			return s
		}}
		cfg := nodeConfig("n1", g.Absent("n1"), newRecStore(ms), orders(runs.Run))
		cfg.Spaces = append(cfg.Spaces, bench)
		n := start(t, context.Background(), cfg)
		synctest.Wait()

		failures := []struct {
			space, key, want string
		}{
			{"carts", "k", `tenure: unknown space "carts"`},
			{"", "k", `tenure: unknown space ""`},
			{"bench", "4", `tenure: bench: ShardOf("4") = 4 is outside [0, 4)`},
			{"bench", "x", `tenure: bench: ShardOf("x") = -1 is outside [0, 4)`},
		}
		for _, f := range failures {
			m, err := n.Route(f.space, f.key)
			if err == nil || err.Error() != f.want || errors.Is(err, tenure.ErrNoMembers) || m.ID != "" {
				t.Errorf("Route(%q, %q) = %+v, %v, want %q", f.space, f.key, m, err, f.want)
			}
			sh, err := n.Local(f.space, f.key)
			if err == nil || err.Error() != f.want || errors.Is(err, tenure.ErrNotOwner) || sh != nil {
				t.Errorf("Local(%q, %q) = %v, %v, want %q", f.space, f.key, sh, err, f.want)
			}
		}

		benchOwner := ownerOf(bal("bench", 4, "n1", "n2"))
		for s := range 4 {
			key := strconv.Itoa(s)
			wantRoute(t, n, "bench", key, benchOwner[s])
			sh, err := n.Local("bench", key)
			if benchOwner[s] == "n2" {
				wantNotOwner(t, err, "bench", s, "n2")
				continue
			}
			if err != nil || sh.Space != "bench" || sh.ID != s || sh.Check() != nil {
				t.Errorf("Local(bench, %q) = %+v, %v, want bench/%d owned", key, sh, err, s)
			}
		}
		ordersOwner := ownerOf(bal("orders", 8, "n1", "n2"))
		for _, key := range []string{"0", "1", "2", "3", "a", "b", "c", "d"} {
			wantRoute(t, n, "orders", key, ordersOwner[core.KeyShard(key, 8)])
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN52NoMembers checks Route's fourth step in the draining case: once every member of the
// group drains, no member is eligible, so no shard has a placement key; Local then names no route.
func TestATRUN52NoMembers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newPairRig(t)
		n := start(t, context.Background(), r.config())
		r.g.SetDraining("n2", true)
		synctest.Wait()
		for _, s := range allShards {
			wantRoute(t, n, "orders", keyOf(s), "n1")
		}

		if err := n.Drain(context.Background()); err != nil {
			t.Fatalf("Drain() = %v, want nil", err)
		}
		synctest.Wait()
		for _, s := range allShards {
			key := keyOf(s)
			want := fmt.Sprintf("tenure: no members: orders/%d: no placement keys", s)
			m, err := n.Route("orders", key)
			if err == nil || err.Error() != want || !errors.Is(err, tenure.ErrNoMembers) || m.ID != "" {
				t.Errorf("Route(orders, %q) = %+v, %v, want %q matching ErrNoMembers", key, m, err, want)
			}
			sh, err := n.Local("orders", key)
			if sh != nil {
				t.Errorf("Local(orders, %q) = %v, want nil", key, sh)
			}
			wantNotOwner(t, err, "orders", s, "")
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN53Local checks Local: before the Acquire of a shard returns, its error names the member
// Route names, this one for its own shards; once the member owns the shard, Local returns the
// Shard its Run got; a shard whose Check fails is not served even while it is still published.
func TestATRUN53Local(t *testing.T) {
	t.Run("owned", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newPairRig(t)
			owner := ownerOf(bal("orders", 8, "n1", "n2"))
			release := r.fs.Block("Acquire")
			t0 := time.Now()
			n := start(t, context.Background(), r.config())
			synctest.Wait()
			for _, s := range allShards {
				sh, err := n.Local("orders", keyOf(s))
				if sh != nil {
					t.Errorf("Local(orders/%d) = %v while its Acquire blocks, want nil", s, sh)
				}
				wantNotOwner(t, err, "orders", s, owner[s])
			}

			sleepUntil(t0.Add(time.Second))
			release()
			synctest.Wait()
			for _, s := range allShards {
				sh, err := n.Local("orders", keyOf(s))
				if owner[s] == "n2" {
					wantNotOwner(t, err, "orders", s, "n2")
					continue
				}
				runs := r.runs.Of("orders", s)
				if err != nil || len(runs) != 1 || sh != runs[0].Shard || sh != n.PublishedShard("orders", s) || sh.Check() != nil {
					t.Errorf("Local(orders/%d) = %v, %v, want the published Shard its Run got (%v)", s, sh, err, runs)
				}
			}
			closeNode(t, n)
			for _, s := range allShards {
				if owner[s] == "n1" && r.runs.Of("orders", s)[0].Shard.Check() == nil {
					t.Errorf("orders/%d is still owned after Close", s)
				}
			}
			check()
		})
	})

	t.Run("expired", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			r := newShardRig(t)
			stuck := make(chan struct{})
			r.runs.Script(func(context.Context, *tenure.Shard, int) error {
				<-stuck // ignores ctx, so the Shard stays published past the deadline
				return nil
			})
			s0 := time.Now()
			n := start(t, context.Background(), r.config())
			r.fs.Fail("KeepAlive", errors.New("boom"))
			synctest.Wait()
			key := keyOf(3)
			if sh, err := n.Local("orders", key); err != nil || sh.ID != 3 {
				t.Errorf("Local(orders/3) = %v, %v before the deadline, want orders/3", sh, err)
			}

			sleepUntil(s0.Add(10 * time.Second))
			synctest.Wait()
			if n.PublishedShard("orders", 3) == nil {
				t.Fatal("orders/3 is not published while its Run is stuck")
			}
			sh, err := n.Local("orders", key)
			if sh != nil {
				t.Errorf("Local(orders/3) = %v after the deadline, want nil", sh)
			}
			wantNotOwner(t, err, "orders", 3, "n1")
			close(stuck)
			closeNode(t, n)
			check()
		})
	})
}

// TestATRUN30RunPerShard starts n1, n2 and n3 on one memstore and one group: once they have
// converged, each shard has exactly one Run, on the member every node's Route names, whose Local
// serves the Shard of that Run; the others name it in their errors. No two Runs of a shard ever
// overlap.
func TestATRUN30RunPerShard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ms := memstore.New(nil)
		g := newGroup(t)
		ids := []string{"n1", "n2", "n3"}
		runs := map[string]*runLog{}
		nodes := map[string]*tenure.Node{}
		for _, id := range ids {
			runs[id] = &runLog{}
			nodes[id] = start(t, context.Background(), nodeConfig(id, g.Absent(id), newRecStore(ms), orders(runs[id].Run)))
		}
		time.Sleep(time.Minute)
		synctest.Wait()

		owner := ownerOf(bal("orders", 8, ids...))
		for _, s := range allShards {
			key := keyOf(s)
			var running []runCall
			for _, id := range ids {
				for _, c := range runs[id].Of("orders", s) {
					if !c.Done {
						running = append(running, c)
					}
				}
			}
			if len(running) != 1 || len(runs[owner[s]].Of("orders", s)) == 0 || running[0] != lastOf(runs[owner[s]].Of("orders", s)) {
				t.Errorf("orders/%d: running %v, want one Run on %s", s, running, owner[s])
				continue
			}
			for _, id := range ids {
				wantRoute(t, nodes[id], "orders", key, owner[s])
				sh, err := nodes[id].Local("orders", key)
				if id == owner[s] {
					if err != nil || sh != running[0].Shard {
						t.Errorf("%s: Local(orders/%d) = %v, %v, want the Shard of %v", id, s, sh, err, running[0])
					}
					continue
				}
				wantNotOwner(t, err, "orders", s, owner[s])
			}
		}
		for _, id := range ids {
			closeNode(t, nodes[id])
		}
		for _, s := range allShards {
			var all []runCall
			for _, id := range ids {
				all = append(all, runs[id].Of("orders", s)...)
			}
			slices.SortFunc(all, func(a, b runCall) int { return cmp.Compare(a.Seq, b.Seq) })
			for i := 1; i < len(all); i++ {
				if !all[i-1].Done || all[i-1].EndSeq > all[i].Seq {
					t.Errorf("orders/%d: %v overlaps %v", s, all[i-1], all[i])
				}
			}
		}
		check()
	})
}

// lastOf returns the last of calls.
func lastOf(calls []runCall) runCall { return calls[len(calls)-1] }

// TestATRUN20ViewsReachRoute changes the group 50 times at one instant: the latest view wins, and
// Route ends on the balanced owners over n1, n2 and n3 with their latest addresses.
func TestATRUN20ViewsReachRoute(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		var log stepLog
		n := start(t, log.ctx(context.Background()), r.config())
		synctest.Wait()
		t1 := time.Now()
		for i := range 24 {
			id := []string{"n2", "n3"}[i%2]
			r.g.Join(tenure.Member{ID: id, Addr: "old-" + strconv.Itoa(i)})
			r.g.Leave(id)
		}
		r.g.Join(tenure.Member{ID: "n2", Addr: "a2"})
		r.g.Join(tenure.Member{ID: "n3", Addr: "a3"})
		final := r.g.View()
		synctest.Wait()

		if !time.Now().Equal(t1) {
			t.Errorf("the view changes took %v, want none", time.Since(t1))
		}
		if v := published(t, n); v.Version != final.Version {
			t.Errorf("published view %d, want the latest %d", v.Version, final.Version)
		}
		recs, _ := log.records()
		views := 0
		for _, rec := range recs {
			if _, ok := rec.Input.(core.ViewChanged); ok {
				views++
			}
		}
		if views < 2 || views > 51 {
			t.Errorf("%d ViewChanged steps, want the first view and at most one per change", views)
		}
		owner := ownerOf(bal("orders", 8, "n1", "n2", "n3"))
		for _, s := range allShards {
			wantRoute(t, n, "orders", keyOf(s), owner[s])
		}
		closeNode(t, n)
		check()
	})
}

// TestATRUN92ConcurrentReads reads Route and Local from 8 goroutines while the group changes, so
// the node moves shards, and while Close runs; run under -race. Every answer is a member of the
// group, an ErrNoMembers or ErrNotOwner error, or ErrClosed, and only ErrClosed once Close has
// returned.
func TestATRUN92ConcurrentReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newPairRig(t)
		n := start(t, context.Background(), r.config())

		var closed atomic.Bool
		var wg sync.WaitGroup
		for w := range 8 {
			wg.Go(func() {
				for i := range 300 {
					key := fmt.Sprintf("w%d-%d", w, i)
					after := closed.Load()
					m, err := n.Route("orders", key)
					switch {
					case after && !errors.Is(err, tenure.ErrClosed):
						t.Errorf("Route(%q) = %+v, %v after Close, want ErrClosed", key, m, err)
					case err == nil && !slices.Contains([]string{"n1", "n2", "n3"}, m.ID):
						t.Errorf("Route(%q) = %+v, want a member of the group", key, m)
					case err != nil && !errors.Is(err, tenure.ErrClosed) && !errors.Is(err, tenure.ErrNoMembers):
						t.Errorf("Route(%q) = %v, want ErrNoMembers or ErrClosed", key, err)
					}
					sh, err := n.Local("orders", key)
					switch {
					case after && !errors.Is(err, tenure.ErrClosed):
						t.Errorf("Local(%q) = %v, %v after Close, want ErrClosed", key, sh, err)
					case err == nil:
						_ = sh.Valid()
					case !errors.Is(err, tenure.ErrClosed) && !errors.Is(err, tenure.ErrNotOwner):
						t.Errorf("Local(%q) = %v, want ErrNotOwner or ErrClosed", key, err)
					}
					time.Sleep(50 * time.Millisecond)
				}
			})
		}
		for i := range 20 {
			if i%2 == 0 {
				r.g.Join(tenure.Member{ID: "n3", Addr: "a3"})
			} else {
				r.g.Leave("n3")
			}
			time.Sleep(500 * time.Millisecond)
		}
		closeNode(t, n)
		closed.Store(true)
		wg.Wait()
		check()
	})
}

// TestRouteWhileLoopSteps checks that Route and Local never wait for the loop: they answer while
// the loop is stopped in the middle of a step, and no bubble time passes.
func TestRouteWhileLoopSteps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		var hold atomic.Bool
		held, resume := make(chan struct{}), make(chan struct{})
		ctx := tenure.WithStepHook(context.Background(), func(tenure.StepRecord) {
			if hold.CompareAndSwap(true, false) {
				close(held)
				<-resume
			}
		})
		n := start(t, ctx, r.config())
		synctest.Wait()
		hold.Store(true)
		r.g.Join(tenure.Member{ID: "n2", Addr: "a2"})
		<-held

		t1 := time.Now()
		owner := ownerOf(bal("orders", 8, "n1", "n2"))
		for _, s := range allShards {
			wantRoute(t, n, "orders", keyOf(s), owner[s])
			if _, err := n.Local("orders", keyOf(s)); owner[s] == "n2" {
				wantNotOwner(t, err, "orders", s, "n2")
			}
		}
		if !time.Now().Equal(t1) {
			t.Errorf("Route and Local took %v while the loop stepped, want none", time.Since(t1))
		}
		close(resume)
		closeNode(t, n)
		check()
	})
}

// TestRouteLocalAllocs checks that Route and Local allocate nothing when they succeed: a key
// routed to a member, and the key of a shard this member serves.
func TestRouteLocalAllocs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		n := start(t, context.Background(), r.config())
		synctest.Wait()
		key := keyOf(3)
		var err error
		if got := testing.AllocsPerRun(100, func() { _, err = n.Route("orders", key) }); got != 0 || err != nil {
			t.Errorf("Route(orders, %q) allocates %v times, error %v, want 0 and nil", key, got, err)
		}
		if got := testing.AllocsPerRun(100, func() { _, err = n.Local("orders", key) }); got != 0 || err != nil {
			t.Errorf("Local(orders, %q) allocates %v times, error %v, want 0 and nil", key, got, err)
		}
		closeNode(t, n)
		check()
	})
}

// TestShardOfPanics checks that a ShardOf that panics fails only its caller: the panic reaches the
// goroutine that called Route or Local, and the node goes on routing keys and closes.
func TestShardOfPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		runs := &runLog{}
		bench := tenure.Space{Name: "bench", Shards: 4, Run: runs.Run, ShardOf: func(key string) int {
			if key == "boom" {
				panic("boom")
			}
			s, _ := strconv.Atoi(key)
			return s
		}}
		n := start(t, context.Background(), nodeConfig("n1", newGroup(t).Absent("n1"), newRecStore(memstore.New(nil)), bench))
		synctest.Wait()

		calls := []struct {
			name string
			f    func()
		}{
			{"Route", func() { _, _ = n.Route("bench", "boom") }},
			{"Local", func() { _, _ = n.Local("bench", "boom") }},
		}
		for _, c := range calls {
			func() {
				defer func() {
					if p := recover(); p != "boom" {
						t.Errorf("%s(bench, boom) panics with %v, want boom", c.name, p)
					}
				}()
				c.f()
			}()
		}
		for s := range 4 {
			key := strconv.Itoa(s)
			wantRoute(t, n, "bench", key, "n1")
			if sh, err := n.Local("bench", key); err != nil || sh.ID != s {
				t.Errorf("Local(bench, %q) = %v, %v after the panics, want bench/%d", key, sh, err, s)
			}
		}
		closeNode(t, n)
		check()
	})
}
