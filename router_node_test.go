// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// The end-to-end check of the Router against real nodes (AT-RTR-03): groups of nodes on one
// memstore and one static list, with the spaces markets (64 shards) and orders (8 shards), and a
// Router on the same group. After each change the test waits until every node and the Router
// hold the same view, then compares the Router's route of every shard of every space with every
// node's Node.Route. The steps were written for this test from the issue's text.

// nodeRouterSpaces returns the spaces of the AT-RTR-03 groups, each Run waiting for its ctx.
func nodeRouterSpaces() []tenure.Space {
	run := func(ctx context.Context, _ *tenure.Shard) error { <-ctx.Done(); return nil }
	return []tenure.Space{{Name: "markets", Shards: 64, Run: run}, {Name: "orders", Shards: 8, Run: run}}
}

// slotSource is what slotMembership reads slot data from: memstore's Slots.
type slotSource interface {
	Slots(ctx context.Context) (count int, holders []string, err error)
}

// slotMembership is a Membership whose views carry slot data, for a Router in a slotted group:
// it follows the views of another Membership and merges into each the slot count and holders
// that it reads from a store, as a node merges its own (RUN-220), reading them again every
// period and delivering the latest merged view, which replaces one not yet received. The
// merged view's Version is the membership view's plus the number of slot data changes seen, so
// it grows with each. It stands in for the merged views a node will serve (Node.Watch and
// viewhttp) to a Router outside the group. Join, SetDraining and Leave go to the Membership.
type slotMembership struct {
	tenure.Membership
	slots  slotSource
	period time.Duration
}

// Watch opens a watch of the Membership and returns a channel of its merged views, closed when
// that watch ends or ctx is done.
func (m *slotMembership) Watch(ctx context.Context) (<-chan core.View, error) {
	in, err := m.Membership.Watch(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan core.View, 1)
	go m.follow(ctx, in, out)
	return out, nil
}

// follow merges each view of in, and each change of the slot data read every period, into out
// until in closes or ctx is done, then closes out.
func (m *slotMembership) follow(ctx context.Context, in <-chan core.View, out chan core.View) {
	defer close(out)
	tick := time.NewTicker(m.period)
	defer tick.Stop()
	var (
		mem     core.View
		hasMem  bool
		count   int
		holders []string
		bumps   uint64
	)
	read := func() bool {
		c, h, err := m.slots.Slots(ctx)
		if err != nil || c == count && slices.Equal(h, holders) {
			return false
		}
		count, holders = c, h
		bumps++
		return true
	}
	for {
		select {
		case v, ok := <-in:
			if !ok {
				return
			}
			mem, hasMem = v, true
			read()
		case <-tick.C:
			if !read() {
				continue
			}
		case <-ctx.Done():
			return
		}
		if hasMem {
			v := mergeSlots(mem, count, holders)
			v.Version += bumps
			sendLatest(out, v)
		}
	}
}

// mergeSlots returns v with the slot data merged in, as a node merges it: Slots is count,
// SlotHolders is holders cut or padded with "" to count entries (nil for no slots), and each
// member's Slot is the lowest slot whose holder's ID part, before "/", is the member's ID, else
// -1. The members slice is a copy.
func mergeSlots(v core.View, count int, holders []string) core.View {
	out := core.View{Version: v.Version, Slots: count}
	if count > 0 {
		out.SlotHolders = make([]string, count)
		copy(out.SlotHolders, holders)
	}
	out.Members = make([]core.Member, len(v.Members))
	for i, m := range v.Members {
		m.Slot = slices.IndexFunc(out.SlotHolders, func(h string) bool {
			id, _, _ := strings.Cut(h, "/")
			return h != "" && id == m.ID
		})
		out.Members[i] = m
	}
	return out
}

// nodeGroup is one AT-RTR-03 group: nodes on one memstore and one static list, and a Router on
// the same group.
type nodeGroup struct {
	t     *testing.T
	ms    *memstore.Store
	g     *group
	slots int // Config.Slots of every node
	nodes map[string]*tenure.Node
	// drained holds the running nodes whose Drain has returned: their sessions are closed.
	drained map[string]bool
	router  *tenure.Router
	keys    map[string][]string // a key of each shard, by space
}

// newNodeGroup starts the nodes ids, one after the other, with Config.Slots slots, and a Router
// on the same group: on the bare static list without slots, and with slots on a slotMembership
// over it that reads the memstore, since static's views carry no slot data.
func newNodeGroup(t *testing.T, slots int, ids ...string) *nodeGroup {
	t.Helper()
	ng := &nodeGroup{t: t, ms: memstore.New(nil), g: newGroup(t), slots: slots,
		nodes: make(map[string]*tenure.Node), drained: make(map[string]bool), keys: make(map[string][]string)}
	for _, sp := range nodeRouterSpaces() {
		ng.keys[sp.Name] = shardKeys(sp.Shards)
	}
	for _, id := range ids {
		ng.join(id)
	}
	var m tenure.Membership = ng.g.Absent("router")
	if slots > 0 {
		m = &slotMembership{Membership: m, slots: ng.ms, period: core.DefaultTiming.RetryPeriod}
	}
	r, err := tenure.NewRouter(m, nodeRouterSpaces())
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	ng.router = r
	return ng
}

// shardKeys returns a key of each of n shards: the first "k<i>" that core.KeyShard puts there.
func shardKeys(n int) []string {
	keys := make([]string, n)
	found := 0
	for i := 0; found < n; i++ {
		k := "k" + strconv.Itoa(i)
		if s := core.KeyShard(k, n); keys[s] == "" {
			keys[s] = k
			found++
		}
	}
	return keys
}

// join starts member id.
func (ng *nodeGroup) join(id string) {
	ng.t.Helper()
	cfg := tenure.Config{
		Self:       tenure.Member{ID: id, Addr: addrOf(id)},
		Membership: ng.g.Absent(id),
		Store:      ng.ms,
		Spaces:     nodeRouterSpaces(),
		Slots:      ng.slots,
	}
	ng.nodes[id] = start(ng.t, context.Background(), cfg)
}

// drain drains member id, which stays in the group, draining, until it leaves.
func (ng *nodeGroup) drain(id string) {
	ng.t.Helper()
	if err := ng.nodes[id].Drain(context.Background()); err != nil {
		ng.t.Fatalf("%s: Drain() = %v, want nil", id, err)
	}
	ng.drained[id] = true
}

// leave closes member id, which leaves the group.
func (ng *nodeGroup) leave(id string) {
	ng.t.Helper()
	closeNode(ng.t, ng.nodes[id])
	delete(ng.nodes, id)
	delete(ng.drained, id)
}

// close closes the Router and every node.
func (ng *nodeGroup) close() {
	ng.t.Helper()
	if err := ng.router.Close(); err != nil {
		ng.t.Fatalf("Router.Close() = %v, want nil", err)
	}
	for _, id := range ng.ids() {
		ng.leave(id)
	}
}

// ids returns the IDs of the running nodes, sorted.
func (ng *nodeGroup) ids() []string {
	ids := make([]string, 0, len(ng.nodes))
	for id := range ng.nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// sameView reports whether a and b have the same members, slot count and slot holders. The
// Version is not compared: a node's merged view counts its own slot reads into it.
func sameView(a, b core.View) bool {
	return reflect.DeepEqual(a.Members, b.Members) && a.Slots == b.Slots && slices.Equal(a.SlotHolders, b.SlotHolders)
}

// routerView returns the view the Router has applied, without its Version, and false before the
// first.
func routerView(r *tenure.Router) (core.View, bool) {
	tab := r.Table()
	if tab.Version == 0 {
		return core.View{}, false
	}
	return core.View{Members: tab.Members, Slots: tab.Slots, SlotHolders: tab.SlotHolders}, true
}

// converged returns the view every node and the Router have published and applied, and false
// with what each holds when they differ.
func (ng *nodeGroup) converged() (core.View, string, bool) {
	v, ok := routerView(ng.router)
	desc := fmt.Sprintf("router: %s", describeView(v, ok))
	same := ok
	for _, id := range ng.ids() {
		nv, ok := ng.nodes[id].PublishedView()
		desc += fmt.Sprintf("\n%s: %s", id, describeView(nv, ok))
		same = same && ok && sameView(nv, v)
	}
	return v, desc, same
}

// describeView formats v for a failure message.
func describeView(v core.View, ok bool) string {
	if !ok {
		return "no view"
	}
	members := make([]string, len(v.Members))
	for i, m := range v.Members {
		members[i] = fmt.Sprintf("%s(slot %d, draining %t)", m.ID, m.Slot, m.Draining)
	}
	return fmt.Sprintf("members %v, %d slots %q", members, v.Slots, v.SlotHolders)
}

// settle waits until every node and the Router have held the same view for longer than
// SlotGrace and two RetryPeriods, so that a slot vacancy in it has expired on all of them and
// each has read the slot data since it last changed, then checks the routes.
func (ng *nodeGroup) settle(step string) {
	ng.t.Helper()
	timing := core.DefaultTiming
	hold := timing.SlotGrace + 2*timing.RetryPeriod
	deadline := time.Now().Add(10 * time.Minute)
	var (
		held  core.View
		since time.Time
		has   bool
	)
	for {
		time.Sleep(time.Second)
		synctest.Wait()
		v, desc, ok := ng.converged()
		switch {
		case ok && has && sameView(v, held):
			if time.Since(since) > hold {
				ng.checkRoutes(step)
				return
			}
		case ok:
			held, since, has = v, time.Now(), true
		default:
			has = false
		}
		if time.Now().After(deadline) {
			ng.t.Fatalf("%s: the nodes and the router hold no common view for %v within 10 minutes:\n%s", step, hold, desc)
		}
	}
}

// checkRoutes fails the test unless the Router routes every shard of every space, by
// RouteShard and by Route of a key of the shard, as the Node.Route of every node that has not
// finished draining does: the same member by value, or an error with the same text, both
// matching ErrNoMembers.
//
// A node whose Drain has returned is left out. Its session is closed, and core.Machine expires a
// slot vacancy (and so moves the shards of a slot vacant for SlotGrace) only while the session is
// neither lost nor closing (Machine.Step): such a node's table follows only new views, not a
// vacancy expiry, so once SlotGrace has passed it may still name a slot vacant that the members,
// and the Router, have dropped. Its view still has to converge with the others in settle.
func (ng *nodeGroup) checkRoutes(step string) {
	ng.t.Helper()
	r := ng.router
	for _, sp := range nodeRouterSpaces() {
		for shard, key := range ng.keys[sp.Name] {
			got, err := r.RouteShard(sp.Name, shard)
			byKey, keyErr := r.Route(sp.Name, key)
			if !reflect.DeepEqual(byKey, got) || errText(keyErr) != errText(err) {
				ng.t.Fatalf("%s: Router.Route(%q, %q) = %+v, %v; RouteShard(%d) = %+v, %v",
					step, sp.Name, key, byKey, keyErr, shard, got, err)
			}
			for _, id := range ng.ids() {
				if ng.drained[id] {
					continue
				}
				want, wantErr := ng.nodes[id].Route(sp.Name, key)
				if !reflect.DeepEqual(got, want) || errText(err) != errText(wantErr) {
					ng.t.Fatalf("%s: Router.RouteShard(%q, %d) = %+v, %v; %s's Node.Route(%q, %q) = %+v, %v",
						step, sp.Name, shard, got, err, id, sp.Name, key, want, wantErr)
				}
				if wantErr != nil && (!errors.Is(err, tenure.ErrNoMembers) || !errors.Is(wantErr, tenure.ErrNoMembers)) {
					ng.t.Fatalf("%s: %s/%d: errors %v and %v do not both match ErrNoMembers", step, sp.Name, shard, err, wantErr)
				}
			}
		}
	}
}

// errText returns err's text, or "" for nil.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestATRTR03RouterLikeNodes runs 3 to 7 nodes without slots and a Router on the bare static
// group, and checks after each join, drain and leave that the Router routes every shard of both
// spaces as every node does (AT-RTR-03).
func TestATRTR03RouterLikeNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ng := newNodeGroup(t, 0, "n1", "n2", "n3")
		ng.settle("start n1 to n3")
		for _, id := range []string{"n4", "n5", "n6", "n7"} {
			ng.join(id)
			ng.settle("join " + id)
		}
		ng.drain("n2")
		ng.settle("drain n2")
		ng.leave("n2")
		ng.settle("leave n2")
		ng.leave("n5")
		ng.settle("leave n5")
		for _, id := range []string{"n1", "n3", "n4"} {
			ng.join("r" + id[1:])
			ng.settle("join r" + id[1:])
			ng.leave(id)
			ng.settle("leave " + id)
		}
		ng.close()
		check()
	})
}

// TestATRTR03RouterLikeNodesWithSlots runs nodes with slots and a Router on a slotMembership
// over the same group, and checks after each join, drain, leave, slot count change and step of a
// rolling replacement that the Router routes every shard of both spaces as every node does
// (AT-RTR-03).
func TestATRTR03RouterLikeNodesWithSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ng := newNodeGroup(t, 3, "n1", "n2", "n3")
		ng.settle("start n1 to n3 on 3 slots")
		ng.join("n4")
		ng.settle("join n4 with no free slot")
		setSlots := func(count int) {
			t.Helper()
			if err := ng.nodes[ng.ids()[0]].SetSlots(context.Background(), count); err != nil {
				t.Fatalf("SetSlots(%d) = %v, want nil", count, err)
			}
			ng.settle("SetSlots(" + strconv.Itoa(count) + ")")
		}
		setSlots(5)
		ng.join("n5")
		ng.settle("join n5")
		ng.drain("n2")
		ng.settle("drain n2")
		ng.leave("n2")
		ng.settle("leave n2")
		setSlots(3)
		ng.join("n6")
		ng.join("n7")
		ng.settle("join n6 and n7")
		// Roll every member over to a new one, as a rollout replaces pods: start the new member,
		// then close the old one, which hands its slot over.
		for _, id := range ng.ids() {
			ng.join("r" + id[1:])
			ng.settle("join r" + id[1:])
			ng.leave(id)
			ng.settle("leave " + id)
		}
		setSlots(4)
		ng.close()
		check()
	})
}
