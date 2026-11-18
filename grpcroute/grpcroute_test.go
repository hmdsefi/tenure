// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/grpcroute"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/static"
)

// The end-to-end check of grpcroute against a gRPC service (AT-RTR-20): three nodes on one
// memstore and one static list each serve the test service, which answers with Misdirected when
// Node.Local names another member. A client channel routes through a Router whose view the test
// can hold back. Calls go to their owners in one attempt, calls without a key go to every member,
// and while the Router's view lags a drain, calls and streams for the drained member's shards
// are corrected by its trailer and reach their new owners; no call takes more than 3 attempts.
// The steps were written for this test from the issue's text.

// fastTiming keeps the nodes' handovers short.
var fastTiming = core.Timing{
	LeaseDuration: 2 * time.Second,
	RenewDeadline: time.Second,
	RetryPeriod:   50 * time.Millisecond,
	SlotGrace:     2 * time.Second,
}

// settle is how long the test waits for the group to settle.
const settle = 20 * time.Second

// orderSpaces is the group's one space, orders, of 8 shards whose Run waits for its context.
func orderSpaces() []tenure.Space {
	run := func(ctx context.Context, _ *tenure.Shard) error { <-ctx.Done(); return nil }
	return []tenure.Space{{Name: "orders", Shards: 8, Run: run}}
}

// gatedMembership is a Membership whose views can be held back: while held, it keeps the latest
// view of the Membership it wraps and delivers it on release. Join, SetDraining and Leave go to
// the wrapped Membership.
type gatedMembership struct {
	tenure.Membership

	mu      sync.Mutex
	held    bool
	pending *core.View       // the latest view not yet delivered
	out     chan core.View   // the current watch's channel, holding one view
	done    bool             // the current watch's channel is closed
	watches []chan core.View // every channel Watch returned
}

func (g *gatedMembership) Watch(ctx context.Context) (<-chan core.View, error) {
	in, err := g.Membership.Watch(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan core.View, 1)
	g.mu.Lock()
	g.out, g.done, g.pending = out, false, nil
	g.watches = append(g.watches, out)
	g.mu.Unlock()
	go func() {
		for v := range in {
			g.mu.Lock()
			g.pending = &v
			if !g.held {
				g.deliverLocked()
			}
			g.mu.Unlock()
		}
		g.mu.Lock()
		g.done = true
		close(out)
		g.mu.Unlock()
	}()
	return out, nil
}

// deliverLocked puts the pending view in the channel, in place of one not yet received.
func (g *gatedMembership) deliverLocked() {
	if g.pending == nil || g.done {
		return
	}
	select {
	case <-g.out:
	default:
	}
	g.out <- *g.pending
	g.pending = nil
}

// hold stops delivering views.
func (g *gatedMembership) hold() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.held = true
}

// release delivers the latest view and every later one.
func (g *gatedMembership) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.held = false
	g.deliverLocked()
}

// localHandler checks each call with node.Local, as a member's handler does: it lets the owner
// answer, answers Misdirected to the owner the node names, and Unavailable when it names none or
// itself.
func localHandler(node *tenure.Node) handler {
	return func(ctx context.Context, id, key string) error {
		_, err := node.Local("orders", key)
		var notOwner *tenure.NotOwnerError
		switch {
		case err == nil:
			return nil
		case errors.As(err, &notOwner) && notOwner.Route.ID != "" && notOwner.Route.ID != id:
			return grpcroute.Misdirected(ctx, notOwner.Route)
		default:
			return status.Error(codes.Unavailable, err.Error())
		}
	}
}

// nodeGroup is the AT-RTR-20 group: three nodes with their servers, a Router behind a gated
// Membership, and one key per shard.
type nodeGroup struct {
	servers []*testServer
	nodes   []*tenure.Node
	gate    *gatedMembership
	router  *tenure.Router
	keys    []string // keys[s] is a key of shard s
}

func newNodeGroup(t *testing.T) *nodeGroup {
	t.Helper()
	g, err := static.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	store := memstore.New(nil)
	ng := &nodeGroup{gate: &gatedMembership{Membership: g.Share()}}
	for _, id := range []string{"n1", "n2", "n3"} {
		s := newServer(t, id)
		node, err := tenure.Start(context.Background(), tenure.Config{
			Self:       s.member(),
			Membership: g.Share(),
			Store:      store,
			Spaces:     orderSpaces(),
			Timing:     fastTiming,
		})
		if err != nil {
			t.Fatalf("Start %s: %v", id, err)
		}
		t.Cleanup(func() { _ = node.Close(context.Background()) })
		s.set(localHandler(node))
		ng.servers = append(ng.servers, s)
		ng.nodes = append(ng.nodes, node)
	}
	ng.router, err = tenure.NewRouter(ng.gate, orderSpaces(), tenure.WithRouterTiming(fastTiming))
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	t.Cleanup(func() { _ = ng.router.Close() })
	h, err := ng.router.Space("orders")
	if err != nil {
		t.Fatal(err)
	}
	ng.keys = make([]string, h.Shards())
	for i, found := 0, 0; found < len(ng.keys); i++ {
		key := fmt.Sprintf("order-%d", i)
		shard, err := h.Shard(key)
		if err != nil {
			t.Fatal(err)
		}
		if ng.keys[shard] == "" {
			ng.keys[shard] = key
			found++
		}
	}
	return ng
}

// owners returns the ID of the one node whose Local accepts each key, or false while a key has
// none or more than one.
func (ng *nodeGroup) owners() (map[string]string, bool) {
	out := map[string]string{}
	for _, key := range ng.keys {
		for i, n := range ng.nodes {
			if _, err := n.Local("orders", key); err == nil {
				if out[key] != "" {
					return nil, false
				}
				out[key] = ng.servers[i].id
			}
		}
		if out[key] == "" {
			return nil, false
		}
	}
	return out, true
}

// routed returns the ID of the member the Router names for key.
func (ng *nodeGroup) routed(key string) string {
	m, err := ng.router.Route("orders", key)
	if err != nil {
		return ""
	}
	return m.ID
}

// settled waits until every key has exactly one owner that accepts it and, when withRouter is
// set, the Router names that owner for every key; it returns the owners.
func (ng *nodeGroup) settled(t *testing.T, withRouter bool) map[string]string {
	t.Helper()
	deadline := time.Now().Add(settle)
	for {
		if owners, ok := ng.owners(); ok {
			agree := true
			for key, id := range owners {
				agree = agree && (!withRouter || ng.routed(key) == id)
			}
			if agree {
				return owners
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the group did not settle within %v", settle)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// within3 fails t when a call took more than 3 attempts.
func within3(t *testing.T, what string, attempts int) {
	t.Helper()
	if attempts > 3 {
		t.Fatalf("%s took %d attempts, want at most 3", what, attempts)
	}
}

// TestATRTR20 is AT-RTR-20.
func TestATRTR20(t *testing.T) {
	ng := newNodeGroup(t)
	owners := ng.settled(t, true)
	conn := dial(t, ng.router, interceptors(ng.router)...)
	(&cluster{servers: ng.servers, router: ng.router}).waitReady(t, conn)

	t.Run("calls go to the owner's connection", func(t *testing.T) {
		for _, key := range ng.keys {
			id, attempts, err := call(conn, key)
			if err != nil || id != owners[key] || attempts != 1 {
				t.Fatalf("call for %s = %s in %d attempts, %v; want its owner %s in 1", key, id, attempts, err, owners[key])
			}
		}
	})

	t.Run("no key round-robins", func(t *testing.T) {
		before := map[string]int{}
		for _, s := range ng.servers {
			before[s.id] = len(s.received())
		}
		for i := range 30 {
			key := ng.keys[i%len(ng.keys)]
			ctx, n := counted()
			id, err := who(ctx, conn, key, false)
			if err != nil || id != owners[key] {
				t.Fatalf("call without a key for %s = %s, %v; want its owner %s", key, id, err, owners[key])
			}
			within3(t, "a call without a key", int(n.Load()))
		}
		for _, s := range ng.servers {
			if got := len(s.received()) - before[s.id]; got < 10 {
				t.Errorf("%s received %d of 30 calls without a key, want at least its 10 first attempts", s.id, got)
			}
		}
	})

	t.Run("a trailer corrects and retries", func(t *testing.T) {
		ng.gate.hold()
		drained := ng.servers[2]
		ctx, cancel := context.WithTimeout(context.Background(), settle)
		defer cancel()
		if err := ng.nodes[2].Drain(ctx); err != nil {
			t.Fatalf("Drain %s: %v", drained.id, err)
		}
		moved := ng.settled(t, false)

		stale := 0
		for shard, key := range ng.keys {
			if ng.routed(key) != drained.id {
				continue
			}
			stale++
			if shard%2 == 0 {
				id, attempts, err := call(conn, key)
				if err != nil || id != moved[key] || attempts != 2 {
					t.Fatalf("call for %s = %s in %d attempts, %v; want its new owner %s in 2", key, id, attempts, err, moved[key])
				}
			} else {
				ids, attempts, err := watch(conn, watchMethod, key)
				if err != nil || len(ids) != 3 || ids[0] != moved[key] || attempts != 2 {
					t.Fatalf("stream for %s = %v in %d attempts, %v; want 3 from its new owner %s in 2", key, ids, attempts, err, moved[key])
				}
			}
			if got, want := corrected(t, ng.router, key), ng.servers[idIndex(moved[key])].addr; got != want {
				t.Fatalf("correction of %s = %q, want %s", key, got, want)
			}
			id, attempts, err := call(conn, key)
			if err != nil || id != moved[key] || attempts != 1 {
				t.Fatalf("call for %s after the correction = %s in %d attempts, %v; want %s in 1", key, id, attempts, err, moved[key])
			}
		}
		if stale == 0 {
			t.Fatalf("the held Router routes no shard to %s; the test has nothing to correct", drained.id)
		}
		t.Logf("%d shards of %s corrected while the Router's view was held", stale, drained.id)

		// The Router's corrections make its held snapshot route like the group, so wait for
		// the snapshot of the released view before checking that it routes on its own.
		held := ng.router.Version()
		ng.gate.release()
		for deadline := time.Now().Add(settle); ng.router.Version() == held; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the Router applied no view after the release")
			}
		}
		ng.settled(t, true)
		for _, key := range ng.keys {
			id, attempts, err := call(conn, key)
			if err != nil || id != moved[key] || attempts != 1 {
				t.Fatalf("call for %s with the Router caught up = %s in %d attempts, %v; want %s in 1", key, id, attempts, err, moved[key])
			}
		}
	})
}

// idIndex returns the index of the member n<i+1>.
func idIndex(id string) int {
	var i int
	_, _ = fmt.Sscanf(id, "n%d", &i)
	return i - 1
}
