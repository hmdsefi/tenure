// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/hmdsefi/tenure"
)

// cluster is three test servers and a ready Router on a static group of them.
type cluster struct {
	servers []*testServer
	router  *tenure.Router
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	c := &cluster{}
	var members []tenure.Member
	for _, id := range []string{"n1", "n2", "n3"} {
		s := newServer(t, id)
		c.servers = append(c.servers, s)
		members = append(members, s.member())
	}
	_, c.router = newGroup(t, members...)
	return c
}

// owner returns the ID of the member that owns key in the Router's snapshot.
func (c *cluster) owner(t *testing.T, key string) string {
	t.Helper()
	m, err := c.router.Route("orders", key)
	if err != nil {
		t.Fatalf("Route(%q): %v", key, err)
	}
	return m.ID
}

// keyOf returns a key that member id owns.
func (c *cluster) keyOf(t *testing.T, id string) string {
	t.Helper()
	for i := range 10000 {
		if key := fmt.Sprintf("key-%d", i); c.owner(t, key) == id {
			return key
		}
	}
	t.Fatalf("no key of %s", id)
	return ""
}

// waitReady waits until conn has a ready connection to every server: a keyed call to each
// member's key reaches that member.
func (c *cluster) waitReady(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	for _, s := range c.servers {
		key := c.keyOf(t, s.id)
		for {
			if got, err := who(ctx, conn, key, true); err == nil && got == s.id {
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("no ready connection to %s", s.id)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestRTR910KeyedCallsReachOwners checks that a call with a key goes to the member that owns the
// key in the Router's snapshot, in one call.
func TestRTR910KeyedCallsReachOwners(t *testing.T) {
	c := newCluster(t)
	conn := dial(t, c.router)
	c.waitReady(t, conn)
	before := map[string]int{}
	for _, s := range c.servers {
		before[s.id] = len(s.received())
	}

	owned := map[string]int{}
	for i := range 100 {
		key := fmt.Sprintf("order-%d", i)
		got, err := who(context.Background(), conn, key, true)
		if err != nil {
			t.Fatalf("Who(%s): %v", key, err)
		}
		if want := c.owner(t, key); got != want {
			t.Fatalf("Who(%s) answered by %s, want its owner %s", key, got, want)
		}
		owned[got]++
	}
	for _, s := range c.servers {
		if got := len(s.received()) - before[s.id]; got != owned[s.id] {
			t.Errorf("%s received %d calls, want the %d of its keys", s.id, got, owned[s.id])
		}
	}
}

// TestRTR910KeylessCallsSpread checks that calls without a key go to the members in turn.
func TestRTR910KeylessCallsSpread(t *testing.T) {
	c := newCluster(t)
	conn := dial(t, c.router)
	c.waitReady(t, conn)
	got := map[string]int{}
	for i := range 30 {
		id, err := who(context.Background(), conn, fmt.Sprintf("order-%d", i), false)
		if err != nil {
			t.Fatalf("Who: %v", err)
		}
		got[id]++
	}
	for _, s := range c.servers {
		if got[s.id] != 10 {
			t.Errorf("30 calls without a key: %v, want 10 for each member", got)
			break
		}
	}
}

// TestRTR910OwnerNotReadyFallsBack checks that a call whose owner's server is down goes to a
// ready connection instead.
func TestRTR910OwnerNotReadyFallsBack(t *testing.T) {
	c := newCluster(t)
	conn := dial(t, c.router)
	c.waitReady(t, conn)
	down := c.servers[0]
	key := c.keyOf(t, down.id)
	down.srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	for {
		got, err := who(ctx, conn, key, true)
		if err == nil {
			if got == down.id {
				t.Fatalf("a call reached %s after its server stopped", got)
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("the call never fell back: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for range 20 {
		got, err := who(context.Background(), conn, key, true)
		if err != nil {
			t.Fatalf("Who(%s) with its owner down: %v", key, err)
		}
		if got == down.id {
			t.Fatalf("Who(%s) answered by the stopped %s", key, got)
		}
	}
}
