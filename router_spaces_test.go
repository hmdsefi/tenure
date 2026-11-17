// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

func TestAdvertised(t *testing.T) {
	self := Member{ID: "n1", Meta: map[string]string{"zone": "a"}}
	got := advertised(self, []Space{{Name: "orders", Shards: 8}, {Name: "markets", Shards: 64}, {Name: "a,b:c%", Shards: 2}})
	want := map[string]string{"zone": "a", "tenure.spaces": "a%2Cb%3Ac%25:2,markets:64,orders:8"}
	if !maps.Equal(got.Meta, want) {
		t.Fatalf("Meta = %v, want %v", got.Meta, want)
	}
	if len(self.Meta) != 1 {
		t.Fatalf("advertised modified the caller's Meta: %v", self.Meta)
	}
	for space, n := range map[string]int{"orders": 8, "markets": 64, "a,b:c%": 2} {
		if got, ok := advertisedShards(got.Meta[spacesMetaKey], space); !ok || got != n {
			t.Errorf("advertisedShards(%q) = %d, %v, want %d", space, got, ok, n)
		}
	}
	if _, ok := advertisedShards(got.Meta[spacesMetaKey], "carts"); ok {
		t.Error("advertisedShards found a space not advertised")
	}
	if _, ok := advertisedShards("orders:x,junk,:3", "orders"); ok {
		t.Error("advertisedShards read an unreadable count")
	}
	if m := advertised(Member{ID: "n1"}, nil); m.Meta != nil {
		t.Fatalf("no spaces: Meta = %v, want nil", m.Meta)
	}
	if m := advertised(Member{ID: "n1"}, []Space{{Name: "s", Shards: 1}}); m.Meta[spacesMetaKey] != "s:1" {
		t.Fatalf("nil Meta: %v", m.Meta)
	}
}

// advertising returns v with every member listed in ids advertising adv.
func advertising(v core.View, adv string, ids ...string) core.View {
	v.Members = append([]core.Member(nil), v.Members...)
	for i := range v.Members {
		for _, id := range ids {
			if v.Members[i].ID == id {
				v.Members[i].Meta = map[string]string{spacesMetaKey: adv}
			}
		}
	}
	return v
}

// TestATRTR10RouterMismatch checks that a Router with 8 shards against a member that advertises
// 16 fails the space's lookups with ErrSpaceMismatch, without allocating, refuses corrections in
// it, routes its other spaces, ignores members that advertise nothing or that it does not route
// to, and recovers once no member disagrees (AT-RTR-10).
func TestATRTR10RouterMismatch(t *testing.T) {
	spaces := routerSpaces()
	r := newTestRouter(t, spaces)
	h, _ := r.Space("orders")

	// Members that advertise nothing are not checked; one that agrees is fine.
	v := advertising(plainView(1, "n1", "n2", "n3"), "markets:64,orders:8", "n2")
	applyAt(r, v, 0)
	checkLikeNode(t, r, spaces, v, routerWall)

	v = advertising(plainView(2, "n1", "n2", "n3"), "markets:64,orders:16", "n3")
	applyAt(r, v, time.Second)
	_, err := r.Route("orders", "k1")
	var mm *SpaceMismatchError
	if !errors.Is(err, ErrSpaceMismatch) || !errors.As(err, &mm) {
		t.Fatalf("Route(orders) = %v, want a *SpaceMismatchError", err)
	}
	want := SpaceMismatchError{Space: "orders", Shards: 8, Member: "n3", MemberShards: 16}
	if *mm != want {
		t.Fatalf("error = %+v, want %+v", *mm, want)
	}
	if err.Error() != "tenure: space mismatch: orders has 8 shards here and 16 on member n3" {
		t.Fatalf("text = %q", err.Error())
	}
	for shard := range 8 {
		if _, err := h.RouteShard(shard); err != mm {
			t.Fatalf("RouteShard(%d) = %v, want the same mismatch", shard, err)
		}
	}
	if m, err := r.Route("markets", "k1"); err != nil || m.ID == "" {
		t.Fatalf("Route(markets) = %+v, %v; the other space should route", m, err)
	}
	if r.Correct("orders", 0, "n1:80") {
		t.Fatal("Correct in a mismatched space = true")
	}
	for name, f := range map[string]func(){
		"Route":            func() { _, _ = r.Route("orders", "k1") },
		"RouteShard":       func() { _, _ = r.RouteShard("orders", 1) },
		"RouteSpace.Route": func() { _, _ = h.Route("k1") },
	} {
		if n := testing.AllocsPerRun(100, f); n != 0 {
			t.Errorf("%s with a mismatch: %v allocations, want 0", name, n)
		}
	}

	// A draining member is not routed to without slots, so its count is not checked.
	v = advertising(plainView(3, "n1", "n2", "n3"), "markets:64,orders:16", "n3")
	v.Members[2].Draining = true
	applyAt(r, v, 2*time.Second)
	if _, err := r.Route("orders", "k1"); err != nil {
		t.Fatalf("Route with only a draining member disagreeing = %v", err)
	}

	// Recovery: every member agrees.
	applyAt(r, advertising(plainView(4, "n1", "n2"), "markets:64,orders:16", "n2"), 3*time.Second)
	if _, err := r.Route("orders", "k1"); !errors.Is(err, ErrSpaceMismatch) {
		t.Fatalf("mismatch again: %v", err)
	}
	v = advertising(plainView(5, "n1", "n2", "n3"), "orders:8,markets:64", "n1", "n2", "n3")
	applyAt(r, v, 4*time.Second)
	checkLikeNode(t, r, spaces, v, routerWall)
	if !r.Correct("orders", 0, "n1:80") {
		t.Fatal("Correct after recovery = false")
	}
}

// TestRouterMismatchWithSlots checks that with slots, the holder of a slot is checked even while
// it drains, since the table still routes to it.
func TestRouterMismatchWithSlots(t *testing.T) {
	r := newTestRouter(t, routerSpaces())
	v := advertising(slottedView(1, []string{"n1", "n2"}, "n1", "n2"), "markets:32,orders:8", "n2")
	v.Members[1].Draining = true
	applyAt(r, v, 0)
	var mm *SpaceMismatchError
	if _, err := r.Route("markets", "k"); !errors.As(err, &mm) || mm.Member != "n2" || mm.MemberShards != 32 || mm.Shards != 64 {
		t.Fatalf("Route(markets) = %v, want n2's mismatch", err)
	}
	if _, err := r.Route("orders", "k"); err != nil {
		t.Fatalf("Route(orders) = %v", err)
	}
}
