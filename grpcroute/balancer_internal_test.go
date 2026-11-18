// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/static"
)

// fakeSubConn is a SubConn that only has a name.
type fakeSubConn struct {
	balancer.SubConn
	addr string
}

// pickerOver returns a picker of space over ready connections to addrs.
func pickerOver(space *tenure.RouteSpace, addrs ...string) balancer.Picker {
	pb := &keyPickerBuilder{}
	pb.space.Store(space)
	info := base.PickerBuildInfo{ReadySCs: map[balancer.SubConn]base.SubConnInfo{}}
	for _, a := range addrs {
		info.ReadySCs[&fakeSubConn{addr: a}] = base.SubConnInfo{Address: resolver.Address{Addr: a}}
	}
	return pb.Build(info)
}

// testSpace returns the space "orders" of 8 shards of a ready Router on members n1 to n3 at
// addresses a1 to a3.
func testSpace(t testing.TB) *tenure.RouteSpace {
	t.Helper()
	g, err := static.New([]tenure.Member{{ID: "n1", Addr: "a1"}, {ID: "n2", Addr: "a2"}, {ID: "n3", Addr: "a3"}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := tenure.NewRouter(g, []tenure.Space{{Name: "orders", Shards: 8}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	space, err := r.Space("orders")
	if err != nil {
		t.Fatal(err)
	}
	return space
}

// picked returns the address of the connection p picks for ctx.
func picked(t *testing.T, p balancer.Picker, ctx context.Context) string {
	t.Helper()
	res, err := p.Pick(balancer.PickInfo{FullMethodName: "/s/m", Ctx: ctx})
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	sc, _ := res.SubConn.(*fakeSubConn)
	return sc.addr
}

// TestRTR910PickOwner checks that a call with a key gets its owner's connection.
func TestRTR910PickOwner(t *testing.T) {
	space := testSpace(t)
	p := pickerOver(space, "a1", "a2", "a3")
	for i := range 50 {
		key := fmt.Sprintf("k%d", i)
		m, err := space.Route(key)
		if err != nil {
			t.Fatal(err)
		}
		for range 3 {
			if got := picked(t, p, WithKey(context.Background(), key)); got != m.Addr {
				t.Fatalf("Pick for %s = %s, want its owner %s", key, got, m.Addr)
			}
		}
	}
}

// TestRTR910PickRoundRobin checks that calls without a key, calls whose owner is not ready and
// calls on a picker without a RouteSpace take the ready connections in turn.
func TestRTR910PickRoundRobin(t *testing.T) {
	space := testSpace(t)
	notReady := ""
	for i := 0; notReady == ""; i++ {
		if m, _ := space.Route(fmt.Sprintf("k%d", i)); m.Addr == "a3" {
			notReady = fmt.Sprintf("k%d", i)
		}
	}
	for name, tc := range map[string]struct {
		space *tenure.RouteSpace
		ctx   context.Context
	}{
		"no key":          {space, context.Background()},
		"owner not ready": {space, WithKey(context.Background(), notReady)},
		"no RouteSpace":   {nil, WithKey(context.Background(), notReady)},
	} {
		t.Run(name, func(t *testing.T) {
			p := pickerOver(tc.space, "a2", "a1")
			first := picked(t, p, tc.ctx)
			want := map[string]string{"a1": "a2", "a2": "a1"}
			prev := first
			for range 6 {
				got := picked(t, p, tc.ctx)
				if got != want[prev] {
					t.Fatalf("after %s picked %s, want %s", prev, got, want[prev])
				}
				prev = got
			}
		})
	}
}

// TestRTR910PickNoneReady checks that a picker over no ready connection makes the call wait.
func TestRTR910PickNoneReady(t *testing.T) {
	p := pickerOver(testSpace(t))
	if _, err := p.Pick(balancer.PickInfo{Ctx: context.Background()}); !errors.Is(err, balancer.ErrNoSubConnAvailable) {
		t.Fatalf("Pick = %v, want ErrNoSubConnAvailable", err)
	}
}

// TestRTR910PickAllocatesNothing checks that Pick allocates nothing, with a key and without.
func TestRTR910PickAllocatesNothing(t *testing.T) {
	p := pickerOver(testSpace(t), "a1", "a2", "a3")
	for name, ctx := range map[string]context.Context{
		"key":    WithKey(context.Background(), "order-17"),
		"no key": context.Background(),
	} {
		info := balancer.PickInfo{FullMethodName: "/s/m", Ctx: ctx}
		if n := testing.AllocsPerRun(1000, func() { _, _ = p.Pick(info) }); n != 0 {
			t.Errorf("Pick (%s) allocates %v times, want 0", name, n)
		}
	}
}

// BenchmarkPick measures Pick with a key and without.
func BenchmarkPick(b *testing.B) {
	p := pickerOver(testSpace(b), "a1", "a2", "a3")
	for name, ctx := range map[string]context.Context{
		"key":    WithKey(context.Background(), "order-17"),
		"no-key": context.Background(),
	} {
		b.Run(name, func(b *testing.B) {
			info := balancer.PickInfo{FullMethodName: "/s/m", Ctx: ctx}
			b.ReportAllocs()
			for b.Loop() {
				_, _ = p.Pick(info)
			}
		})
	}
}

// TestRTR920PickRoute checks that the attempt after a redirect gets the connection the redirect
// names while it is ready, and the key's owner otherwise.
func TestRTR920PickRoute(t *testing.T) {
	space := testSpace(t)
	p := pickerOver(space, "a1", "a2", "a3")
	const key = "order-17"
	owner, err := space.Route(key)
	if err != nil {
		t.Fatal(err)
	}
	other := "a1"
	if owner.Addr == other {
		other = "a2"
	}
	ctx := WithKey(context.Background(), key)
	if got := picked(t, p, context.WithValue(ctx, routeCtx{}, other)); got != other {
		t.Fatalf("Pick after a redirect to %s = %s", other, got)
	}
	if got := picked(t, p, context.WithValue(ctx, routeCtx{}, "a9")); got != owner.Addr {
		t.Fatalf("Pick after a redirect to an address without a connection = %s, want the owner %s", got, owner.Addr)
	}
	info := balancer.PickInfo{FullMethodName: "/s/m", Ctx: context.WithValue(ctx, routeCtx{}, other)}
	if n := testing.AllocsPerRun(1000, func() { _, _ = p.Pick(info) }); n != 0 {
		t.Errorf("Pick after a redirect allocates %v times, want 0", n)
	}
}

// TestSpaceOf checks which targets name a space.
func TestSpaceOf(t *testing.T) {
	for target, want := range map[string]string{
		"tenure:///orders":    "orders",
		"tenure:orders":       "orders",
		"tenure://x/orders":   "orders",
		"tenure:///":          "",
		"dns:///orders":       "",
		"passthrough:///a:80": "",
		"127.0.0.1:80":        "",
		"%":                   "",
	} {
		got, ok := spaceOf(target)
		if got != want || ok != (want != "") {
			t.Errorf("spaceOf(%q) = %q, %v; want %q", target, got, ok, want)
		}
	}
}
