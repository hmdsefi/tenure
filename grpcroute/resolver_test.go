// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute_test

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/grpcroute"
	"github.com/hmdsefi/tenure/static"
)

// wait is how long a test waits for something that should happen at once.
const wait = 5 * time.Second

// quiet is how long a test waits to see that something does not happen.
const quiet = 200 * time.Millisecond

// fakeCC is a resolver.ClientConn that records what a resolver sends it.
type fakeCC struct {
	states chan resolver.State
	errs   chan error
}

func newFakeCC() *fakeCC {
	return &fakeCC{states: make(chan resolver.State, 64), errs: make(chan error, 64)}
}

func (c *fakeCC) UpdateState(s resolver.State) error { c.states <- s; return nil }
func (c *fakeCC) ReportError(err error)              { c.errs <- err }
func (c *fakeCC) NewAddress([]resolver.Address)      {}
func (c *fakeCC) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{Err: errors.New("fakeCC parses no service config")}
}

// next returns the next state the resolver sent, failing t if none comes within wait.
func (c *fakeCC) next(t *testing.T) resolver.State {
	t.Helper()
	select {
	case s := <-c.states:
		return s
	case err := <-c.errs:
		t.Fatalf("resolver reported %v, want a state", err)
	case <-time.After(wait):
		t.Fatal("no state from the resolver")
	}
	return resolver.State{}
}

// none fails t if the resolver sends anything within quiet.
func (c *fakeCC) none(t *testing.T) {
	t.Helper()
	select {
	case s := <-c.states:
		t.Fatalf("resolver sent %s, want nothing", addrs(s))
	case err := <-c.errs:
		t.Fatalf("resolver reported %v, want nothing", err)
	case <-time.After(quiet):
	}
}

// addrs formats the addresses of s as "id@addr" in order.
func addrs(s resolver.State) string {
	var out []string
	for _, a := range s.Addresses {
		out = append(out, grpcroute.MemberID(a)+"@"+a.Addr)
	}
	return strings.Join(out, " ")
}

// member returns the member id with the address "<addr>:80".
func member(id, addr string) tenure.Member {
	return tenure.Member{ID: id, Addr: addr + ":80"}
}

// newRouter returns a ready Router on m with the one space "orders" of 8 shards.
func newRouter(t *testing.T, m tenure.Membership) *tenure.Router {
	t.Helper()
	r, err := tenure.NewRouter(m, []tenure.Space{{Name: "orders", Shards: 8}})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	if err := r.Ready(ctx); err != nil {
		t.Fatalf("Ready: %v", err)
	}
	return r
}

// newGroup returns a static group of members and a ready Router on it.
func newGroup(t *testing.T, members ...tenure.Member) (*static.Membership, *tenure.Router) {
	t.Helper()
	g, err := static.New(members)
	if err != nil {
		t.Fatalf("static.New: %v", err)
	}
	return g, newRouter(t, g.Share())
}

// target returns the resolver target of s.
func target(t *testing.T, s string) resolver.Target {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return resolver.Target{URL: *u}
}

// build builds a resolver of b for tenure:///orders on a new fakeCC and closes it at the end.
func build(t *testing.T, b resolver.Builder) (resolver.Resolver, *fakeCC) {
	t.Helper()
	cc := newFakeCC()
	res, err := b.Build(target(t, "tenure:///orders"), cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Cleanup(res.Close)
	return res, cc
}

// TestRTR900ResolverFollowsMembers checks that a resolver lists every member of the Router's
// snapshot, sorted by address and with its ID, and follows joins and leaves of a static group.
func TestRTR900ResolverFollowsMembers(t *testing.T) {
	g, r := newGroup(t, member("n2", "10.0.0.2"), member("n1", "10.0.0.1"))
	b := grpcroute.NewResolverBuilder(r)
	if got := b.Scheme(); got != "tenure" {
		t.Fatalf("Scheme() = %q, want tenure", got)
	}
	_, cc := build(t, b)
	if got, want := addrs(cc.next(t)), "n1@10.0.0.1:80 n2@10.0.0.2:80"; got != want {
		t.Fatalf("first state = %s, want %s", got, want)
	}

	n3 := g.Share()
	if err := n3.Join(context.Background(), member("n3", "10.0.0.3")); err != nil {
		t.Fatalf("Join n3: %v", err)
	}
	if got, want := addrs(cc.next(t)), "n1@10.0.0.1:80 n2@10.0.0.2:80 n3@10.0.0.3:80"; got != want {
		t.Fatalf("after n3 joined = %s, want %s", got, want)
	}

	n1 := g.Share()
	if err := n1.Join(context.Background(), member("n1", "10.0.0.1")); err != nil {
		t.Fatalf("Join n1: %v", err)
	}
	cc.none(t) // n1 was listed already: the same members at the same addresses
	if err := n1.Leave(context.Background()); err != nil {
		t.Fatalf("Leave n1: %v", err)
	}
	if got, want := addrs(cc.next(t)), "n2@10.0.0.2:80 n3@10.0.0.3:80"; got != want {
		t.Fatalf("after n1 left = %s, want %s", got, want)
	}

	if err := n3.SetDraining(context.Background(), true); err != nil {
		t.Fatalf("SetDraining n3: %v", err)
	}
	cc.none(t) // a new snapshot with the same addresses sends nothing
	if err := n3.Leave(context.Background()); err != nil {
		t.Fatalf("Leave n3: %v", err)
	}
	if got, want := addrs(cc.next(t)), "n2@10.0.0.2:80"; got != want {
		t.Fatalf("after n3 left = %s, want %s", got, want)
	}
}

// TestRTR900ResolverMemberAddresses checks that members without an address are left out and
// that members sharing an address give the lowest ID.
func TestRTR900ResolverMemberAddresses(t *testing.T) {
	_, r := newGroup(t,
		tenure.Member{ID: "n0"},
		member("n9", "10.0.0.1"),
		member("n5", "10.0.0.2"),
		member("n4", "10.0.0.1"),
	)
	_, cc := build(t, grpcroute.NewResolverBuilder(r))
	if got, want := addrs(cc.next(t)), "n4@10.0.0.1:80 n5@10.0.0.2:80"; got != want {
		t.Fatalf("state = %s, want %s", got, want)
	}
}

// TestRTR900ResolverResolversShareBuilder checks that every resolver of one builder gets the
// updates, also one built after the first snapshot, and that a closed resolver sends nothing.
func TestRTR900ResolverResolversShareBuilder(t *testing.T) {
	g, r := newGroup(t, member("n1", "10.0.0.1"))
	b := grpcroute.NewResolverBuilder(r)
	first, cc1 := build(t, b)
	if got := addrs(cc1.next(t)); got != "n1@10.0.0.1:80" {
		t.Fatalf("first resolver = %s", got)
	}
	_, cc2 := build(t, b)
	if got := addrs(cc2.next(t)); got != "n1@10.0.0.1:80" {
		t.Fatalf("second resolver = %s", got)
	}
	first.Close()
	first.Close() // a second Close does nothing
	first.ResolveNow(resolver.ResolveNowOptions{})

	if err := g.Share().Join(context.Background(), member("n2", "10.0.0.2")); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if got, want := addrs(cc2.next(t)), "n1@10.0.0.1:80 n2@10.0.0.2:80"; got != want {
		t.Fatalf("second resolver after a join = %s, want %s", got, want)
	}
	cc1.none(t)
}

// TestRTR900ResolverBuildErrors checks the errors of Build: an unknown space, no Router and a
// closed Router.
func TestRTR900ResolverBuildErrors(t *testing.T) {
	_, r := newGroup(t, member("n1", "10.0.0.1"))
	_, err := grpcroute.NewResolverBuilder(r).Build(target(t, "tenure:///carts"), newFakeCC(), resolver.BuildOptions{})
	if want := `grpcroute: target "tenure:///carts": tenure: unknown space "carts"`; err == nil || err.Error() != want {
		t.Errorf("Build(tenure:///carts) = %v, want %s", err, want)
	}
	_, err = grpcroute.NewResolverBuilder(nil).Build(target(t, "tenure:///orders"), newFakeCC(), resolver.BuildOptions{})
	if want := "grpcroute: NewResolverBuilder has no Router"; err == nil || err.Error() != want {
		t.Errorf("Build without a Router = %v, want %s", err, want)
	}
	_ = r.Close()
	_, err = grpcroute.NewResolverBuilder(r).Build(target(t, "tenure:///orders"), newFakeCC(), resolver.BuildOptions{})
	if !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("Build on a closed Router = %v, want ErrClosed", err)
	}
}

// TestRTR900ResolverRouterClosed checks that a resolver reports ErrClosed once its Router is
// closed, and nothing after that.
func TestRTR900ResolverRouterClosed(t *testing.T) {
	_, r := newGroup(t, member("n1", "10.0.0.1"))
	_, cc := build(t, grpcroute.NewResolverBuilder(r))
	cc.next(t)
	_ = r.Close()
	select {
	case err := <-cc.errs:
		if !errors.Is(err, tenure.ErrClosed) || err.Error() != "grpcroute: tenure: closed" {
			t.Fatalf("reported %q, want grpcroute: tenure: closed", err)
		}
	case s := <-cc.states:
		t.Fatalf("sent %s after Close, want ErrClosed", addrs(s))
	case <-time.After(wait):
		t.Fatal("no error after the Router closed")
	}
	cc.none(t)
}

// viewlessMembership is a Membership that never delivers a view.
type viewlessMembership struct{}

func (viewlessMembership) Join(context.Context, core.Member) error { return nil }
func (viewlessMembership) SetDraining(context.Context, bool) error { return nil }
func (viewlessMembership) Leave(context.Context) error             { return nil }
func (viewlessMembership) Watch(ctx context.Context) (<-chan core.View, error) {
	return make(chan core.View), nil
}

// TestRTR900ResolverNoViewYet checks that a resolver sends nothing before its Router has a view.
func TestRTR900ResolverNoViewYet(t *testing.T) {
	r, err := tenure.NewRouter(viewlessMembership{}, []tenure.Space{{Name: "orders", Shards: 8}})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	_, cc := build(t, grpcroute.NewResolverBuilder(r))
	cc.none(t)
}

// TestRTR900ResolverOnClientConn checks the resolver inside a real gRPC channel: a call on
// tenure:///orders reaches a member's server.
func TestRTR900ResolverOnClientConn(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	_, r := newGroup(t, tenure.Member{ID: "n1", Addr: lis.Addr().String()})
	conn, err := grpc.NewClient("tenure:///orders",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(grpcroute.NewResolverBuilder(r)),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Check = %v, want SERVING", resp.GetStatus())
	}
}

// TestMemberIDOtherAddress checks that MemberID is "" for an address no resolver made.
func TestMemberIDOtherAddress(t *testing.T) {
	if got := grpcroute.MemberID(resolver.Address{Addr: "10.0.0.1:80"}); got != "" {
		t.Fatalf("MemberID = %q, want \"\"", got)
	}
}
