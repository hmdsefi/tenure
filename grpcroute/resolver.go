// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"

	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"

	"github.com/hmdsefi/tenure"
)

// Scheme is the scheme of the targets that the resolvers of NewResolverBuilder resolve. The
// target "tenure:///orders" names the space "orders".
const Scheme = "tenure"

// memberIDKey is the key of the member ID in the Attributes of an address that a resolver makes.
type memberIDKey struct{}

// MemberID returns the ID of the member whose address addr is, when a resolver of
// NewResolverBuilder made addr, and "" for any other address.
func MemberID(addr resolver.Address) string {
	id, _ := addr.Attributes.Value(memberIDKey{}).(string)
	return id
}

// NewResolverBuilder returns a gRPC resolver builder that resolves the target
// "tenure:///<space>" to the members of r: every address of a member of r's latest snapshot,
// with the member's ID as an attribute that MemberID reads. Each resolver sends gRPC the new
// address list after every snapshot of r that changes it, and none before r has applied its
// first view, so calls wait for it. A target whose space r does not have fails to build. Once r
// is closed, the resolvers report tenure.ErrClosed.
//
// Pass the builder to grpc.NewClient with grpc.WithResolvers, and select the tenure_key policy
// with ServiceConfig so that calls go to their owners; the resolver sends no service config of
// its own. The builder follows r from its first Build until r is closed, and serves any number
// of resolvers and targets.
func NewResolverBuilder(r *tenure.Router) resolver.Builder {
	return &resolverBuilder{router: r}
}

// resolverBuilder is the resolver.Builder of NewResolverBuilder. It takes one Subscribe channel
// of its Router and passes each version on to the live resolvers, because a Subscribe channel
// lives until the Router is closed and gRPC builds a new resolver each time a channel leaves
// idle.
type resolverBuilder struct {
	router *tenure.Router
	once   sync.Once

	mu      sync.Mutex
	live    map[*routeResolver]struct{}
	version uint64 // the latest version received, 0 before the first view
	closed  bool   // the Subscribe channel was closed: the Router is closed
}

func (b *resolverBuilder) Scheme() string { return Scheme }

func (b *resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	if b.router == nil {
		return nil, errors.New("grpcroute: NewResolverBuilder has no Router")
	}
	space, err := b.router.Space(target.Endpoint())
	if err != nil {
		return nil, fmt.Errorf("grpcroute: target %q: %w", target.String(), err)
	}
	b.once.Do(b.follow)
	r := &routeResolver{
		builder: b,
		space:   space,
		cc:      cc,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	b.mu.Lock()
	if b.live == nil {
		b.live = make(map[*routeResolver]struct{})
	}
	b.live[r] = struct{}{}
	if b.version > 0 || b.closed {
		r.notify()
	}
	b.mu.Unlock()
	r.wg.Add(1)
	go r.run()
	return r, nil
}

// follow subscribes to the Router and wakes every live resolver on each version, and once more
// when the Router is closed.
func (b *resolverBuilder) follow() {
	versions := b.router.Subscribe()
	go func() {
		for v := range versions {
			b.mu.Lock()
			b.version = v
			for r := range b.live {
				r.notify()
			}
			b.mu.Unlock()
		}
		b.mu.Lock()
		b.closed = true
		for r := range b.live {
			r.notify()
		}
		b.mu.Unlock()
	}()
}

// routerClosed reports whether the Router's Subscribe channel was closed.
func (b *resolverBuilder) routerClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// routeResolver is one resolver of a resolverBuilder: it sends its ClientConn the members of the
// Router from a goroutine of its own, never inside Build or a call from gRPC.
type routeResolver struct {
	builder *resolverBuilder
	space   *tenure.RouteSpace
	cc      resolver.ClientConn

	wake      chan struct{} // holds one wake-up; a later one is merged into it
	done      chan struct{} // closed by Close
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// notify wakes the resolver's goroutine without blocking.
func (r *routeResolver) notify() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// run sends the ClientConn the address list after each wake-up that changed it, until Close,
// and reports tenure.ErrClosed once the Router is closed.
func (r *routeResolver) run() {
	defer r.wg.Done()
	var last []resolver.Address
	for {
		select {
		case <-r.done:
			return
		case <-r.wake:
		}
		if r.builder.routerClosed() {
			r.cc.ReportError(fmt.Errorf("grpcroute: %w", tenure.ErrClosed))
			return
		}
		t := r.builder.router.Table()
		if t.Version == 0 {
			continue
		}
		addrs := memberAddresses(t.Members)
		if last != nil && slices.EqualFunc(addrs, last, sameAddress) {
			continue
		}
		last = addrs
		_ = r.cc.UpdateState(resolver.State{Addresses: addrs, Attributes: attributes.New(routeSpaceKey{}, r.space)})
	}
}

// ResolveNow does nothing: the resolver sends every change of the Router as it happens.
func (r *routeResolver) ResolveNow(resolver.ResolveNowOptions) {}

// Close stops the resolver and waits until its goroutine has returned.
func (r *routeResolver) Close() {
	r.closeOnce.Do(func() { close(r.done) })
	r.wg.Wait()
	r.builder.mu.Lock()
	delete(r.builder.live, r)
	r.builder.mu.Unlock()
}

// memberAddresses returns one address for each distinct non-empty Addr of members, sorted by
// Addr, with the ID of the member as an attribute; members that share an Addr give the lowest
// ID. The result is never nil.
func memberAddresses(members []tenure.Member) []resolver.Address {
	byID := slices.Clone(members)
	slices.SortFunc(byID, func(a, b tenure.Member) int { return cmp.Compare(a.ID, b.ID) })
	seen := make(map[string]bool, len(byID))
	addrs := make([]resolver.Address, 0, len(byID))
	for _, m := range byID {
		if m.Addr == "" || seen[m.Addr] {
			continue
		}
		seen[m.Addr] = true
		addrs = append(addrs, resolver.Address{Addr: m.Addr, Attributes: attributes.New(memberIDKey{}, m.ID)})
	}
	slices.SortFunc(addrs, func(a, b resolver.Address) int { return cmp.Compare(a.Addr, b.Addr) })
	return addrs
}

// sameAddress reports whether a and b name the same member at the same address.
func sameAddress(a, b resolver.Address) bool {
	return a.Addr == b.Addr && MemberID(a) == MemberID(b)
}
