// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package grpcroute

import (
	"cmp"
	"context"
	"math/rand/v2"
	"slices"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/hmdsefi/tenure"
)

// BalancerName is the name of the load-balancing policy that sends each call to the member that
// owns its key. A channel selects it through its service config: see ServiceConfig.
const BalancerName = "tenure_key"

// ServiceConfig is the service config that selects the tenure_key policy. Pass it to
// grpc.NewClient with grpc.WithDefaultServiceConfig, or merge its loadBalancingConfig into the
// service config the channel already has.
const ServiceConfig = `{"loadBalancingConfig":[{"` + BalancerName + `":{}}]}`

// gRPC finds load-balancing policies by name in a registry that may be written only while the
// program initializes.
func init() { //nolint:gochecknoinits // gRPC registers balancers in init only
	balancer.Register(keyBalancerBuilder{})
}

// keyCtx is the context key of the key that WithKey sets.
type keyCtx struct{}

// WithKey returns a copy of ctx that carries key, the key of the call made with it. On a channel
// whose target is "tenure:///<space>" and whose policy is tenure_key, the call goes to the
// connection of the member that owns key in the space, in the latest snapshot of the Router.
// While that connection is not ready, or when the Router names no member for key, the call goes
// to any ready connection, and a member that does not own key answers with Misdirected.
func WithKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, keyCtx{}, key)
}

// keyFrom returns the key of ctx that WithKey set.
func keyFrom(ctx context.Context) (string, bool) {
	key, ok := ctx.Value(keyCtx{}).(string)
	return key, ok
}

// routeSpaceKey is the key of the RouteSpace in the State.Attributes a resolver sends.
type routeSpaceKey struct{}

// routeSpaceOf returns the RouteSpace of a resolver state of NewResolverBuilder, or nil.
func routeSpaceOf(s resolver.State) *tenure.RouteSpace {
	space, _ := s.Attributes.Value(routeSpaceKey{}).(*tenure.RouteSpace)
	return space
}

// keyBalancerBuilder builds the tenure_key balancers.
type keyBalancerBuilder struct{}

func (keyBalancerBuilder) Name() string { return BalancerName }

// Build returns a base balancer, which keeps a connection open to every address of the resolver
// and builds a picker over the ready ones, wrapped to hand the picker builder the RouteSpace of
// the resolver's state.
func (keyBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	pb := &keyPickerBuilder{}
	return &keyBalancer{
		Balancer: base.NewBalancerBuilder(BalancerName, pb, base.Config{}).Build(cc, opts),
		pickers:  pb,
	}
}

// keyBalancer is a base balancer that passes the RouteSpace of each resolver state to its
// picker builder before the base balancer builds a picker for that state.
type keyBalancer struct {
	balancer.Balancer
	pickers *keyPickerBuilder
}

func (b *keyBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	b.pickers.space.Store(routeSpaceOf(s.ResolverState))
	return b.Balancer.UpdateClientConnState(s)
}

// keyPickerBuilder builds the pickers of one balancer.
type keyPickerBuilder struct {
	space atomic.Pointer[tenure.RouteSpace] // nil until a resolver of NewResolverBuilder names it
}

// Build returns a picker over the ready connections: a map from address to connection, for the
// owners, and the connections sorted by address, for round-robin.
func (pb *keyPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	type ready struct {
		addr string
		sc   balancer.SubConn
	}
	all := make([]ready, 0, len(info.ReadySCs))
	for sc, sci := range info.ReadySCs {
		all = append(all, ready{addr: sci.Address.Addr, sc: sc})
	}
	slices.SortFunc(all, func(a, b ready) int { return cmp.Compare(a.addr, b.addr) })
	p := &keyPicker{
		space:  pb.space.Load(),
		byAddr: make(map[string]balancer.SubConn, len(all)),
		ready:  make([]balancer.SubConn, len(all)),
	}
	for i, r := range all {
		p.byAddr[r.addr] = r.sc
		p.ready[i] = r.sc
	}
	// Start the round-robin at a random connection, so that channels do not all begin with the
	// same member.
	p.next.Store(rand.Uint32()) //nolint:gosec // spreads calls; nothing depends on it being unpredictable
	return p
}

// keyPicker picks the connection of each call: the one a followed redirect names, while it is
// ready; else the owner's for a call with a key, while it is ready; else the next ready one in
// turn. Pick allocates nothing.
type keyPicker struct {
	space  *tenure.RouteSpace // nil: round-robin only
	byAddr map[string]balancer.SubConn
	ready  []balancer.SubConn // sorted by address, at least one
	next   atomic.Uint32
}

func (p *keyPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if addr, ok := routeFrom(info.Ctx); ok {
		if sc, ok := p.byAddr[addr]; ok {
			return balancer.PickResult{SubConn: sc}, nil
		}
	}
	if p.space != nil {
		if key, ok := keyFrom(info.Ctx); ok {
			if m, err := p.space.Route(key); err == nil {
				if sc, ok := p.byAddr[m.Addr]; ok {
					return balancer.PickResult{SubConn: sc}, nil
				}
			}
		}
	}
	n := p.next.Add(1)
	return balancer.PickResult{SubConn: p.ready[n%uint32(len(p.ready))]}, nil //nolint:gosec // len is a small positive count
}
