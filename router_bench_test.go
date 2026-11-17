// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"strconv"
	"testing"
)

// benchRouterKeys are the keys the lookup benchmarks cycle through: 25 bytes each, as in RTR §12.
var benchRouterKeys = func() []string {
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "orderbook/customer-" + strconv.Itoa(100000+i)
	}
	return keys
}()

// benchRouter returns a Router with markets (64 shards) routed over 16 members.
func benchRouter(b *testing.B) *Router {
	b.Helper()
	r := newTestRouter(b, routerSpaces())
	ids := make([]string, 16)
	for i := range ids {
		ids[i] = "n" + strconv.Itoa(10+i)
	}
	applyAt(r, plainView(1, ids...), 0)
	return r
}

// BenchmarkRouterRoute measures Router.Route with no correction set.
func BenchmarkRouterRoute(b *testing.B) {
	r := benchRouter(b)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, _ = r.Route("markets", benchRouterKeys[i&1023])
		i++
	}
}

// BenchmarkRouterRouteWithCorrection measures Router.Route while every shard has a correction.
func BenchmarkRouterRouteWithCorrection(b *testing.B) {
	r := benchRouter(b)
	for shard := range 64 {
		r.Correct("markets", shard, "n10:80")
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, _ = r.Route("markets", benchRouterKeys[i&1023])
		i++
	}
}
