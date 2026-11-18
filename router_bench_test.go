// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/hmdsefi/tenure/core"
)

// benchRouterKeys are the keys the lookup benchmarks cycle through: 25 bytes each, as in RTR §12.
var benchRouterKeys = func() []string {
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "orderbook/customer-" + strconv.Itoa(100000+i)
	}
	return keys
}()

// benchIDs returns n member IDs that sort in order: "n0000", "n0001", ….
func benchIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%04d", i)
	}
	return ids
}

// benchRouter returns a Router with markets (64 shards) routed over 16 members.
func benchRouter(tb testing.TB) *Router {
	tb.Helper()
	r := newTestRouter(tb, routerSpaces())
	applyAt(r, plainView(1, benchIDs(16)...), 0)
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

// BenchmarkRouterSpaceRoute measures RouteSpace.Route, which skips the space lookup of
// Router.Route, with no correction set.
func BenchmarkRouterSpaceRoute(b *testing.B) {
	r := benchRouter(b)
	sp, err := r.Space("markets")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, _ = sp.Route(benchRouterKeys[i&1023])
		i++
	}
}

// BenchmarkRouterRouteParallel measures Router.Route from GOMAXPROCS goroutines at once.
func BenchmarkRouterRouteParallel(b *testing.B) {
	benchRouteParallel(b, benchRouter(b))
}

// benchRouteParallel measures r.Route from GOMAXPROCS goroutines, each cycling through the keys
// from its own offset.
func benchRouteParallel(b *testing.B, r *Router) {
	b.ReportAllocs()
	var next sync.Mutex
	offset := 0
	b.RunParallel(func(pb *testing.PB) {
		next.Lock()
		i := offset
		offset += 97
		next.Unlock()
		for pb.Next() {
			_, _ = r.Route("markets", benchRouterKeys[i&1023])
			i++
		}
	})
}

// BenchmarkRouterRouteWithCorrection measures Router.Route while every shard has a correction.
func BenchmarkRouterRouteWithCorrection(b *testing.B) {
	r := benchRouter(b)
	for shard := range 64 {
		r.Correct("markets", shard, "n0000:80")
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, _ = r.Route("markets", benchRouterKeys[i&1023])
		i++
	}
}

// BenchmarkRouterRouteDuringRebuild measures Router.Route while another goroutine applies views
// without a pause, each with other placement keys than the one before, so that each runs
// Balanced and publishes a new snapshot. The allocation counts include those of the builds, which
// the lookups do not make; snapshots reports how many snapshots were published meanwhile.
func BenchmarkRouterRouteDuringRebuild(b *testing.B) {
	r := benchRouter(b)
	ids := benchIDs(17)
	views := [2]core.View{plainView(0, ids[:16]...), plainView(0, ids[1:]...)}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			v := views[i&1]
			v.Version = uint64(i + 2)
			applyAt(r, v, 0)
		}
	}()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, _ = r.Route("markets", benchRouterKeys[i&1023])
		i++
	}
	b.StopTimer()
	close(stop)
	<-done
	b.ReportMetric(float64(r.Version()), "snapshots")
}

// BenchmarkRouterRebuild measures applying a view whose placement keys differ from the last, so
// that the Router runs Balanced and builds a new snapshot, for the sizes of RTR §12: one space of
// the given shards over the given keys (members). The sizes with 4096 shards are skipped under
// -short.
func BenchmarkRouterRebuild(b *testing.B) {
	for _, size := range []struct{ shards, keys int }{
		{64, 3}, {1024, 16}, {1024, 64}, {4096, 64}, {4096, 256},
	} {
		b.Run(fmt.Sprintf("shards=%d/keys=%d", size.shards, size.keys), func(b *testing.B) {
			if testing.Short() && size.shards > 1024 {
				b.Skip("skipped under -short")
			}
			r := newTestRouter(b, []Space{{Name: "s", Shards: size.shards}})
			ids := benchIDs(size.keys + 1)
			// Each view drops one member and adds another, so the keys change every time.
			views := [2]core.View{plainView(0, ids[:size.keys]...), plainView(0, ids[1:]...)}
			v := views[0]
			v.Version = 1
			applyAt(r, v, 0)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				v := views[(i+1)&1]
				v.Version = uint64(i + 2)
				applyAt(r, v, 0)
				i++
			}
			if st := buildStats(r); st.balanced != st.snapshots {
				b.Fatalf("%d of %d snapshots ran Balanced, want all", st.balanced, st.snapshots)
			}
		})
	}
}

// BenchmarkRouterRebuildRemap measures applying a view whose placement keys stay the same and
// whose members change only their Addr: the Router maps the keys to the new members without
// Balanced, for 4096 shards over 64 members.
func BenchmarkRouterRebuildRemap(b *testing.B) {
	r := newTestRouter(b, []Space{{Name: "s", Shards: 4096}})
	views := [2]core.View{plainView(0, benchIDs(64)...), plainView(0, benchIDs(64)...)}
	for i := range views[1].Members {
		views[1].Members[i].Addr += "1"
	}
	v := views[0]
	v.Version = 1
	applyAt(r, v, 0)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		v := views[(i+1)&1]
		v.Version = uint64(i + 2)
		applyAt(r, v, 0)
		i++
	}
	if st := buildStats(r); st.balanced != 1 {
		b.Fatalf("%d snapshots ran Balanced, want only the first", st.balanced)
	}
}

// TestRouterParallelScaling checks RTR §12's loose gate on parallel lookups: at GOMAXPROCS=N,
// for N = 2, 4 and the machine's cores up to 8, the ns/op of RunParallel is at most 3 times the
// serial ns/op divided by N. It runs where timing means something: not under -short, -race or
// coverage, and on 4 cores or more. A noisy measurement gets two more tries.
func TestRouterParallelScaling(t *testing.T) {
	switch {
	case testing.Short():
		t.Skip("timing test skipped under -short")
	case raceEnabled:
		t.Skip("timing test skipped under -race")
	case testing.CoverMode() != "":
		t.Skip("timing test skipped under coverage")
	case runtime.NumCPU() < 4:
		t.Skipf("timing test needs 4 cores, have %d", runtime.NumCPU())
	}
	r := benchRouter(t)
	serial := func(b *testing.B) {
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			_, _ = r.Route("markets", benchRouterKeys[i&1023])
			i++
		}
	}
	parallel := func(b *testing.B) { benchRouteParallel(b, r) }
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)

	limit := min(runtime.NumCPU(), 8)
	procs := []int{2, 4}
	if limit > 4 {
		procs = append(procs, limit)
	}
	serialNs := nsPerOp(testing.Benchmark(serial))
	for _, n := range procs {
		var parallelNs float64
		ok := false
		for try := 0; try < 3 && !ok; try++ {
			if try > 0 { // measure both again
				runtime.GOMAXPROCS(prev)
				serialNs = nsPerOp(testing.Benchmark(serial))
			}
			runtime.GOMAXPROCS(n)
			parallelNs = nsPerOp(testing.Benchmark(parallel))
			ok = parallelNs <= 3*serialNs/float64(n)
		}
		t.Logf("GOMAXPROCS=%d: serial %.2f ns/op, parallel %.2f ns/op, bound %.2f ns/op",
			n, serialNs, parallelNs, 3*serialNs/float64(n))
		if !ok {
			t.Errorf("GOMAXPROCS=%d: parallel lookups take %.2f ns/op, above 3 x %.2f / %d = %.2f",
				n, parallelNs, serialNs, n, 3*serialNs/float64(n))
		}
	}
}

// nsPerOp returns res's time per operation in nanoseconds, with fractions.
func nsPerOp(res testing.BenchmarkResult) float64 {
	if res.N == 0 {
		return 0
	}
	return float64(res.T.Nanoseconds()) / float64(res.N)
}
