# Routing requests

How a process outside a tenure group sends each request straight to the member that owns its
shard. This page starts with the measurements the routing packages rest on; the guide to the
patterns follows with the v0.2.0 release.

## Measurements

### What is measured

The benchmarks are in [`router_bench_test.go`](../router_bench_test.go). Each lookup benchmark
routes keys of 25 bytes in a space of 64 shards over 16 members:

| Benchmark | What it measures |
|---|---|
| `RouterRoute` | `Router.Route`, no correction set |
| `RouterSpaceRoute` | `RouteSpace.Route`, which skips the space's map lookup |
| `RouterRouteParallel` | `Router.Route` from GOMAXPROCS goroutines at once |
| `RouterRouteWithCorrection` | `Router.Route` while every shard has a live correction |
| `RouterRouteDuringRebuild` | `Router.Route` while another goroutine applies views without a pause, each of which reruns the balanced assignment and publishes a new snapshot |
| `RouterRebuild/shards=S/keys=K` | applying a view that changes the placement keys: the balanced assignment of S shards over K keys, and a new snapshot |
| `RouterRebuildRemap` | applying a view that changes only members' addresses, for 4096 shards over 64 members: the members are mapped again without the balanced assignment |

To reproduce them, from the repository root:

```sh
go test -run '^$' -bench Router -benchmem -count 10 ./ > router.txt
go test -run '^$' -bench '^BenchmarkRouterRoute(Parallel)?$' -benchmem -count 10 -cpu 1,2,4 ./ > parallel.txt
go run golang.org/x/perf/cmd/benchstat@v0.0.0-20261117185222-4febdb082b2c router.txt
go run golang.org/x/perf/cmd/benchstat@v0.0.0-20261117185222-4febdb082b2c parallel.txt
```

The [router bench](../.github/workflows/router-bench.yml) workflow runs the same commands on a
GitHub-hosted runner when started by hand, and attaches the raw outputs and benchstat's summary
to the run.

### Runs

| Run | Commit | Go | Hardware |
|---|---|---|---|
| CI | `247cc10`, 2026-11-18 | go1.27.4 linux/amd64 | GitHub-hosted `ubuntu-latest` runner (ubuntu26 image 20261111.196.1): AMD EPYC 9V45, 4 cores, 15 GiB |
| Laptop | `247cc10`, 2026-11-18 | go1.27.3 darwin/arm64 | Apple M5 Pro, 18 cores, 48 GiB, macOS 27.2.1 |

Each number is benchstat's median of 10 runs. The variation was at most ±3% on the runner and
±1% on the laptop.

### Lookups

| Benchmark | CI runner | Laptop | Allocations |
|---|---:|---:|---:|
| `RouterRoute` | 40.3 ns | 36.8 ns | 0 |
| `RouterSpaceRoute` | 26.9 ns | 19.3 ns | 0 |
| `RouterRouteWithCorrection` | 84.6 ns | 63.4 ns | 0 |
| `RouterRouteDuringRebuild` | 49.4 ns | 42.8 ns | 0 in the lookups |
| `RouterRouteParallel`, all cores | 13.6 ns (4 cores) | 2.03 ns (18 cores) | 0 |

During `RouterRouteDuringRebuild` the builder published about 21,000 snapshots on the runner and
42,500 on the laptop per run; its allocations, 25 B per lookup on the runner, are the builds',
not the lookups'.

A lookup reads one immutable snapshot through an atomic pointer and writes no shared memory, so
lookups from many goroutines don't contend:

| GOMAXPROCS | CI runner | Laptop | Ideal on the laptop (serial ÷ N) |
|---:|---:|---:|---:|
| 1 (serial `RouterRoute`) | 40.6 ns | 37.0 ns | 37.0 ns |
| 2 | 16.8 ns | 13.0 ns | 18.5 ns |
| 4 | 13.7 ns | 6.54 ns | 9.24 ns |
| 8 | | 3.71 ns | 4.62 ns |
| 18 | | 2.08 ns | 2.05 ns |

`RunParallel` with one goroutine takes 33.2 ns on the runner and 24.6 ns on the laptop: it
spreads its loop's own cost differently from the serial benchmark, which is why some parallel
numbers beat the ideal.

### Rebuilds

| Benchmark | CI runner | Laptop | Allocations |
|---|---:|---:|---:|
| `RouterRebuild/shards=64/keys=3` | 0.0071 ms | 0.0048 ms | 14 |
| `RouterRebuild/shards=1024/keys=16` | 1.40 ms | 0.89 ms | 22 |
| `RouterRebuild/shards=1024/keys=64` | 6.12 ms | 4.15 ms | 24 |
| `RouterRebuild/shards=4096/keys=64` | 27.4 ms | 18.4 ms | 24 |
| `RouterRebuild/shards=4096/keys=256` | 120 ms | 79.4 ms | 26 |
| `RouterRebuildRemap` (4096 shards) | 0.053 ms | 0.034 ms | 19 |

A rebuild runs on the Router's own goroutine, never on the lookup path: lookups keep reading the
last snapshot until the new one is published. The balanced assignment runs only when the
placement keys change. With slots the placement keys are the slots, so a rolling deploy that
keeps the slot count only remaps.

### Budgets

| Budget | Result | How it is held |
|---|---|---|
| Lookups allocate nothing, also on errors and with corrections | 0 allocations everywhere above | gated: `TestATRTR04RouterAllocs` and `TestATRTR04RouterAllocsWithCorrection` run in every test job |
| Parallel lookups at GOMAXPROCS=N take at most 1.5× the serial time ÷ N | 1.35× on the runner at 4 cores, 1.01× on the laptop at 18 | gated loosely, at 3×, by `TestRouterParallelScaling` in the Go build job, for N = 2, 4 and the cores up to 8 |
| Lookups during continuous rebuilds stay within 2× of idle | 1.23× on the runner, 1.16× on the laptop | published |
| A remap without the balanced assignment stays under 1 ms for 4096 shards | 0.053 ms on the runner | published |

The ns/op of a lookup is published, not gated: hosted runners vary too much from one run to the
next for a time budget to mean anything.
