# Routing requests

How a process outside a tenure group sends each request straight to the member that owns its
key's shard: which pattern fits which service, what each costs, and how each behaves when views
lag.

- [The problem](#the-problem)
- [The patterns](#the-patterns)
- [A Router](#a-router)
- [Choosing a membership source](#choosing-a-membership-source)
- [The protocol](#the-protocol)
- [Staleness and corrections](#staleness-and-corrections)
- [Running many routers](#running-many-routers)
- [Building a proxy](#building-a-proxy)
- [Security of the view relay](#security-of-the-view-relay)
- [Measurements](#measurements)

## The problem

tenure answers "which member owns this key" inside the group: `Node.Route` and `Node.Local` read
the table that every member computes from the same views. A caller outside the group can't ask
them. Sent to any member behind a Kubernetes Service, a request lands on its owner with
probability 1/M on a group of M members: with 3 members two requests of three take a second hop,
with 20 members nineteen of twenty do. The second hop doubles the request's latency and adds the
load of every request to the members that don't own it.

A `tenure.Router` computes the members' table in the caller's process, from the same views, so in
steady state every request takes one hop. tenure stays a library: it ships the packages that
route and no gateway, proxy or routing service. A routing process is whatever process sends the
requests: a client, another service, or a proxy you write.

## The patterns

| Pattern | Where it runs | Hops in steady state | What it needs |
|---|---|---|---|
| `Node.Route` and `Node.Local` | in the members | 1 for requests that arrive at their owner, else 2 (forward) or a redirect | nothing beyond the node |
| A `Router` and your own code | in any process that sends requests | 1 | a membership source the Router can watch |
| `routehttp.Transport` | an HTTP client with a `Router` | 1, with redirects followed for it | members that answer as the protocol says |
| `grpcroute` | a gRPC client with a `Router` | 1, with redirects followed for it | members that answer as the protocol says |
| A view relay (`viewhttp`) | members, or a relay Deployment, serve views to Routers | as above | an HTTP endpoint inside the cluster |

**Inside the members.** A member that receives a request for a key it doesn't own can forward it
to the owner (the signer's Kubernetes program does) or redirect the caller with a `421` (the orderbook
does). That keeps callers simple and costs the extra hop on most requests. Every member checks
ownership with `Node.Local` before it acts on a request in any pattern: the routing packages make
the first send right, the member's check and the fence keep it safe.

**A Router in the caller.** The caller embeds a `Router` that follows the group's views and looks
the owner up per request: a hash and two array reads, with no lock and no allocation. Use it
directly, or through one of the transports.

**The transports.** `routehttp.Transport` is an `http.RoundTripper` and `grpcroute` a resolver, a
load-balancing policy and interceptors. Both send each request to its owner and follow a member's
redirect when the Router's view lags; `routehttp.Transport` also reports each send through
`OnSend`. `grpcroute` is a module of its own, so that the root module stays free of gRPC:
`go get github.com/hmdsefi/tenure/grpcroute`.

**A relay.** Each Router watches the membership source on its own. With many routers, or callers
that may not read Leases, the members relay their views over HTTP instead, and the store's load
no longer grows with the number of routers.

## A Router

`tenure.NewRouter` takes a `Membership` and the same `[]tenure.Space` the members use; it reads
only each space's name, shard count and `ShardOf`. On Kubernetes, a watch-only Adapter follows the
group's Leases and writes none:

```go
kube := k8s.New(client, "trading", "orderbook", k8s.WithWatchOnly())
router, err := tenure.NewRouter(kube, spaces) // the same []tenure.Space as the members
if err != nil {
	return err
}
defer router.Close()
if err := router.Ready(ctx); err != nil { // the first view is applied
	return err
}
owner, err := router.Route("markets", "BTC-USD")
if err != nil {
	return err // no member serves the shard yet, for example
}
// owner.ID and owner.Addr name the member that owns the key's shard.
```

The Router never joins the group: it only calls `Watch`, never `Join`, `SetDraining` or `Leave`,
and builds its table on its own goroutine.
`Route`, `RouteShard` and the handles of `Router.Space` read the latest snapshot. `Table` returns
a copy of it for debugging, and `debughttp.RouterHandler` serves that as JSON.

The members answer requests for keys they don't own as [the protocol](#the-protocol) says; the
check is a few lines in each handler:

```go
sh, err := s.node.Local("orders", key)
var notOwner *tenure.NotOwnerError
switch {
case err == nil:
	serveOrder(w, r, sh) // this member owns the key's shard
case errors.As(err, &notOwner) && notOwner.Route.ID != "" && notOwner.Route.ID != s.self:
	routehttp.Misdirected(w, notOwner.Route) // 421 with Tenure-Route
case errors.As(err, &notOwner), errors.Is(err, tenure.ErrClosed):
	routehttp.Unavailable(w, time.Second) // 503 with Retry-After
default:
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
```

An HTTP caller then sends every request through a `routehttp.Transport`. Its `Key` picks a
request's key, and its `Fallback`, such as the members' Service, takes requests the Router has no
member for:

```go
orders := &http.Client{
	Timeout: 10 * time.Second,
	Transport: &routehttp.Transport{
		Router:   router,
		Space:    "orders",
		Key:      func(r *http.Request) (string, error) { return r.Header.Get("Order-Key"), nil },
		Fallback: "orders.trading.svc:8080", // any member: it answers or redirects
	},
}
// The host is replaced by the owner's address.
req, err := http.NewRequest(http.MethodGet, "http://orders/orders/o-17", nil)
if err != nil {
	return err
}
req.Header.Set("Order-Key", "o-17")
resp, err := orders.Do(req)
if err != nil {
	return err
}
defer resp.Body.Close()
```

A gRPC caller dials `tenure:///<space>` with `grpcroute`'s resolver, policy and interceptors, and
puts each call's key in its context. A call whose owner's connection isn't ready, or whose key
the Router has no member for, goes to any ready connection, whose member redirects it; a stream
is redirected only until its first message:

```go
conn, err := grpc.NewClient("tenure:///orders",
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithResolvers(grpcroute.NewResolverBuilder(router)),
	grpc.WithDefaultServiceConfig(grpcroute.ServiceConfig),
	grpc.WithChainUnaryInterceptor(grpcroute.UnaryClientInterceptor(router)),
	grpc.WithChainStreamInterceptor(grpcroute.StreamClientInterceptor(router)),
)
if err != nil {
	return err
}
defer conn.Close()
// Each call names its key in its context, for example with a generated client:
//   reply, err := pb.NewOrdersClient(conn).Get(grpcroute.WithKey(ctx, req.Key), req)
ctx = grpcroute.WithKey(ctx, "o-17")
```

The orderbook example runs this on kind: its load client pods each embed a Router on a
watch-only Adapter and send every order through a `routehttp.Transport`; see
[examples/orderbook](../examples/orderbook/README.md).

## Choosing a membership source

A Router follows any `Membership`. Two sources fit many routers.

**A direct watch** (`k8s.New(…, k8s.WithWatchOnly())`). Each Router's Adapter watches the
group's member and slot Leases, but not its shard Leases, which are most of the writes during a
rollout. It needs read access to Leases, which
[`k8s/rbac-readonly.yaml`](../k8s/rbac-readonly.yaml) grants: get, list and watch, nothing else.
Each member renews its member Lease every `RetryPeriod` (2 s by default), so each watcher receives
about M/2 events per second, and R routers make the API server send R×M/2: 2,500 events per
second for 100 routing pods on 50 members. That is within what an API server serves from its
watch cache, but every router decodes every renewal, and every router's pod holds Lease
permissions.

**A relay** (`viewhttp`). The members serve their merged views over HTTP, and Routers long-poll
them. Views change only when members join or leave, drain, or slots change, not on renewals, so a
relay sends almost nothing between changes, and the API server's load no longer depends on R.
Each member serves the requests of R/S routers when S members sit behind the relay's Service,
holding one idle long poll per router, and answers each once per view change, or with a
`304 Not Modified` when the poll's wait (30 s by default) ends first:

```go
mux := http.NewServeMux()
mux.Handle("/orders/", orders)
mux.Handle("/tenure/view", viewhttp.Handler(node)) // internal: see Security below
srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
```

A Router follows the relay through a watch-only `viewhttp.Membership`. A Kubernetes Service
serves as one endpoint; list several to fail over between them:

```go
relay, err := viewhttp.NewMembership([]string{"http://orders.trading.svc:8080/tenure"})
if err != nil {
	return err
}
router, err := tenure.NewRouter(relay, spaces)
if err != nil {
	return err
}
```

A dedicated relay Deployment works too: it runs a watch-only Adapter and serves
`viewhttp.Handler` over it, so the members serve nothing but their own requests.

[Many routers on kind](#many-routers-on-kind) measures both sources with 2, 8 and 32 client pods
on five members: with a direct watch each Router added 2.5 Lease watch events per second to the
API server's load, exactly M/2, so 32 Routers added 80; with the relay the API server sent no more
events than with no client at all, whatever the number of Routers. Every order took one send with
either source. Use a direct watch for a few routers whose pods may read Leases, and a relay for
many routers, or for callers that shouldn't hold Lease permissions.

## The protocol

A member that doesn't own a request's key acts on nothing and answers in one of two ways:

| Answer | HTTP | gRPC |
|---|---|---|
| Another member owns the key | `421 Misdirected Request` with `Tenure-Route: <host:port>` of that member | `FailedPrecondition` with a `tenure-route` trailer |
| No member can serve the key yet | `503 Service Unavailable` with `Retry-After` | `Unavailable` |

The HTTP answers carry `Tenure-Unprocessed: 1`, a promise that the request had no effect, so a
caller may send it again whatever its method; `FailedPrecondition` with the trailer makes the
same promise. A `421` always means the request was not taken. A member that has begun to act on a
request, or can't tell, answers a `503` without the promise, and the caller doesn't send it
again. On gRPC, `Unavailable` makes no promise and `grpcroute` doesn't resend it; gRPC's own
retry policy can. `routehttp.Misdirected`, `routehttp.Unavailable` and `grpcroute.Misdirected`
write these answers; a gRPC handler returns `status.Error(codes.Unavailable, …)` itself. Each send of `routehttp.Transport` carries
`Tenure-Hops`, the number of earlier sends of the same request that moved it to another member,
0 on the first, so a member that forwards requests itself can stop a loop.

## Staleness and corrections

A Router's view lags the members' by the time the membership source takes to deliver a change:
the watch's delay for a direct watch, the long poll's for a relay. While a shard moves, the old
owner answers with a redirect to the new one. The transports then call `Router.Correct`, which
makes the Router send that shard's requests to the named member until the next snapshot or until
the correction is older than `WithCorrectionTTL` (the `LeaseDuration`, 15 s, by default), and
send the request again. A correction is kept only for a member of the Router's current snapshot.

Each transport follows at most 2 redirects per request by default
(`routehttp.Transport.MaxRedirects`, `grpcroute.WithMaxRedirects`), so two members whose views
disagree can't bounce a request forever: a gRPC call takes at most 3 attempts, and the HTTP
Transport returns the last `421` as the response. The HTTP Transport also sends a request again
after a `503` with `Tenure-Unprocessed`, until the request's context ends, and once to `Fallback`
after its member refused the connection. A wrong route costs a hop, never a wrong write: the
owner's `Node.Local` check and the fence stay the authority.

When the Router's watch ends or fails, or the source reports itself stale (a watch-only Adapter
whose watch has gone quiet, a relay client that has heard nothing for its wait plus
`RenewDeadline`), the Router keeps routing with its last table and reports `Stale()` and the
`router.stale` event. When its watch ended it calls `Watch` again every `RetryPeriod`; a source
that reports itself stale recovers on its own. An old table still sends most requests to their owner,
and corrections handle the rest. A caller that prefers to stop trusting a stale table reads
`Stale()` or the events.

## Running many routers

Routers are independent: any number of processes route without coordinating, and each holds its
own table, a few dozen bytes per shard per space plus the members. The cost to watch for is the
membership source's.

- Give each routing process one Router per group and share it among goroutines: lookups scale
  with cores.
- Pick the source by the number of routers, as above. With a direct watch, every router pod runs
  under a ServiceAccount bound to the read-only Role.
- Give HTTP callers the members' Service as `Fallback`, so requests that a Router has no member
  for, or whose member's address refuses connections, still reach a member that answers or
  redirects. gRPC callers need none: such a call goes to any ready connection.
- Count sends and hops with `routehttp.Transport.OnSend`; in steady state nearly every request
  takes one send.
- Keep the Router's spaces equal to the members'. Members advertise their spaces' shard counts,
  and a Router whose count differs fails that space's lookups with `ErrSpaceMismatch` rather than
  route every request wrong; with a `Fallback`, the HTTP Transport sends that space's requests
  there, so watch for the `router.mismatch` event. Members of versions that advertise nothing
  aren't checked, and `ShardOf` can't be compared: it must match the members' by construction.

## Building a proxy

tenure ships no routing service. A team that wants one, for callers that can't embed a Router,
builds it from an `httputil.ReverseProxy` whose `Transport` is a `routehttp.Transport`:

```go
proxy := &httputil.ReverseProxy{
	Rewrite: func(r *httputil.ProxyRequest) {
		r.SetXForwarded()
		r.Out.URL.Scheme = "http"
		r.Out.URL.Host = "orders" // the Transport replaces it with the owner's address
	},
	Transport: &routehttp.Transport{
		Router:   router,
		Space:    "orders",
		Key:      func(r *http.Request) (string, error) { return r.Header.Get("Order-Key"), nil },
		Fallback: "orders.trading.svc:8080",
	},
}
srv := &http.Server{Addr: ":8080", Handler: proxy, ReadHeaderTimeout: 10 * time.Second}
```

The Transport sends a request again after a redirect only when it can replay the body, which it
buffers up to `MaxBufferedBody` (1 MiB by default); a larger body is sent once. The proxy is one
more routing process: it embeds a Router like any other, needs a membership
source like any other, and scales by running more replicas, each with its own Router. Its callers
pay a hop to reach it, which a Router embedded in the callers saves.

## Security of the view relay

`viewhttp.Handler` serves the group's views: member IDs, addresses, metadata, drains and slots.
It has no authentication of its own. Serve it only on an address that callers inside the cluster
reach, never through an Ingress or a public load balancer, and restrict it with a NetworkPolicy
or the mesh's policy where the cluster has one. The handler holds one goroutine per waiting
request; the server's own limits (timeouts, connection limits) bound what a caller can hold.

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
