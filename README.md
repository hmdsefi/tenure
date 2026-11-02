[![license](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE) [![coverage](https://img.shields.io/github/issues/detail/title/hmdsefi/tenure/235?label=coverage&color=brightgreen)](https://github.com/hmdsefi/tenure/actions/workflows/build.yml?query=branch%3Amain)

# tenure

**Shard stateful Go services: every shard has one owner at a time, and a stale owner's writes can be
rejected.**

> **Status: design complete, implementation starting.** There is no code to import yet. The work is
> planned as public issues: see the pinned roadmap issue and the
> [milestones](https://github.com/hmdsefi/tenure/milestones). v0.1.0 targets Kubernetes.

Some services can't be scaled by adding replicas, because each piece of their state needs exactly
one writer: a relayer's signer per wallet (two signers reuse a nonce), a matching engine per market
(two matchers double-fill), an order manager per account, an indexer per chain. Sharding them safely
takes membership, placement, leases, fencing and a clean handoff, so many teams run one instance in
production instead. tenure is a library that does that part.

## How it will work

You give tenure a `Run` function per space of shards. tenure calls it on the member that owns a
shard, and cancels its context when ownership ends:

```go
kube := k8s.New(client, "trading", "orderbook") // namespace, group

node, err := tenure.Start(ctx, tenure.Config{
	Self:       tenure.Member{ID: os.Getenv("POD_NAME"), Addr: os.Getenv("POD_IP") + ":8080"},
	Membership: kube,
	Store:      kube,
	Slots:      3, // one per replica: a rollout moves each shard once
	Spaces: []tenure.Space{{
		Name:   "markets",
		Shards: 64,
		Fenced: true,
		Run: func(ctx context.Context, s *tenure.Shard) error {
			// This member owns shard s.ID until ctx is done.
			// Raise the shard's fence to s.Term before reading state,
			// then write fills in transactions that check the fence.
			return serveMarkets(ctx, s)
		},
	}},
})
```

- **Spaces.** Keys hash into a fixed number of shards. Each shard is held by one member at a time,
  under a lease in a store: Kubernetes Leases by default (one RBAC rule, no extra infrastructure),
  Postgres or etcd later.
- **Terms.** Every acquisition gets a term, a number that only grows. A lease alone can't stop a
  paused process, so tenure doesn't pretend it can: a downstream that checks the term at a per-shard
  fence point rejects a stale owner's writes. tenure provides the helper for Postgres.
- **Clean handoffs.** On `SIGTERM`, a member stops its runs, releases its shards and its slot, and
  the next member starts at once instead of waiting for leases to expire.
- **Routing.** tenure tells you which member owns a key (`node.Route`); you keep your own gRPC or
  HTTP server and serialization.
- **A resolver** for keys that don't need strict ownership, and **partition trees** that split one
  hot key across members, the way Temporal's matching service splits task queues.

All decisions (assignment, placement, ownership state, timing) live in a pure, deterministic
package, `tenure/core`, with no dependencies outside the standard library. It is tested with
deterministic simulation in [faultline](https://github.com/hmdsefi/faultline), and
[lockstep](https://github.com/hmdsefi/lockstep) will use it for placing virtual actors.

## What it doesn't do

tenure moves ownership, not data: it stores no user state, runs no consensus algorithm of its own,
offers no cross-shard transactions and carries no requests. It makes no performance claims until
published benchmarks back them.

## Proof of work

Besides lockstep, the plan includes running [Temporal](https://github.com/temporalio/temporal) on
tenure instead of Ringpop: first a fresh cluster, then a live cluster migrated step by step, with
benchmarks against Ringpop, losses included.

## Contributing

Bug reports, fixes and tests are welcome. The [contributor guide](.github/CONTRIBUTING.md)
explains how to build and test tenure, the determinism rules of `core`, and the commit conventions.

## License

[MPL-2.0](LICENSE).
