# orderbook

A sharded order book that loses no order when a member goes away: a matching engine per market,
16 fenced shards, and a fill journal that rejects a stale owner's writes. Its clients send each
order straight to the member that owns the order's market, with a `tenure.Router`.

## In one process

```sh
go run ./orderbook                 # from examples/: three nodes, four routing clients, a drain of n3
go run ./orderbook -router=false   # the clients follow the members' 421 redirects instead
```

The program prints the ownership table before and after the drain, then the orders accepted, the
fills, the shards moved, and `sends`, the HTTP requests the accepted orders took. With a Router
nearly every order takes one send; without one, an order that reaches a member that doesn't
own its market takes a second. [`main.go`](main.go) documents
the flags.

## On Kubernetes

[`k8s/deploy`](k8s/deploy) runs three orderbook pods with one slot each, the journal service that
holds the journal and its fences, and two load client pods ([`k8s/client`](k8s/client)). No
Service selects the clients and no proxy stands between them and the members: each client pod
embeds a Router on a watch-only Adapter, under the `tenure-readonly` ServiceAccount, which may
only get, list and watch Leases, and sends every order through a `routehttp.Transport` to its
owner:

```go
kube := k8s.New(client, namespace, "orderbook", k8s.WithWatchOnly()) // as tenure-readonly
router, err := tenure.NewRouter(kube, orderbook.Spaces())            // the members' markets space
if err != nil {
	return err
}
orders := &http.Client{Transport: &routehttp.Transport{
	Router:   router,
	Space:    orderbook.SpaceName,
	Key:      marketOf,         // an order's key is its market
	Fallback: "orderbook:8080", // the members' Service
}}
```

A member that doesn't own an order's market answers `421` with the owner's address, which the
Transport follows and records with `Router.Correct`; that happens only while a shard moves.
`TestOrderbookKubernetes` ([`k8s/e2e_test.go`](k8s/e2e_test.go)) checks on kind that at least 99%
of the orders take one send in steady state, and that a rolling restart of the orderbook and the
deletion of each client pod under load lose, duplicate and fail no order.

```sh
docker build -f examples/signer/k8s/Dockerfile --build-arg PKG=./orderbook/k8s -t tenure-orderbook:dev .
docker build -f examples/signer/k8s/Dockerfile --build-arg PKG=./orderbook/journalsvc -t tenure-journalsvc:dev .
docker build -f examples/signer/k8s/Dockerfile --build-arg PKG=./orderbook/k8s/client -t tenure-orderbook-client:dev .
kind create cluster --name tenure-examples
kind load docker-image tenure-orderbook:dev tenure-journalsvc:dev tenure-orderbook-client:dev --name tenure-examples
kubectl apply -k examples/orderbook/k8s/deploy
kubectl -n tenure-orderbook rollout status deploy/journalsvc deploy/orderbook deploy/client --timeout 120s
```

Run the commands from the repository root. Each client pod serves its counts at `GET /stats`:
orders sent and ok, failures by reason, sends and hops per order, latencies, and its Router's
route of every shard.

The orderbook pods also relay their views at `GET /tenure/view`. With `MEMBERSHIP=relay` a client
pod follows the group through that relay instead of watching the Leases;
[`k8s/deploy-load`](k8s/deploy-load) and the load test in [`k8s/load_test.go`](k8s/load_test.go)
compare the two with up to 32 client pods. The [routing guide](../../docs/routing.md) explains
when to use which.
