// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package readme

import (
	"context"
	"errors"
	"net/http"
	"net/http/httputil"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/kubernetes"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/examples/internal/orderbook"
	"github.com/hmdsefi/tenure/grpcroute"
	"github.com/hmdsefi/tenure/k8s"
	"github.com/hmdsefi/tenure/routehttp"
	"github.com/hmdsefi/tenure/viewhttp"
)

// The functions below hold the Go blocks of the README after the first and those of
// docs/routing.md and examples/orderbook/README.md, each between the markers "// <file> block <n> begin" and
// "// <file> block <n> end", where n counts the file's Go blocks from 1. A block is the code
// between its markers with their common indentation removed.

// RoutedClient builds an HTTP client that sends each request of the markets space to its owner,
// with a Router on a watch-only Adapter.
func RoutedClient(client kubernetes.Interface, spaces []tenure.Space, marketOf func(*http.Request) (string, error)) error {
	// README.md block 2 begin
	kube := k8s.New(client, "trading", "orderbook", k8s.WithWatchOnly()) // reads Leases, writes none
	router, err := tenure.NewRouter(kube, spaces)                        // the members' own []tenure.Space
	if err != nil {
		return err
	}
	markets := &http.Client{Transport: &routehttp.Transport{
		Router: router, Space: "markets", Key: marketOf,
		Fallback: "orderbook.trading.svc:8080", // any member, until the Router has a view
	}}
	// README.md block 2 end
	return use(markets)
}

// RouterOnLeases looks up the owner of one key with a Router on a watch-only Adapter.
func RouterOnLeases(ctx context.Context, client kubernetes.Interface, spaces []tenure.Space) error {
	// docs/routing.md block 1 begin
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
	// docs/routing.md block 1 end
	return use(owner)
}

// OrdersServer is a member's HTTP server for the orders space.
type OrdersServer struct {
	node *tenure.Node
	self string // this member's ID
}

// ServeHTTP answers a request for a key the member does not own as the routing protocol says.
func (s *OrdersServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Order-Key")
	// docs/routing.md block 2 begin
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
	// docs/routing.md block 2 end
}

// OrdersClient builds an HTTP client that sends each request of the orders space to its owner.
func OrdersClient(router *tenure.Router) error {
	// docs/routing.md block 3 begin
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
	// docs/routing.md block 3 end
	return use(resp)
}

// ServeRelay serves a member's views next to its other handlers.
func ServeRelay(node *tenure.Node, orders http.Handler) error {
	// docs/routing.md block 5 begin
	mux := http.NewServeMux()
	mux.Handle("/orders/", orders)
	mux.Handle("/tenure/view", viewhttp.Handler(node)) // internal: see Security below
	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	// docs/routing.md block 5 end
	return srv.ListenAndServe()
}

// RouterOnRelay builds a Router that follows the group through the members' relay.
func RouterOnRelay(spaces []tenure.Space) error {
	// docs/routing.md block 6 begin
	relay, err := viewhttp.NewMembership([]string{"http://orders.trading.svc:8080/tenure"})
	if err != nil {
		return err
	}
	router, err := tenure.NewRouter(relay, spaces)
	if err != nil {
		return err
	}
	// docs/routing.md block 6 end
	return use(router)
}

// GRPCClient dials the orders space's members over gRPC, each call to its key's owner.
func GRPCClient(ctx context.Context, router *tenure.Router) error {
	// docs/routing.md block 4 begin
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
	// docs/routing.md block 4 end
	return use(ctx)
}

// Proxy serves a routing proxy for the orders space: the few lines that a user who wants a
// routing service writes.
func Proxy(router *tenure.Router) error {
	// docs/routing.md block 7 begin
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
	// docs/routing.md block 7 end
	return srv.ListenAndServe()
}

// OrderbookClient builds the HTTP client of the orderbook's load client pods, as
// orderbook/k8s/client does through demo.Client.
func OrderbookClient(client kubernetes.Interface, namespace string, marketOf func(*http.Request) (string, error)) error {
	// examples/orderbook/README.md block 1 begin
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
	// examples/orderbook/README.md block 1 end
	return use(orders)
}

// use stands for the service's own code that uses v.
func use(any) error { return nil }

// serveOrder stands for the member's own code that serves one request with shard sh.
func serveOrder(w http.ResponseWriter, _ *http.Request, _ *tenure.Shard) {
	w.WriteHeader(http.StatusNoContent)
}
