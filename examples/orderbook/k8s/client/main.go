// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command client runs one load client pod of the Kubernetes orderbook: it routes each order to
// the pod that owns its market with a tenure.Router of its own.
//
//	kubectl apply -k examples/orderbook/k8s/deploy
//
// The manifests in orderbook/k8s/deploy start two replicas of it next to the orderbook, with no
// Service in front of them and no proxy between them and the orderbook: each pod follows the
// group "orderbook" through a watch-only k8s Adapter, under a ServiceAccount that may only get,
// list and watch Leases, builds a Router on it for the orderbook's markets space and sends every
// order through a routehttp.Transport straight to its owner. With MEMBERSHIP=relay it follows the
// group through the orderbook pods' view relay instead, with a viewhttp Membership, and reads no
// Lease. Build its image from the repository
// root as the Command comment of orderbook/k8s says, with PKG=./orderbook/k8s/client and the tag
// tenure-orderbook-client:dev.
//
// The program takes no flags. It reads POD_NAME (the prefix of its order IDs, <pod>-<k>), RATE
// (orders per second, 40 when unset), DURATION (how long to send, a Go duration; until SIGTERM
// when unset), FALLBACK_URL (where an order goes when the Router names no owner, the
// orderbook's Service in the manifests, http://orderbook:8080 when unset), MEMBERSHIP (how the
// Router follows the group: direct, the default, watches the Leases; relay long-polls the view
// relay), RELAY_URL (the relay's base URL, http://orderbook:8080/tenure when unset: the
// orderbook pods serve GET /tenure/view) and the namespace from the service account's files.
//
// It serves GET /stats, the counts of its orders as JSON (sent, ok, failed by reason, how many
// sends and hops each order took, their latencies, 503 retries, done, the Router's route of
// every shard, and the mean time of one Router lookup, which the pod measures every 5 s), and
// GET /healthz, 200 once the Router has applied its first view. The load starts once the Router
// routes every shard to a member. On SIGTERM, or when DURATION has
// passed, it stops starting orders, lets those in flight finish, sets done, and keeps serving
// /stats for 10 s more before it exits 0, so that a test can read its final counts inside the
// pod's termination grace.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/orderbook"
	"github.com/hmdsefi/tenure/k8s"
	"github.com/hmdsefi/tenure/routehttp"
	"github.com/hmdsefi/tenure/viewhttp"
)

const (
	// namespaceFile is where Kubernetes mounts the pod's namespace.
	namespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	// group is the orderbook's tenure group.
	group = "orderbook"

	// port is the port the pod serves on.
	port = "8080"

	defaultRate     = 40
	defaultFallback = "http://orderbook:8080"
	defaultRelay    = "http://orderbook:8080/tenure"

	// The MEMBERSHIP values.
	membershipDirect = "direct"
	membershipRelay  = "relay"

	// lookupEvery is how often the pod measures its Router's lookups, and lookupBatch how many
	// lookups each measurement times.
	lookupEvery = 5 * time.Second
	lookupBatch = 100000

	// linger is how long the stats stay served once the load is done.
	linger = 10 * time.Second
)

func main() {
	os.Exit(exitCode())
}

// exitCode runs the program until SIGINT or SIGTERM and returns its exit code.
func exitCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv, os.ReadFile); err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		return 1
	}
	return 0
}

// config is what the pod reads from its environment.
type config struct {
	podName    string
	rate       float64
	duration   time.Duration
	fallback   string
	membership string // membershipDirect or membershipRelay
	relay      string
	namespace  string
}

// parseEnv reads the configuration from env and the namespace file's content. It returns an
// error that names the first variable that is missing or wrong.
func parseEnv(env func(string) string, namespace string) (config, error) {
	c := config{
		podName:    env("POD_NAME"),
		rate:       defaultRate,
		fallback:   strings.TrimSuffix(env("FALLBACK_URL"), "/"),
		membership: env("MEMBERSHIP"),
		relay:      env("RELAY_URL"),
		namespace:  strings.TrimSpace(namespace),
	}
	if c.podName == "" {
		return config{}, errors.New("POD_NAME is not set")
	}
	if c.namespace == "" {
		return config{}, errors.New("the namespace is not set")
	}
	if c.fallback == "" {
		c.fallback = defaultFallback
	}
	switch c.membership {
	case "":
		c.membership = membershipDirect
	case membershipDirect, membershipRelay:
	default:
		return config{}, fmt.Errorf("MEMBERSHIP %q is neither %s nor %s", c.membership, membershipDirect, membershipRelay)
	}
	if c.relay == "" {
		c.relay = defaultRelay
	}
	if s := env("RATE"); s != "" {
		r, err := strconv.ParseFloat(s, 64)
		if err != nil || r <= 0 || r > 10000 {
			return config{}, fmt.Errorf("RATE %q is not a rate in (0, 10000] orders per second", s)
		}
		c.rate = r
	}
	if s := env("DURATION"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d < 0 {
			return config{}, fmt.Errorf("DURATION %q is not a duration", s)
		}
		c.duration = d
	}
	return c, nil
}

// run reads the configuration, builds the in-cluster client and runs until ctx ends.
func run(ctx context.Context, env func(string) string, readFile func(string) ([]byte, error)) error {
	ns, err := readFile(namespaceFile)
	if err != nil {
		return fmt.Errorf("read the namespace: %w", err)
	}
	cfg, err := parseEnv(env, string(ns))
	if err != nil {
		return err
	}
	m, err := membership(cfg)
	if err != nil {
		return err
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+port)
	if err != nil {
		return err
	}
	return serve(ctx, cfg, m, ln, linger)
}

// membership returns how the Router follows the group: a watch-only k8s Adapter on the
// in-cluster client, or a viewhttp Membership on the relay.
func membership(cfg config) (tenure.Membership, error) {
	if cfg.membership == membershipRelay {
		return viewhttp.NewMembership([]string{cfg.relay})
	}
	rc, err := k8s.RESTConfig(rest.InClusterConfig())
	if err != nil {
		return nil, fmt.Errorf("in-cluster configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	return k8s.New(client, cfg.namespace, group, k8s.WithWatchOnly()), nil
}

// serve builds the Router on m, serves the stats on ln and runs the load until ctx ends or the
// duration passes; then it serves the final stats for lingerFor and returns. opts are the
// Router's.
func serve(ctx context.Context, cfg config, m tenure.Membership, ln net.Listener, lingerFor time.Duration, opts ...tenure.RouterOption) error {
	router, err := tenure.NewRouter(m, orderbook.Spaces(), opts...)
	if err != nil {
		_ = ln.Close()
		return err
	}
	defer func() { _ = router.Close() }()

	dc := &demo.Client{Router: router, Space: orderbook.SpaceName}
	l := newLoad(dc, cfg.fallback+orderbook.OrdersPath, cfg.podName, cfg.rate)
	l.duration = cfg.duration
	dc.OnSend = func(s routehttp.SendInfo) { l.retry503(s.Status) }

	srv := &http.Server{Handler: handler(l, router), ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	if err := routesAll(ctx, router); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("router: %w", err)
	}
	lctx, stopLookups := context.WithCancel(ctx)
	defer stopLookups()
	go measureLookups(lctx, router, l)
	loadDone := make(chan struct{})
	go func() {
		l.run(ctx)
		close(loadDone)
	}()
	select {
	case <-loadDone:
	case err := <-serveErr:
		return err
	}
	st := l.stats()
	fmt.Fprintf(os.Stderr, "client: done: %d sent, %d ok, failed %v\n", st.Sent, st.OK, st.Failed)
	select {
	case <-time.After(lingerFor):
	case err := <-serveErr:
		return err
	}
	return nil
}

// routesAll waits until the Router has applied its first view and routes every shard of the
// markets space to a member, so that the load does not start while the group is still forming.
func routesAll(ctx context.Context, router *tenure.Router) error {
	if err := router.Ready(ctx); err != nil {
		return err
	}
	for {
		missing := 0
		for shard := range orderbook.Shards {
			if _, err := router.RouteShard(orderbook.SpaceName, shard); err != nil {
				missing++
			}
		}
		if missing == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// measureLookups times lookupBatch lookups of the markets space at once and then every
// lookupEvery until ctx ends, and keeps the mean time of one in l's stats.
func measureLookups(ctx context.Context, router *tenure.Router, l *load) {
	keys := make([]string, orderbook.MarketCount)
	for i := range keys {
		keys[i] = orderbook.MarketName(i)
	}
	tick := time.NewTicker(lookupEvery)
	defer tick.Stop()
	for {
		start := time.Now()
		for i := range lookupBatch {
			_, _ = router.Route(orderbook.SpaceName, keys[i%len(keys)])
		}
		ns := float64(time.Since(start)) / lookupBatch
		l.mu.Lock()
		l.st.LookupNs = ns
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// handler serves GET /stats and GET /healthz.
func handler(l *load, router *tenure.Router) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		st := l.stats()
		t := router.Table()
		st.Stale = t.Stale
		for _, sp := range t.Spaces {
			if sp.Name == orderbook.SpaceName {
				for _, r := range sp.Routes {
					st.Routes = append(st.Routes, r.Member)
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if router.Table().Version == 0 {
			http.Error(w, "the router has no view yet", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}
