// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command k8s runs one pod of the signer on Kubernetes, with Leases as its membership and store.
//
//	kubectl apply -k examples/signer/k8s/deploy
//
// The pod is built to run in a cluster: the manifests in signer/k8s/deploy start three replicas
// of it, next to one noncesvc that holds the nonces' fence and a fake chain, so nothing outside
// the cluster is touched and no transaction is real. Build the two images from the repository
// root, load them into a kind cluster and apply the manifests:
//
//	docker build -f examples/signer/k8s/Dockerfile --build-arg PKG=./signer/k8s -t tenure-signer:dev .
//	docker build -f examples/signer/k8s/Dockerfile --build-arg PKG=./signer/noncesvc -t tenure-noncesvc:dev .
//	kind create cluster --name tenure-examples
//	kind load docker-image tenure-signer:dev tenure-noncesvc:dev --name tenure-examples
//	kubectl apply -k examples/signer/k8s/deploy
//	kubectl -n tenure-signer rollout status deploy/noncesvc deploy/signer --timeout 120s
//
// The program takes no flags. It reads POD_NAME (the member ID), POD_IP (the address it tells the
// group, POD_IP:8080; it serves on port 8080 of every address, so that a port-forward reaches it),
// SLOTS (the slot count, 3 in the manifests, one per replica, so that a rolling deploy moves each
// shard once) and NONCESVC_URL (the nonce service), and the namespace from the service account's
// files. It builds its client with k8s.RESTConfig over the
// in-cluster configuration and joins the group "signer" in that namespace.
//
// The wallets space has 8 shards; POST /sign takes {"wallet","payload"} and, on a pod that does
// not own the wallet's shard, forwards the request to the pod that does. GET /healthz answers 200
// once the node has started, which is the pods' readiness. On SIGTERM the program drains the node
// for up to 25 s and closes it for up to 3 s, so that its shards move to the other pods before
// the pod ends, and exits 0. The program has no mode that runs outside a cluster: its unit tests
// start it against a fake Kubernetes API, and the test of the whole deployment runs on kind.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/examples/internal/signing"
	"github.com/hmdsefi/tenure/k8s"
)

const (
	// namespaceFile is where Kubernetes mounts the pod's namespace.
	namespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	// group is the name of the tenure group, and so the prefix of the Leases.
	group = "signer"

	// port is the port the pod serves on.
	port = "8080"

	// startTimeout bounds tenure.Start.
	startTimeout = 30 * time.Second

	// drainTimeout and closeTimeout bound the stop on SIGTERM; with the shutdown of the HTTP
	// server they fit in the pod's 30 s of termination grace.
	drainTimeout = 25 * time.Second
	closeTimeout = 3 * time.Second

	// shutdownTimeout bounds the wait for requests in flight after the node is closed. With the
	// drain and the close it makes 29 s, inside the pod's 30 s of termination grace.
	shutdownTimeout = time.Second

	// httpTimeout bounds one call to the nonce service.
	httpTimeout = 5 * time.Second
)

func main() {
	os.Exit(exitCode())
}

// exitCode runs the program until SIGINT or SIGTERM and returns its exit code.
func exitCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv, os.ReadFile); err != nil {
		fmt.Fprintln(os.Stderr, "signer:", err)
		return 1
	}
	return 0
}

// config is what the pod reads from its environment.
type config struct {
	podName   string
	podIP     string
	slots     int
	noncesvc  string
	namespace string
}

// addr is the address the pod serves on and tells the group.
func (c config) addr() string { return net.JoinHostPort(c.podIP, port) }

// parseEnv reads the configuration from env and the namespace file's content. It returns an
// error that names the first variable that is missing or wrong.
func parseEnv(env func(string) string, namespace string) (config, error) {
	c := config{
		podName:   env("POD_NAME"),
		podIP:     env("POD_IP"),
		noncesvc:  env("NONCESVC_URL"),
		namespace: strings.TrimSpace(namespace),
	}
	for _, v := range []struct{ name, val string }{
		{"POD_NAME", c.podName}, {"POD_IP", c.podIP}, {"NONCESVC_URL", c.noncesvc},
		{"the namespace", c.namespace},
	} {
		if v.val == "" {
			return config{}, fmt.Errorf("%s is not set", v.name)
		}
	}
	if net.ParseIP(c.podIP) == nil {
		return config{}, fmt.Errorf("POD_IP %q is not an IP address", c.podIP)
	}
	if s := env("SLOTS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return config{}, fmt.Errorf("SLOTS %q is not a count", s)
		}
		c.slots = n
	}
	return c, nil
}

// run reads the configuration, builds the in-cluster client and serves until ctx ends.
func run(ctx context.Context, env func(string) string, readFile func(string) ([]byte, error)) error {
	ns, err := readFile(namespaceFile)
	if err != nil {
		return fmt.Errorf("read the namespace: %w", err)
	}
	cfg, err := parseEnv(env, string(ns))
	if err != nil {
		return err
	}
	rc, err := k8s.RESTConfig(rest.InClusterConfig())
	if err != nil {
		return fmt.Errorf("in-cluster configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+port)
	if err != nil {
		return err
	}
	return serve(ctx, cfg, client, ln)
}

// serve starts the node on client and serves on ln until ctx ends, then drains and closes the
// node. The listener opens before the node starts, so that a pod that does not own a shard yet
// can already be a forwarding target; until Start returns, every path but /healthz answers 503
// and /healthz 503 too, which keeps the pod out of the Service.
func serve(ctx context.Context, cfg config, client kubernetes.Interface, ln net.Listener) error {
	hc := &http.Client{Timeout: httpTimeout}
	signer := signing.New(signing.NewHTTPNonces(cfg.noncesvc, hc), signing.NewHTTPChain(cfg.noncesvc, hc))

	var node atomic.Pointer[http.Handler]
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := node.Load()
			switch {
			case h == nil:
				w.Header().Set("Retry-After", "1")
				http.Error(w, "starting", http.StatusServiceUnavailable)
			case r.URL.Path == "/healthz":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok\n"))
			default:
				(*h).ServeHTTP(w, r)
			}
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	kube := k8s.New(client, cfg.namespace, group)
	sctx, cancel := context.WithTimeout(ctx, startTimeout)
	n, err := tenure.Start(sctx, tenure.Config{
		Self:       tenure.Member{ID: cfg.podName, Addr: cfg.addr()},
		Membership: kube,
		Store:      kube,
		Slots:      cfg.slots,
		Spaces:     []tenure.Space{signer.Space()},
	})
	cancel()
	if err != nil {
		_ = srv.Close()
		return fmt.Errorf("start: %w", err)
	}
	h := signer.Handler(n, cfg.podName, true)
	node.Store(&h)

	select {
	case err := <-serveErr:
		stopNode(n)
		return err
	case <-ctx.Done():
	}

	stopNode(n)
	sd, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sd); err != nil {
		// Requests still in flight (a forwarded one waiting on its owner) do not hold the pod
		// back: the node is closed already, so cut them and exit 0 as the doc comment says.
		_ = srv.Close()
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// stopNode hands the node's shards over and closes it: Drain for 25 s, then Close for 3 s. The
// HTTP server keeps answering meanwhile, so requests that still arrive are forwarded or refused
// with a route.
func stopNode(n *tenure.Node) {
	dctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	if err := n.Drain(dctx); err != nil {
		fmt.Fprintln(os.Stderr, "signer: drain:", err)
	}
	cancel()
	cctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	if err := n.Close(cctx); err != nil {
		fmt.Fprintln(os.Stderr, "signer: close:", err)
	}
}
