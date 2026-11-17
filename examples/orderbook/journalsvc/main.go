// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command journalsvc serves the journal of the Kubernetes orderbook over HTTP.
//
//	go run ./orderbook/journalsvc
//
// The orderbook pods of orderbook/k8s share one journalsvc, so that a stale pod and its
// successor meet at the same fence: it keeps every shard's journal of the markets space in
// memory, behind a fence per shard. It serves POST /raise, POST /append, GET /entries?shard=N and
// GET /journal on the address in LISTEN_ADDR (default :8080) until SIGINT or SIGTERM, and exits 0.
// The manifests in orderbook/k8s/deploy run it as a Deployment of one replica behind a Service
// named journalsvc; deploy them to a kind cluster with kubectl as the Command comment of
// orderbook/k8s says. Its state is in memory, and a restart empties it.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hmdsefi/tenure/examples/internal/fencedb"
	"github.com/hmdsefi/tenure/examples/internal/orderbook"
)

const (
	defaultAddr = ":8080"

	// shutdownTimeout bounds the wait for requests in flight once the program is told to stop.
	shutdownTimeout = 5 * time.Second
)

func main() {
	os.Exit(exitCode())
}

// exitCode runs the program until SIGINT or SIGTERM and returns its exit code.
func exitCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "journalsvc:", err)
		return 1
	}
	return 0
}

// run listens on LISTEN_ADDR, as env gives it, and serves until ctx ends.
func run(ctx context.Context, env func(string) string) error {
	addr := env("LISTEN_ADDR")
	if addr == "" {
		addr = defaultAddr
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return serve(ctx, ln)
}

// serve serves the journal service on ln until ctx ends.
func serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: orderbook.NewJournalHandler(fencedb.New()), ReadHeaderTimeout: 10 * time.Second}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return err
	}
	if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
