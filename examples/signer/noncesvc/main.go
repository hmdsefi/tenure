// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command noncesvc serves the nonce fence and the fake chain of the Kubernetes signer over HTTP.
//
//	go run ./signer/noncesvc
//
// The signer pods of signer/k8s share one noncesvc, so that a stale pod and its successor meet
// at the same fence: it keeps the nonces and the fence of the wallets space in memory, next to a
// fake chain that records every conflict and gap. It serves POST /raise, POST /reserve, POST
// /broadcast and GET /report on the address in LISTEN_ADDR (default :8080) until SIGINT or
// SIGTERM, and exits 0. The manifests in signer/k8s/deploy run it as a Deployment of one
// replica behind a Service named noncesvc; deploy them to a kind cluster with kubectl as the
// Command comment of signer/k8s says. Its state is in memory, a restart empties it, and the
// chain it serves is a fake: no transaction leaves the program.
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
	"github.com/hmdsefi/tenure/examples/internal/signing"
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
		fmt.Fprintln(os.Stderr, "noncesvc:", err)
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

// serve serves the nonce service on ln until ctx ends.
func serve(ctx context.Context, ln net.Listener) error {
	handler := signing.NewNoncesvc(signing.NewNonces(fencedb.New()), signing.NewChain())
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
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
