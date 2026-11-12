// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/examples/internal/signing"
)

func TestServe(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln) }()

	chain := signing.NewHTTPChain("http://"+ln.Addr().String(), http.DefaultClient)
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	rep, err := chain.Report(rctx)
	if err != nil || rep.Accepted != 0 {
		t.Fatalf("report = %+v, %v; want an empty report", rep, err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v after its context ended", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after its context ended")
	}
}

func TestRunListenError(t *testing.T) {
	env := func(k string) string {
		if k == "LISTEN_ADDR" {
			return "not an address"
		}
		return ""
	}
	if err := run(t.Context(), env); err == nil {
		t.Fatal("run with a bad LISTEN_ADDR succeeded")
	}
}
