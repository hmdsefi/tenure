// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/examples/internal/orderbook"
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

	j := orderbook.NewHTTPJournal("http://"+ln.Addr().String(), nil)
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	if err := j.Append(rctx, 1, 1, 0, []byte(`{}`)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	all, err := j.All(rctx)
	if err != nil || len(all) != orderbook.Shards || len(all[1]) != 1 {
		t.Fatalf("All = %q, %v; want one line in shard 1", all, err)
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
