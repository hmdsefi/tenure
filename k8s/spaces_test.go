// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
)

// TestATRTR11NodeAdvertisesSpaces starts a node on an Adapter and checks that its member record,
// as another Adapter of the group sees it, carries "tenure.spaces" with the node's spaces and
// shard counts, sorted by name, next to its own Meta (AT-RTR-11).
func TestATRTR11NodeAdvertisesSpaces(t *testing.T) {
	client := newFake()
	a := fakeAdapter(t, client)
	run := func(ctx context.Context, _ *tenure.Shard) error { <-ctx.Done(); return nil }
	n, err := tenure.Start(context.Background(), tenure.Config{
		Self:       tenure.Member{ID: "pod-0", Addr: "10.0.0.1:80", Meta: map[string]string{"zone": "a"}},
		Membership: a,
		Store:      a,
		Timing:     T,
		Spaces:     []tenure.Space{{Name: "orders", Shards: 8, Run: run}, {Name: "markets", Shards: 64, Run: run}},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := n.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	watcher := fakeAdapter(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch, err := watcher.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	for v := range ch {
		for _, m := range v.Members {
			if m.ID != "pod-0" {
				continue
			}
			if got := m.Meta["tenure.spaces"]; got != "markets:64,orders:8" || m.Meta["zone"] != "a" {
				t.Fatalf("pod-0's Meta = %v, want tenure.spaces \"markets:64,orders:8\" and zone a", m.Meta)
			}
			return
		}
	}
	t.Fatal("the watch ended before pod-0 appeared")
}
