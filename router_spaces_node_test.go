// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/memstore"
)

// TestATRTR11SpacesAdvertised checks that Config rejects a Self.Meta key with the reserved prefix,
// that a node started on static membership and a memstore store joins with "tenure.spaces", and
// that a Router on the same group routes when its spaces agree and reports a mismatch when they
// do not (AT-RTR-11, with AT-RTR-10 end to end).
func TestATRTR11SpacesAdvertised(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGroup(t)
		run := func(ctx context.Context, _ *tenure.Shard) error { <-ctx.Done(); return nil }
		spaces := []tenure.Space{{Name: "orders", Shards: 8, Run: run}, {Name: "markets", Shards: 64, Run: run}}
		cfg := tenure.Config{
			Self:       tenure.Member{ID: "n1", Addr: "n1:80", Meta: map[string]string{"tenure.x": "1"}},
			Membership: g.Absent("n1"),
			Store:      memstore.New(nil),
			Spaces:     spaces,
		}
		if _, err := tenure.Start(context.Background(), cfg); err == nil ||
			!strings.Contains(err.Error(), `Self.Meta: key "tenure.x" is reserved`) {
			t.Fatalf("Start with Meta tenure.x = %v, want the reserved-key error", err)
		}
		cfg.Self.Meta = map[string]string{"zone": "a"}
		n, err := tenure.Start(context.Background(), cfg)
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = n.Close(context.Background()) }()
		synctest.Wait()
		v := g.View()
		if len(v.Members) != 1 || v.Members[0].Meta["tenure.spaces"] != "markets:64,orders:8" || v.Members[0].Meta["zone"] != "a" {
			t.Fatalf("view members = %+v, want n1 with tenure.spaces markets:64,orders:8 and zone a", v.Members)
		}
		if cfg.Self.Meta["tenure.spaces"] != "" {
			t.Fatal("Start modified the caller's Self.Meta")
		}

		same, err := tenure.NewRouter(g.Absent("router-1"), spaces)
		if err != nil {
			t.Fatal(err)
		}
		defer same.Close()
		other, err := tenure.NewRouter(g.Absent("router-2"), []tenure.Space{{Name: "orders", Shards: 16}, {Name: "markets", Shards: 64}})
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close()
		synctest.Wait()
		for i := range 8 {
			want, err := n.Route("orders", "k"+string(rune('a'+i)))
			got, gerr := same.Route("orders", "k"+string(rune('a'+i)))
			if err != nil || gerr != nil || got.ID != want.ID {
				t.Fatalf("Router.Route = %+v, %v; Node.Route = %+v, %v", got, gerr, want, err)
			}
		}
		if _, err := other.Route("orders", "k"); !errors.Is(err, tenure.ErrSpaceMismatch) {
			t.Fatalf("a Router with 16 shards against a member with 8: %v, want ErrSpaceMismatch", err)
		}
		if _, err := other.Route("markets", "k"); err != nil {
			t.Fatalf("a Router's matching space: %v", err)
		}
	})
}
