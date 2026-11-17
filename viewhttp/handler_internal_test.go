// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package viewhttp

import (
	"context"
	"testing"

	"github.com/hmdsefi/tenure/core"
)

// nilSource is a Source that is never watched.
type nilSource struct{}

func (nilSource) Watch(context.Context) (<-chan core.View, error) { return nil, nil }

// TestEncodeOnce checks that a view is encoded once per Version and its gzip body made once, and
// that the encoding of an older view does not replace the newest one.
func TestEncodeOnce(t *testing.T) {
	h, ok := Handler(nilSource{}).(*handler)
	if !ok {
		t.Fatal("Handler does not return a *handler")
	}
	v5 := core.View{Version: 5, Members: []core.Member{{ID: "m1", Slot: -1}}}
	a, err := h.encode(v5)
	if err != nil {
		t.Fatalf("encode(v5) = %v", err)
	}
	b, _ := h.encode(v5)
	if a != b {
		t.Error("encode(v5) twice made two encodings, want one shared")
	}
	if g1, g2 := a.gzipped(), b.gzipped(); &g1[0] != &g2[0] {
		t.Error("gzipped twice made two bodies, want one shared")
	}
	old, _ := h.encode(core.View{Version: 4})
	if old == a || h.last != a {
		t.Errorf("encode(v4): kept %d, want the newest, 5", h.last.version)
	}
	if c, _ := h.encode(v5); c != a {
		t.Error("encode(v5) after encode(v4) made a new encoding, want the kept one")
	}
}
