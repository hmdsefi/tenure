// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package debughttp_test

import (
	"bytes"
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/debughttp"
	"github.com/hmdsefi/tenure/static"
)

var updateRouter = flag.Bool("update-router", false, "rewrite testdata/router.json")

// routerWall is the fixed time of the router golden test's clock.
var routerWall = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// goldenRouter returns a Router with a fixed clock on a static group of three members: n1
// advertises 16 shards for orders against the Router's 8, n2 drains, and markets has a
// correction.
func goldenRouter(t *testing.T) *tenure.Router {
	t.Helper()
	g, err := static.New([]core.Member{
		{ID: "n1", Addr: "n1:80", Slot: -1, Meta: map[string]string{"tenure.spaces": "markets:4,orders:16", "zone": "a"}},
		{ID: "n2", Addr: "n2:80", Slot: -1, Draining: true, JoinAt: routerWall.Add(-time.Hour)},
		{ID: "n3", Addr: "n3:80", Slot: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := tenure.NewRouter(g, []tenure.Space{{Name: "orders", Shards: 8}, {Name: "markets", Shards: 4}},
		tenure.WithClock(func() time.Time { return routerWall }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	m, err := r.RouteShard("markets", 0)
	if err != nil {
		t.Fatal(err)
	}
	other := "n1:80"
	if m.Addr == other {
		other = "n3:80"
	}
	if !r.Correct("markets", 0, other) {
		t.Fatal("Correct = false")
	}
	return r
}

// TestRouterHandlerGolden checks the JSON of RouterHandler against testdata/router.json; run with
// -update-router to rewrite it.
func TestRouterHandlerGolden(t *testing.T) {
	r := goldenRouter(t)
	srv := httptest.NewServer(debughttp.RouterHandler(r))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json; charset=utf-8" ||
		resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("status %d, headers %v", resp.StatusCode, resp.Header)
	}
	path := filepath.Join("testdata", "router.json")
	if *updateRouter {
		if err := os.WriteFile(path, body.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body.Bytes(), want) {
		t.Fatalf("body differs from %s:\n%s", path, body.String())
	}
}

func TestRouterHandlerAnswers(t *testing.T) {
	r := goldenRouter(t)
	h := debughttp.RouterHandler(r)
	for _, c := range []struct {
		method, path string
		code         int
		body         string
	}{
		{http.MethodGet, "/table", http.StatusNotFound, "{\n  \"error\": \"not found: /table\"\n}\n"},
		{http.MethodPost, "/", http.StatusMethodNotAllowed, "{\n  \"error\": \"method POST not allowed\"\n}\n"},
		{http.MethodHead, "/", http.StatusOK, ""},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(c.method, c.path, nil))
		if w.Code != c.code || w.Body.String() != c.body {
			t.Errorf("%s %s = %d %q, want %d %q", c.method, c.path, w.Code, w.Body.String(), c.code, c.body)
		}
		if c.method == http.MethodPost && w.Header().Get("Allow") != "GET, HEAD" {
			t.Errorf("POST: Allow = %q", w.Header().Get("Allow"))
		}
		if c.method == http.MethodHead && w.Header().Get("Content-Length") == "0" {
			t.Error("HEAD: Content-Length 0, want the GET body's")
		}
	}

	// After Close the table shows closed true.
	_ = r.Close()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"closed": true`)) {
		t.Fatalf("after Close: %d %s", w.Code, w.Body.String())
	}

	defer func() {
		if recover() == nil {
			t.Error("RouterHandler(nil) did not panic")
		}
	}()
	debughttp.RouterHandler(nil)
}
