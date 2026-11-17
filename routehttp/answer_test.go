// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package routehttp_test

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/routehttp"
)

func TestHeaderNames(t *testing.T) {
	for _, c := range []struct{ got, want string }{
		{routehttp.RouteHeader, "Tenure-Route"},
		{routehttp.UnprocessedHeader, "Tenure-Unprocessed"},
		{routehttp.HopsHeader, "Tenure-Hops"},
	} {
		if c.got != c.want {
			t.Errorf("header = %q, want %q", c.got, c.want)
		}
		if k := http.CanonicalHeaderKey(c.got); k != c.got {
			t.Errorf("header %q is not canonical: %q", c.got, k)
		}
	}
}

// TestRTR800Misdirected checks the 421 answer: status, Tenure-Route, Tenure-Unprocessed and a
// text body, with values the caller set before replaced.
func TestRTR800Misdirected(t *testing.T) {
	w := httptest.NewRecorder()
	w.Header().Set(routehttp.RouteHeader, "stale:1")
	w.Header().Set(routehttp.UnprocessedHeader, "0")
	w.Header().Set("Content-Length", "99")
	routehttp.Misdirected(w, tenure.Member{ID: "n2", Addr: "10.0.0.2:8080"})

	res := w.Result()
	if res.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want 421", res.StatusCode)
	}
	checkHeader(t, res.Header, routehttp.RouteHeader, "10.0.0.2:8080")
	checkHeader(t, res.Header, routehttp.UnprocessedHeader, "1")
	checkHeader(t, res.Header, "Content-Type", "text/plain; charset=utf-8")
	checkHeader(t, res.Header, "X-Content-Type-Options", "nosniff")
	checkHeader(t, res.Header, "Retry-After")
	checkHeader(t, res.Header, "Content-Length")
	if body := w.Body.String(); body != "tenure: misdirected request: send it to 10.0.0.2:8080\n" {
		t.Errorf("body = %q", body)
	}
}

// TestRTR800MisdirectedNoAddr checks that a member without an address, which a 421 could not
// name, gets the 503 answer instead.
func TestRTR800MisdirectedNoAddr(t *testing.T) {
	w := httptest.NewRecorder()
	routehttp.Misdirected(w, tenure.Member{ID: "n2"})

	res := w.Result()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.StatusCode)
	}
	checkHeader(t, res.Header, "Retry-After", "1")
	checkHeader(t, res.Header, routehttp.UnprocessedHeader, "1")
	checkHeader(t, res.Header, routehttp.RouteHeader)
}

// TestRTR800Unavailable checks the 503 answer: status, Tenure-Unprocessed, a text body, and
// Retry-After in whole seconds, rounded up and at least 1, with values the caller set before
// replaced.
func TestRTR800Unavailable(t *testing.T) {
	for _, c := range []struct {
		d    time.Duration
		want string
	}{
		{-time.Second, "1"},
		{0, "1"},
		{time.Nanosecond, "1"},
		{time.Second, "1"},
		{time.Second + time.Nanosecond, "2"},
		{1500 * time.Millisecond, "2"},
		{2 * time.Second, "2"},
		{90 * time.Second, "90"},
		{time.Hour, "3600"},
	} {
		w := httptest.NewRecorder()
		w.Header().Set("Retry-After", "77")
		w.Header().Set(routehttp.UnprocessedHeader, "0")
		w.Header().Set("Content-Length", "99")
		routehttp.Unavailable(w, c.d)

		res := w.Result()
		if res.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("Unavailable(%v): status = %d, want 503", c.d, res.StatusCode)
		}
		checkHeader(t, res.Header, "Retry-After", c.want)
		checkHeader(t, res.Header, routehttp.UnprocessedHeader, "1")
		checkHeader(t, res.Header, routehttp.RouteHeader)
		checkHeader(t, res.Header, "Content-Type", "text/plain; charset=utf-8")
		checkHeader(t, res.Header, "X-Content-Type-Options", "nosniff")
		checkHeader(t, res.Header, "Content-Length")
		if body := w.Body.String(); body != "tenure: no member can serve the request yet\n" {
			t.Errorf("Unavailable(%v): body = %q", c.d, body)
		}
	}
}

// TestRTR800OverHTTP checks that the answers reach a client through a real server unchanged.
func TestRTR800OverHTTP(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/misdirected", func(w http.ResponseWriter, _ *http.Request) {
		routehttp.Misdirected(w, tenure.Member{ID: "n1", Addr: "127.0.0.1:9"})
	})
	mux.HandleFunc("/unavailable", func(w http.ResponseWriter, _ *http.Request) {
		routehttp.Unavailable(w, 3*time.Second)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	for _, c := range []struct {
		path   string
		status int
		header string
		value  string
	}{
		{"/misdirected", http.StatusMisdirectedRequest, routehttp.RouteHeader, "127.0.0.1:9"},
		{"/unavailable", http.StatusServiceUnavailable, "Retry-After", "3"},
	} {
		res, err := http.Post(srv.URL+c.path, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != c.status {
			t.Errorf("%s: status = %d, want %d", c.path, res.StatusCode, c.status)
		}
		checkHeader(t, res.Header, c.header, c.value)
		checkHeader(t, res.Header, routehttp.UnprocessedHeader, "1")
	}
}

// checkHeader checks that h holds exactly the values want for name; with no want, that h holds
// none.
func checkHeader(t *testing.T, h http.Header, name string, want ...string) {
	t.Helper()
	if got := h.Values(name); !slices.Equal(got, want) {
		t.Errorf("%s = %q, want %q", name, got, want)
	}
}
