// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package demo_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/examples/internal/demo"
)

// script is a test server that answers each request with the next of its replies and records
// what it was sent. After the last reply it repeats the last.
type script struct {
	*httptest.Server

	mu      sync.Mutex
	replies []reply
	got     []sent
}

// reply is one answer of a script.
type reply struct {
	status int
	route  string // the Tenure-Route header, when not empty
	body   string
}

// sent is one request a script received.
type sent struct {
	method, path, query, contentType, body string
}

func newScript(t *testing.T, replies ...reply) *script {
	t.Helper()
	s := &script{replies: replies}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		n := len(s.got)
		s.got = append(s.got, sent{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Content-Type"), string(b)})
		rep := s.replies[min(n, len(s.replies)-1)]
		s.mu.Unlock()
		if rep.route != "" {
			w.Header().Set("Tenure-Route", rep.route)
		}
		w.WriteHeader(rep.status)
		_, _ = io.WriteString(w, rep.body)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *script) setReplies(replies ...reply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies = replies
}

func (s *script) requests() []sent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sent(nil), s.got...)
}

func host(s *script) string { return strings.TrimPrefix(s.URL, "http://") }

// TestATEXM06Client checks the route-following client. The steps were written from the issue's
// text.
func TestATEXM06Client(t *testing.T) {
	hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	post := func(t *testing.T, ctx context.Context, url string, body string) (demo.Response, error) {
		t.Helper()
		return demo.Post(ctx, hc, url, []byte(body))
	}

	t.Run("421, 503, 503, 200 takes 4 sends", func(t *testing.T) {
		owner := newScript(t,
			reply{status: 503}, reply{status: 503}, reply{status: 200, body: `{"id":"o1"}`})
		other := newScript(t, reply{status: 421, route: host(owner)})

		start := time.Now()
		res, err := post(t, testCtx(t, 10*time.Second), other.URL+"/orders?x=1", `{"id":"o1"}`)
		if err != nil {
			t.Fatalf("Post: %v", err)
		}
		if res.Status != 200 || string(res.Body) != `{"id":"o1"}` || res.Sends != 4 {
			t.Fatalf("Post = %d %q after %d sends; want 200 {\"id\":\"o1\"} after 4", res.Status, res.Body, res.Sends)
		}
		if d := time.Since(start); d < 100*time.Millisecond {
			t.Errorf("Post took %v: two 503 answers should have waited 50 ms each", d)
		}
		if got := other.requests(); len(got) != 1 {
			t.Errorf("the first member got %d requests, want 1", len(got))
		}
		got := owner.requests()
		if len(got) != 3 {
			t.Fatalf("the owner got %d requests, want 3", len(got))
		}
		for i, r := range got {
			want := sent{"POST", "/orders", "x=1", "application/json", `{"id":"o1"}`}
			if r != want {
				t.Errorf("request %d to the owner = %+v, want %+v", i, r, want)
			}
		}
	})

	t.Run("a 421 loop stops after 3 sends", func(t *testing.T) {
		// Two members that each name the other as the owner.
		a := newScript(t, reply{status: 421})
		b := newScript(t, reply{status: 421})
		a.setReplies(reply{status: 421, route: host(b)})
		b.setReplies(reply{status: 421, route: host(a)})

		res, err := post(t, testCtx(t, 10*time.Second), a.URL+"/orders", `{}`)
		if !errors.Is(err, demo.ErrRouteLoop) {
			t.Fatalf("Post = %v, want ErrRouteLoop", err)
		}
		if !strings.Contains(err.Error(), "demo: route loop") {
			t.Errorf("error %q does not say route loop", err)
		}
		if res.Sends != 3 || res.Status != http.StatusMisdirectedRequest {
			t.Errorf("Response = %+v, want 3 sends and the last status 421", res)
		}
		if n := len(a.requests()) + len(b.requests()); n != 3 {
			t.Errorf("the members got %d requests, want 3", n)
		}
	})

	t.Run("a member that names itself", func(t *testing.T) {
		s := newScript(t, reply{status: 421})
		s.setReplies(reply{status: 421, route: host(s)})
		res, err := post(t, testCtx(t, 10*time.Second), s.URL+"/orders", `{}`)
		if !errors.Is(err, demo.ErrRouteLoop) || res.Sends != 3 || len(s.requests()) != 3 {
			t.Errorf("Post = %+v, %v after %d requests; want ErrRouteLoop after 3 sends", res, err, len(s.requests()))
		}
	})

	t.Run("a 421 without a route", func(t *testing.T) {
		for name, route := range map[string]string{"missing": "", "not a host:port": "n2"} {
			s := newScript(t, reply{status: 421, route: route})
			res, err := post(t, testCtx(t, 10*time.Second), s.URL+"/orders", `{}`)
			if err == nil || errors.Is(err, demo.ErrRouteLoop) {
				t.Errorf("%s: Post = %v, want an error about the route", name, err)
			}
			if res.Sends != 1 {
				t.Errorf("%s: %d sends, want 1", name, res.Sends)
			}
		}
	})

	t.Run("the run of 421 answers restarts after another status", func(t *testing.T) {
		s := newScript(t)
		self := reply{status: 421, route: host(s)}
		s.setReplies(self, self, reply{status: 503}, self, self, reply{status: 200, body: "done"})
		res, err := post(t, testCtx(t, 10*time.Second), s.URL+"/orders", `{}`)
		if err != nil || res.Status != 200 || string(res.Body) != "done" || res.Sends != 6 {
			t.Errorf("Post = %+v, %v; want 200 done after 6 sends", res, err)
		}
	})

	t.Run("other statuses are results", func(t *testing.T) {
		for _, status := range []int{200, 400, 409, 404, 422, 500} {
			s := newScript(t, reply{status: status, body: "body"})
			res, err := post(t, testCtx(t, 10*time.Second), s.URL+"/orders", `{}`)
			if err != nil || res.Status != status || string(res.Body) != "body" || res.Sends != 1 {
				t.Errorf("status %d: Post = %+v, %v; want the answer after 1 send", status, res, err)
			}
		}
	})

	t.Run("503 goes on until the context ends", func(t *testing.T) {
		s := newScript(t, reply{status: 503, body: "later"})
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		res, err := post(t, ctx, s.URL+"/orders", `{}`)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Post = %v, want DeadlineExceeded", err)
		}
		if res.Status != 503 || string(res.Body) != "later" || res.Sends < 2 {
			t.Errorf("Response = %+v, want the last 503 after at least 2 sends", res)
		}
	})

	t.Run("a member that is down is an error", func(t *testing.T) {
		s := newScript(t, reply{status: 200})
		url := s.URL
		s.Close()
		res, err := post(t, testCtx(t, 10*time.Second), url, `{}`)
		if err == nil || res.Sends != 1 {
			t.Errorf("Post to a closed server = %+v, %v; want an error after 1 send", res, err)
		}
	})

	t.Run("bad urls", func(t *testing.T) {
		for _, u := range []string{"", "/orders", "localhost:8080/orders", "http://[::1"} {
			if _, err := post(t, testCtx(t, time.Second), u, `{}`); err == nil {
				t.Errorf("Post(%q) succeeded", u)
			}
		}
	})

	t.Run("default client", func(t *testing.T) {
		var n atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			n.Add(1)
			w.WriteHeader(http.StatusConflict)
		}))
		defer srv.Close()
		res, err := demo.Post(testCtx(t, 10*time.Second), nil, srv.URL, nil)
		if err != nil || res.Status != 409 || n.Load() != 1 {
			t.Errorf("Post with a nil client = %+v, %v after %d requests", res, err, n.Load())
		}
		http.DefaultClient.CloseIdleConnections()
	})
}
