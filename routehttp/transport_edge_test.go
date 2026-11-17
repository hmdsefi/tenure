// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package routehttp_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/routehttp"
)

// stub is a Base RoundTripper that reads and closes each request's body before it answers, so
// that a test sees every body's fate when RoundTrip returns. It records what each send carried.
type stub struct {
	answer func(n int, req *http.Request) (*http.Response, error)

	mu    sync.Mutex
	sent  []stubSend
	calls int
}

// stubSend is what one request that a stub received carried.
type stubSend struct {
	body          string
	contentLength int64
	getBody       bool
}

func (s *stub) RoundTrip(req *http.Request) (*http.Response, error) {
	var got stubSend
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		got.body = string(b)
	}
	got.contentLength, got.getBody = req.ContentLength, req.GetBody != nil
	s.mu.Lock()
	n := s.calls
	s.calls++
	s.sent = append(s.sent, got)
	s.mu.Unlock()
	return s.answer(n, req)
}

// answer returns a response with status and the header key: value, when key is set.
func answer(status int, key, value string) *http.Response {
	res := &http.Response{StatusCode: status, Header: http.Header{}, Body: http.NoBody}
	if key != "" {
		res.Header.Set(key, value)
	}
	return res
}

// misdirectOnce answers the first send with a 421 naming 10.0.0.9:80 and every other with a 200.
func misdirectOnce(n int, _ *http.Request) (*http.Response, error) {
	if n == 0 {
		return answer(http.StatusMisdirectedRequest, routehttp.RouteHeader, "10.0.0.9:80"), nil
	}
	return answer(http.StatusOK, "", ""), nil
}

// TestTransportRetryAfter checks the waits Retry-After values ask for: seconds, an HTTP date,
// at most an hour, and 1 s for a missing or invalid value.
func TestTransportRetryAfter(t *testing.T) {
	for _, c := range []struct {
		value    string
		min, max time.Duration
	}{
		{"", time.Second, time.Second},
		{"soon", time.Second, time.Second},
		{"-1", time.Second, time.Second},
		{"0", 0, 0},
		{"3", 3 * time.Second, 3 * time.Second},
		{"86400", time.Hour, time.Hour},
		{time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat), 0, 0},
		{time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat), 28 * time.Second, 30 * time.Second},
		{time.Now().Add(3 * time.Hour).UTC().Format(http.TimeFormat), time.Hour, time.Hour},
	} {
		if got := routehttp.RetryAfter(c.value); got < c.min || got > c.max {
			t.Errorf("Retry-After %q: wait %v, want between %v and %v", c.value, got, c.min, c.max)
		}
	}
}

// TestTransportDefaultBase checks the Base a Transport makes when it has none: its own clone of
// http.DefaultTransport with 256 idle connections per host and HTTP/2 attempted, made once.
func TestTransportDefaultBase(t *testing.T) {
	r, _, _ := pair(t)
	tr := &routehttp.Transport{Router: r, Space: "orders", Key: headerKey}
	b, ok := routehttp.BaseOf(tr).(*http.Transport)
	if !ok || b == http.DefaultTransport {
		t.Fatalf("Base = %T %p, want a clone of http.DefaultTransport", routehttp.BaseOf(tr), b)
	}
	if b.MaxIdleConnsPerHost != 256 || !b.ForceAttemptHTTP2 {
		t.Errorf("Base has MaxIdleConnsPerHost %d and ForceAttemptHTTP2 %v, want 256 and true",
			b.MaxIdleConnsPerHost, b.ForceAttemptHTTP2)
	}
	if again := routehttp.BaseOf(tr); again != b {
		t.Error("the Transport made its Base twice")
	}
	own := &stub{answer: misdirectOnce}
	if got := routehttp.BaseOf(&routehttp.Transport{Router: r, Space: "orders", Key: headerKey, Base: own}); got != own {
		t.Errorf("Base = %v, want the one set", got)
	}
}

// TestTransportBufferedBody checks that a body the Transport buffered is sent again with its
// length and a GetBody, also when it is exactly MaxBufferedBody bytes long.
func TestTransportBufferedBody(t *testing.T) {
	r, _, _ := pair(t)
	base := &stub{answer: misdirectOnce}
	tr := &routehttp.Transport{Router: r, Space: "orders", Key: headerKey, Base: base, MaxBufferedBody: 5}
	res, err := tr.RoundTrip(request(t, context.Background(), http.MethodPost, opaque{strings.NewReader("hello")}))
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want the 200 after the 421", res.StatusCode)
	}
	if len(base.sent) != 2 {
		t.Fatalf("Base received %d sends, want 2", len(base.sent))
	}
	for i, got := range base.sent {
		if got.body != "hello" || got.contentLength != 5 || !got.getBody {
			t.Errorf("send %d carried %+v, want the body, Content-Length 5 and a GetBody", i, got)
		}
	}
}

// failingBody is a body whose Read fails, and which counts its Close calls.
type failingBody struct{ closed int }

func (b *failingBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func (b *failingBody) Close() error {
	b.closed++
	return nil
}

// TestTransportClosesBodyOnce checks that the Transport closes, exactly once, a body it sends
// once (longer than MaxBufferedBody), one whose read fails, and returns GetBody's error.
func TestTransportClosesBodyOnce(t *testing.T) {
	r, _, _ := pair(t)
	long := strings.Repeat("a", 100)
	for _, c := range []struct {
		name   string
		answer func(int, *http.Request) (*http.Response, error)
	}{
		{"Misdirected", misdirectOnce},
		{"DialError", func(int, *http.Request) (*http.Response, error) {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			base := &stub{answer: c.answer}
			tr := &routehttp.Transport{Router: r, Space: "orders", Key: headerKey, Base: base,
				MaxBufferedBody: 16, Fallback: "10.0.0.8:80"}
			body := &closeCheck{Reader: strings.NewReader(long)}
			res, _ := tr.RoundTrip(request(t, context.Background(), http.MethodPost, body))
			if res != nil {
				_ = res.Body.Close()
			}
			if body.closed != 1 {
				t.Errorf("body closed %d times, want once", body.closed)
			}
			if len(base.sent) != 1 || base.sent[0].body != long {
				t.Errorf("Base received %d sends, want the whole body once", len(base.sent))
			}
		})
	}
	t.Run("ReadFails", func(t *testing.T) {
		base := &stub{answer: misdirectOnce}
		tr := &routehttp.Transport{Router: r, Space: "orders", Key: headerKey, Base: base}
		body := &failingBody{}
		if err := roundTripErr(t, tr, request(t, context.Background(), http.MethodPost, body)); err == nil || err.Error() != "read failed" {
			t.Errorf("RoundTrip = %v, want the read error", err)
		}
		if body.closed != 1 || len(base.sent) != 0 {
			t.Errorf("body closed %d times and %d sends, want once and none", body.closed, len(base.sent))
		}
	})
	t.Run("GetBodyFails", func(t *testing.T) {
		base := &stub{answer: misdirectOnce}
		tr := &routehttp.Transport{Router: r, Space: "orders", Key: headerKey, Base: base}
		req := request(t, context.Background(), http.MethodPost, strings.NewReader("x"))
		errGetBody := errors.New("no body again")
		req.GetBody = func() (io.ReadCloser, error) { return nil, errGetBody }
		if err := roundTripErr(t, tr, req); !errors.Is(err, errGetBody) {
			t.Errorf("RoundTrip = %v, want GetBody's error", err)
		}
		if len(base.sent) != 1 {
			t.Errorf("Base received %d sends, want 1", len(base.sent))
		}
	})
}

// TestTransportOtherErrors checks that an error other than a dial error, after which the member
// may have received the request, is returned and not sent to Fallback.
func TestTransportOtherErrors(t *testing.T) {
	r, owner, _ := pair(t)
	base := &stub{answer: func(int, *http.Request) (*http.Response, error) {
		return nil, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	}}
	var s sends
	tr := &routehttp.Transport{Router: r, Space: "orders", Key: headerKey, Base: base, Fallback: "10.0.0.8:80", OnSend: s.on}
	err := roundTripErr(t, tr, request(t, context.Background(), http.MethodGet, nil))
	var op *net.OpError
	if !errors.As(err, &op) || op.Op != "read" {
		t.Fatalf("RoundTrip = %v, want the read error", err)
	}
	s.check(t, routehttp.SendInfo{Space: "orders", Addr: owner.addr, Err: err})
}

// TestTransportFallbackOnce checks that a request goes to Fallback after a dial error only once:
// a member that Fallback names and that refuses connections too ends the request.
func TestTransportFallbackOnce(t *testing.T) {
	dead := deadAddr(t)
	fallback := newMember(t, "svc")
	fallback.set(misdirect(dead))
	r := newRouter(t, tenure.Member{ID: "n1", Addr: dead})
	var s sends
	tr := transport(r, &s)
	tr.Fallback = fallback.addr
	if err := roundTripErr(t, tr, request(t, context.Background(), http.MethodGet, nil)); !isDial(err) {
		t.Fatalf("RoundTrip = %v, want the dial error", err)
	}
	s.check(t,
		routehttp.SendInfo{Space: "orders", Addr: dead, Err: errors.New("dial")},
		routehttp.SendInfo{Space: "orders", Addr: fallback.addr, Status: 421, Hop: 1},
		routehttp.SendInfo{Space: "orders", Addr: dead, Hop: 2, Err: errors.New("dial")},
	)
}

// TestTransportFallbackRedirects checks that a 421 from Fallback, for a request the Router names
// no member for, is followed with the key's shard.
func TestTransportFallbackRedirects(t *testing.T) {
	owner := newMember(t, "n1")
	fallback := newMember(t, "svc")
	fallback.set(misdirect(owner.addr))
	r := newRouter(t)
	var s sends
	tr := transport(r, &s)
	tr.Fallback = fallback.addr
	res, err := tr.RoundTrip(request(t, context.Background(), http.MethodGet, nil))
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if body := readBody(t, res); body != "ok n1" {
		t.Errorf("body = %q, want the owner's", body)
	}
	s.check(t,
		routehttp.SendInfo{Space: "orders", Shard: -1, Addr: fallback.addr, Status: 421},
		routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 200, Hop: 1},
	)
}
