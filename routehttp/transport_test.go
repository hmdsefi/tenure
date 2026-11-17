// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package routehttp_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/routehttp"
	"github.com/hmdsefi/tenure/static"
)

// keyHeader is the request header the tests' Transports read the key from.
const keyHeader = "Order-Key"

// errNoKey is the error of headerKey for a request without a key.
var errNoKey = errors.New("no key")

// headerKey reads the key from keyHeader.
func headerKey(r *http.Request) (string, error) {
	if k := r.Header.Get(keyHeader); k != "" {
		return k, nil
	}
	return "", errNoKey
}

// received is what a member received of one request.
type received struct {
	hops string
	host string
	body string
	at   time.Time
}

// member is an httptest server acting as a member of the group: it records each request and
// answers it with its handler, 200 "ok" until one is set.
type member struct {
	id   string
	addr string

	mu     sync.Mutex
	handle http.HandlerFunc
	got    []received
}

func newMember(t *testing.T, id string) *member {
	t.Helper()
	m := &member{id: id}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.got = append(m.got, received{hops: r.Header.Get(routehttp.HopsHeader), host: r.Host, body: string(body), at: time.Now()})
		h := m.handle
		m.mu.Unlock()
		if h == nil {
			_, _ = io.WriteString(w, "ok "+id)
			return
		}
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	m.addr = srv.Listener.Addr().String()
	return m
}

// set makes h answer the member's requests.
func (m *member) set(h http.HandlerFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handle = h
}

// requests returns what the member received so far.
func (m *member) requests() []received {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]received(nil), m.got...)
}

// hops returns the Tenure-Hops of each request the member received.
func (m *member) hops() []string {
	var out []string
	for _, r := range m.requests() {
		out = append(out, r.hops)
	}
	return out
}

// newRouter returns a ready Router on a static group of members, with the one space "orders" of
// one shard, so that every key has shard 0.
func newRouter(t *testing.T, members ...tenure.Member) *tenure.Router {
	t.Helper()
	g, err := static.New(members)
	if err != nil {
		t.Fatalf("static.New: %v", err)
	}
	r, err := tenure.NewRouter(g, []tenure.Space{{Name: "orders", Shards: 1}})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Ready(ctx); err != nil {
		t.Fatalf("Ready: %v", err)
	}
	return r
}

// pair returns a Router on two members, the member that owns shard 0 in its view, and the other.
func pair(t *testing.T) (r *tenure.Router, owner, other *member) {
	t.Helper()
	a, b := newMember(t, "n1"), newMember(t, "n2")
	r = newRouter(t, tenure.Member{ID: a.id, Addr: a.addr}, tenure.Member{ID: b.id, Addr: b.addr})
	if routed(t, r) == a.addr {
		return r, a, b
	}
	return r, b, a
}

// routed returns the address r names for shard 0 of orders.
func routed(t *testing.T, r *tenure.Router) string {
	t.Helper()
	m, err := r.RouteShard("orders", 0)
	if err != nil {
		t.Fatalf("RouteShard: %v", err)
	}
	return m.Addr
}

// sends records what OnSend reports.
type sends struct {
	mu    sync.Mutex
	infos []routehttp.SendInfo
}

func (s *sends) on(i routehttp.SendInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.infos = append(s.infos, i)
}

// check compares the sends with want; an Err in want stands for any error.
func (s *sends) check(t *testing.T, want ...routehttp.SendInfo) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.infos) != len(want) {
		t.Fatalf("OnSend saw %d sends, want %d: %+v", len(s.infos), len(want), s.infos)
	}
	for i, w := range want {
		got := s.infos[i]
		if (got.Err != nil) != (w.Err != nil) {
			t.Errorf("send %d: Err = %v, want error: %v", i, got.Err, w.Err != nil)
		}
		got.Err, w.Err = nil, nil
		if got != w {
			t.Errorf("send %d = %+v, want %+v", i, got, w)
		}
	}
}

// transport returns a Transport for orders on r whose sends s records.
func transport(r *tenure.Router, s *sends) *routehttp.Transport {
	return &routehttp.Transport{Router: r, Space: "orders", Key: headerKey, OnSend: s.on}
}

// request returns a request for key k1 to a host no test serves. A ctx without a deadline gets
// one of 10 s, so that a Transport that sends again and again fails the test instead of hanging.
func request(t *testing.T, ctx context.Context, method string, body io.Reader) *http.Request {
	t.Helper()
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		t.Cleanup(cancel)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://orders.invalid/orders", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(keyHeader, "k1")
	return req
}

// readBody reads and closes res's body.
func readBody(t *testing.T, res *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// roundTripErr sends req through tr, where the test wants an error: a response fails the test.
func roundTripErr(t *testing.T, tr http.RoundTripper, req *http.Request) error {
	t.Helper()
	res, err := tr.RoundTrip(req)
	if res != nil {
		_ = res.Body.Close()
		t.Fatalf("RoundTrip answered %d (error %v), want an error", res.StatusCode, err)
	}
	return err
}

// opaque hides a reader's type, so that http.NewRequest sets no GetBody.
type opaque struct{ io.Reader }

// deadAddr returns the address of a closed listener, which refuses connections.
func deadAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// misdirect answers 421 naming addr.
func misdirect(addr string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		routehttp.Misdirected(w, tenure.Member{ID: "x", Addr: addr})
	}
}

// TestATRTR18OneSend checks that a request goes once, straight to its owner, with Tenure-Hops 0
// and the owner's address as its host, and that the caller's request is left as it was
// (AT-RTR-18).
func TestATRTR18OneSend(t *testing.T) {
	r, owner, other := pair(t)
	var s sends
	tr := transport(r, &s)

	req := request(t, context.Background(), http.MethodGet, nil)
	res, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if body := readBody(t, res); res.StatusCode != http.StatusOK || body != "ok "+owner.id {
		t.Fatalf("answer = %d %q, want 200 from the owner %s", res.StatusCode, body, owner.id)
	}
	got := owner.requests()
	if len(got) != 1 || got[0].hops != "0" || got[0].host != owner.addr {
		t.Errorf("owner received %+v, want one request with Tenure-Hops 0 and Host %s", got, owner.addr)
	}
	if n := len(other.requests()); n != 0 {
		t.Errorf("other member received %d requests", n)
	}
	s.check(t, routehttp.SendInfo{Space: "orders", Shard: 0, Addr: owner.addr, Status: 200, Hop: 0})
	if req.URL.Host != "orders.invalid" || req.Host != "orders.invalid" || req.Header.Get(routehttp.HopsHeader) != "" {
		t.Errorf("caller's request changed: URL %s, Host %q, header %v", req.URL, req.Host, req.Header)
	}
}

// TestATRTR18Redirect checks that a 421 corrects the Router and sends the request, body and all,
// to the member it names, with Tenure-Hops 1 (AT-RTR-18).
func TestATRTR18Redirect(t *testing.T) {
	for _, c := range []struct {
		name string
		body func() io.Reader
	}{
		{"GetBody", func() io.Reader { return strings.NewReader("order-1") }},
		{"Buffered", func() io.Reader { return opaque{strings.NewReader("order-1")} }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, owner, other := pair(t)
			owner.set(misdirect(other.addr))
			var s sends
			tr := transport(r, &s)

			res, err := tr.RoundTrip(request(t, context.Background(), http.MethodPost, c.body()))
			if err != nil {
				t.Fatalf("RoundTrip: %v", err)
			}
			if body := readBody(t, res); res.StatusCode != http.StatusOK || body != "ok "+other.id {
				t.Fatalf("answer = %d %q, want 200 from %s", res.StatusCode, body, other.id)
			}
			if got := routed(t, r); got != other.addr {
				t.Errorf("after the 421 the Router routes to %s, want the corrected %s", got, other.addr)
			}
			s.check(t,
				routehttp.SendInfo{Space: "orders", Shard: 0, Addr: owner.addr, Status: 421, Hop: 0},
				routehttp.SendInfo{Space: "orders", Shard: 0, Addr: other.addr, Status: 200, Hop: 1},
			)
			for _, m := range []*member{owner, other} {
				got := m.requests()
				if len(got) != 1 || got[0].body != "order-1" {
					t.Errorf("%s received %+v, want one request with the body", m.id, got)
				}
			}
			if h := other.hops(); len(h) != 1 || h[0] != "1" {
				t.Errorf("Tenure-Hops at the named member = %q, want 1", h)
			}
		})
	}
}

// TestATRTR18RedirectLoop checks that two members that name each other bounce a request only
// MaxRedirects times: the next 421 is returned (AT-RTR-18).
func TestATRTR18RedirectLoop(t *testing.T) {
	for _, c := range []struct {
		max   int
		sends int
	}{{0, 3}, {2, 3}, {1, 2}, {3, 4}, {-1, 1}} {
		r, owner, other := pair(t)
		owner.set(misdirect(other.addr))
		other.set(misdirect(owner.addr))
		var s sends
		tr := transport(r, &s)
		tr.MaxRedirects = c.max

		res, err := tr.RoundTrip(request(t, context.Background(), http.MethodPost, strings.NewReader("x")))
		if err != nil {
			t.Fatalf("MaxRedirects %d: RoundTrip: %v", c.max, err)
		}
		_ = readBody(t, res)
		if res.StatusCode != http.StatusMisdirectedRequest {
			t.Errorf("MaxRedirects %d: status = %d, want the last 421", c.max, res.StatusCode)
		}
		var want []routehttp.SendInfo
		for i := range c.sends {
			addr := owner.addr
			if i%2 == 1 {
				addr = other.addr
			}
			want = append(want, routehttp.SendInfo{Space: "orders", Addr: addr, Status: 421, Hop: i})
		}
		s.check(t, want...)
	}
}

// TestTransportUnusableRoute checks that a 421 without a usable Tenure-Route is the response.
func TestTransportUnusableRoute(t *testing.T) {
	for _, route := range []string{"", "nohost", ":80", "host:", "http://10.0.0.1:80"} {
		r, owner, _ := pair(t)
		owner.set(func(w http.ResponseWriter, _ *http.Request) {
			if route != "" {
				w.Header().Set(routehttp.RouteHeader, route)
			}
			w.WriteHeader(http.StatusMisdirectedRequest)
		})
		var s sends
		res, err := transport(r, &s).RoundTrip(request(t, context.Background(), http.MethodGet, nil))
		if err != nil {
			t.Fatalf("route %q: RoundTrip: %v", route, err)
		}
		_ = readBody(t, res)
		if res.StatusCode != http.StatusMisdirectedRequest {
			t.Errorf("route %q: status = %d, want 421", route, res.StatusCode)
		}
		s.check(t, routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 421})
	}
}

// jitterLog records the bounds a Transport draws its waits from, and the waits drawn.
type jitterLog struct {
	mu     sync.Mutex
	bounds []int64
	waits  []int64
}

// use makes Transports draw from draw through l until the test ends.
func (l *jitterLog) use(t *testing.T, draw func(int64) int64) {
	t.Cleanup(routehttp.SetJitter(func(n int64) int64 {
		w := draw(n)
		l.mu.Lock()
		defer l.mu.Unlock()
		l.bounds = append(l.bounds, n)
		l.waits = append(l.waits, w)
		return w
	}))
}

// unavailableFor answers 503 with Tenure-Unprocessed and retryAfter to the first n requests, then
// 200.
func unavailableFor(n int, retryAfter string) http.HandlerFunc {
	var mu sync.Mutex
	return func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n--
		left := n
		mu.Unlock()
		if left < 0 {
			_, _ = io.WriteString(w, "ok")
			return
		}
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.Header().Set(routehttp.UnprocessedHeader, "1")
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

// TestATRTR18Retry checks that a 503 with Tenure-Unprocessed is sent again to the owner after a
// random wait within min(Retry-After, MaxRetryWait), the waits differing, with Tenure-Hops
// unchanged (AT-RTR-18).
func TestATRTR18Retry(t *testing.T) {
	var l jitterLog
	l.use(t, routehttp.DefaultJitter)
	r, owner, _ := pair(t)
	const retries = 8
	owner.set(unavailableFor(retries, "1"))
	var s sends
	tr := transport(r, &s)
	tr.MaxRetryWait = 20 * time.Millisecond

	res, err := tr.RoundTrip(request(t, context.Background(), http.MethodPut, strings.NewReader("x")))
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if body := readBody(t, res); res.StatusCode != http.StatusOK {
		t.Fatalf("answer = %d %q, want 200", res.StatusCode, body)
	}
	var want []routehttp.SendInfo
	for range retries {
		want = append(want, routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 503})
	}
	s.check(t, append(want, routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 200})...)

	got := owner.requests()
	distinct := map[int64]bool{}
	for i, w := range l.waits {
		if l.bounds[i] != int64(20*time.Millisecond)+1 {
			t.Errorf("wait %d drawn from [0, %v), want [0, 20ms]", i, time.Duration(l.bounds[i]))
		}
		if w < 0 || w >= l.bounds[i] {
			t.Errorf("wait %d = %v, outside [0, 20ms]", i, time.Duration(w))
		}
		distinct[w] = true
		if gap := got[i+1].at.Sub(got[i].at); gap < time.Duration(w) {
			t.Errorf("send %d came %v after the one before, before its wait %v", i+1, gap, time.Duration(w))
		}
		if got[i+1].hops != "0" || got[i+1].body != "x" {
			t.Errorf("retry %d: Tenure-Hops %q body %q, want 0 and x", i, got[i+1].hops, got[i+1].body)
		}
	}
	if len(l.waits) != retries {
		t.Fatalf("drew %d waits, want %d", len(l.waits), retries)
	}
	if len(distinct) < 2 {
		t.Errorf("waits %v are all equal: no jitter", l.waits)
	}
}

// TestTransportRetryCap checks the bound of the wait: Retry-After capped at MaxRetryWait, one
// second without a usable Retry-After.
func TestTransportRetryCap(t *testing.T) {
	for _, c := range []struct {
		retryAfter string
		max        time.Duration
		want       time.Duration
	}{
		{"1", 5 * time.Second, time.Second},
		{"3", 0, time.Second},
		{"3", 5 * time.Second, 3 * time.Second},
		{"", 5 * time.Second, time.Second},
		{"soon", 5 * time.Second, time.Second},
		{"0", 5 * time.Second, 0},
		{"1", 50 * time.Millisecond, 50 * time.Millisecond},
	} {
		var l jitterLog
		l.use(t, func(int64) int64 { return 0 })
		r, owner, _ := pair(t)
		owner.set(unavailableFor(1, c.retryAfter))
		tr := transport(r, &sends{})
		tr.MaxRetryWait = c.max
		res, err := tr.RoundTrip(request(t, context.Background(), http.MethodGet, nil))
		if err != nil {
			t.Fatalf("Retry-After %q: RoundTrip: %v", c.retryAfter, err)
		}
		_ = readBody(t, res)
		if len(l.bounds) != 1 || l.bounds[0] != int64(c.want)+1 {
			t.Errorf("Retry-After %q, MaxRetryWait %v: bounds %v, want [0, %v]", c.retryAfter, c.max, l.bounds, c.want)
		}
	}
}

// TestATRTR18RetryLooksUp checks that the Transport looks the owner up again before it sends a
// request again after a 503: here the Router learns of another owner meanwhile.
func TestATRTR18RetryLooksUp(t *testing.T) {
	var l jitterLog
	l.use(t, func(int64) int64 { return 0 })
	r, owner, other := pair(t)
	owner.set(func(w http.ResponseWriter, _ *http.Request) {
		r.Correct("orders", 0, other.addr)
		routehttp.Unavailable(w, time.Second)
	})
	var s sends
	res, err := transport(r, &s).RoundTrip(request(t, context.Background(), http.MethodGet, nil))
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if body := readBody(t, res); body != "ok "+other.id {
		t.Errorf("body = %q, want the new owner's", body)
	}
	s.check(t,
		routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 503},
		routehttp.SendInfo{Space: "orders", Addr: other.addr, Status: 200},
	)
}

// closeLog is a Base that sends through http.DefaultTransport and records, for each answer,
// whether its body was closed and how much of it was read.
type closeLog struct {
	mu     sync.Mutex
	bodies []*trackedBody
}

type trackedBody struct {
	io.ReadCloser
	read   int
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.read += n
	return n, err
}

func (b *trackedBody) Close() error {
	b.closed = true
	return b.ReadCloser.Close()
}

func (c *closeLog) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	b := &trackedBody{ReadCloser: res.Body}
	res.Body = b
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bodies = append(c.bodies, b)
	return res, nil
}

// TestTransportDrainsAnswers checks that every answer the Transport sends again after is read,
// at most 64 KiB of it, and closed, and that the answer it returns is left open.
func TestTransportDrainsAnswers(t *testing.T) {
	var l jitterLog
	l.use(t, func(int64) int64 { return 0 })
	r, owner, other := pair(t)
	owner.set(misdirect(other.addr))
	big := strings.Repeat("b", 100<<10)
	var calls atomic.Int32
	other.set(func(w http.ResponseWriter, _ *http.Request) {
		// The 421 corrected the Router, so the 503's retry goes to other again.
		if calls.Add(1) == 1 {
			w.Header().Set(routehttp.UnprocessedHeader, "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, big)
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	var base closeLog
	tr := transport(r, &sends{})
	tr.Base = &base
	res, err := tr.RoundTrip(request(t, context.Background(), http.MethodGet, nil))
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	base.mu.Lock()
	bodies := base.bodies
	base.mu.Unlock()
	if len(bodies) != 3 {
		t.Fatalf("%d answers, want 3 (421, 503, 200)", len(bodies))
	}
	for i, b := range bodies[:2] {
		if !b.closed {
			t.Errorf("answer %d was not closed", i)
		}
	}
	if r := bodies[1].read; r < 64<<10 || r > 64<<10+32<<10 {
		t.Errorf("read %d bytes of the 503's 100 KiB body, want about 64 KiB", r)
	}
	if bodies[2].closed {
		t.Error("the returned answer was closed")
	}
	if body := readBody(t, res); body != "ok" {
		t.Errorf("body = %q", body)
	}
}

// TestATRTR18NoRetry checks that a 503 without Tenure-Unprocessed, and any 503 when retries are
// off, is the response after one send (AT-RTR-18).
func TestATRTR18NoRetry(t *testing.T) {
	for _, c := range []struct {
		name        string
		unprocessed bool
		max         time.Duration
	}{
		{"WithoutUnprocessed", false, 0},
		{"RetriesOff", true, -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, owner, _ := pair(t)
			owner.set(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "1")
				if c.unprocessed {
					w.Header().Set(routehttp.UnprocessedHeader, "1")
				}
				w.WriteHeader(http.StatusServiceUnavailable)
			})
			var s sends
			tr := transport(r, &s)
			tr.MaxRetryWait = c.max
			res, err := tr.RoundTrip(request(t, context.Background(), http.MethodPost, strings.NewReader("x")))
			if err != nil {
				t.Fatalf("RoundTrip: %v", err)
			}
			_ = readBody(t, res)
			if res.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503", res.StatusCode)
			}
			s.check(t, routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 503})
		})
	}
}

// TestATRTR18RetryUntilContext checks that a request answered 503 with Tenure-Unprocessed again
// and again is sent until its context ends, and then fails with the context's error.
func TestATRTR18RetryUntilContext(t *testing.T) {
	r, owner, _ := pair(t)
	owner.set(unavailableFor(1<<30, "1"))
	var s sends
	tr := transport(r, &s)
	tr.MaxRetryWait = 5 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := roundTripErr(t, tr, request(t, ctx, http.MethodGet, nil))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip = %v, want the context's error", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("RoundTrip took %v after the context ended", d)
	}
	if n := len(owner.requests()); n < 3 {
		t.Errorf("owner received %d requests, want several retries", n)
	}
}

// TestTransportContextEndsDuringWait checks that a context that ends while the Transport waits to
// send a request again ends the request at once, with no further send.
func TestTransportContextEndsDuringWait(t *testing.T) {
	var l jitterLog
	l.use(t, func(n int64) int64 { return n - 1 }) // the whole Retry-After: 1 s
	r, owner, _ := pair(t)
	owner.set(unavailableFor(1<<30, "1"))
	var s sends
	tr := transport(r, &s)
	tr.MaxRetryWait = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := roundTripErr(t, tr, request(t, ctx, http.MethodGet, nil))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip = %v, want the context's error", err)
	}
	if d := time.Since(start); d > 900*time.Millisecond {
		t.Errorf("RoundTrip took %v: it waited past the context's end", d)
	}
	s.check(t, routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 503})
}

// TestATRTR18NotReplayable checks that a body the Transport cannot replay, longer than
// MaxBufferedBody and without GetBody, is sent once, whole, and that its 421, its 503 or its dial
// error is what the caller gets (AT-RTR-18).
func TestATRTR18NotReplayable(t *testing.T) {
	long := strings.Repeat("a", 100)
	for _, c := range []struct {
		name   string
		answer func(other string) http.HandlerFunc
		status int
	}{
		{"Misdirected", misdirect, http.StatusMisdirectedRequest},
		{"Unavailable", func(string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) { routehttp.Unavailable(w, time.Second) }
		}, http.StatusServiceUnavailable},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, owner, other := pair(t)
			owner.set(c.answer(other.addr))
			var s sends
			tr := transport(r, &s)
			tr.MaxBufferedBody = 16
			tr.Fallback = other.addr
			res, err := tr.RoundTrip(request(t, context.Background(), http.MethodPost, opaque{strings.NewReader(long)}))
			if err != nil {
				t.Fatalf("RoundTrip: %v", err)
			}
			_ = readBody(t, res)
			if res.StatusCode != c.status {
				t.Errorf("status = %d, want %d", res.StatusCode, c.status)
			}
			s.check(t, routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: c.status})
			if got := owner.requests(); len(got) != 1 || got[0].body != long {
				t.Errorf("owner received %d requests, body %d bytes; want the whole body once", len(got), len(got[0].body))
			}
			if n := len(other.requests()); n != 0 {
				t.Errorf("other member received %d requests", n)
			}
		})
	}
	t.Run("NoBuffering", func(t *testing.T) {
		r, owner, other := pair(t)
		owner.set(misdirect(other.addr))
		var s sends
		tr := transport(r, &s)
		tr.MaxBufferedBody = -1
		res, err := tr.RoundTrip(request(t, context.Background(), http.MethodPost, opaque{strings.NewReader("x")}))
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		_ = readBody(t, res)
		s.check(t, routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 421})
	})
	t.Run("DialError", func(t *testing.T) {
		fallback := newMember(t, "svc")
		dead := deadAddr(t)
		r := newRouter(t, tenure.Member{ID: "n1", Addr: dead})
		var s sends
		tr := transport(r, &s)
		tr.MaxBufferedBody = 16
		tr.Fallback = fallback.addr
		err := roundTripErr(t, tr, request(t, context.Background(), http.MethodPost, opaque{strings.NewReader(long)}))
		if err == nil {
			t.Fatal("RoundTrip succeeded, want the dial error")
		}
		s.check(t, routehttp.SendInfo{Space: "orders", Addr: dead, Err: err})
		if n := len(fallback.requests()); n != 0 {
			t.Errorf("Fallback received %d requests", n)
		}
	})
}

// TestATRTR18DialFallback checks that a member that refuses connections sends the request once
// to Fallback, with Tenure-Hops 1, and that a second dial error is returned (AT-RTR-18).
func TestATRTR18DialFallback(t *testing.T) {
	t.Run("Fallback", func(t *testing.T) {
		fallback := newMember(t, "svc")
		dead := deadAddr(t)
		r := newRouter(t, tenure.Member{ID: "n1", Addr: dead})
		var s sends
		tr := transport(r, &s)
		tr.Fallback = fallback.addr
		res, err := tr.RoundTrip(request(t, context.Background(), http.MethodPost, opaque{strings.NewReader("x")}))
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		if body := readBody(t, res); body != "ok svc" {
			t.Errorf("body = %q, want the Fallback's", body)
		}
		s.check(t,
			routehttp.SendInfo{Space: "orders", Addr: dead, Hop: 0, Err: errors.New("dial")},
			routehttp.SendInfo{Space: "orders", Addr: fallback.addr, Status: 200, Hop: 1},
		)
		if got := fallback.requests(); len(got) != 1 || got[0].hops != "1" || got[0].body != "x" {
			t.Errorf("Fallback received %+v, want one request with Tenure-Hops 1 and the body", got)
		}
	})
	t.Run("FallbackDown", func(t *testing.T) {
		dead, dead2 := deadAddr(t), deadAddr(t)
		r := newRouter(t, tenure.Member{ID: "n1", Addr: dead})
		var s sends
		tr := transport(r, &s)
		tr.Fallback = dead2
		if err := roundTripErr(t, tr, request(t, context.Background(), http.MethodGet, nil)); err == nil {
			t.Fatal("RoundTrip succeeded, want the dial error")
		}
		s.check(t,
			routehttp.SendInfo{Space: "orders", Addr: dead, Err: errors.New("dial")},
			routehttp.SendInfo{Space: "orders", Addr: dead2, Hop: 1, Err: errors.New("dial")},
		)
	})
	t.Run("NoFallback", func(t *testing.T) {
		dead := deadAddr(t)
		r := newRouter(t, tenure.Member{ID: "n1", Addr: dead})
		var s sends
		err := roundTripErr(t, transport(r, &s), request(t, context.Background(), http.MethodGet, nil))
		var op *net.OpError
		if !errors.As(err, &op) || op.Op != "dial" {
			t.Fatalf("RoundTrip = %v, want the dial error", err)
		}
		s.check(t, routehttp.SendInfo{Space: "orders", Addr: dead, Err: err})
	})
	t.Run("AfterRedirect", func(t *testing.T) {
		// The member a 421 names refuses connections: the request goes on to Fallback.
		fallback := newMember(t, "svc")
		r, owner, _ := pair(t)
		dead := deadAddr(t)
		owner.set(misdirect(dead))
		var s sends
		tr := transport(r, &s)
		tr.Fallback = fallback.addr
		res, err := tr.RoundTrip(request(t, context.Background(), http.MethodGet, nil))
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		_ = readBody(t, res)
		s.check(t,
			routehttp.SendInfo{Space: "orders", Addr: owner.addr, Status: 421},
			routehttp.SendInfo{Space: "orders", Addr: dead, Hop: 1, Err: errors.New("dial")},
			routehttp.SendInfo{Space: "orders", Addr: fallback.addr, Status: 200, Hop: 2},
		)
	})
}

// TestATRTR18NoRoute checks that a request the Router names no member for goes to Fallback with
// Shard -1, or fails with the lookup error without a Fallback (AT-RTR-18).
func TestATRTR18NoRoute(t *testing.T) {
	mismatched := newMember(t, "n1")
	for _, c := range []struct {
		name    string
		members []tenure.Member
		want    error
	}{
		{"NoMembers", nil, tenure.ErrNoMembers},
		{"SpaceMismatch", []tenure.Member{{ID: "n1", Addr: mismatched.addr, Meta: map[string]string{"tenure.spaces": "orders:16"}}},
			tenure.ErrSpaceMismatch},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRouter(t, c.members...)

			var s sends
			err := roundTripErr(t, transport(r, &s), request(t, context.Background(), http.MethodGet, nil))
			if !errors.Is(err, c.want) {
				t.Fatalf("without Fallback: RoundTrip = %v, want %v", err, c.want)
			}
			s.check(t)

			fallback := newMember(t, "svc")
			tr := transport(r, &s)
			tr.Fallback = fallback.addr
			res, err := tr.RoundTrip(request(t, context.Background(), http.MethodGet, nil))
			if err != nil {
				t.Fatalf("with Fallback: RoundTrip: %v", err)
			}
			if body := readBody(t, res); body != "ok svc" {
				t.Errorf("body = %q, want the Fallback's", body)
			}
			s.check(t, routehttp.SendInfo{Space: "orders", Shard: -1, Addr: fallback.addr, Status: 200})
			if h := fallback.hops(); len(h) != 1 || h[0] != "0" {
				t.Errorf("Tenure-Hops at the Fallback = %q, want 0", h)
			}
		})
	}
}

// TestTransportErrors checks the requests a Transport fails without sending, and that it closes
// their bodies.
func TestTransportErrors(t *testing.T) {
	r, owner, _ := pair(t)
	closed := newRouter(t, tenure.Member{ID: "n1", Addr: owner.addr})
	_ = closed.Close()
	closedLater := newRouter(t, tenure.Member{ID: "n1", Addr: owner.addr})
	// With a Fallback, so that a closed Router's error is seen not to go there.
	later := &routehttp.Transport{Router: closedLater, Space: "orders", Key: headerKey, Fallback: owner.addr}
	if res, err := later.RoundTrip(request(t, context.Background(), http.MethodGet, nil)); err != nil {
		t.Fatalf("before Close: %v", err)
	} else {
		_ = readBody(t, res)
	}
	_ = closedLater.Close()

	for _, c := range []struct {
		name string
		tr   *routehttp.Transport
		key  string
		want string
	}{
		{"NoRouter", &routehttp.Transport{Space: "orders", Key: headerKey}, "k1", "routehttp: Transport has no Router"},
		{"NoKey", &routehttp.Transport{Router: r, Space: "orders"}, "k1", "routehttp: Transport has no Key"},
		{"UnknownSpace", &routehttp.Transport{Router: r, Space: "carts", Key: headerKey}, "k1", `tenure: unknown space "carts"`},
		{"KeyError", &routehttp.Transport{Router: r, Space: "orders", Key: headerKey}, "", errNoKey.Error()},
		{"Closed", &routehttp.Transport{Router: closed, Space: "orders", Key: headerKey}, "k1", tenure.ErrClosed.Error()},
		{"ClosedLater", later, "k1", tenure.ErrClosed.Error()},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := &closeCheck{Reader: strings.NewReader("x")}
			req := request(t, context.Background(), http.MethodPost, body)
			req.Header.Set(keyHeader, c.key)
			for range 2 {
				body.closed = false
				if err := roundTripErr(t, c.tr, req); err == nil || err.Error() != c.want {
					t.Fatalf("RoundTrip = %v, want error %q", err, c.want)
				}
				if !body.closed {
					t.Error("RoundTrip left the body open")
				}
			}
		})
	}
	if n := len(owner.requests()); n != 1 {
		t.Errorf("member received %d requests, want only the one before Close", n)
	}
}

// closeCheck is a body that records Close.
type closeCheck struct {
	io.Reader
	closed bool
}

func (c *closeCheck) Close() error {
	c.closed = true
	return nil
}

// canned is a Base RoundTripper that answers every request with the same 200 and sends nothing.
type canned struct{ res *http.Response }

func (c canned) RoundTrip(*http.Request) (*http.Response, error) { return c.res, nil }

// benchTransport returns a Transport whose Base is canned, and a request for it.
func benchTransport(tb testing.TB) (*routehttp.Transport, *http.Request) {
	g, err := static.New([]tenure.Member{{ID: "n1", Addr: "10.0.0.1:8080"}, {ID: "n2", Addr: "10.0.0.2:8080"}})
	if err != nil {
		tb.Fatal(err)
	}
	r, err := tenure.NewRouter(g, []tenure.Space{{Name: "orders", Shards: 64}})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = r.Close() })
	if err := r.Ready(context.Background()); err != nil {
		tb.Fatal(err)
	}
	res := &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: http.Header{}}
	tr := &routehttp.Transport{Router: r, Space: "orders", Key: headerKey, Base: canned{res}}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://orders.invalid/orders", nil)
	if err != nil {
		tb.Fatal(err)
	}
	req.Header.Set(keyHeader, "k1")
	req.Header.Set("Accept", "application/json")
	return tr, req
}

// BenchmarkTransport measures one send to the owner, through a Base that answers at once, so
// that only the Transport's own work counts.
func BenchmarkTransport(b *testing.B) {
	tr, req := benchTransport(b)
	b.ReportAllocs()
	for b.Loop() {
		res, err := tr.RoundTrip(req)
		if err != nil {
			b.Fatal(err)
		}
		_ = res.Body.Close()
	}
}

// TestTransportAllocs checks that a send to the owner allocates at most 2 times more than
// http.Request.Clone.
func TestTransportAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	tr, req := benchTransport(t)
	clone := testing.AllocsPerRun(1000, func() { _ = req.Clone(req.Context()) })
	send := testing.AllocsPerRun(1000, func() {
		res, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
	})
	t.Logf("Clone: %v allocations, RoundTrip: %v", clone, send)
	if send > clone+2 {
		t.Errorf("RoundTrip allocates %v times, Clone %v: more than 2 beyond Clone", send, clone)
	}
}
