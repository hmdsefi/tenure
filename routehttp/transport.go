// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package routehttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
)

// The defaults of a Transport's fields.
const (
	// DefaultMaxRedirects is the number of 421 answers a Transport follows per request when
	// MaxRedirects is zero.
	DefaultMaxRedirects = 2

	// DefaultMaxBufferedBody is the size in bytes up to which a Transport buffers a request
	// body that it cannot otherwise replay, when MaxBufferedBody is zero: 1 MiB.
	DefaultMaxBufferedBody = 1 << 20

	// DefaultMaxRetryWait caps a Transport's wait before it sends a request again after a 503,
	// when MaxRetryWait is zero.
	DefaultMaxRetryWait = time.Second
)

// drainLimit is the most of an answer's body that a Transport reads before it closes the answer
// and sends the request again, so that the connection can be reused.
const drainLimit = 64 << 10

// Transport is an http.RoundTripper that sends each request straight to the member of a tenure
// group that owns its key, as a Router names it, and follows the protocol of this package when
// the Router's view lags the members'.
//
// For each request, Transport reads the key with Key, looks up the owner of the key's shard in
// the Router, and sends a copy of the request whose URL host is the owner's address through
// Base. It never modifies the caller's request. Then:
//
//   - On a 421 with a Tenure-Route header that holds a host:port, it records the correction
//     with Router.Correct and sends the request to that address, at most MaxRedirects times per
//     request; the next 421 is returned as the response. It follows the address for the request
//     also when the Router refuses the correction. A 421 without a usable Tenure-Route is
//     returned.
//   - On a 503 with Tenure-Unprocessed, it waits a random time between zero and Retry-After (1 s
//     when the header is missing or invalid), capped at MaxRetryWait, looks the owner up again and sends the
//     request again, until the request's context ends; then it returns the context's error.
//   - When it cannot connect to the member (a dial error), it sends the request once more, to
//     Fallback, with the request's scheme. Without a Fallback, or when the Fallback already
//     failed, it returns the error.
//   - Any other answer is the response.
//
// When the Router has no member for the shard, because no member serves it yet or because the
// members disagree on the space's shard count (errors matching tenure.ErrNoMembers and
// tenure.ErrSpaceMismatch), the request goes to Fallback when it is set, else the lookup error
// is returned. Other errors, such as tenure.ErrClosed, are returned.
//
// It sends a request again only when it can replay the body: a request without a body, one with
// GetBody set (http.NewRequest sets it for common readers), or one whose body it read and
// buffered in full, up to MaxBufferedBody bytes. A longer body is sent once, and a 421 or 503
// for it is returned as the response; a dial error is returned. Before it sends a request again,
// it reads at most 64 KiB of the answer's body and closes it.
//
// Each send carries a Tenure-Hops header: the number of earlier sends of the request that moved
// it to another member, which each followed 421 and the send to Fallback after a dial error add
// one to; a 503 retry does not. The first send carries 0.
//
// The zero value needs only Router, Space and Key. Set the fields before the first request and
// do not change them later; a Transport is safe for concurrent use, and must not be copied
// after first use.
type Transport struct {
	// Router names the owner of each request's key.
	Router *tenure.Router

	// Space is the name of the Router's space whose keys the requests carry. When the Router
	// does not have it, every request fails with the Router's error.
	Space string

	// Key returns the key of a request, for example from a header or the URL path. A request
	// for which it returns an error is not sent, and RoundTrip returns the error. Key must not
	// read the request's body; a caller that keys requests by their body reads it first, as the
	// package documentation shows.
	Key func(*http.Request) (string, error)

	// Base sends each copy of the request. When nil, the Transport uses a clone of
	// http.DefaultTransport with MaxIdleConnsPerHost set to 256 and ForceAttemptHTTP2 set, made
	// once and kept, so that each member has its own pool of connections.
	Base http.RoundTripper

	// MaxRedirects is the number of 421 answers the Transport follows per request. Zero means
	// DefaultMaxRedirects, and a negative value follows none.
	MaxRedirects int

	// MaxBufferedBody is the size in bytes up to which the Transport buffers a body that it
	// cannot otherwise replay. Zero means DefaultMaxBufferedBody, and a negative value buffers
	// nothing, so that only requests without a body or with GetBody are sent again.
	MaxBufferedBody int64

	// MaxRetryWait caps the wait before the Transport sends a request again after a 503 with
	// Tenure-Unprocessed. Zero means DefaultMaxRetryWait, and a negative value turns the retries
	// off: such a 503 is returned as the response.
	MaxRetryWait time.Duration

	// Fallback is the host:port the Transport sends a request to when the Router names no
	// member for it, and, once per request, when a member's address does not accept a
	// connection: for example a Kubernetes Service in front of the members, whose member
	// answers with a 421 when it does not own the key. Empty means none.
	Fallback string

	// OnSend, when set, is called after each send of each request, with what the send did. It
	// is called on the goroutine that called RoundTrip, and must not block.
	OnSend func(SendInfo)

	once         sync.Once
	space        *tenure.RouteSpace
	err          error // the error of every RoundTrip: no Router, no Key, or the Router's Space error
	base         http.RoundTripper
	maxRedirects int
	maxBody      int64
	maxWait      time.Duration
}

// SendInfo describes one send of a request by a Transport.
type SendInfo struct {
	// Space is the Transport's space.
	Space string

	// Shard is the shard of the request's key, or -1 when the Router named no member for it and
	// the request went to the Fallback.
	Shard int

	// Addr is the host:port the request was sent to.
	Addr string

	// Status is the status code of the answer, or 0 when the send failed.
	Status int

	// Hop is the value of the send's Tenure-Hops header: the number of earlier sends of the
	// request that moved it to another member.
	Hop int

	// Err is why the send failed, or nil when an answer came.
	Err error
}

// init applies the defaults and looks up the space, once.
func (t *Transport) init() {
	t.maxRedirects = defaultInt(t.MaxRedirects, DefaultMaxRedirects)
	t.maxBody = defaultInt(t.MaxBufferedBody, DefaultMaxBufferedBody)
	t.maxWait = defaultInt(t.MaxRetryWait, DefaultMaxRetryWait)
	t.base = t.Base
	if t.base == nil {
		// A program may have replaced http.DefaultTransport with a RoundTripper of its own,
		// which the Transport then uses as it is.
		t.base = http.DefaultTransport
		if d, ok := http.DefaultTransport.(*http.Transport); ok {
			b := d.Clone()
			b.MaxIdleConnsPerHost = 256
			b.ForceAttemptHTTP2 = true
			t.base = b
		}
	}
	switch {
	case t.Router == nil:
		t.err = errors.New("routehttp: Transport has no Router")
	case t.Key == nil:
		t.err = errors.New("routehttp: Transport has no Key")
	default:
		t.space, t.err = t.Router.Space(t.Space)
	}
}

// defaultInt returns def for zero, and v otherwise.
func defaultInt[T int | int64 | time.Duration](v, def T) T {
	if v == 0 {
		return def
	}
	return v
}

// RoundTrip sends req to the owner of its key, and on to other members as the answers say. See
// Transport.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.once.Do(t.init)
	if t.err != nil {
		closeBody(req)
		return nil, t.err
	}
	key, err := t.Key(req)
	if err != nil {
		closeBody(req)
		return nil, err
	}
	shard, err := t.space.Shard(key)
	if err != nil {
		closeBody(req)
		return nil, err
	}

	s := send{t: t, req: req, shard: shard}
	if err := s.prepareBody(); err != nil {
		return nil, err
	}
	addr, err := s.lookup()
	if err != nil {
		if s.buffered == nil || s.once {
			closeBody(req) // prepareBody closed a body it buffered in full
		}
		return nil, err
	}
	redirects := 0
	for {
		res, err := s.do(addr)
		if err != nil {
			// A dial error at the Fallback itself, which a request the Router names no member
			// for goes to first, is not sent there again.
			if !isDial(err) || t.Fallback == "" || addr == t.Fallback || s.usedFallback || !s.replayable() {
				return nil, err
			}
			s.usedFallback = true
			s.hops++
			s.shard = s.keyShard
			addr = t.Fallback
			continue
		}
		switch res.StatusCode {
		case http.StatusMisdirectedRequest:
			next := res.Header.Get(RouteHeader)
			if redirects >= t.maxRedirects || !isHostPort(next) || !s.replayable() {
				return res, nil
			}
			redirects++
			s.hops++
			t.Router.Correct(t.Space, s.keyShard, next)
			s.shard = s.keyShard
			drain(res)
			addr = next
		case http.StatusServiceUnavailable:
			if t.maxWait < 0 || res.Header.Get(UnprocessedHeader) == "" || !s.replayable() {
				return res, nil
			}
			wait := min(retryAfter(res.Header.Get("Retry-After")), t.maxWait)
			drain(res)
			if err := sleep(req.Context(), time.Duration(jitter(int64(wait)+1))); err != nil {
				return nil, err
			}
			if addr, err = s.lookup(); err != nil {
				return nil, err
			}
		default:
			return res, nil
		}
	}
}

// send is the state of one request of a Transport.
type send struct {
	t            *Transport
	req          *http.Request
	shard        int    // the key's shard, or -1 while the Router names no member for it
	keyShard     int    // the key's shard
	hops         int    // the Tenure-Hops of the next send
	n            int    // sends so far
	usedFallback bool   // a dial error has sent the request to the Fallback
	buffered     []byte // the body, read in full, when the request had no GetBody
	once         bool   // the body can be sent only once
}

// prepareBody makes the request's body replayable when it can: a body without GetBody is read
// and buffered up to the limit. A longer body is kept to be sent once.
func (s *send) prepareBody() error {
	s.keyShard = s.shard
	r := s.req
	if r.Body == nil || r.Body == http.NoBody || r.GetBody != nil {
		return nil
	}
	if s.t.maxBody < 0 {
		s.once = true
		return nil
	}
	buf, err := io.ReadAll(io.LimitReader(r.Body, s.t.maxBody+1))
	if err != nil {
		_ = r.Body.Close()
		return err
	}
	if int64(len(buf)) > s.t.maxBody {
		s.once = true
	} else {
		_ = r.Body.Close()
	}
	s.buffered = buf
	return nil
}

// replayable reports whether the request can be sent again.
func (s *send) replayable() bool {
	return !s.once
}

// lookup returns the address of the shard's owner, or the Fallback when the Router names none
// and the Transport has one.
func (s *send) lookup() (string, error) {
	m, err := s.t.space.RouteShard(s.keyShard)
	if err == nil {
		s.shard = s.keyShard
		return m.Addr, nil
	}
	if s.t.Fallback != "" && (errors.Is(err, tenure.ErrNoMembers) || errors.Is(err, tenure.ErrSpaceMismatch)) {
		s.shard = -1
		return s.t.Fallback, nil
	}
	return "", err
}

// do sends a copy of the request to addr and reports the send.
func (s *send) do(addr string) (*http.Response, error) {
	r := s.req
	// Clone copies everything but the header, which withHops copies with room for Tenure-Hops,
	// so that setting it allocates nothing more and never grows the map.
	shallow := *r
	shallow.Header = nil
	out := shallow.Clone(r.Context())
	out.URL.Host = addr
	out.Host = ""
	out.Header = withHops(r.Header, s.hops)
	switch {
	case s.buffered != nil && s.once:
		out.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(s.buffered), r.Body), r.Body}
	case s.buffered != nil && len(s.buffered) == 0:
		out.Body = http.NoBody
		out.ContentLength = 0
	case s.buffered != nil:
		buf := s.buffered
		out.Body = io.NopCloser(bytes.NewReader(buf))
		out.ContentLength = int64(len(buf))
		out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf)), nil }
	case s.n > 0 && r.GetBody != nil:
		body, err := r.GetBody()
		if err != nil {
			return nil, err
		}
		out.Body = body
	}
	s.n++
	res, err := s.t.base.RoundTrip(out)
	if s.t.OnSend != nil {
		info := SendInfo{Space: s.t.Space, Shard: s.shard, Addr: addr, Hop: s.hops, Err: err}
		if err == nil {
			info.Status = res.StatusCode
		}
		s.t.OnSend(info)
	}
	return res, err
}

// withHops returns a copy of h, as http.Header.Clone makes it, with Tenure-Hops set to hops. Like
// Clone it allocates the map and one array for all values, both with room for Tenure-Hops.
func withHops(h http.Header, hops int) http.Header {
	nv := 1
	for _, vv := range h {
		nv += len(vv)
	}
	sv := make([]string, nv)
	out := make(http.Header, len(h)+1)
	for k, vv := range h {
		if vv == nil {
			out[k] = nil
			continue
		}
		n := copy(sv, vv)
		out[k] = sv[:n:n]
		sv = sv[n:]
	}
	sv[0] = strconv.Itoa(hops)
	out[HopsHeader] = sv[:1:1]
	return out
}

// isDial reports whether err is a failure to connect, after which the request was not sent.
func isDial(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// isHostPort reports whether s is a host:port with both parts set.
func isHostPort(s string) bool {
	host, port, err := net.SplitHostPort(s)
	return err == nil && host != "" && port != ""
}

// retryAfter returns the wait a Retry-After value asks for, in seconds or until an HTTP date,
// at most an hour, or one second when it is missing or invalid.
func retryAfter(v string) time.Duration {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
		return time.Duration(min(n, int64(time.Hour/time.Second))) * time.Second
	}
	if at, err := http.ParseTime(v); err == nil {
		return min(max(time.Until(at), 0), time.Hour)
	}
	return time.Second
}

// jitter returns a uniform random number in [0, n). Tests replace it.
var jitter = rand.Int64N

// sleep waits for d, or until ctx ends; then it returns ctx's error.
func sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-tm.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// drain reads at most drainLimit bytes of res's body and closes it, so that its connection can
// carry the next request.
func drain(res *http.Response) {
	_, _ = io.CopyN(io.Discard, res.Body, drainLimit)
	_ = res.Body.Close()
}

// closeBody closes req's body, as a RoundTripper must also when it sends nothing.
func closeBody(req *http.Request) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
}
