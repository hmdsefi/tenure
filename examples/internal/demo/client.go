// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package demo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/routehttp"
)

// Client posts requests to a sharded service. Without a Router it is Post and PostSettled: it
// sends each request to the url's member and follows the 421 answers. With a Router it sends
// each request straight to the member that owns its key, through a routehttp.Transport.
//
// Set the fields before the first Post and do not change them later. A Client is safe for
// concurrent use, and must not be copied after first use.
type Client struct {
	// HTTP is the client to use, http.DefaultClient when nil. With a Router, its Transport
	// sends each copy of a request (the Transport's Base); when HTTP or its Transport is nil,
	// the routehttp.Transport's own default does.
	HTTP *http.Client

	// Router, when not nil, names the member that owns each request's key.
	Router *tenure.Router

	// Space is the name of the Router's space whose keys the requests carry.
	Space string

	// OnSend, when set and Router is not nil, is called after each send of each request, as
	// routehttp.Transport.OnSend is. It must not block.
	OnSend func(routehttp.SendInfo)

	once    sync.Once
	base    http.RoundTripper // counts the sends of each Post, then sends through HTTP's transport
	clients sync.Map          // the url's host, the Fallback -> *http.Client over a routehttp.Transport
}

// postState is what one routed Post passes to its Transport in the request's context: the key,
// and the sends it counts.
type postState struct {
	key   string
	sends int
	hops  int
}

// stateKey is the context key of a *postState.
type stateKey struct{}

// Post sends body to url as a POST with Content-Type application/json, as the package's Post
// does when c.Router is nil; key is then unused.
//
// With a Router, it sends the request to the member that owns key's shard in c.Space, and the
// url's host is only the Fallback: the address for a key the Router has no member for, and
// for a member that does not accept connections. The routehttp.Transport follows up to two 421
// answers and sends again after each 503 with Tenure-Unprocessed, waiting at most 50 ms, until
// ctx ends. A third 421 is returned with ErrRouteLoop. Any other answer is the result, a 503
// without Tenure-Unprocessed included, since its request may have had an effect. Response.Sends
// counts the sends of this Post and Response.Hops is the highest Tenure-Hops among them.
func (c *Client) Post(ctx context.Context, url, key string, body []byte) (Response, error) {
	if c.Router == nil {
		return Post(ctx, c.HTTP, url, body)
	}
	u, err := parseTarget(url)
	if err != nil {
		return Response{}, err
	}
	st := &postState{key: key}
	status, _, data, err := send(context.WithValue(ctx, stateKey{}, st), c.routed(u.Host), url, body)
	res := Response{Sends: st.sends, Hops: st.hops}
	if err != nil {
		return res, err
	}
	res.Status, res.Body = status, data
	if status == http.StatusMisdirectedRequest {
		return res, fmt.Errorf("%w: %d sends, the last one answered 421", ErrRouteLoop, st.sends)
	}
	return res, nil
}

// PostSettled is Post that sends the request again, after 50 ms, each time Post gives up with
// ErrRouteLoop, until ctx ends, as the package's PostSettled does.
func (c *Client) PostSettled(ctx context.Context, url, key string, body []byte) (Response, error) {
	for {
		res, err := c.Post(ctx, url, key, body)
		if !errors.Is(err, ErrRouteLoop) {
			return res, err
		}
		select {
		case <-time.After(routeLoopWait):
		case <-ctx.Done():
			return res, ctx.Err()
		}
	}
}

// routed returns the client that sends through a routehttp.Transport whose Fallback is
// fallback, making it the first time.
func (c *Client) routed(fallback string) *http.Client {
	if hc, ok := c.clients.Load(fallback); ok {
		return hc.(*http.Client)
	}
	c.once.Do(func() {
		var base http.RoundTripper
		if c.HTTP != nil {
			base = c.HTTP.Transport
		}
		if base == nil {
			base = defaultBase()
		}
		c.base = countSends{base}
	})
	hc := &http.Client{Transport: &routehttp.Transport{
		Router:       c.Router,
		Space:        c.Space,
		Key:          stateKeyOf,
		Base:         c.base,
		MaxRetryWait: retryWait,
		Fallback:     fallback,
		OnSend:       c.OnSend,
	}}
	if c.HTTP != nil {
		hc.Timeout = c.HTTP.Timeout
	}
	got, _ := c.clients.LoadOrStore(fallback, hc)
	return got.(*http.Client)
}

// defaultBase returns what routehttp.Transport uses when its Base is nil: a clone of
// http.DefaultTransport that keeps up to 256 idle connections per member and attempts HTTP/2.
// The Client makes it itself because it wraps it to count sends.
func defaultBase() http.RoundTripper {
	d, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport
	}
	b := d.Clone()
	b.MaxIdleConnsPerHost = 256
	b.ForceAttemptHTTP2 = true
	return b
}

// stateKeyOf is the Key of a Client's Transport: the key Post put in the request's context.
func stateKeyOf(r *http.Request) (string, error) {
	if st, ok := r.Context().Value(stateKey{}).(*postState); ok {
		return st.key, nil
	}
	return "", errors.New("demo: request has no key")
}

// countSends counts each send of a Post in its postState, and records the highest Tenure-Hops.
// The Transport calls it on the goroutine of the Post.
type countSends struct {
	base http.RoundTripper
}

func (b countSends) RoundTrip(r *http.Request) (*http.Response, error) {
	if st, ok := r.Context().Value(stateKey{}).(*postState); ok {
		st.sends++
		if h, err := strconv.Atoi(r.Header.Get(routehttp.HopsHeader)); err == nil && h > st.hops {
			st.hops = h
		}
	}
	return b.base.RoundTrip(r)
}
