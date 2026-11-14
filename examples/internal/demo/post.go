// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package demo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

// ErrRouteLoop means Post was sent on three times in a row by 421 answers and gave up.
var ErrRouteLoop = errors.New("demo: route loop")

const (
	// routeHeader is the response header that names the member to send a request to instead.
	routeHeader = "Tenure-Route"

	// maxRoutes is the number of consecutive 421 answers after which Post gives up.
	maxRoutes = 3

	// retryWait is how long Post waits after a 503 before it sends again. The demo runs fast,
	// so it ignores a Retry-After header that asks for more.
	retryWait = 50 * time.Millisecond

	// routeLoopWait is how long PostSettled waits after Post gave up on a route loop.
	routeLoopWait = 50 * time.Millisecond

	// maxBody is the most of a response body that Post reads.
	maxBody = 1 << 20
)

// Response is the last answer Post received.
type Response struct {
	// Status is the HTTP status code of the answer.
	Status int

	// Body is the answer's body.
	Body []byte

	// Sends is the number of requests Post sent, the one that got this answer included.
	Sends int
}

// Post sends body to url as a POST with Content-Type application/json and follows the routes of
// a sharded service, which a plain client would take for failures:
//
//   - 421 (misdirected): the member that got the request does not own the shard. Post reads
//     the Tenure-Route header, a member's host:port, and sends the same request, with the same
//     path, to that member. A 421 without the header is an error. After 3 consecutive 421 answers
//     Post gives up with ErrRouteLoop.
//   - 503 (unavailable): the shard has no owner that can serve it yet. Post waits 50 ms and
//     sends again, until ctx ends.
//   - any other status, 400 and 409 included, is the result: Post returns it with its body and
//     a nil error.
//
// The run of 421 answers starts again after an answer that is not 421. hc is the client to use,
// http.DefaultClient when nil. On an error Post returns the last answer it got, if any, with
// Sends set; a failure to reach a member is not retried.
func Post(ctx context.Context, hc *http.Client, url string, body []byte) (Response, error) {
	if hc == nil {
		hc = http.DefaultClient
	}
	u, err := parseTarget(url)
	if err != nil {
		return Response{}, err
	}
	var res Response
	routes := 0
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		status, header, data, err := send(ctx, hc, u.String(), body)
		res.Sends++
		if err != nil {
			return res, err
		}
		res.Status, res.Body = status, data
		switch res.Status {
		case http.StatusMisdirectedRequest:
			routes++
			if routes >= maxRoutes {
				return res, fmt.Errorf("%w: %d sends ended in 421, last to %s", ErrRouteLoop, routes, u.Host)
			}
			host := header.Get(routeHeader)
			if err := checkHost(host); err != nil {
				return res, fmt.Errorf("demo: 421 from %s without a usable %s header: %w", u.Host, routeHeader, err)
			}
			u.Host = host
		case http.StatusServiceUnavailable:
			routes = 0
			select {
			case <-time.After(retryWait):
			case <-ctx.Done():
				return res, ctx.Err()
			}
		default:
			return res, nil
		}
	}
}

// PostSettled is Post that sends the request again, after 50 ms, each time Post gives up with
// ErrRouteLoop, until ctx ends. Right after a start or a drain two members can name each other
// as the owner of a shard until their views agree, so a route loop is not an answer. A 421 means
// no member took the request, so sending it again does nothing twice.
func PostSettled(ctx context.Context, hc *http.Client, url string, body []byte) (Response, error) {
	for {
		res, err := Post(ctx, hc, url, body)
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

// send makes one request and reads the whole answer: its status, headers and body.
func send(ctx context.Context, hc *http.Client, target string, body []byte) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, data, nil
}

func parseTarget(target string) (*url.URL, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("demo: %q is not an absolute URL", target)
	}
	return u, nil
}

// checkHost accepts a host:port.
func checkHost(host string) error {
	if host == "" {
		return errors.New("empty")
	}
	if _, _, err := net.SplitHostPort(host); err != nil {
		return err
	}
	return nil
}
