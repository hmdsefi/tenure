// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package routehttp

import (
	"net/http"
	"strconv"
	"time"

	"github.com/hmdsefi/tenure"
)

// The headers of the protocol.
const (
	// RouteHeader is the header of a 421 answer that names the host:port of the member to send
	// the request to instead.
	RouteHeader = "Tenure-Route"

	// UnprocessedHeader is the header of a 421 or 503 answer that promises that the request had
	// no effect, so that it may be sent again. Its value is "1".
	UnprocessedHeader = "Tenure-Unprocessed"

	// HopsHeader is the request header that counts the earlier sends of the request that moved
	// it to another member: "0" on the first send.
	HopsHeader = "Tenure-Hops"
)

// Misdirected answers a request that the member serving it does not own: 421 Misdirected
// Request, with Tenure-Route set to m.Addr, the address of the member that owns the request's key
// in this member's view, and Tenure-Unprocessed set to 1. The body is a line of text. Call it
// only before the request has had any effect, and before anything else is written to w.
//
// A member without an address cannot be named, so for an m whose Addr is empty Misdirected
// answers as Unavailable with a wait of one second.
func Misdirected(w http.ResponseWriter, m tenure.Member) {
	if m.Addr == "" {
		Unavailable(w, time.Second)
		return
	}
	h := w.Header()
	h.Set(RouteHeader, m.Addr)
	h.Set(UnprocessedHeader, "1")
	http.Error(w, "tenure: misdirected request: send it to "+m.Addr, http.StatusMisdirectedRequest)
}

// Unavailable answers a request that no member can serve yet, as while its shard moves: 503
// Service Unavailable, with Retry-After set to retryAfter in whole seconds, rounded up and at
// least 1, and Tenure-Unprocessed set to 1. The body is a line of text. Call it only before the
// request has had any effect, and before anything else is written to w.
func Unavailable(w http.ResponseWriter, retryAfter time.Duration) {
	h := w.Header()
	h.Set("Retry-After", strconv.FormatInt(retrySeconds(retryAfter), 10))
	h.Set(UnprocessedHeader, "1")
	http.Error(w, "tenure: no member can serve the request yet", http.StatusServiceUnavailable)
}

// retrySeconds is d in whole seconds, rounded up, and at least 1.
func retrySeconds(d time.Duration) int64 {
	s := int64(d / time.Second)
	if d%time.Second > 0 {
		s++
	}
	return max(s, 1)
}
