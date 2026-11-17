// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package viewhttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// MaxWait is the longest a request waits for a new view. A longer wait parameter is cut to it.
const MaxWait = 60 * time.Second

// epochBytes is the number of random bytes of a handler's epoch, 16 hex digits.
const epochBytes = 8

// Source is what a Handler serves the views of: anything with the Watch method of
// tenure.Membership, such as a *tenure.Node or a Membership.
type Source interface {
	// Watch returns a channel that holds the current view and receives each later one, as
	// tenure.Membership's Watch does. The channel must be closed when ctx is done.
	Watch(ctx context.Context) (<-chan core.View, error)
}

// Option configures a Handler.
type Option func(*handler)

// WithRand sets the source of the random bytes of the handler's epoch, the first part of every
// ETag it sends. The default is crypto/rand.Reader. Handler panics when it cannot read 8 bytes
// from r.
func WithRand(r io.Reader) Option {
	return func(h *handler) { h.rand = r }
}

// Handler returns an http.Handler that serves the views of src as JSON, with long polling.
//
// It answers GET requests for any path whose last element is "view", such as "/view" or
// "/tenure/view", so it can be mounted under a prefix as it is. Any other path gets 404 Not
// Found, and any other method on such a path, HEAD included, 405 Method Not Allowed with the
// header Allow: GET.
//
// Each answer of a view has the header ETag: "<epoch>-<version>", where epoch is 16 hex digits
// drawn at random when Handler is called and version is the view's Version, so tags from two
// handlers, or from a handler before and after a restart, never match. A request without
// If-None-Match, or whose If-None-Match names no tag of the current view, gets 200 OK with the
// current view at once. A request whose If-None-Match names the current view's tag gets 304 Not
// Modified at once, unless it has the query parameter wait, a duration in the format of
// time.ParseDuration such as "30s": then the handler holds the request until the source has a
// view with another tag, which it answers with 200 OK, or until the wait ends, when it answers
// 304 Not Modified. A wait longer than MaxWait is cut to MaxWait; a wait that does not parse or
// is negative gets 400 Bad Request.
//
// Every answer of a view or 304 has the headers Cache-Control: no-cache and Vary:
// Accept-Encoding. The body of a 200 answer has Content-Type: application/json, and is
// gzip-encoded, with Content-Encoding: gzip, when the request's Accept-Encoding accepts gzip.
//
// The body is one JSON object and a newline: {"version": V, "members": [...], "slots": S,
// "slot_holders": [...]}, from the view's fields of the same names. A member object has the keys
// id, addr, meta, draining, slot, join_at and leave_at; meta is left out when the member has
// none, and join_at and leave_at, times in RFC 3339 format with nanoseconds in UTC, when they are
// zero. Lists are [] when empty, never null. Each view is encoded once, and its gzip encoding made
// once when a request first accepts it; every request for the same view shares them.
//
// Each request calls src.Watch with a context that ends with the request, and waits on that
// channel: the handler starts no goroutine of its own and keeps no state per client. While the
// source has no view yet, a request waits for its first, for at most its wait when it has one.
// When src.Watch fails, when its channel closes before the request is answered, or when the wait
// ends before the source's first view, the request gets 503 Service Unavailable with the error's
// text.
//
// Handler panics if src is nil.
func Handler(src Source, opts ...Option) http.Handler {
	if src == nil {
		panic("viewhttp: nil source")
	}
	h := &handler{src: src, rand: rand.Reader}
	for _, o := range opts {
		o(h)
	}
	b := make([]byte, epochBytes)
	if _, err := io.ReadFull(h.rand, b); err != nil {
		panic(fmt.Sprintf("viewhttp: draw epoch: %v", err))
	}
	h.epoch = hex.EncodeToString(b)
	return h
}

// handler is the http.Handler that Handler returns.
type handler struct {
	src   Source
	rand  io.Reader
	epoch string // 16 hex digits

	mu   sync.Mutex
	last *encoded // the encoding of the newest view seen, nil before the first
}

// encoded is one view as the handler serves it.
type encoded struct {
	version uint64
	etag    string // with its quotes
	body    []byte

	gzOnce sync.Once
	gz     []byte
}

// gzipped returns the gzip encoding of e's body, made at the first call.
func (e *encoded) gzipped() []byte {
	e.gzOnce.Do(func() {
		var b bytes.Buffer
		zw := gzip.NewWriter(&b)
		_, _ = zw.Write(e.body) // writes to a bytes.Buffer do not fail
		_ = zw.Close()
		e.gz = b.Bytes()
	})
	return e.gz
}

// encode returns the encoding of v: the one the handler keeps when it is of v's Version, else a
// new one, which the handler keeps from now on unless it has one of a newer Version.
func (h *handler) encode(v core.View) (*encoded, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.last != nil && h.last.version == v.Version {
		return h.last, nil
	}
	body, err := encodeView(v)
	if err != nil {
		return nil, err
	}
	e := &encoded{version: v.Version, etag: h.etag(v.Version), body: body}
	if h.last == nil || v.Version > h.last.version {
		h.last = e
	}
	return e, nil
}

// etag returns the ETag of the view with Version version, quotes included.
func (h *handler) etag(version uint64) string {
	return `"` + h.epoch + "-" + strconv.FormatUint(version, 10) + `"`
}

// ServeHTTP implements http.Handler. It checks the path, the method and the wait, then watches
// the source with the request's context.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path[strings.LastIndexByte(path, '/')+1:] != "view" {
		http.Error(w, "viewhttp: not found: "+path, http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "viewhttp: method "+r.Method+" not allowed", http.StatusMethodNotAllowed)
		return
	}
	wait, err := waitParam(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	ch, err := h.src.Watch(ctx)
	if err != nil {
		http.Error(w, "viewhttp: watch: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	var timeout <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		timeout = t.C
	}
	tags := r.Header.Get("If-None-Match")
	var cur *encoded // the encoding of the latest view received
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				http.Error(w, "viewhttp: watch channel closed", http.StatusServiceUnavailable)
				return
			}
			if cur != nil && v.Version == cur.version {
				continue
			}
			if cur, err = h.encode(v); err != nil {
				http.Error(w, "viewhttp: encode: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if !matches(tags, cur.etag) {
				writeView(w, r, cur)
				return
			}
			if timeout == nil {
				writeNotModified(w, cur)
				return
			}
		case <-timeout:
			if cur == nil { // no view yet: the wait ends before the source has one
				http.Error(w, "viewhttp: no view within the wait", http.StatusServiceUnavailable)
				return
			}
			writeNotModified(w, cur)
			return
		case <-ctx.Done():
			return // the client has gone: nobody reads an answer
		}
	}
}

// waitParam returns the request's wait parameter, cut to MaxWait, or 0 without one.
func waitParam(r *http.Request) (time.Duration, error) {
	q := r.URL.Query()
	if !q.Has("wait") {
		return 0, nil
	}
	s := q.Get("wait")
	d, err := time.ParseDuration(s)
	switch {
	case err != nil:
		return 0, fmt.Errorf("viewhttp: invalid wait %q", s)
	case d < 0:
		return 0, fmt.Errorf("viewhttp: negative wait %q", s)
	}
	return min(d, MaxWait), nil
}

// matches reports whether the If-None-Match header value tags names etag, or is "*".
func matches(tags, etag string) bool {
	for t := range strings.SplitSeq(tags, ",") {
		t = strings.TrimSpace(t)
		if t == "*" || strings.TrimPrefix(t, "W/") == etag {
			return true
		}
	}
	return false
}

// setCommon sets the headers of every answer of a view or 304.
func setCommon(w http.ResponseWriter, e *encoded) {
	hdr := w.Header()
	hdr.Set("ETag", e.etag)
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("Vary", "Accept-Encoding")
}

// writeView answers 200 with e, gzip-encoded when r accepts it.
func writeView(w http.ResponseWriter, r *http.Request, e *encoded) {
	setCommon(w, e)
	hdr := w.Header()
	hdr.Set("Content-Type", "application/json")
	body := e.body
	if acceptsGzip(r.Header.Values("Accept-Encoding")) {
		body = e.gzipped()
		hdr.Set("Content-Encoding", "gzip")
	}
	hdr.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body) // a failed write means the client has gone: nothing is left to tell it
}

// writeNotModified answers 304 for e.
func writeNotModified(w http.ResponseWriter, e *encoded) {
	setCommon(w, e)
	w.WriteHeader(http.StatusNotModified)
}

// acceptsGzip reports whether the Accept-Encoding header values accept gzip: they list gzip, or
// x-gzip, with no q of 0.
func acceptsGzip(values []string) bool {
	for _, v := range values {
		for part := range strings.SplitSeq(v, ",") {
			coding, params, _ := strings.Cut(part, ";")
			coding = strings.ToLower(strings.TrimSpace(coding))
			if coding != "gzip" && coding != "x-gzip" {
				continue
			}
			if q, ok := strings.CutPrefix(strings.ReplaceAll(params, " ", ""), "q="); ok {
				if f, err := strconv.ParseFloat(q, 64); err == nil && f == 0 {
					continue
				}
			}
			return true
		}
	}
	return false
}
