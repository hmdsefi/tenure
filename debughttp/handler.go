// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package debughttp

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"

	"github.com/hmdsefi/tenure"
)

// Handler returns a read-only HTTP handler that serves n's ownership table and recent events as
// JSON. It serves three paths, matched exactly:
//
//   - "/", and the empty path, serve the root body: the table, next_seq and the events;
//   - "/table" serves the table body;
//   - "/events" serves the events body. With the query parameter since=S, where S is a decimal
//     sequence number, it holds only the events whose Seq is at least S, so the next_seq of one
//     answer, given as since, fetches only the events that came after it.
//
// next_seq is the Seq of the newest event that n keeps plus one, or 1 when it keeps none, whether
// or not since leaves that event out. The events are those of n.RecentEvents, oldest first.
//
// Mount the handler under a prefix with http.StripPrefix, for example
//
//	mux.Handle("/debug/tenure/", http.StripPrefix("/debug/tenure", debughttp.Handler(n)))
//
// The handler answers GET and HEAD; a HEAD answer has the status and headers of the GET answer
// and no body. Every answer, errors included, has the headers Content-Type: application/json;
// charset=utf-8, Cache-Control: no-store and X-Content-Type-Options: nosniff. An error answer's
// body is an object with one key, error, whose value says what was wrong: the status is 404 for
// any other path, 405 with the header Allow: GET, HEAD for any other method, and 400 when since
// is not a decimal sequence number.
//
// The handler only reads n, through n.Table and n.RecentEvents, and never changes it. It keeps
// working after n.Close, when the table shows closed true. It is safe for concurrent use.
// Handler panics if n is nil.
func Handler(n *tenure.Node) http.Handler {
	if n == nil {
		panic("debughttp: nil node")
	}
	return &handler{n: n}
}

// handler is the http.Handler that Handler returns.
type handler struct {
	n *tenure.Node
}

// ServeHTTP implements http.Handler. It checks the path, then the method, then since, and encodes
// the whole body into a buffer before it writes any of it, so that an encoding error still turns
// into an error answer and a HEAD answer carries the GET body's Content-Length.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch path {
	case "", "/", "/table", "/events":
	default:
		writeError(w, r, http.StatusNotFound, "not found: "+path)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, r, http.StatusMethodNotAllowed, "method "+r.Method+" not allowed")
		return
	}

	var b bytes.Buffer
	var err error
	switch path {
	case "/table":
		err = encodeTable(&b, h.n.Table())
	case "/events":
		var since uint64 // without since: every event
		if q := r.URL.Query(); q.Has("since") {
			s := q.Get("since")
			if since, err = strconv.ParseUint(s, 10, 64); err != nil {
				writeError(w, r, http.StatusBadRequest,
					fmt.Sprintf("invalid since %q: want a decimal sequence number", s))
				return
			}
		}
		events := h.n.RecentEvents()
		err = encodeEvents(&b, nextSeq(events), fromSeq(events, since))
	default:
		t := h.n.Table()
		events := h.n.RecentEvents()
		err = encodeRoot(&b, t, nextSeq(events), events)
	}
	if err != nil {
		b.Reset()
		writeError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	write(w, r, http.StatusOK, &b)
}

// nextSeq returns the Seq of the newest of events, which are oldest first, plus one, and 1 when
// there are none.
func nextSeq(events []tenure.Event) uint64 {
	if len(events) == 0 {
		return 1
	}
	return events[len(events)-1].Seq + 1
}

// fromSeq returns the events, which are oldest first, whose Seq is at least since.
func fromSeq(events []tenure.Event, since uint64) []tenure.Event {
	for i, e := range events {
		if e.Seq >= since {
			return events[i:]
		}
	}
	return nil
}

// errorJSON is the body of an error answer.
type errorJSON struct {
	Error string `json:"error"`
}

// writeError writes an error answer with status code and the error text msg.
func writeError(w http.ResponseWriter, r *http.Request, code int, msg string) {
	var b bytes.Buffer
	_ = encode(&b, errorJSON{Error: msg}) // a struct of one string always encodes
	write(w, r, code, &b)
}

// write writes an answer with status code and body b, with the headers every answer has and b's
// Content-Length. For HEAD it writes no body.
func write(w http.ResponseWriter, r *http.Request, code int, b *bytes.Buffer) {
	hdr := w.Header()
	hdr.Set("Content-Type", "application/json; charset=utf-8")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("Content-Length", strconv.Itoa(b.Len()))
	w.WriteHeader(code)
	if r.Method != http.MethodHead {
		_, _ = b.WriteTo(w) // a failed write means the client has gone: nothing is left to tell it
	}
}
