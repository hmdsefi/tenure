// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package debughttp

import (
	"bytes"
	"io"
	"net/http"

	"github.com/hmdsefi/tenure"
)

// RouterHandler returns a read-only HTTP handler that serves r's table, as r.Table returns it,
// as JSON. It serves the path "/" and the empty path; mount it under a prefix with
// http.StripPrefix, for example
//
//	mux.Handle("/debug/router/", http.StripPrefix("/debug/router", debughttp.RouterHandler(r)))
//
// The body is the router table object, in the encoding the package doc describes, with the keys
// taken, version, stale, closed, view_version, members, slots, slot_holders and spaces. A router
// space object has the keys name, shards, mismatch and routes, and a route object the keys shard,
// member, addr, slot, error and corrected; mismatch, member, addr, error and corrected are "" when
// unused, and slot is -1 for a route without a slot.
//
// The handler answers as Handler does: GET and HEAD only, with the same headers and error
// answers, and 404 for any other path. It keeps working after r.Close, when the table shows
// closed true. It is safe for concurrent use. RouterHandler panics if r is nil.
func RouterHandler(r *tenure.Router) http.Handler {
	if r == nil {
		panic("debughttp: nil router")
	}
	return &routerHandler{r: r}
}

// routerHandler is the http.Handler that RouterHandler returns.
type routerHandler struct {
	r *tenure.Router
}

// ServeHTTP implements http.Handler, as handler.ServeHTTP does.
func (h *routerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if path := r.URL.Path; path != "" && path != "/" {
		writeError(w, r, http.StatusNotFound, "not found: "+path)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, r, http.StatusMethodNotAllowed, "method "+r.Method+" not allowed")
		return
	}
	var b bytes.Buffer
	if err := encodeRouterTable(&b, h.r.Table()); err != nil {
		b.Reset()
		writeError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	write(w, r, http.StatusOK, &b)
}

// encodeRouterTable writes t as the router table object and one newline.
func encodeRouterTable(w io.Writer, t tenure.RouterTable) error {
	return encode(w, newRouterTableJSON(t))
}

// routerTableJSON is the router table object: the fields of tenure.RouterTable in their order.
type routerTableJSON struct {
	Taken       jsonTime          `json:"taken"`
	Version     uint64            `json:"version"`
	Stale       bool              `json:"stale"`
	Closed      bool              `json:"closed"`
	ViewVersion uint64            `json:"view_version"`
	Members     []memberJSON      `json:"members"`
	Slots       int               `json:"slots"`
	SlotHolders []string          `json:"slot_holders"`
	Spaces      []routerSpaceJSON `json:"spaces"`
}

// routerSpaceJSON is a router space object: the fields of tenure.RouterSpaceTable in their order.
type routerSpaceJSON struct {
	Name     string      `json:"name"`
	Shards   int         `json:"shards"`
	Mismatch string      `json:"mismatch"`
	Routes   []routeJSON `json:"routes"`
}

// routeJSON is a route object: the shard, then the fields of tenure.RouterRoute in their order.
type routeJSON struct {
	Shard     int    `json:"shard"`
	Member    string `json:"member"`
	Addr      string `json:"addr"`
	Slot      int    `json:"slot"`
	Error     string `json:"error"`
	Corrected string `json:"corrected"`
}

// newRouterTableJSON returns the router table object of t. Its lists and meta maps are never nil.
func newRouterTableJSON(t tenure.RouterTable) routerTableJSON {
	j := routerTableJSON{
		Taken:       jsonTime(t.Taken),
		Version:     t.Version,
		Stale:       t.Stale,
		Closed:      t.Closed,
		ViewVersion: t.ViewVersion,
		Members:     make([]memberJSON, len(t.Members)),
		Slots:       t.Slots,
		SlotHolders: make([]string, len(t.SlotHolders)),
		Spaces:      make([]routerSpaceJSON, len(t.Spaces)),
	}
	for i, m := range t.Members {
		j.Members[i] = newMemberJSON(m)
	}
	copy(j.SlotHolders, t.SlotHolders)
	for i, sp := range t.Spaces {
		s := routerSpaceJSON{Name: sp.Name, Shards: sp.Shards, Mismatch: sp.Mismatch, Routes: make([]routeJSON, len(sp.Routes))}
		for k, rt := range sp.Routes {
			s.Routes[k] = routeJSON{Shard: k, Member: rt.Member, Addr: rt.Addr, Slot: rt.Slot, Error: rt.Err, Corrected: rt.Corrected}
		}
		j.Spaces[i] = s
	}
	return j
}
