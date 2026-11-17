// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
	"github.com/hmdsefi/tenure/routehttp"
)

const (
	// marketShards is the number of shards of the markets space.
	marketShards = 16

	// maxOrderBody is the most of a request body that the server reads.
	maxOrderBody = 64 << 10
)

// space returns the markets space served by o: 16 shards, fenced, so that a node that cannot
// reach the store keeps its shards running and relies on the journal's fence to stop it.
func (o *orderbook) space() tenure.Space {
	return tenure.Space{
		Name:   marketsSpace,
		Shards: marketShards,
		Fenced: true,
		Run:    o.Run,
	}
}

// newBooks returns n orderbooks, one for each node, that journal to db.
func newBooks(db *fencedb.DB, n int) []*orderbook {
	books := make([]*orderbook, n)
	for i := range books {
		books[i] = newOrderbook(db)
	}
	return books
}

// startCluster starts one demo node for each of books: node i runs books[i] and serves its
// orders over HTTP.
func startCluster(ctx context.Context, books []*orderbook) (*demo.Cluster, error) {
	return demo.Start(ctx, demo.Options{
		Nodes:  len(books),
		Spaces: func(i int) []tenure.Space { return []tenure.Space{books[i].space()} },
		Serve: func(i int, n *tenure.Node) http.Handler {
			return newServer(n, books[i], memberID(i))
		},
	})
}

// server is the HTTP front of one node: it takes an order for the shard this node owns and sends
// every other request to the owner.
type server struct {
	node *tenure.Node
	ob   *orderbook
	self string // the node's member ID
}

// newServer returns the handler of node n, whose member ID is self and which runs ob.
func newServer(n *tenure.Node, ob *orderbook, self string) http.Handler {
	s := &server{node: n, ob: ob, self: self}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.postOrder)
	return mux
}

// result is the body of a 200 answer.
type result struct {
	ID    string `json:"id"`
	Fills []fill `json:"fills"`
}

// postOrder takes one order:
//
//   - 400: the body is not one JSON order, or the order is invalid.
//   - 421 with Tenure-Route: this node does not own the market's shard, and the header names the
//     member that does. With no known owner, 503.
//   - 503 with Retry-After: 1: the shard's engine is not ready, is for another term, refuses
//     orders, or the fence rejected the order's commit. In the last case the engine refuses every
//     further order of its term.
//   - 409: the order's ID is journaled already.
//   - 200: the order was matched and journaled, and the body lists its fills.
//
// The 421 and every 503 carry Tenure-Unprocessed: 1, as routehttp.Misdirected and
// routehttp.Unavailable write them, because none follows an effect: a 503 after a rejected
// commit neither, since the fence rejects the whole commit and the engine keeps the order's match
// only when the commit succeeded.
func (s *server) postOrder(w http.ResponseWriter, r *http.Request) {
	o, err := decodeOrder(http.MaxBytesReader(w, r.Body, maxOrderBody))
	if err != nil {
		reply(w, http.StatusBadRequest, errorBody(err))
		return
	}

	sh, err := s.node.Local(marketsSpace, o.Market)
	if err != nil {
		var notOwner *tenure.NotOwnerError
		// A route to this member itself means the shard is about to be ours: no one else
		// can serve it, so the client waits.
		if errors.As(err, &notOwner) && notOwner.Route.Addr != "" && notOwner.Route.ID != s.self {
			routehttp.Misdirected(w, notOwner.Route)
			return
		}
		routehttp.Unavailable(w, time.Second)
		return
	}
	e := s.ob.engine(sh.ID, sh.Term)
	if e == nil {
		routehttp.Unavailable(w, time.Second)
		return
	}

	fills, err := e.submit(o)
	switch {
	case err == nil:
		reply(w, http.StatusOK, result{ID: o.ID, Fills: fills})
	case errors.Is(err, errInvalidOrder):
		reply(w, http.StatusBadRequest, errorBody(err))
	case errors.Is(err, errDuplicate):
		reply(w, http.StatusConflict, errorBody(err))
	case errors.Is(err, errRefusing), errors.Is(err, fencedb.ErrStale):
		routehttp.Unavailable(w, time.Second)
	default:
		reply(w, http.StatusInternalServerError, errorBody(err))
	}
}

// decodeOrder reads one order from r: a single JSON object with the order's fields and no others.
func decodeOrder(r io.Reader) (order, error) {
	var o order
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&o); err != nil {
		return order{}, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return order{}, errors.New("orderbook: data after the order")
	}
	return o, o.validate()
}

func errorBody(err error) map[string]string {
	return map[string]string{"error": err.Error()}
}

// reply writes v as the JSON body of an answer with the given status.
func reply(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}
