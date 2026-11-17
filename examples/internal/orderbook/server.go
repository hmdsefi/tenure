// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orderbook

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

const (
	// Shards is the number of shards of the markets space.
	Shards = 16

	// OrdersPath is the path of POST /orders.
	OrdersPath = "/orders"

	// maxOrderBody is the most of a request body that the server reads.
	maxOrderBody = 64 << 10

	// routeHeader is the response header that names the member to send the request to instead.
	routeHeader = "Tenure-Route"
)

// Space returns the markets space served by o: 16 shards, fenced, so that a node that cannot
// reach the store keeps its shards running and relies on the journal's fence to stop it.
func (o *Orderbook) Space() tenure.Space {
	sp := marketsSpace()
	sp.Run = o.Run
	return sp
}

// Spaces returns the spaces of the orderbook as a Router takes them: the markets space, as
// Space describes it, without a Run.
func Spaces() []tenure.Space {
	return []tenure.Space{marketsSpace()}
}

func marketsSpace() tenure.Space {
	return tenure.Space{Name: SpaceName, Shards: Shards, Fenced: true}
}

// server is the HTTP front of one node: it takes an order for the shard this node owns and sends
// every other request to the owner.
type server struct {
	node *tenure.Node
	ob   *Orderbook
	self string // the node's member ID
}

// NewServer returns the handler of node n, whose member ID is self and which runs ob: POST
// /orders, as postOrder describes it.
func NewServer(n *tenure.Node, ob *Orderbook, self string) http.Handler {
	s := &server{node: n, ob: ob, self: self}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+OrdersPath, s.postOrder)
	return mux
}

// Result is the body of a 200 answer to POST /orders.
type Result struct {
	ID    string `json:"id"`
	Fills []Fill `json:"fills"`
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
func (s *server) postOrder(w http.ResponseWriter, r *http.Request) {
	o, err := decodeOrder(http.MaxBytesReader(w, r.Body, maxOrderBody))
	if err != nil {
		reply(w, http.StatusBadRequest, errorBody(err))
		return
	}

	sh, err := s.node.Local(SpaceName, o.Market)
	if err != nil {
		var notOwner *tenure.NotOwnerError
		// A route to this member itself means the shard is about to be ours: no one else
		// can serve it, so the client waits.
		if errors.As(err, &notOwner) && notOwner.Route.Addr != "" && notOwner.Route.ID != s.self {
			w.Header().Set(routeHeader, notOwner.Route.Addr)
			reply(w, http.StatusMisdirectedRequest, errorBody(err))
			return
		}
		unavailable(w, err)
		return
	}
	e := s.ob.Engine(sh.ID, sh.Term)
	if e == nil {
		unavailable(w, errors.New("orderbook: the shard's engine is not ready"))
		return
	}

	fills, err := e.submit(o)
	switch {
	case err == nil:
		reply(w, http.StatusOK, Result{ID: o.ID, Fills: fills})
	case errors.Is(err, errInvalidOrder):
		reply(w, http.StatusBadRequest, errorBody(err))
	case errors.Is(err, errDuplicate):
		reply(w, http.StatusConflict, errorBody(err))
	case errors.Is(err, errRefusing), errors.Is(err, fencedb.ErrStale):
		unavailable(w, err)
	default:
		reply(w, http.StatusInternalServerError, errorBody(err))
	}
}

// decodeOrder reads one order from r: a single JSON object with the order's fields and no others.
func decodeOrder(r io.Reader) (Order, error) {
	var o Order
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&o); err != nil {
		return Order{}, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Order{}, errors.New("orderbook: data after the order")
	}
	return o, o.validate()
}

// unavailable answers 503 and asks the client to retry in a second.
func unavailable(w http.ResponseWriter, err error) {
	w.Header().Set("Retry-After", "1")
	reply(w, http.StatusServiceUnavailable, errorBody(err))
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
