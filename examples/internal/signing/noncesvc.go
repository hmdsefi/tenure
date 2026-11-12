// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package signing

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/hmdsefi/tenure/core"
)

// Paths of the nonce service.
const (
	// RaisePath is the path of POST /raise.
	RaisePath = "/raise"

	// ReservePath is the path of POST /reserve.
	ReservePath = "/reserve"

	// BroadcastPath is the path of POST /broadcast.
	BroadcastPath = "/broadcast"

	// ReportPath is the path of GET /report.
	ReportPath = "/report"
)

// The error names of a 409 answer of the nonce service.
const (
	errNameStale    = "stale"
	errNameConflict = "conflict"
	errNameGap      = "gap"
)

// raiseRequest is the body of POST /raise.
type raiseRequest struct {
	Shard int       `json:"shard"`
	Term  core.Term `json:"term"`
}

// reserveRequest is the body of POST /reserve.
type reserveRequest struct {
	Shard  int       `json:"shard"`
	Term   core.Term `json:"term"`
	Wallet string    `json:"wallet"`
}

// reserveResponse is the body of the answer to POST /reserve.
type reserveResponse struct {
	Nonce uint64 `json:"nonce"`
}

// NewNoncesvc returns the HTTP handler of the nonce service, which puts the nonces and the
// chain behind one address so that several signer pods share them:
//
//   - POST /raise {"shard","term"} raises the shard's fence; 200 with {}.
//   - POST /reserve {"shard","term","wallet"} reserves the wallet's next nonce; 200 with
//     {"nonce"}.
//   - POST /broadcast with a Tx submits it to the chain; 200 with {}.
//   - GET /report answers 200 with the chain's Report.
//
// A stale term, and a transaction the chain rejects, answer 409 with {"error":"stale"},
// {"error":"conflict"} or {"error":"gap"}; a body that is not one JSON request of the endpoint
// answers 400 (413 over 64 KiB), and any other failure 500. NewHTTPNonces and NewHTTPChain turn
// these answers back into ErrStale, ErrConflict and ErrGap.
func NewNoncesvc(nonces Nonces, chain *FakeChain) http.Handler {
	s := &noncesvc{nonces: nonces, chain: chain}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+RaisePath, s.raise)
	mux.HandleFunc("POST "+ReservePath, s.reserve)
	mux.HandleFunc("POST "+BroadcastPath, s.broadcast)
	mux.HandleFunc("GET "+ReportPath, s.report)
	return mux
}

type noncesvc struct {
	nonces Nonces
	chain  *FakeChain
}

func (s *noncesvc) raise(w http.ResponseWriter, r *http.Request) {
	var req raiseRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if err := s.nonces.Raise(r.Context(), req.Shard, req.Term); err != nil {
		replyFailure(w, err)
		return
	}
	reply(w, http.StatusOK, struct{}{})
}

func (s *noncesvc) reserve(w http.ResponseWriter, r *http.Request) {
	var req reserveRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Wallet == "" {
		reply(w, http.StatusBadRequest, errorBody(errors.New("signing: empty wallet")))
		return
	}
	nonce, err := s.nonces.Reserve(r.Context(), req.Shard, req.Term, req.Wallet)
	if err != nil {
		replyFailure(w, err)
		return
	}
	reply(w, http.StatusOK, reserveResponse{Nonce: nonce})
}

func (s *noncesvc) broadcast(w http.ResponseWriter, r *http.Request) {
	var tx Tx
	if !decodeBody(w, r, &tx) {
		return
	}
	if err := s.chain.Broadcast(r.Context(), tx); err != nil {
		replyFailure(w, err)
		return
	}
	reply(w, http.StatusOK, struct{}{})
}

func (s *noncesvc) report(w http.ResponseWriter, _ *http.Request) {
	reply(w, http.StatusOK, s.chain.Report())
}

// decodeBody reads the body of r, up to maxBody bytes, as exactly one JSON value of v's type
// with no unknown fields. When it fails it has answered 400 (413 for a body that is too large)
// and returns false.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err == nil {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		err = dec.Decode(v)
		if err == nil {
			if _, terr := dec.Token(); !errors.Is(terr, io.EOF) {
				err = errors.New("signing: data after the request")
			}
		}
	}
	if err != nil {
		status := http.StatusBadRequest
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			status = http.StatusRequestEntityTooLarge
		}
		reply(w, status, errorBody(err))
		return false
	}
	return true
}

// replyFailure answers a failed Nonces or Chain call: 409 with the error's name for a stale
// term, a conflict or a gap, otherwise 500.
func replyFailure(w http.ResponseWriter, err error) {
	var name string
	switch {
	case errors.Is(err, ErrStale):
		name = errNameStale
	case errors.Is(err, ErrConflict):
		name = errNameConflict
	case errors.Is(err, ErrGap):
		name = errNameGap
	default:
		reply(w, http.StatusInternalServerError, errorBody(err))
		return
	}
	reply(w, http.StatusConflict, map[string]string{"error": name, "detail": err.Error()})
}

// statusText names an HTTP status for an error message.
func statusText(code int) string { return strconv.Itoa(code) + " " + http.StatusText(code) }
