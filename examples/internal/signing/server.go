// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package signing

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/hmdsefi/tenure"
)

const (
	// maxBody is the most of a request body that the server reads.
	maxBody = 64 << 10

	// RouteHeader is the response header that names the member to send the request to instead.
	RouteHeader = "Tenure-Route"

	// HopsHeader is the request header that counts the members a forwarded request has been
	// through.
	HopsHeader = "Tenure-Hops"

	// maxHops is the number of hops after which a request that is still not local is refused.
	maxHops = 2
)

// SignRequest is the body of POST /sign.
type SignRequest struct {
	Wallet  string `json:"wallet"`
	Payload string `json:"payload"`
}

// server is the HTTP front of one node.
type server struct {
	signer  *Signer
	node    *tenure.Node
	self    string // the node's member ID
	forward bool
	proxy   *httputil.ReverseProxy
}

// proxyTarget carries the owner's address from notLocal to the proxy's Rewrite, in a header of
// the request that the proxy drops from what it sends.
const proxyTarget = "X-Tenure-Proxy-Target"

// Handler returns the HTTP handler of node n, whose member ID is self: POST /sign takes
// {"wallet","payload"}, signs the payload with the wallet's next nonce and broadcasts it.
//
//   - 413: the body is over 64 KiB.
//   - 400: the body is not one JSON request, or the wallet is empty, or Tenure-Hops is not a
//     number.
//   - Not the owner of the wallet's shard: without forward, 421 with Tenure-Route naming the
//     member that owns it. With forward, the request is sent on to that member by a reverse
//     proxy, with Tenure-Hops one higher than it came in; a request that arrives with Tenure-Hops
//     of 2 or more and is still not local gets 508. With no known owner, or an owner that is
//     this node itself (the shard is about to be ours, and no one else can serve it), 503 with
//     Retry-After: 1.
//   - 503 with Retry-After: 1: the shard is not ready for the term, refuses requests, or the
//     fence rejected the nonce reservation. In the last case the shard refuses every further
//     request of its term.
//   - 500: the chain rejected the transaction, or any other failure.
//   - 200: the body is the signed Tx.
func (s *Signer) Handler(n *tenure.Node, self string, forward bool) http.Handler {
	srv := &server{signer: s, node: n, self: self, forward: forward}
	srv.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: "http", Host: pr.In.Header.Get(proxyTarget)})
			hops, _ := strconv.Atoi(pr.In.Header.Get(HopsHeader))
			pr.Out.Header.Del(proxyTarget)
			pr.Out.Header.Set(HopsHeader, strconv.Itoa(hops+1))
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			unavailable(w, err)
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sign", srv.postSign)
	return mux
}

func (s *server) postSign(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		status := http.StatusBadRequest
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			status = http.StatusRequestEntityTooLarge
		}
		reply(w, status, errorBody(err))
		return
	}
	req, err := decodeRequest(raw)
	if err != nil {
		reply(w, http.StatusBadRequest, errorBody(err))
		return
	}
	hops := 0
	if h := r.Header.Get(HopsHeader); h != "" {
		if hops, err = strconv.Atoi(h); err != nil || hops < 0 {
			reply(w, http.StatusBadRequest, errorBody(errors.New("signing: bad "+HopsHeader+" header")))
			return
		}
	}

	sh, err := s.node.Local(SpaceName, req.Wallet)
	if err != nil {
		s.notLocal(w, r, raw, hops, err)
		return
	}
	tx, err := s.signer.sign(r.Context(), sh.ID, sh.Term, req.Wallet, req.Payload)
	switch {
	case err == nil:
		reply(w, http.StatusOK, tx)
	case errors.Is(err, errNotPublished), errors.Is(err, errRefusing), errors.Is(err, ErrStale):
		unavailable(w, err)
	default:
		reply(w, http.StatusInternalServerError, errorBody(err))
	}
}

// notLocal answers a request for a shard this node does not serve.
func (s *server) notLocal(w http.ResponseWriter, r *http.Request, raw []byte, hops int, err error) {
	var notOwner *tenure.NotOwnerError
	// A route to this member itself means the shard is about to be ours: no one else can serve
	// it, so the client waits.
	if !errors.As(err, &notOwner) || notOwner.Route.Addr == "" || notOwner.Route.ID == s.self {
		unavailable(w, err)
		return
	}
	if !s.forward {
		w.Header().Set(RouteHeader, notOwner.Route.Addr)
		reply(w, http.StatusMisdirectedRequest, errorBody(err))
		return
	}
	if hops >= maxHops {
		reply(w, http.StatusLoopDetected, errorBody(errors.New("signing: too many hops")))
		return
	}
	out := r.Clone(r.Context())
	out.Body = io.NopCloser(bytes.NewReader(raw))
	out.ContentLength = int64(len(raw))
	out.Header.Set(proxyTarget, notOwner.Route.Addr)
	out.Header.Set(HopsHeader, strconv.Itoa(hops))
	s.proxy.ServeHTTP(w, out)
}

// decodeRequest reads one sign request from raw: a single JSON object with the request's
// fields and no others, and a wallet.
func decodeRequest(raw []byte) (SignRequest, error) {
	var req SignRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return SignRequest{}, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return SignRequest{}, errors.New("signing: data after the request")
	}
	if req.Wallet == "" {
		return SignRequest{}, errors.New("signing: empty wallet")
	}
	return req, nil
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
