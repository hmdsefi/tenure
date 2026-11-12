// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package signing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// defaultTimeout bounds one call of a client made without an http.Client of its own, so that a
// call to a service that does not answer cannot wait for ever.
const defaultTimeout = 10 * time.Second

// maxAnswer is the most of an answer of the nonce service that a client reads.
const maxAnswer = 1 << 20

// HTTPNonces is Nonces served by a nonce service at a URL. Make one with NewHTTPNonces.
type HTTPNonces struct {
	c *httpClient
}

// NewHTTPNonces returns Nonces that call the nonce service at base, such as
// "http://noncesvc:8080", with hc; a nil hc means a client with a 10 s timeout. A stale answer
// of the service comes back as an error that wraps ErrStale.
func NewHTTPNonces(base string, hc *http.Client) *HTTPNonces {
	return &HTTPNonces{c: newHTTPClient(base, hc)}
}

// Raise calls POST /raise.
func (n *HTTPNonces) Raise(ctx context.Context, shard int, term core.Term) error {
	return n.c.do(ctx, http.MethodPost, RaisePath, raiseRequest{Shard: shard, Term: term}, nil)
}

// Reserve calls POST /reserve. An error that is not ErrStale does not say whether the service
// reserved the nonce: a lost answer can leave a nonce spent that no one signed.
func (n *HTTPNonces) Reserve(ctx context.Context, shard int, term core.Term, wallet string) (uint64, error) {
	var resp reserveResponse
	err := n.c.do(ctx, http.MethodPost, ReservePath, reserveRequest{Shard: shard, Term: term, Wallet: wallet}, &resp)
	if err != nil {
		return 0, err
	}
	return resp.Nonce, nil
}

// HTTPChain is Chain, and the chain's Report, served by a nonce service at a URL. Make one with
// NewHTTPChain.
type HTTPChain struct {
	c *httpClient
}

// NewHTTPChain returns a Chain that calls the nonce service at base with hc; a nil hc means
// a client with a 10 s timeout. A rejection of the service comes back as an error that wraps
// ErrConflict or ErrGap.
func NewHTTPChain(base string, hc *http.Client) *HTTPChain {
	return &HTTPChain{c: newHTTPClient(base, hc)}
}

// Broadcast calls POST /broadcast. The signer calls it after it reserved the transaction's
// nonce, on a context that does not end with the client's request, and a failed Broadcast, a
// rejection or a lost answer, burns that nonce: the signer does not reuse it, and the chain's
// later transactions of the wallet are rejected as gaps if the chain never saw this one.
func (c *HTTPChain) Broadcast(ctx context.Context, tx Tx) error {
	return c.c.do(ctx, http.MethodPost, BroadcastPath, tx, nil)
}

// Report calls GET /report.
func (c *HTTPChain) Report(ctx context.Context) (Report, error) {
	var rep Report
	if err := c.c.do(ctx, http.MethodGet, ReportPath, nil, &rep); err != nil {
		return Report{}, err
	}
	return rep, nil
}

// httpClient is the call machinery of both clients.
type httpClient struct {
	base string
	hc   *http.Client
}

func newHTTPClient(base string, hc *http.Client) *httpClient {
	if hc == nil {
		hc = &http.Client{Timeout: defaultTimeout}
	}
	return &httpClient{base: strings.TrimSuffix(base, "/"), hc: hc}
}

// do sends body as JSON (none when nil) and decodes a 200 answer into out (nothing when nil).
// A 409 answer becomes the error that its name stands for; any other status is an error that
// says the status and the service's message.
func (c *httpClient) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("signing: encode %s: %w", path, err)
		}
		rd = bytes.NewReader(raw)
	}
	// The base URL is the operator's own configuration (NONCESVC_URL), not request input.
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd) //nolint:gosec // G704: base is configuration
	if err != nil {
		return fmt.Errorf("signing: %s: %w", path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req) //nolint:gosec // G704: base is configuration
	if err != nil {
		return fmt.Errorf("signing: %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswer))
	if err != nil {
		return fmt.Errorf("signing: %s: read the answer: %w", path, err)
	}

	var msg struct {
		Error  string `json:"error"`
		Detail string `json:"detail"`
	}
	if resp.StatusCode == http.StatusOK {
		if out == nil {
			return nil
		}
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("signing: %s: decode the answer: %w", path, err)
		}
		return nil
	}
	_ = json.Unmarshal(raw, &msg)
	if resp.StatusCode == http.StatusConflict {
		var sentinel error
		switch msg.Error {
		case errNameStale:
			sentinel = ErrStale
		case errNameConflict:
			sentinel = ErrConflict
		case errNameGap:
			sentinel = ErrGap
		}
		if sentinel != nil {
			if msg.Detail != "" {
				return fmt.Errorf("%w (%s)", sentinel, msg.Detail)
			}
			return sentinel
		}
	}
	return fmt.Errorf("signing: %s: %s: %s", path, statusText(resp.StatusCode), msg.Error)
}
