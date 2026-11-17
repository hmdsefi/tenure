// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orderbook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

// Paths of the journal service.
const (
	// RaisePath is the path of POST /raise.
	RaisePath = "/raise"

	// AppendPath is the path of POST /append.
	AppendPath = "/append"

	// EntriesPath is the path of GET /entries?shard=N.
	EntriesPath = "/entries"

	// JournalPath is the path of GET /journal.
	JournalPath = "/journal"
)

const (
	// errNameStale is the error name of a 409 answer of the journal service.
	errNameStale = "stale"

	// maxRequest is the most of a request body that the journal service reads.
	maxRequest = 64 << 10

	// maxAnswer is the most of an answer of the journal service that a client reads.
	maxAnswer = 64 << 20

	// defaultTimeout bounds one call of a client made without an http.Client of its own.
	defaultTimeout = 10 * time.Second
)

// raiseRequest is the body of POST /raise.
type raiseRequest struct {
	Shard int       `json:"shard"`
	Term  core.Term `json:"term"`
}

// appendRequest is the body of POST /append.
type appendRequest struct {
	Shard int             `json:"shard"`
	Term  core.Term       `json:"term"`
	Line  json.RawMessage `json:"line"`
}

// entriesResponse is the body of the answer to GET /entries.
type entriesResponse struct {
	Entries []json.RawMessage `json:"entries"`
}

// journalResponse is the body of the answer to GET /journal.
type journalResponse struct {
	Shards [][]json.RawMessage `json:"shards"`
}

// NewJournalHandler returns the HTTP handler of the journal service, which puts the journal of
// db behind one address so that several orderbook pods share it:
//
//   - POST /raise {"shard","term"} raises the shard's fence; 200 with {}.
//   - POST /append {"shard","term","line"} appends line, a JSON value, to the shard's journal
//     under term; 200 with {}.
//   - GET /entries?shard=N answers 200 with {"entries":[line, ...]}.
//   - GET /journal answers 200 with {"shards":[[line, ...], ...]}, every shard's journal.
//
// A stale term answers 409 with {"error":"stale"}; a request that is not one JSON request of
// the endpoint, or a shard outside the space, answers 400 (413 over 64 KiB), and any other
// failure 500. NewHTTPJournal turns the 409 back into fencedb.ErrStale.
func NewJournalHandler(db *fencedb.DB) http.Handler {
	s := &journalsvc{j: NewDBJournal(db)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+RaisePath, s.raise)
	mux.HandleFunc("POST "+AppendPath, s.append)
	mux.HandleFunc("GET "+EntriesPath, s.entries)
	mux.HandleFunc("GET "+JournalPath, s.journal)
	return mux
}

type journalsvc struct {
	j *DBJournal
}

func (s *journalsvc) raise(w http.ResponseWriter, r *http.Request) {
	var req raiseRequest
	if !decodeRequest(w, r, &req) || !validShard(w, req.Shard) {
		return
	}
	replyJournal(w, s.j.Raise(r.Context(), req.Shard, req.Term), struct{}{})
}

func (s *journalsvc) append(w http.ResponseWriter, r *http.Request) {
	var req appendRequest
	if !decodeRequest(w, r, &req) || !validShard(w, req.Shard) {
		return
	}
	if len(req.Line) == 0 {
		reply(w, http.StatusBadRequest, errorBody(errors.New("orderbook: no line")))
		return
	}
	replyJournal(w, s.j.Append(r.Context(), req.Shard, req.Term, req.Line), struct{}{})
}

func (s *journalsvc) entries(w http.ResponseWriter, r *http.Request) {
	shard, err := strconv.Atoi(r.URL.Query().Get("shard"))
	if err != nil {
		reply(w, http.StatusBadRequest, errorBody(fmt.Errorf("orderbook: shard: %w", err)))
		return
	}
	if !validShard(w, shard) {
		return
	}
	lines, err := s.j.Entries(r.Context(), shard)
	replyJournal(w, err, entriesResponse{Entries: rawLines(lines)})
}

func (s *journalsvc) journal(w http.ResponseWriter, r *http.Request) {
	resp := journalResponse{Shards: make([][]json.RawMessage, Shards)}
	for shard := range Shards {
		lines, err := s.j.Entries(r.Context(), shard)
		if err != nil {
			replyJournal(w, err, nil)
			return
		}
		resp.Shards[shard] = rawLines(lines)
	}
	reply(w, http.StatusOK, resp)
}

// rawLines returns lines as JSON values, never nil.
func rawLines(lines [][]byte) []json.RawMessage {
	out := make([]json.RawMessage, len(lines))
	for i, l := range lines {
		out[i] = l
	}
	return out
}

// validShard answers 400 and returns false when shard is not a shard of the markets space.
func validShard(w http.ResponseWriter, shard int) bool {
	if shard < 0 || shard >= Shards {
		reply(w, http.StatusBadRequest, errorBody(fmt.Errorf("orderbook: shard %d is not in [0, %d)", shard, Shards)))
		return false
	}
	return true
}

// decodeRequest reads the body of r, up to maxRequest bytes, as exactly one JSON value of v's
// type with no unknown fields. When it fails it has answered 400 (413 for a body that is too
// large) and returns false.
func decodeRequest(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequest))
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil {
		if _, terr := dec.Token(); !errors.Is(terr, io.EOF) {
			err = errors.New("orderbook: data after the request")
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

// replyJournal answers a Journal call: 200 with v when err is nil, 409 with the name "stale"
// for a stale term, otherwise 500.
func replyJournal(w http.ResponseWriter, err error, v any) {
	switch {
	case err == nil:
		reply(w, http.StatusOK, v)
	case errors.Is(err, fencedb.ErrStale):
		reply(w, http.StatusConflict, map[string]string{"error": errNameStale, "detail": err.Error()})
	default:
		reply(w, http.StatusInternalServerError, errorBody(err))
	}
}

// HTTPJournal is a Journal served by a journal service at a URL. Make one with NewHTTPJournal.
type HTTPJournal struct {
	base string
	hc   *http.Client
}

// NewHTTPJournal returns a Journal that calls the journal service at base, such as
// "http://journalsvc:8080", with hc; a nil hc means a client with a 10 s timeout. A stale
// answer of the service comes back as an error that wraps fencedb.ErrStale.
func NewHTTPJournal(base string, hc *http.Client) *HTTPJournal {
	if hc == nil {
		hc = &http.Client{Timeout: defaultTimeout}
	}
	return &HTTPJournal{base: strings.TrimSuffix(base, "/"), hc: hc}
}

// Raise calls POST /raise.
func (j *HTTPJournal) Raise(ctx context.Context, shard int, term core.Term) error {
	return j.do(ctx, http.MethodPost, RaisePath, raiseRequest{Shard: shard, Term: term}, nil)
}

// Entries calls GET /entries.
func (j *HTTPJournal) Entries(ctx context.Context, shard int) ([][]byte, error) {
	var resp entriesResponse
	if err := j.do(ctx, http.MethodGet, EntriesPath+"?shard="+strconv.Itoa(shard), nil, &resp); err != nil {
		return nil, err
	}
	return byteLines(resp.Entries), nil
}

// Append calls POST /append. line must be one JSON value, as an Entry's encoding is.
func (j *HTTPJournal) Append(ctx context.Context, shard int, term core.Term, line []byte) error {
	return j.do(ctx, http.MethodPost, AppendPath, appendRequest{Shard: shard, Term: term, Line: line}, nil)
}

// All calls GET /journal and returns every shard's journal lines, indexed by shard.
func (j *HTTPJournal) All(ctx context.Context) ([][][]byte, error) {
	var resp journalResponse
	if err := j.do(ctx, http.MethodGet, JournalPath, nil, &resp); err != nil {
		return nil, err
	}
	out := make([][][]byte, len(resp.Shards))
	for i, s := range resp.Shards {
		out[i] = byteLines(s)
	}
	return out, nil
}

// byteLines returns JSON values as journal lines.
func byteLines(raw []json.RawMessage) [][]byte {
	out := make([][]byte, len(raw))
	for i, r := range raw {
		out[i] = r
	}
	return out
}

// do sends body as JSON (none when nil) and decodes a 200 answer into out (nothing when nil).
// A 409 answer named "stale" becomes an error that wraps fencedb.ErrStale; any other status is
// an error that says the status and the service's message.
func (j *HTTPJournal) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("orderbook: encode %s: %w", path, err)
		}
		rd = bytes.NewReader(raw)
	}
	// The base URL is the operator's own configuration (JOURNAL_URL), not request input.
	req, err := http.NewRequestWithContext(ctx, method, j.base+path, rd) //nolint:gosec // G704: base is configuration
	if err != nil {
		return fmt.Errorf("orderbook: %s: %w", path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := j.hc.Do(req) //nolint:gosec // G704: base is configuration
	if err != nil {
		return fmt.Errorf("orderbook: %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswer))
	if err != nil {
		return fmt.Errorf("orderbook: %s: read the answer: %w", path, err)
	}
	if resp.StatusCode == http.StatusOK {
		if out == nil {
			return nil
		}
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("orderbook: %s: decode the answer: %w", path, err)
		}
		return nil
	}
	var msg struct {
		Error  string `json:"error"`
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(raw, &msg)
	if resp.StatusCode == http.StatusConflict && msg.Error == errNameStale {
		return fmt.Errorf("%w (%s)", fencedb.ErrStale, msg.Detail)
	}
	return fmt.Errorf("orderbook: %s: %d %s: %s", path, resp.StatusCode, http.StatusText(resp.StatusCode), msg.Error)
}
