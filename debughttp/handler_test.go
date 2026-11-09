// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package debughttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/debughttp"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/static"
)

// holderA is the node's holder: its member ID, "/" and the 16 bytes of 0x11 its Rand draws, in
// lowercase hex.
const holderA = "a/11111111111111111111111111111111"

// startNode starts AT-OBS-02's single node: member "a" at "a:1", listed by a static membership
// from the start, over one memstore.New(nil), with core.DefaultTiming, a Rand that draws only
// 0x11 bytes, so the holder is holderA, and space "s" of 2 unfenced shards whose Run blocks until
// its ctx ends. The root package's harness lives in its _test files, which this package cannot
// import, so the node is built here from the same parts.
func startNode(t *testing.T) *tenure.Node {
	t.Helper()
	self := tenure.Member{ID: "a", Addr: "a:1"}
	m, err := static.New([]core.Member{self})
	if err != nil {
		t.Fatalf("static.New = %v, want nil", err)
	}
	n, err := tenure.Start(context.Background(), tenure.Config{
		Self:       self,
		Membership: m,
		Store:      memstore.New(nil),
		Spaces: []tenure.Space{{Name: "s", Shards: 2, Run: func(ctx context.Context, _ *tenure.Shard) error {
			<-ctx.Done()
			return nil
		}}},
		Timing: core.DefaultTiming,
		Rand:   fill(0x11),
	})
	if err != nil {
		t.Fatalf("Start = %v, want nil", err)
	}
	return n
}

// fill is a Rand that draws only its own byte.
type fill byte

func (f fill) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(f)
	}
	return len(p), nil
}

// bubbleMark matches the synctest bubble that a goroutine's state names in runtime.Stack's output.
var bubbleMark = regexp.MustCompile(`synctest bubble \d+`)

// baseline notes the goroutines of the caller's synctest bubble. check calls synctest.Wait and
// fails t, listing their stacks, when the bubble has goroutines that were not there at the start.
func baseline(t *testing.T) (check func()) {
	t.Helper()
	bubble := bubbleMark.FindString(stacks(false))
	if bubble == "" {
		t.Fatal("baseline: not inside a synctest bubble")
	}
	start := bubbleGoroutines(bubble)
	return func() {
		t.Helper()
		synctest.Wait()
		var leaked []string
		for id, g := range bubbleGoroutines(bubble) {
			if _, ok := start[id]; !ok {
				leaked = append(leaked, g)
			}
		}
		if len(leaked) > 0 {
			t.Errorf("baseline: %d goroutine(s) not there at the start:\n\n%s", len(leaked), strings.Join(leaked, "\n\n"))
		}
	}
}

// bubbleGoroutines returns the stack of every goroutine of bubble, by its header line.
func bubbleGoroutines(bubble string) map[string]string {
	out := make(map[string]string)
	for g := range strings.SplitSeq(stacks(true), "\n\n") {
		header, _, _ := strings.Cut(g, "\n")
		if bubbleMark.FindString(header) == bubble {
			id, _, _ := strings.Cut(header, " [")
			out[id] = g
		}
	}
	return out
}

// stacks returns runtime.Stack's output for the caller's goroutine, or for all of them.
func stacks(all bool) string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, all)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

// serve serves one request for target through h and returns the answer.
func serve(h http.Handler, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, http.NoBody))
	return rec
}

// wantAnswer fails t unless the answer has status code, the three headers every answer has and,
// unless what names a HEAD request, whose answer has no body, the body's Content-Length.
func wantAnswer(t *testing.T, what string, rec *httptest.ResponseRecorder, code int) {
	t.Helper()
	if rec.Code != code {
		t.Errorf("%s: status %d, want %d; body %s", what, rec.Code, code, rec.Body)
	}
	for k, v := range map[string]string{
		"Content-Type":           "application/json; charset=utf-8",
		"Cache-Control":          "no-store",
		"X-Content-Type-Options": "nosniff",
	} {
		if got := rec.Header().Get(k); got != v {
			t.Errorf("%s: %s = %q, want %q", what, k, got, v)
		}
	}
	if got, want := rec.Header().Get("Content-Length"), strconv.Itoa(rec.Body.Len()); !strings.HasPrefix(what, "HEAD ") && got != want {
		t.Errorf("%s: Content-Length = %q, want %q", what, got, want)
	}
}

// wantError fails t unless the answer is an error answer with status code and the error text
// msg, encoded as every body is.
func wantError(t *testing.T, what string, rec *httptest.ResponseRecorder, code int, msg string) {
	t.Helper()
	wantAnswer(t, what, rec, code)
	m, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if want := "{\n  \"error\": " + string(m) + "\n}\n"; rec.Body.String() != want {
		t.Errorf("%s: body %q, want %q", what, rec.Body, want)
	}
}

// topKeys returns the keys of the JSON object b, in order.
func topKeys(t *testing.T, b []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("body %s: want an object (%v)", b, err)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		key, ok := tok.(string)
		if !ok {
			t.Fatalf("body %s: key %v is not a string", b, tok)
		}
		keys = append(keys, key)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

// tableBody is the part of a table object the test reads.
type tableBody struct {
	Taken  string `json:"taken"`
	Member struct {
		ID string `json:"id"`
	} `json:"member"`
	Holder      string `json:"holder"`
	Closed      bool   `json:"closed"`
	SessionLive bool   `json:"session_live"`
	Spaces      []struct {
		Name string `json:"name"`
		Rows []struct {
			Shard   int    `json:"shard"`
			State   string `json:"state"`
			Holder  string `json:"holder"`
			Running bool   `json:"running"`
		} `json:"rows"`
	} `json:"spaces"`
}

// eventBody is the part of an event object the test reads.
type eventBody struct {
	Seq  uint64 `json:"seq"`
	Kind string `json:"kind"`
}

// eventsBody is an events body, or the root body without its table.
type eventsBody struct {
	NextSeq uint64      `json:"next_seq"`
	Events  []eventBody `json:"events"`
}

// decode decodes the body of rec into v and fails t if it cannot.
func decode(t *testing.T, what string, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("%s: body %s: %v", what, rec.Body, err)
	}
}

// wantEvents fails t unless got holds the kinds and Seqs of want, in order.
func wantEvents(t *testing.T, what string, got []eventBody, want []tenure.Event) {
	t.Helper()
	w := make([]eventBody, len(want))
	for i, e := range want {
		w[i] = eventBody{Seq: e.Seq, Kind: e.Kind.String()}
	}
	if len(got) == 0 {
		got = []eventBody{}
	}
	if !reflect.DeepEqual(got, w) {
		t.Errorf("%s: events %+v, want %+v", what, got, w)
	}
}

// TestATOBS23Endpoints serves AT-OBS-02's node behind http.StripPrefix("/debug/tenure", ...) once
// it owns both shards: "/" and "" serve the table, next_seq and the events; "/table" the table;
// "/events?since=3" the events from Seq 3 on; HEAD the GET headers without a body; a POST 405
// with Allow; another path 404; a bad since 400; and the node's events are what they were. After
// Close, "/table" shows closed true and "/events" ends with node.closed, and no goroutine is left.
// Handler(nil) panics. The test's steps were written from the issue's text, because the original
// acceptance-test text is unavailable.
func TestATOBS23Endpoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		n := startNode(t)
		synctest.Wait()
		h := http.StripPrefix("/debug/tenure", debughttp.Handler(n))
		before, beforeTable := n.RecentEvents(), n.Table()
		if len(before) < 4 {
			t.Fatalf("RecentEvents() = %v, want at least 4 events", before)
		}
		next := before[len(before)-1].Seq + 1

		for _, target := range []string{"/debug/tenure/", "/debug/tenure", "/debug/tenure/?since=abc"} {
			what := "GET " + target
			rec := serve(h, http.MethodGet, target)
			wantAnswer(t, what, rec, http.StatusOK)
			if keys := topKeys(t, rec.Body.Bytes()); !reflect.DeepEqual(keys, []string{"table", "next_seq", "events"}) {
				t.Errorf("%s: keys %v, want table, next_seq, events", what, keys)
			}
			var root struct {
				Table tableBody `json:"table"`
				eventsBody
			}
			decode(t, what, rec, &root)
			if root.Table.Member.ID != "a" || root.Table.Closed || root.NextSeq != next {
				t.Errorf("%s: member %q, closed %t, next_seq %d, want a, false, %d", what, root.Table.Member.ID, root.Table.Closed, root.NextSeq, next)
			}
			wantEvents(t, what, root.Events, before)
		}

		rec := serve(h, http.MethodGet, "/debug/tenure/table")
		wantAnswer(t, "GET /table", rec, http.StatusOK)
		if keys := topKeys(t, rec.Body.Bytes()); len(keys) != 14 || keys[0] != "taken" || keys[13] != "spaces" {
			t.Errorf("GET /table: keys %v, want the table's 14 from taken to spaces", keys)
		}
		var tab tableBody
		decode(t, "GET /table", rec, &tab)
		if want := time.Now().UTC().Format(time.RFC3339Nano); tab.Taken != want {
			t.Errorf("GET /table: taken %q, want %q", tab.Taken, want)
		}
		if tab.Member.ID != "a" || tab.Holder != holderA || tab.Closed || !tab.SessionLive {
			t.Errorf("GET /table: member %q, holder %q, closed %t, session_live %t, want a, %q, false, true",
				tab.Member.ID, tab.Holder, tab.Closed, tab.SessionLive, holderA)
		}
		if len(tab.Spaces) != 1 || tab.Spaces[0].Name != "s" || len(tab.Spaces[0].Rows) != 2 {
			t.Fatalf("GET /table: spaces %+v, want s with 2 rows", tab.Spaces)
		}
		for i, r := range tab.Spaces[0].Rows {
			if r.Shard != i || r.State != "owned" || r.Holder != holderA || !r.Running {
				t.Errorf("GET /table: row %d = %+v, want shard %d owned by %q and running", i, r, i, holderA)
			}
		}
		tableLen := rec.Body.Len()

		for _, tt := range []struct {
			query string
			want  []tenure.Event
		}{
			{"?since=3", before[2:]},
			{"?since=%33", before[2:]},
			{"?x=%zz&since=3&since=abc", before[2:]},
			{"", before},
			{"?since=0", before},
			{"?since=1", before},
			{"?since=" + strconv.FormatUint(next, 10), nil},
			{"?since=18446744073709551615", nil},
		} {
			what := "GET /events" + tt.query
			rec := serve(h, http.MethodGet, "/debug/tenure/events"+tt.query)
			wantAnswer(t, what, rec, http.StatusOK)
			if keys := topKeys(t, rec.Body.Bytes()); !reflect.DeepEqual(keys, []string{"next_seq", "events"}) {
				t.Errorf("%s: keys %v, want next_seq, events", what, keys)
			}
			var ev eventsBody
			decode(t, what, rec, &ev)
			if ev.NextSeq != next {
				t.Errorf("%s: next_seq %d, want %d", what, ev.NextSeq, next)
			}
			wantEvents(t, what, ev.Events, tt.want)
			if tt.want == nil && !strings.HasSuffix(rec.Body.String(), "\"events\": []\n}\n") {
				t.Errorf("%s: body %s, want events []", what, rec.Body)
			}
		}

		rec = serve(h, http.MethodHead, "/debug/tenure/table")
		wantAnswer(t, "HEAD /table", rec, http.StatusOK)
		if rec.Body.Len() != 0 {
			t.Errorf("HEAD /table: body %q, want none", rec.Body)
		}
		if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(tableLen) {
			t.Errorf("HEAD /table: Content-Length %q, want the GET body's %d", got, tableLen)
		}

		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			for _, path := range []string{"/", "/table", "/events?since=abc"} {
				what := method + " " + path
				rec := serve(h, method, "/debug/tenure"+path)
				wantError(t, what, rec, http.StatusMethodNotAllowed, "method "+method+" not allowed")
				if got := rec.Header().Get("Allow"); got != "GET, HEAD" {
					t.Errorf("%s: Allow = %q, want %q", what, got, "GET, HEAD")
				}
			}
		}
		var posted, wantPosted map[string]any
		decode(t, "POST /", serve(h, http.MethodPost, "/debug/tenure/"), &posted)
		if err := json.Unmarshal([]byte(`{"error": "method POST not allowed"}`), &wantPosted); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(posted, wantPosted) {
			t.Errorf("POST /: body %v, want %v", posted, wantPosted)
		}

		for _, path := range []string{"/x", "/table/", "/events/", "//", "/Table"} {
			wantError(t, "GET "+path, serve(h, http.MethodGet, "/debug/tenure"+path), http.StatusNotFound, "not found: "+path)
		}
		wantError(t, "POST /x", serve(h, http.MethodPost, "/debug/tenure/x"), http.StatusNotFound, "not found: /x")
		rec = serve(h, http.MethodHead, "/debug/tenure/x")
		wantAnswer(t, "HEAD /x", rec, http.StatusNotFound)
		if rec.Body.Len() != 0 {
			t.Errorf("HEAD /x: body %q, want none", rec.Body)
		}

		for _, since := range []string{"abc", "", "-1", "+1", "1.5", "0x10", "18446744073709551616"} {
			what := "GET /events?since=" + since
			wantError(t, what, serve(h, http.MethodGet, "/debug/tenure/events?since="+url.QueryEscape(since)), http.StatusBadRequest,
				"invalid since \""+since+"\": want a decimal sequence number")
		}
		// A since that a query parser would drop, because it cannot be unescaped or holds a
		// semicolon, is still a bad since, and the first since counts.
		for query, since := range map[string]string{
			"since=%zz":          "%zz",
			"since=3;x=1":        "3;x=1",
			"since=abc&since=3":  "abc",
			"x=1&since=4%zz&y=2": "4%zz",
		} {
			what := "GET /events?" + query
			wantError(t, what, serve(h, http.MethodGet, "/debug/tenure/events?"+query), http.StatusBadRequest,
				"invalid since \""+since+"\": want a decimal sequence number")
		}

		if after := n.RecentEvents(); !reflect.DeepEqual(after, before) {
			t.Errorf("RecentEvents() after the requests = %v, want %v", after, before)
		}
		// No time passes in the bubble while the test serves requests, so even Taken is the same.
		if after := n.Table(); !reflect.DeepEqual(after, beforeTable) {
			t.Errorf("Table() after the requests = %+v, want %+v", after, beforeTable)
		}

		if err := n.Close(context.Background()); err != nil {
			t.Fatalf("Close = %v, want nil", err)
		}
		rec = serve(h, http.MethodGet, "/debug/tenure/table")
		wantAnswer(t, "GET /table after Close", rec, http.StatusOK)
		if !strings.Contains(rec.Body.String(), "\n  \"closed\": true,\n") {
			t.Errorf("GET /table after Close: body %s, want \"closed\": true", rec.Body)
		}
		rec = serve(h, http.MethodGet, "/debug/tenure/events")
		wantAnswer(t, "GET /events after Close", rec, http.StatusOK)
		var ev eventsBody
		decode(t, "GET /events after Close", rec, &ev)
		wantEvents(t, "GET /events after Close", ev.Events, n.RecentEvents())
		if k := len(ev.Events); k == 0 || ev.Events[k-1].Kind != "node.closed" || ev.NextSeq != ev.Events[k-1].Seq+1 {
			t.Errorf("GET /events after Close: %+v, want node.closed last and next_seq after it", ev)
		}
		check()
	})

	defer func() {
		if r := recover(); r != "debughttp: nil node" {
			t.Errorf("Handler(nil) panicked with %v, want %q", r, "debughttp: nil node")
		}
	}()
	debughttp.Handler(nil)
}
