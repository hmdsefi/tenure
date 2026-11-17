// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package viewhttp_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/viewhttp"
)

// The tests of Handler. Steps of the TestATRTR16 tests were written for them from the issue's
// text.

// source is a Source for the tests: it holds a current view, which set replaces, and hands it to
// every open Watch channel, latest wins. fail makes Watch fail.
type source struct {
	mu    sync.Mutex
	cur   *core.View
	subs  map[chan core.View]struct{}
	fail  error
	calls int // Watch calls
}

func newSource(v *core.View) *source {
	return &source{cur: v, subs: make(map[chan core.View]struct{})}
}

func (s *source) Watch(ctx context.Context) (<-chan core.View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.fail != nil {
		return nil, s.fail
	}
	ch := make(chan core.View, 1)
	if s.cur != nil {
		ch <- *s.cur
	}
	s.subs[ch] = struct{}{}
	context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.subs[ch]; ok {
			delete(s.subs, ch)
			close(ch)
		}
	})
	return ch, nil
}

// set makes v the current view and sends it to every open channel.
func (s *source) set(v core.View) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur = &v
	for ch := range s.subs {
		select {
		case <-ch:
		default:
		}
		ch <- v
	}
}

// closeAll closes every open channel.
func (s *source) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subs {
		delete(s.subs, ch)
		close(ch)
	}
}

// open returns the number of open channels.
func (s *source) open() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

// viewOf returns a view of Version version with the members ids.
func viewOf(version uint64, ids ...string) core.View {
	v := core.View{Version: version}
	for _, id := range ids {
		v.Members = append(v.Members, core.Member{ID: id, Addr: "addr-" + id, Slot: -1})
	}
	return v
}

// get serves a GET of target with the header If-None-Match tag (none when "") on h, in the
// caller's goroutine, and returns the recorded answer.
func get(h http.Handler, target, tag string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	if tag != "" {
		r.Header.Set("If-None-Match", tag)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// tagPattern is the form of every ETag: 16 hex digits of epoch, then the version.
var tagPattern = regexp.MustCompile(`^"[0-9a-f]{16}-([0-9]+)"$`)

// wantView fails t unless w is a 200 answer of a view of Version version, and returns its ETag.
func wantView(t *testing.T, w *httptest.ResponseRecorder, version string) string {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status %d (%s), want 200", w.Code, strings.TrimSpace(w.Body.String()))
	}
	tag := w.Header().Get("ETag")
	m := tagPattern.FindStringSubmatch(tag)
	if m == nil || m[1] != version {
		t.Fatalf("ETag %q, want the form \"<16 hex digits>-%s\"", tag, version)
	}
	if !strings.HasPrefix(w.Body.String(), `{"version":`+version+",") {
		t.Fatalf("body %q, want a view of version %s", w.Body.String(), version)
	}
	for k, want := range map[string]string{
		"Content-Type": "application/json", "Cache-Control": "no-cache", "Vary": "Accept-Encoding",
	} {
		if got := w.Header().Get(k); got != want {
			t.Errorf("%s: %q, want %q", k, got, want)
		}
	}
	return tag
}

// wantNotModified fails t unless w is a 304 answer with the ETag tag.
func wantNotModified(t *testing.T, w *httptest.ResponseRecorder, tag string) {
	t.Helper()
	if w.Code != http.StatusNotModified {
		t.Fatalf("status %d (%s), want 304", w.Code, strings.TrimSpace(w.Body.String()))
	}
	if got := w.Header().Get("ETag"); got != tag {
		t.Errorf("ETag %q, want %q", got, tag)
	}
	if got := w.Header().Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control %q, want no-cache", got)
	}
}

// TestATRTR16LongPoll checks the long poll: a request without a tag gets the view at once; with
// the current tag it gets 304 at once without wait; with wait, a change answers it at once with
// the new view, and no change answers 304 when the wait ends, a wait above 60s being cut to 60s.
func TestATRTR16LongPoll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(new(viewOf(1, "m1")))
		h := viewhttp.Handler(src)
		tag := wantView(t, get(h, "/view", ""), "1")
		wantNotModified(t, get(h, "/view", tag), tag)

		start := time.Now()
		done := make(chan *httptest.ResponseRecorder)
		go func() { done <- get(h, "/view?wait=10s", tag) }()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		src.set(viewOf(2, "m1", "m2"))
		w := <-done
		if d := time.Since(start); d != 3*time.Second {
			t.Errorf("long poll answered after %v, want 3s, at the change", d)
		}
		tag2 := wantView(t, w, "2")
		if !strings.Contains(w.Body.String(), `"id":"m2"`) {
			t.Errorf("body %s, want m2", w.Body.String())
		}

		for _, tt := range []struct {
			wait string
			want time.Duration
		}{{"10s", 10 * time.Second}, {"0s", 0}, {"2m", time.Minute}} {
			start = time.Now()
			wantNotModified(t, get(h, "/view?wait="+tt.wait, tag2), tag2)
			if d := time.Since(start); d != tt.want {
				t.Errorf("wait=%s: 304 after %v, want %v", tt.wait, d, tt.want)
			}
		}
		// An old tag gets the current view at once, even with a wait.
		start = time.Now()
		wantView(t, get(h, "/view?wait=10s", tag), "2")
		if d := time.Since(start); d != 0 {
			t.Errorf("old tag answered after %v, want at once", d)
		}
		synctest.Wait()
		if n := src.open(); n != 0 {
			t.Errorf("%d Watch channels open after every request returned, want 0", n)
		}
	})
}

// TestATRTR16Restart checks that two handlers over the same source, as before and after a
// restart, send different tags for the same view, and that one's tag is a change to the other.
func TestATRTR16Restart(t *testing.T) {
	src := newSource(new(viewOf(7, "m1")))
	h1, h2 := viewhttp.Handler(src), viewhttp.Handler(src)
	tag1 := wantView(t, get(h1, "/view", ""), "7")
	tag2 := wantView(t, get(h2, "/view", ""), "7")
	if tag1 == tag2 {
		t.Fatalf("both handlers send ETag %s, want different tags", tag1)
	}
	wantView(t, get(h2, "/view?wait=1m", tag1), "7")
}

// TestATRTR16Gzip checks over a real server that a request that accepts gzip gets the body
// gzip-encoded, that it decodes to the plain body, and that Go's client decodes it itself.
func TestATRTR16Gzip(t *testing.T) {
	v := viewOf(3, "m1", "m2", "m3")
	v.Slots, v.SlotHolders = 2, []string{"m1/x", ""}
	v.Members[0].Slot = 0
	srv := httptest.NewServer(viewhttp.Handler(newSource(&v)))
	defer srv.Close()

	fetch := func(acceptGzip bool) (http.Header, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/view", nil)
		if err != nil {
			t.Fatal(err)
		}
		if acceptGzip {
			req.Header.Set("Accept-Encoding", "gzip")
		} else {
			req.Header.Set("Accept-Encoding", "identity")
		}
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.Header, b
	}
	plainResp, plain := fetch(false)
	if got := plainResp.Get("Content-Encoding"); got != "" {
		t.Errorf("identity: Content-Encoding %q, want none", got)
	}
	zResp, zipped := fetch(true)
	if got := zResp.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("gzip: Content-Encoding %q, want gzip", got)
	}
	if got := zResp.Get("Vary"); got != "Accept-Encoding" {
		t.Errorf("Vary %q, want Accept-Encoding", got)
	}
	zr, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	unzipped, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if !bytes.Equal(unzipped, plain) {
		t.Errorf("gunzipped body %q, want the plain body %q", unzipped, plain)
	}
	if plainResp.Get("ETag") != zResp.Get("ETag") {
		t.Errorf("ETags %q and %q differ by encoding", plainResp.Get("ETag"), zResp.Get("ETag"))
	}

	resp, err := http.Get(srv.URL + "/view")
	if err != nil {
		t.Fatalf("http.Get: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !resp.Uncompressed || !bytes.Equal(b, plain) {
		t.Errorf("http.Get: Uncompressed %t, body %q, want true and the plain body", resp.Uncompressed, b)
	}
}

// TestATRTR16ThousandWaiters checks that 1000 requests waiting on the same tag are all answered
// with the new view on one change, at once.
func TestATRTR16ThousandWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(new(viewOf(1, "m1")))
		h := viewhttp.Handler(src)
		tag := wantView(t, get(h, "/view", ""), "1")
		const n = 1000
		answers := make(chan *httptest.ResponseRecorder, n)
		for range n {
			go func() {
				r := httptest.NewRequest(http.MethodGet, "/view?wait=60s", nil)
				r.Header.Set("If-None-Match", tag)
				r.Header.Set("Accept-Encoding", "gzip")
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				answers <- w
			}()
		}
		synctest.Wait()
		if got := src.open(); got != n {
			t.Fatalf("%d requests waiting, want %d", got, n)
		}
		start := time.Now()
		src.set(viewOf(2, "m1", "m2"))
		var first []byte
		for range n {
			w := <-answers
			if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "gzip" {
				t.Fatalf("status %d, Content-Encoding %q, want 200 gzip", w.Code, w.Header().Get("Content-Encoding"))
			}
			if first == nil {
				first = w.Body.Bytes()
			} else if !bytes.Equal(w.Body.Bytes(), first) {
				t.Fatal("answers to the same view differ")
			}
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("waiters answered after %v, want at once", d)
		}
	})
}

// TestHandlerJSON checks the body of a view with every field set, and of an empty view.
func TestHandlerJSON(t *testing.T) {
	at := time.Date(2026, 11, 17, 10, 30, 0, 123456789, time.FixedZone("x", 3600))
	v := core.View{
		Version: 9,
		Members: []core.Member{
			{ID: "m1", Addr: "10.0.0.1:80", Meta: map[string]string{"zone": "a"}, Slot: 1, JoinAt: at},
			{ID: "m2", Addr: "10.0.0.2:80", Draining: true, Slot: -1, LeaveAt: at.Add(time.Second)},
		},
		Slots:       2,
		SlotHolders: []string{"", "m1/ab"},
	}
	want := `{"version":9,"members":[` +
		`{"id":"m1","addr":"10.0.0.1:80","meta":{"zone":"a"},"draining":false,"slot":1,"join_at":"2026-11-17T09:30:00.123456789Z"},` +
		`{"id":"m2","addr":"10.0.0.2:80","draining":true,"slot":-1,"leave_at":"2026-11-17T09:30:01.123456789Z"}],` +
		`"slots":2,"slot_holders":["","m1/ab"]}` + "\n"
	if got := get(viewhttp.Handler(newSource(&v)), "/view", "").Body.String(); got != want {
		t.Errorf("body:\n%s\nwant:\n%s", got, want)
	}
	const wantEmpty = `{"version":1,"members":[],"slots":0,"slot_holders":[]}` + "\n"
	if got := get(viewhttp.Handler(newSource(&core.View{Version: 1})), "/view", "").Body.String(); got != wantEmpty {
		t.Errorf("empty view body %q, want %q", got, wantEmpty)
	}
}

// TestHandlerErrors checks the answers to wrong paths, methods and waits, and to a source that
// fails or closes its channel.
func TestHandlerErrors(t *testing.T) {
	src := newSource(new(viewOf(1, "m1")))
	h := viewhttp.Handler(src)
	tests := []struct {
		method, target string
		code           int
		body           string
	}{
		{http.MethodGet, "/", http.StatusNotFound, "viewhttp: not found: /"},
		{http.MethodGet, "/views", http.StatusNotFound, "viewhttp: not found: /views"},
		{http.MethodGet, "/view/", http.StatusNotFound, "viewhttp: not found: /view/"},
		{http.MethodPost, "/view", http.StatusMethodNotAllowed, "viewhttp: method POST not allowed"},
		{http.MethodHead, "/a/view", http.StatusMethodNotAllowed, "viewhttp: method HEAD not allowed"},
		{http.MethodGet, "/view?wait=soon", http.StatusBadRequest, `viewhttp: invalid wait "soon"`},
		{http.MethodGet, "/view?wait=", http.StatusBadRequest, `viewhttp: invalid wait ""`},
		{http.MethodGet, "/view?wait=-1s", http.StatusBadRequest, `viewhttp: negative wait "-1s"`},
	}
	for _, tt := range tests {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tt.method, tt.target, nil))
		if got := strings.TrimSpace(w.Body.String()); w.Code != tt.code || got != tt.body {
			t.Errorf("%s %s: %d %q, want %d %q", tt.method, tt.target, w.Code, got, tt.code, tt.body)
		}
		if tt.code == http.StatusMethodNotAllowed && w.Header().Get("Allow") != "GET" {
			t.Errorf("%s %s: Allow %q, want GET", tt.method, tt.target, w.Header().Get("Allow"))
		}
	}
	if src.calls != 0 {
		t.Errorf("source watched %d times for refused requests, want 0", src.calls)
	}
	wantView(t, get(h, "/tenure/view", ""), "1")

	src.fail = errors.New("boom")
	if w := get(h, "/view", ""); w.Code != http.StatusServiceUnavailable ||
		strings.TrimSpace(w.Body.String()) != "viewhttp: watch: boom" {
		t.Errorf("failing Watch: %d %q, want 503 %q", w.Code, w.Body.String(), "viewhttp: watch: boom")
	}
}

// TestHandlerSourceCloses checks that a waiting request gets 503 when the source closes its
// channel, that a request ending while it waits releases its watch, and that a request waits for
// a source's first view, at most for its wait.
func TestHandlerSourceCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newSource(new(viewOf(1, "m1")))
		h := viewhttp.Handler(src)
		tag := wantView(t, get(h, "/view", ""), "1")
		done := make(chan *httptest.ResponseRecorder)
		go func() { done <- get(h, "/view?wait=30s", tag) }()
		synctest.Wait()
		src.closeAll()
		if w := <-done; w.Code != http.StatusServiceUnavailable ||
			strings.TrimSpace(w.Body.String()) != "viewhttp: watch channel closed" {
			t.Errorf("closed channel: %d %q, want 503", w.Code, w.Body.String())
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/view?wait=30s", nil)
			r.Header.Set("If-None-Match", tag)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			done <- w
		}()
		synctest.Wait()
		cancel()
		if w := <-done; w.Code != http.StatusOK || w.Body.Len() != 0 || len(w.Header()) != 0 {
			t.Errorf("ended request wrote %d %v %q, want nothing", w.Code, w.Header(), w.Body.String())
		}
		synctest.Wait()
		if n := src.open(); n != 0 {
			t.Errorf("%d Watch channels open after the request ended, want 0", n)
		}

		empty := newSource(nil)
		he := viewhttp.Handler(empty)
		start := time.Now()
		if w := get(he, "/view?wait=5s", ""); w.Code != http.StatusServiceUnavailable ||
			strings.TrimSpace(w.Body.String()) != "viewhttp: no view within the wait" {
			t.Errorf("no view: %d %q, want 503", w.Code, w.Body.String())
		}
		if d := time.Since(start); d != 5*time.Second {
			t.Errorf("no view: answered after %v, want 5s", d)
		}
		go func() { done <- get(he, "/view", "") }()
		synctest.Wait()
		empty.set(viewOf(4, "m1"))
		wantView(t, <-done, "4")
	})
}

// TestHandlerAcceptEncoding checks which Accept-Encoding values get gzip.
func TestHandlerAcceptEncoding(t *testing.T) {
	h := viewhttp.Handler(newSource(new(viewOf(1, "m1"))))
	for accept, want := range map[string]string{
		"":                "",
		"identity":        "",
		"gzip":            "gzip",
		"GZIP":            "gzip",
		"br, gzip;q=0.5":  "gzip",
		"gzip;q=0":        "",
		"x-gzip":          "gzip",
		"deflate, br":     "",
		"gzip ; q=0.000":  "",
		"br;q=1, x-gzip ": "gzip",
	} {
		r := httptest.NewRequest(http.MethodGet, "/view", nil)
		if accept != "" {
			r.Header.Set("Accept-Encoding", accept)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if got := w.Header().Get("Content-Encoding"); got != want {
			t.Errorf("Accept-Encoding %q: Content-Encoding %q, want %q", accept, got, want)
		}
	}
}

// TestHandlerIfNoneMatch checks the forms of If-None-Match that name the current tag: a list, a
// weak tag and "*".
func TestHandlerIfNoneMatch(t *testing.T) {
	h := viewhttp.Handler(newSource(new(viewOf(1, "m1"))))
	tag := wantView(t, get(h, "/view", ""), "1")
	for _, inm := range []string{tag, `"x", ` + tag, "W/" + tag, "*"} {
		wantNotModified(t, get(h, "/view", inm), tag)
	}
	wantView(t, get(h, "/view", `"x"`), "1")
}

// TestHandlerRand checks WithRand: the epoch is the hex of the reader's first 8 bytes, and a
// reader that runs dry makes Handler panic.
func TestHandlerRand(t *testing.T) {
	src := newSource(new(viewOf(5, "m1")))
	h := viewhttp.Handler(src, viewhttp.WithRand(bytes.NewReader([]byte{0, 1, 2, 3, 4, 5, 6, 0xff})))
	if got, want := get(h, "/view", "").Header().Get("ETag"), `"00010203040506ff-5"`; got != want {
		t.Errorf("ETag %s, want %s", got, want)
	}
	defer func() {
		if r := recover(); r != "viewhttp: draw epoch: unexpected EOF" {
			t.Errorf("Handler with a short reader panics with %v, want the epoch's error", r)
		}
	}()
	viewhttp.Handler(src, viewhttp.WithRand(bytes.NewReader([]byte{1, 2})))
}

// TestHandlerNilSource checks that Handler panics on a nil source.
func TestHandlerNilSource(t *testing.T) {
	defer func() {
		if r := recover(); r != "viewhttp: nil source" {
			t.Errorf("Handler(nil) panics with %v, want %q", r, "viewhttp: nil source")
		}
	}()
	viewhttp.Handler(nil)
}
