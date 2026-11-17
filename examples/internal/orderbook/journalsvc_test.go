// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orderbook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
	"github.com/hmdsefi/tenure/routehttp"
)

// serveJournal starts a journal service over db and returns a client of it.
func serveJournal(t *testing.T, db *fencedb.DB) (*HTTPJournal, string) {
	t.Helper()
	srv := httptest.NewServer(NewJournalHandler(db))
	t.Cleanup(srv.Close)
	return NewHTTPJournal(srv.URL+"/", srv.Client()), srv.URL
}

// TestHTTPJournal: the client and the service together keep the fence and the journal as a
// DBJournal does, and a stale term comes back as fencedb.ErrStale.
func TestHTTPJournal(t *testing.T) {
	ctx := t.Context()
	db := fencedb.New()
	j, _ := serveJournal(t, db)

	if err := j.Raise(ctx, 3, 2); err != nil {
		t.Fatalf("Raise(3, 2): %v", err)
	}
	if err := j.Append(ctx, 3, 2, 0, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("Append at the fence: %v", err)
	}
	if err := j.Append(ctx, 3, 3, 1, []byte(`{"a":2}`)); err != nil {
		t.Fatalf("Append above the fence: %v", err)
	}
	if err := j.Append(ctx, 3, 2, 2, []byte(`{"a":3}`)); !errors.Is(err, fencedb.ErrStale) {
		t.Errorf("Append below the fence: %v, want ErrStale", err)
	}
	// An Append that expects another length appends nothing: neither one that is late, nor one
	// that skips a line.
	for _, at := range []int{1, 3} {
		if err := j.Append(ctx, 3, 3, at, []byte(`{"a":4}`)); !errors.Is(err, ErrLength) || errors.Is(err, fencedb.ErrStale) {
			t.Errorf("Append at line %d of a journal of 2: %v, want ErrLength", at, err)
		}
	}
	if err := j.Raise(ctx, 3, 1); !errors.Is(err, fencedb.ErrStale) {
		t.Errorf("Raise below the fence: %v, want ErrStale", err)
	}

	got, err := j.Entries(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	want, err := NewDBJournal(db).Entries(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(got, want, func(a, b []byte) bool { return string(a) == string(b) }) || len(got) != 2 {
		t.Errorf("Entries = %q, want %q", got, want)
	}
	if empty, err := j.Entries(ctx, 4); err != nil || len(empty) != 0 {
		t.Errorf("Entries of an empty shard = %q, %v", empty, err)
	}

	all, err := j.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != Shards || len(all[3]) != 2 || string(all[3][1]) != `{"a":2}` {
		t.Errorf("All = %q", all)
	}
	if hist := db.History(SpaceName, 3); len(hist) != 2 {
		t.Errorf("history has %d commits, want 2", len(hist))
	}
}

// TestJournalServiceAnswers checks the answers to requests that are not right.
func TestJournalServiceAnswers(t *testing.T) {
	_, url := serveJournal(t, fencedb.New())
	for _, tc := range []struct {
		name, method, path, body string
		want                     int
	}{
		{"not JSON", http.MethodPost, RaisePath, `{`, http.StatusBadRequest},
		{"unknown field", http.MethodPost, RaisePath, `{"shard":1,"term":1,"x":1}`, http.StatusBadRequest},
		{"two requests", http.MethodPost, RaisePath, `{"shard":1,"term":1}{}`, http.StatusBadRequest},
		{"shard out of range", http.MethodPost, RaisePath, `{"shard":16,"term":1}`, http.StatusBadRequest},
		{"term 0", http.MethodPost, RaisePath, `{"shard":1,"term":0}`, http.StatusInternalServerError},
		{"no line", http.MethodPost, AppendPath, `{"shard":1,"term":1}`, http.StatusBadRequest},
		{"too large", http.MethodPost, AppendPath, `{"shard":1,"term":1,"line":"` + strings.Repeat("a", maxRequest) + `"}`, http.StatusRequestEntityTooLarge},
		{"no shard", http.MethodGet, EntriesPath, ``, http.StatusBadRequest},
		{"negative shard", http.MethodGet, EntriesPath + "?shard=-1", ``, http.StatusBadRequest},
		{"wrong method", http.MethodGet, AppendPath, ``, http.StatusMethodNotAllowed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), tc.method, url+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}

	// A client sees a failure that is not stale as an error that is not ErrStale.
	j := NewHTTPJournal(url, nil)
	if err := j.Raise(t.Context(), 1, 0); err == nil || errors.Is(err, fencedb.ErrStale) {
		t.Errorf("Raise at term 0: %v, want an error that is not ErrStale", err)
	}
}

// TestEngineOverHTTP runs an engine on a journal service: a new owner rebuilds the books that
// the old one made, and the old one is fenced.
func TestEngineOverHTTP(t *testing.T) {
	const shard = 5
	db := fencedb.New()
	j, _ := serveJournal(t, db)
	o := New(j)
	e, first := start(t, o, shard, 1)
	if e == nil {
		t.Fatalf("Run returned %v", first.result(t))
	}
	for _, ord := range five() {
		if _, err := e.submit(ord); err != nil {
			t.Fatalf("submit %s: %v", ord.ID, err)
		}
	}
	want := e.snapshot()

	e2, _ := start(t, New(j), shard, 2)
	if e2 == nil {
		t.Fatal("the second Run published nothing")
	}
	if got := e2.snapshot(); !maps.Equal(got, want) {
		t.Errorf("rebuilt books %v, want %v", got, want)
	}
	if _, err := e.submit(Order{ID: "late", Market: "m1", Side: SideBuy, Price: 1, Qty: 1}); !errors.Is(err, fencedb.ErrStale) {
		t.Errorf("submit of the old owner: %v, want ErrStale", err)
	}
}

// flaky is a Journal over a DBJournal whose Append can be told to fail after it appended, as a
// remote journal whose answer was lost does, or before.
type flaky struct {
	*DBJournal
	mu          sync.Mutex
	failAfter   int // the next n Appends fail after they appended
	failEntries int // the next n Entries fail
}

var errLost = errors.New("the answer was lost")

func (f *flaky) Append(ctx context.Context, shard int, term core.Term, at int, line []byte) error {
	if err := f.DBJournal.Append(ctx, shard, term, at, line); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAfter > 0 {
		f.failAfter--
		return errLost
	}
	return nil
}

func (f *flaky) Entries(ctx context.Context, shard int) ([][]byte, error) {
	f.mu.Lock()
	if f.failEntries > 0 {
		f.failEntries--
		f.mu.Unlock()
		return nil, errLost
	}
	f.mu.Unlock()
	return f.DBJournal.Entries(ctx, shard)
}

func (f *flaky) set(after, entries int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfter, f.failEntries = after, entries
}

// TestEngineUnsureAppend: after an Append whose outcome is unknown, the engine reads its journal
// again before the next order, so the books it matches against are the journal's, and an order
// whose answer was lost is a duplicate when it comes again.
func TestEngineUnsureAppend(t *testing.T) {
	const shard = 2
	f := &flaky{DBJournal: NewDBJournal(fencedb.New())}
	o := New(f)
	e, _ := start(t, o, shard, 1)
	if e == nil {
		t.Fatal("no engine")
	}
	if _, err := e.submit(Order{ID: "s1", Market: "m", Side: SideSell, Price: 10, Qty: 5}); err != nil {
		t.Fatal(err)
	}

	// The answer of b1's Append is lost, and so is the first read of the journal after it.
	f.set(1, 1)
	if _, err := e.submit(Order{ID: "b1", Market: "m", Side: SideBuy, Price: 10, Qty: 2}); !errors.Is(err, errLost) {
		t.Fatalf("submit with a lost answer: %v", err)
	}
	if _, err := e.submit(Order{ID: "b2", Market: "m", Side: SideBuy, Price: 10, Qty: 1}); !errors.Is(err, errRefusing) {
		t.Fatalf("submit while the journal cannot be read: %v, want errRefusing", err)
	}
	if _, err := e.submit(Order{ID: "b1", Market: "m", Side: SideBuy, Price: 10, Qty: 2}); !errors.Is(err, errDuplicate) {
		t.Errorf("b1 again after the journal was read: %v, want errDuplicate", err)
	}
	fills, err := e.submit(Order{ID: "b3", Market: "m", Side: SideBuy, Price: 10, Qty: 1})
	if err != nil || !slices.Equal(fills, []Fill{{Maker: "s1", Taker: "b3", Price: 10, Qty: 1}}) {
		t.Errorf("b3: fills %v, err %v", fills, err)
	}
	if got := e.snapshot()["m"]; got != "bids | asks s1@10*2" {
		t.Errorf("book %q, want s1 with 2 left", got)
	}
	lines, err := f.Entries(t.Context(), shard)
	if err != nil {
		t.Fatal(err)
	}
	if err := Replay(lines); err != nil || len(lines) != 3 {
		t.Errorf("journal of %d lines replays with %v", len(lines), err)
	}
}

// late is a Journal over a DBJournal whose Append, once hold is set, fails without appending,
// as a remote journal's does when its client gives up first, and keeps the append to be applied
// later, as the service does when the request reaches it after all. With inEntries the kept
// append is applied right after the next Entries, before the engine's next Append.
type late struct {
	*DBJournal
	mu        sync.Mutex
	hold      bool
	inEntries bool
	held      func() error
	heldErr   error
}

func (l *late) Append(ctx context.Context, shard int, term core.Term, at int, line []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.hold {
		l.hold = false
		l.held = func() error { return l.DBJournal.Append(ctx, shard, term, at, line) }
		return errLost
	}
	return l.DBJournal.Append(ctx, shard, term, at, line)
}

func (l *late) Entries(ctx context.Context, shard int) ([][]byte, error) {
	lines, err := l.DBJournal.Entries(ctx, shard)
	if l.inEntries {
		l.apply()
	}
	return lines, err
}

// apply applies the kept append, once, and keeps its error.
func (l *late) apply() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held != nil {
		l.heldErr, l.held = l.held(), nil
	}
}

// TestEngineLateAppend: an Append of unknown outcome that reaches the journal only after the
// engine read the journal again adds no line the engine's books do not hold, and does not
// journal its order twice when the client sent it again: whichever of it and the engine's next
// Append comes second fails with ErrLength and appends nothing, and the engine then reads the
// journal again.
func TestEngineLateAppend(t *testing.T) {
	const shard = 4
	s1 := Order{ID: "s1", Market: "m", Side: SideSell, Price: 10, Qty: 5}
	b1 := Order{ID: "b1", Market: "m", Side: SideBuy, Price: 10, Qty: 2}
	b2 := Order{ID: "b2", Market: "m", Side: SideBuy, Price: 10, Qty: 4}
	for _, tc := range []struct {
		name      string
		next      Order // the order after b1's lost Append
		inEntries bool  // b1's Append lands between the read and next's Append
		nextErr   error // what next gets
		lateErr   error // what b1's late Append gets
		ids       []string
	}{
		{"after another order", b2, false, nil, ErrLength, []string{"s1", "b2"}},
		{"after b1 again", b1, false, nil, ErrLength, []string{"s1", "b1"}},
		{"before another order", b2, true, ErrLength, nil, []string{"s1", "b1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &late{DBJournal: NewDBJournal(fencedb.New()), inEntries: tc.inEntries}
			e, _ := start(t, New(l), shard, 1)
			if e == nil {
				t.Fatal("no engine")
			}
			if _, err := e.submit(s1); err != nil {
				t.Fatal(err)
			}
			l.mu.Lock()
			l.hold = true
			l.mu.Unlock()
			if _, err := e.submit(b1); !errors.Is(err, errLost) {
				t.Fatalf("submit of b1 with a lost Append: %v", err)
			}
			_, err := e.submit(tc.next)
			l.apply()
			if !errors.Is(err, tc.nextErr) || (tc.nextErr == nil && err != nil) {
				t.Errorf("submit of %s: %v, want %v", tc.next.ID, err, tc.nextErr)
			}
			if !errors.Is(l.heldErr, tc.lateErr) || (tc.lateErr == nil && l.heldErr != nil) {
				t.Errorf("b1's late Append: %v, want %v", l.heldErr, tc.lateErr)
			}

			// One more order, so that the engine has read the journal again if it had to.
			if _, err := e.submit(Order{ID: "z", Market: "z", Side: SideBuy, Price: 1, Qty: 1}); err != nil {
				t.Fatalf("submit of z: %v", err)
			}
			lines, err := l.DBJournal.Entries(t.Context(), shard)
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for _, line := range lines {
				var en Entry
				if err := json.Unmarshal(line, &en); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, en.Order.ID)
			}
			if want := append(slices.Clone(tc.ids), "z"); !slices.Equal(ids, want) {
				t.Errorf("journal holds %v, want %v", ids, want)
			}
			fresh := newEngine(nil, shard, 1)
			if err := fresh.replay(lines); err != nil {
				t.Fatalf("the journal does not replay: %v", err)
			}
			if got, want := e.snapshot(), fresh.snapshot(); !maps.Equal(got, want) {
				t.Errorf("books %v, the journal's %v", got, want)
			}
		})
	}
}

func TestReplay(t *testing.T) {
	var lines [][]byte
	b := new(book)
	for _, o := range five() {
		line, err := json.Marshal(Entry{Order: o, Fills: b.match(o)})
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	if err := Replay(lines); err != nil {
		t.Errorf("Replay of a good journal: %v", err)
	}
	if err := Replay(append(lines, lines[0])); err == nil {
		t.Error("Replay of a journal with an order twice succeeded")
	}
	if err := Replay([][]byte{[]byte("{")}); err == nil {
		t.Error("Replay of a line that is not JSON succeeded")
	}
}

func TestRandomOrder(t *testing.T) {
	a := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a seeded test
	b := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a seeded test
	for i := range 200 {
		o := RandomOrder(a, "x")
		if o != RandomOrder(b, "x") {
			t.Fatal("the same rng state gave two orders")
		}
		if err := o.validate(); err != nil {
			t.Fatalf("order %d: %v", i, err)
		}
	}
	if sp := Spaces(); len(sp) != 1 || sp[0].Name != SpaceName || sp[0].Shards != Shards || !sp[0].Fenced || sp[0].Run != nil {
		t.Errorf("Spaces() = %+v", sp)
	}
	if sp := New(nil).Space(); sp.Name != SpaceName || sp.Shards != Shards || !sp.Fenced || sp.Run == nil {
		t.Errorf("Space() = %+v", sp)
	}
}

// TestServerUnknownAppend checks the answers of POST /orders around an Append of unknown
// outcome: a 500 without Tenure-Unprocessed for the order itself, since it may be journaled; a
// 503 with Tenure-Unprocessed for the next order when the lost Append landed before its own,
// which then journaled nothing; and a 409 when the lost order comes again.
func TestServerUnknownAppend(t *testing.T) {
	l := &late{DBJournal: NewDBJournal(fencedb.New()), inEntries: true}
	ob := New(l)
	c, err := demo.Start(t.Context(), demo.Options{
		Nodes:  1,
		Spaces: func(int) []tenure.Space { return []tenure.Space{ob.Space()} },
		Serve:  func(_ int, n *tenure.Node) http.Handler { return NewServer(n, ob, n.Table().Member.ID) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})
	// answer is the status and the header of an answer to POST /orders.
	type answer struct {
		StatusCode int
		Header     http.Header
	}
	post := func(o Order) answer {
		t.Helper()
		body, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, c.URL(0)+OrdersPath, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return answer{resp.StatusCode, resp.Header}
	}
	deadline := time.Now().Add(10 * time.Second)
	for post(Order{ID: "s1", Market: "m", Side: SideSell, Price: 10, Qty: 5}).StatusCode != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatal("the node never took an order")
		}
		time.Sleep(20 * time.Millisecond)
	}
	shard := core.KeyShard("m", Shards)
	journaled := func() [][]byte {
		t.Helper()
		lines, err := l.DBJournal.Entries(t.Context(), shard)
		if err != nil {
			t.Fatal(err)
		}
		return lines
	}

	l.mu.Lock()
	l.hold = true
	l.mu.Unlock()
	b1 := Order{ID: "b1", Market: "m", Side: SideBuy, Price: 10, Qty: 2}
	if resp := post(b1); resp.StatusCode != http.StatusInternalServerError || resp.Header.Get(routehttp.UnprocessedHeader) != "" {
		t.Errorf("order whose Append's outcome is unknown: %d, header %v; want 500 without %s",
			resp.StatusCode, resp.Header, routehttp.UnprocessedHeader)
	}
	b2 := Order{ID: "b2", Market: "m", Side: SideBuy, Price: 10, Qty: 1}
	if resp := post(b2); resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get(routehttp.UnprocessedHeader) != "1" {
		t.Errorf("order after a lost Append that landed late: %d, header %v; want 503 with %s 1",
			resp.StatusCode, resp.Header, routehttp.UnprocessedHeader)
	}
	if n := len(journaled()); n != 2 {
		t.Errorf("the journal holds %d lines, want s1 and b1", n)
	}
	if resp := post(b2); resp.StatusCode != http.StatusOK {
		t.Errorf("b2 again: %d, want 200", resp.StatusCode)
	}
	if resp := post(b1); resp.StatusCode != http.StatusConflict {
		t.Errorf("b1 again: %d, want 409", resp.StatusCode)
	}
	if lines := journaled(); len(lines) != 3 || Replay(lines) != nil {
		t.Errorf("journal of %d lines replays with %v, want s1, b1 and b2", len(lines), Replay(lines))
	}
}
