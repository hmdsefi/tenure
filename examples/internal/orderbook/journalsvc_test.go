// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
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
	if err := j.Append(ctx, 3, 2, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("Append at the fence: %v", err)
	}
	if err := j.Append(ctx, 3, 3, []byte(`{"a":2}`)); err != nil {
		t.Fatalf("Append above the fence: %v", err)
	}
	if err := j.Append(ctx, 3, 2, []byte(`{"a":3}`)); !errors.Is(err, fencedb.ErrStale) {
		t.Errorf("Append below the fence: %v, want ErrStale", err)
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

func (f *flaky) Append(ctx context.Context, shard int, term core.Term, line []byte) error {
	if err := f.DBJournal.Append(ctx, shard, term, line); err != nil {
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
