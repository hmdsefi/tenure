// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

// attempt is one call of the commit hook.
type attempt struct {
	shard int
	term  core.Term
	id    string
	err   error
}

// hooked returns an orderbook over db whose hooks record what they see.
func hooked(db *fencedb.DB) (o *orderbook, raised *[]core.Term, attempts *[]attempt) {
	o = newOrderbook(db)
	var mu sync.Mutex
	raised, attempts = new([]core.Term), new([]attempt)
	o.raised = func(_ int, term core.Term) {
		mu.Lock()
		defer mu.Unlock()
		*raised = append(*raised, term)
	}
	o.commit = func(shard int, term core.Term, id string, err error) {
		mu.Lock()
		defer mu.Unlock()
		*attempts = append(*attempts, attempt{shard, term, id, err})
	}
	return o, raised, attempts
}

// running is a Run in a goroutine.
type running struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when Run has returned, after err is set
	err    error
}

// start runs o.Run for the shard at term and waits until it has published its engine, or has
// returned. It returns the engine, nil when Run returned first, and the Run.
func start(t *testing.T, o *orderbook, shard int, term core.Term) (*engine, *running) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	r := &running{cancel: cancel, done: make(chan struct{})}
	go func() {
		r.err = o.Run(ctx, &tenure.Shard{Space: marketsSpace, ID: shard, Term: term})
		close(r.done)
	}()
	t.Cleanup(func() {
		cancel()
		<-r.done
	})
	for {
		if e := o.engine(shard, term); e != nil {
			return e, r
		}
		select {
		case <-r.done:
			return nil, r
		case <-ctx.Done():
			t.Fatalf("Run of %d at term %d published nothing", shard, term)
		case <-time.After(time.Millisecond):
		}
	}
}

// stop ends the Run and returns what it returned.
func (r *running) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	return r.result(t)
}

// result waits for the Run to return and returns what it returned.
func (r *running) result(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// five is o1 to o5 of the recovery test: all in one market, the fourth sweeps two sellers and
// the fifth hits a resting buyer.
func five() []order {
	return []order{
		{ID: "o1", Market: "m1", Side: sideSell, Price: 100, Qty: 5},
		{ID: "o2", Market: "m1", Side: sideSell, Price: 101, Qty: 5},
		{ID: "o3", Market: "m1", Side: sideBuy, Price: 99, Qty: 4},
		{ID: "o4", Market: "m1", Side: sideBuy, Price: 101, Qty: 7},
		{ID: "o5", Market: "m1", Side: sideSell, Price: 99, Qty: 6},
	}
}

// journal reads the committed journal of a shard.
func journal(t *testing.T, db *fencedb.DB, shard int) []entry {
	t.Helper()
	var out []entry
	err := db.View(marketsSpace, shard, func(tx *fencedb.Tx) error {
		for _, l := range tx.Entries(journalLog) {
			var en entry
			if err := json.Unmarshal(l, &en); err != nil {
				return err
			}
			out = append(out, en)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// write journals the entries of a shard under term, as an engine would, but with the fills the
// caller chose.
func write(t *testing.T, db *fencedb.DB, shard int, term core.Term, entries ...entry) {
	t.Helper()
	err := db.Update(marketsSpace, shard, term, func(tx *fencedb.Tx) error {
		for _, en := range entries {
			line, err := json.Marshal(en)
			if err != nil {
				return err
			}
			tx.Append(journalLog, line)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestATEXM08Recovery steps were written for this test from the text of issue #107.
func TestATEXM08Recovery(t *testing.T) {
	const shard = 3

	t.Run("o1 to o5 rebuild the same books and fills", func(t *testing.T) {
		db := fencedb.New()
		o, raised, _ := hooked(db)
		e, first := start(t, o, shard, 1)
		if e == nil {
			t.Fatalf("Run returned %v", first.result(t))
		}
		var want [][]fill
		for _, ord := range five() {
			fills, err := e.submit(ord)
			if err != nil {
				t.Fatalf("submit %s: %v", ord.ID, err)
			}
			want = append(want, fills)
		}
		wantFills := []fill{
			{Maker: "o1", Taker: "o4", Price: 100, Qty: 5},
			{Maker: "o2", Taker: "o4", Price: 101, Qty: 2},
		}
		if !slices.Equal(want[3], wantFills) {
			t.Fatalf("o4 filled %v, want %v", want[3], wantFills)
		}
		wantBooks := e.snapshot()
		// o5 sells 6 at 99 into the 4 of o3 and rests with the other 2, ahead of o2.
		if got := wantBooks["m1"]; got != "bids | asks o5@99*2 o2@101*3" {
			t.Fatalf("books after o1 to o5: %v", wantBooks)
		}
		if err := first.stop(t); err != nil {
			t.Fatalf("first Run: %v", err)
		}
		if o.engine(shard, 1) != nil {
			t.Error("the engine is still published after Run returned")
		}
		if _, err := e.submit(order{ID: "late", Market: "m1", Side: sideBuy, Price: 1, Qty: 1}); !errors.Is(err, errRefusing) {
			t.Errorf("submit after Run returned: %v, want errRefusing", err)
		}

		// A new owner at term 2 reads the journal.
		e2, second := start(t, o, shard, 2)
		if e2 == nil {
			t.Fatalf("second Run returned %v", second.result(t))
		}
		if got := e2.snapshot(); !maps.Equal(got, wantBooks) {
			t.Errorf("rebuilt books %v, want %v", got, wantBooks)
		}
		var got [][]fill
		for _, en := range journal(t, db, shard) {
			got = append(got, en.Fills)
		}
		if len(got) != 5 {
			t.Fatalf("journal has %d entries, want 5", len(got))
		}
		for i := range got {
			if !slices.Equal(got[i], want[i]) {
				t.Errorf("o%d journaled %v, want %v", i+1, got[i], want[i])
			}
		}
		if !slices.Equal(*raised, []core.Term{1, 2}) {
			t.Errorf("raised hook saw %v, want [1 2]", *raised)
		}

		// The rebuilt engine knows what is journaled and carries on.
		if _, err := e2.submit(five()[0]); !errors.Is(err, errDuplicate) {
			t.Errorf("resubmitting o1: %v, want errDuplicate", err)
		}
		fills, err := e2.submit(order{ID: "o6", Market: "m1", Side: sideBuy, Price: 101, Qty: 1})
		if err != nil || !slices.Equal(fills, []fill{{Maker: "o5", Taker: "o6", Price: 99, Qty: 1}}) {
			t.Errorf("o6: fills %v, err %v", fills, err)
		}
		var terms []core.Term
		for _, c := range db.History(marketsSpace, shard) {
			terms = append(terms, c.Term)
		}
		if !slices.Equal(terms, []core.Term{1, 1, 1, 1, 1, 2}) {
			t.Errorf("history terms %v", terms)
		}
	})

	t.Run("edited fills make Run return an error", func(t *testing.T) {
		good := func() []entry {
			var out []entry
			b := new(book)
			for _, o := range five() {
				out = append(out, entry{Order: o, Fills: b.match(o)})
			}
			return out
		}
		edits := map[string]func(es []entry){
			"changed quantity": func(es []entry) { es[3].Fills[0].Qty++ },
			"changed price":    func(es []entry) { es[3].Fills[1].Price = 100 },
			"changed maker":    func(es []entry) { es[3].Fills[0].Maker = "o9" },
			"dropped fill":     func(es []entry) { es[3].Fills = es[3].Fills[:1] },
			"added fill":       func(es []entry) { es[0].Fills = []fill{{Maker: "o9", Taker: "o1", Price: 100, Qty: 1}} },
		}
		for name, edit := range edits {
			t.Run(name, func(t *testing.T) {
				es := good()
				edit(es)
				db := fencedb.New()
				write(t, db, shard, 1, es...)
				o, _, _ := hooked(db)
				e, r := start(t, o, shard, 2)
				if e != nil {
					t.Fatal("Run published an engine for a journal with edited fills")
				}
				err := r.result(t)
				if err == nil || !strings.Contains(err.Error(), "fills differ") {
					t.Errorf("Run returned %v, want a fills-differ error", err)
				}
				if errors.Is(err, fencedb.ErrStale) {
					t.Errorf("Run returned %v, which is the stale error", err)
				}
			})
		}

		t.Run("not JSON", func(t *testing.T) {
			db := fencedb.New()
			err := db.Update(marketsSpace, shard, 1, func(tx *fencedb.Tx) error {
				tx.Append(journalLog, []byte("{"))
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			o, _, _ := hooked(db)
			if _, r := start(t, o, shard, 2); r.result(t) == nil {
				t.Error("Run returned nil for a journal that is not JSON")
			}
		})

		t.Run("an order journaled twice", func(t *testing.T) {
			db := fencedb.New()
			o1 := five()[0]
			write(t, db, shard, 1, entry{Order: o1, Fills: []fill{}}, entry{Order: o1, Fills: []fill{}})
			o, _, _ := hooked(db)
			if _, r := start(t, o, shard, 2); r.result(t) == nil {
				t.Error("Run returned nil for a journal with an order twice")
			}
		})
	})

	t.Run("a higher fence makes Run return ErrStale without reading the journal", func(t *testing.T) {
		db := fencedb.New()
		// The journal is not JSON: a Run that read it would fail with a different error.
		err := db.Update(marketsSpace, shard, 5, func(tx *fencedb.Tx) error {
			tx.Append(journalLog, []byte("not a journal entry"))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		o, raised, attempts := hooked(db)
		e, r := start(t, o, shard, 4)
		if e != nil {
			t.Fatal("Run published an engine below the fence")
		}
		if err := r.result(t); !errors.Is(err, fencedb.ErrStale) {
			t.Errorf("Run returned %v, want ErrStale", err)
		}
		if len(*raised) != 0 || len(*attempts) != 0 {
			t.Errorf("raised %v, attempts %v: want none", *raised, *attempts)
		}
		// The fence stays where it was, and a Run at the fence's own term goes on to read the
		// journal, which is where it fails.
		if err := db.Raise(marketsSpace, shard, 4); !errors.Is(err, fencedb.ErrStale) {
			t.Errorf("Raise(4) = %v, want ErrStale", err)
		}
		if _, r := start(t, o, shard, 5); r.result(t) == nil {
			t.Error("Run at the fence's term returned nil for a journal that is not JSON")
		}
	})
}

// TestEngineStale: when the fence rejects a commit, nothing of the order is kept, the engine
// refuses every further order, and the attempt is visible to the commit hook.
func TestEngineStale(t *testing.T) {
	const shard = 7
	db := fencedb.New()
	o, _, attempts := hooked(db)
	e, _ := start(t, o, shard, 1)
	if e == nil {
		t.Fatal("no engine")
	}
	if _, err := e.submit(order{ID: "a", Market: "m", Side: sideSell, Price: 10, Qty: 1}); err != nil {
		t.Fatal(err)
	}
	before := e.snapshot()

	// A new owner takes the shard: its Run raised the fence to 2.
	if err := db.Raise(marketsSpace, shard, 2); err != nil {
		t.Fatal(err)
	}
	fills, err := e.submit(order{ID: "b", Market: "m", Side: sideBuy, Price: 10, Qty: 1})
	if !errors.Is(err, fencedb.ErrStale) || fills != nil {
		t.Fatalf("submit below the fence: %v, %v; want nil and ErrStale", fills, err)
	}
	if got := e.snapshot(); !maps.Equal(got, before) {
		t.Errorf("books changed by a rejected order: %v, was %v", got, before)
	}
	if n := len(db.History(marketsSpace, shard)); n != 1 {
		t.Errorf("history has %d commits, want the 1 before the fence moved", n)
	}
	if got := journal(t, db, shard); len(got) != 1 || got[0].Order.ID != "a" {
		t.Errorf("journal %v, want only a", got)
	}
	// The engine refuses from now on, without trying the fence again.
	if _, err := e.submit(order{ID: "c", Market: "m", Side: sideBuy, Price: 1, Qty: 1}); !errors.Is(err, errRefusing) {
		t.Errorf("submit after ErrStale: %v, want errRefusing", err)
	}
	if len(*attempts) != 2 || (*attempts)[0].err != nil || !errors.Is((*attempts)[1].err, fencedb.ErrStale) ||
		(*attempts)[1].id != "b" || (*attempts)[1].term != 1 || (*attempts)[1].shard != shard {
		t.Errorf("commit hook saw %+v, want a commit of a and a stale attempt of b", *attempts)
	}
}

func TestEngineSubmitErrors(t *testing.T) {
	db := fencedb.New()
	o, _, attempts := hooked(db)
	e, _ := start(t, o, 0, 1)
	if _, err := e.submit(order{ID: "x", Market: "m", Side: "hold", Price: 1, Qty: 1}); !errors.Is(err, errInvalidOrder) {
		t.Errorf("invalid order: %v", err)
	}
	if _, err := e.submit(order{ID: "x", Market: "m", Side: sideBuy, Price: 1, Qty: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.submit(order{ID: "x", Market: "m", Side: sideBuy, Price: 1, Qty: 1}); !errors.Is(err, errDuplicate) {
		t.Errorf("duplicate: %v", err)
	}
	// Neither failure reached the fence, and neither refuses the engine.
	if len(*attempts) != 1 {
		t.Errorf("%d commit attempts, want 1", len(*attempts))
	}
	if _, err := e.submit(order{ID: "y", Market: "m", Side: sideBuy, Price: 1, Qty: 1}); err != nil {
		t.Errorf("submit after rejected orders: %v", err)
	}
}

// TestEngineConcurrent: orders from many goroutines are each journaled once, and the journal
// replays to the fills the callers got.
func TestEngineConcurrent(t *testing.T) {
	const (
		shard   = 1
		workers = 8
		each    = 40
	)
	db := fencedb.New()
	o, _, _ := hooked(db)
	e, r := start(t, o, shard, 1)

	var wg sync.WaitGroup
	var mu sync.Mutex
	got := map[string][]fill{}
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range each {
				side := sideBuy
				if (w+k)%2 == 0 {
					side = sideSell
				}
				ord := order{ID: fmt.Sprintf("c%d-%d", w, k), Market: fmt.Sprintf("m%d", k%3), Side: side, Price: int64(95 + (w*7+k*3)%10), Qty: int64(1 + (w+k)%5)}
				fills, err := e.submit(ord)
				if err != nil {
					t.Errorf("submit %s: %v", ord.ID, err)
					return
				}
				mu.Lock()
				got[ord.ID] = fills
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	want := e.snapshot()

	entries := journal(t, db, shard)
	if len(entries) != workers*each {
		t.Fatalf("journal has %d entries, want %d", len(entries), workers*each)
	}
	for _, en := range entries {
		if !slices.Equal(en.Fills, got[en.Order.ID]) {
			t.Errorf("%s journaled %v, caller got %v", en.Order.ID, en.Fills, got[en.Order.ID])
		}
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	e2, _ := start(t, o, shard, 2)
	if e2 == nil {
		t.Fatal("the journal did not replay")
	}
	if got := e2.snapshot(); !maps.Equal(got, want) {
		t.Errorf("rebuilt books differ:\n got %v\nwant %v", got, want)
	}
}
