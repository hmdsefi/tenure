// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fencedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

// appendEntry is an Update body that appends one entry to the journal log.
func appendEntry(entry string) func(tx *fencedb.Tx) error {
	return func(tx *fencedb.Tx) error {
		tx.Append("journal", []byte(entry))
		return nil
	}
}

// journal returns the entries of the shard's journal log.
func journal(t *testing.T, db *fencedb.DB, space string, shard int) []string {
	t.Helper()
	var out []string
	err := db.View(space, shard, func(tx *fencedb.Tx) error {
		for _, e := range tx.Entries("journal") {
			out = append(out, string(e))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	return out
}

// TestATEXM04FenceDB covers the fence of the downstream: raising, updating under a term,
// rejecting a stale term, atomic rollback, the history and read-only views. The steps were
// written from the issue's text.
func TestATEXM04FenceDB(t *testing.T) {
	t.Run("raise", func(t *testing.T) {
		db := fencedb.New()
		for _, term := range []core.Term{5, 5, 7} {
			if err := db.Raise("orders", 1, term); err != nil {
				t.Fatalf("Raise(%d): %v", term, err)
			}
		}
		if err := db.Raise("orders", 1, 6); !errors.Is(err, fencedb.ErrStale) {
			t.Fatalf("Raise(6) after 7 = %v, want ErrStale", err)
		}
		// The failed Raise changed nothing: 7 still commits, 6 does not.
		if err := db.Update("orders", 1, 7, appendEntry("a")); err != nil {
			t.Fatalf("Update(7): %v", err)
		}
		if err := db.Update("orders", 1, 6, appendEntry("b")); !errors.Is(err, fencedb.ErrStale) {
			t.Fatalf("Update(6) = %v, want ErrStale", err)
		}
		// Fences are per shard and per space.
		if err := db.Update("orders", 2, 1, appendEntry("c")); err != nil {
			t.Fatalf("Update of another shard: %v", err)
		}
		if err := db.Update("fills", 1, 1, appendEntry("d")); err != nil {
			t.Fatalf("Update of another space: %v", err)
		}
		if err := db.Raise("orders", 1, 0); err == nil || errors.Is(err, fencedb.ErrStale) {
			t.Fatalf("Raise(0) = %v, want an error that is not ErrStale", err)
		}
	})

	t.Run("stale update is not called", func(t *testing.T) {
		db := fencedb.New()
		if err := db.Raise("orders", 0, 9); err != nil {
			t.Fatal(err)
		}
		called := false
		err := db.Update("orders", 0, 8, func(*fencedb.Tx) error {
			called = true
			return nil
		})
		if !errors.Is(err, fencedb.ErrStale) {
			t.Fatalf("Update = %v, want ErrStale", err)
		}
		if called {
			t.Error("fn was called for a stale term")
		}
		if h := db.History("orders", 0); len(h) != 0 {
			t.Errorf("History = %v, want none", h)
		}
		if err := db.Update("orders", 0, 0, appendEntry("x")); err == nil || errors.Is(err, fencedb.ErrStale) {
			t.Errorf("Update(0) = %v, want an error that is not ErrStale", err)
		}
	})

	t.Run("a commit raises the fence", func(t *testing.T) {
		db := fencedb.New()
		if err := db.Update("orders", 0, 10, appendEntry("a")); err != nil {
			t.Fatal(err)
		}
		if err := db.Raise("orders", 0, 9); !errors.Is(err, fencedb.ErrStale) {
			t.Errorf("Raise(9) after a commit at 10 = %v, want ErrStale", err)
		}
		if err := db.Update("orders", 0, 10, appendEntry("b")); err != nil {
			t.Errorf("Update at the fence's own term: %v", err)
		}
	})

	t.Run("atomic rollback", func(t *testing.T) {
		db := fencedb.New()
		if err := db.Update("orders", 0, 1, func(tx *fencedb.Tx) error {
			tx.Put("k", []byte("v1"))
			tx.Append("journal", []byte("e1"))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("boom")
		err := db.Update("orders", 0, 2, func(tx *fencedb.Tx) error {
			tx.Put("k", []byte("v2"))
			tx.Put("other", []byte("x"))
			tx.Append("journal", []byte("e2"))
			if v, ok := tx.Get("k"); !ok || string(v) != "v2" {
				t.Errorf("Get inside the Tx = %q, %v; want the staged v2", v, ok)
			}
			if got := tx.Entries("journal"); len(got) != 2 {
				t.Errorf("Entries inside the Tx = %q, want e1 and the staged e2", got)
			}
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("Update = %v, want the error of fn", err)
		}
		if err := db.View("orders", 0, func(tx *fencedb.Tx) error {
			if v, _ := tx.Get("k"); string(v) != "v1" {
				t.Errorf("k = %q after a failed Update, want v1", v)
			}
			if _, ok := tx.Get("other"); ok {
				t.Error("a staged Put survived a failed Update")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if got := journal(t, db, "orders", 0); !slices.Equal(got, []string{"e1"}) {
			t.Errorf("journal = %q, want [e1]", got)
		}
		if h := db.History("orders", 0); len(h) != 1 {
			t.Errorf("History has %d commits after a failed Update, want 1", len(h))
		}
		// The failed Update did not raise the fence either: term 1 still commits.
		if err := db.Update("orders", 0, 1, appendEntry("e3")); err != nil {
			t.Errorf("Update(1) after the failed Update(2): %v", err)
		}
	})

	t.Run("history", func(t *testing.T) {
		db := fencedb.New()
		steps := []struct {
			space string
			shard int
			term  core.Term
		}{
			{"orders", 0, 3}, {"orders", 1, 1}, {"orders", 0, 3}, {"fills", 0, 2}, {"orders", 0, 5},
		}
		for _, s := range steps {
			if err := db.Update(s.space, s.shard, s.term, appendEntry("e")); err != nil {
				t.Fatal(err)
			}
		}
		// A rejected commit leaves no trace.
		if err := db.Update("orders", 0, 4, appendEntry("late")); !errors.Is(err, fencedb.ErrStale) {
			t.Fatalf("Update(4) = %v, want ErrStale", err)
		}
		want := []fencedb.Commit{
			{Space: "orders", Shard: 0, Term: 3, Seq: 1},
			{Space: "orders", Shard: 0, Term: 3, Seq: 3},
			{Space: "orders", Shard: 0, Term: 5, Seq: 5},
		}
		got := db.History("orders", 0)
		if !slices.Equal(got, want) {
			t.Fatalf("History(orders, 0) = %v, want %v", got, want)
		}
		if h := db.History("orders", 1); len(h) != 1 || h[0].Seq != 2 {
			t.Errorf("History(orders, 1) = %v, want one commit with Seq 2", h)
		}
		if h := db.History("fills", 0); len(h) != 1 || h[0].Seq != 4 {
			t.Errorf("History(fills, 0) = %v, want one commit with Seq 4", h)
		}
		if h := db.History("orders", 9); len(h) != 0 {
			t.Errorf("History of an untouched shard = %v, want none", h)
		}
		got[0].Term = 99
		if db.History("orders", 0)[0].Term != 3 {
			t.Error("History returned its own slice, not a copy")
		}
	})

	t.Run("view is read-only", func(t *testing.T) {
		db := fencedb.New()
		if err := db.Raise("orders", 0, 4); err != nil {
			t.Fatal(err)
		}
		// A view needs no term and sees committed state, also of a shard nothing has written.
		if err := db.View("orders", 0, func(tx *fencedb.Tx) error {
			if _, ok := tx.Get("k"); ok {
				t.Error("Get found a key in an empty shard")
			}
			if e := tx.Entries("journal"); len(e) != 0 {
				t.Errorf("Entries = %q, want none", e)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for name, write := range map[string]func(tx *fencedb.Tx){
			"Put":    func(tx *fencedb.Tx) { tx.Put("k", []byte("v")) },
			"Append": func(tx *fencedb.Tx) { tx.Append("journal", []byte("e")) },
		} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("%s on a read-only Tx did not panic", name)
					}
				}()
				_ = db.View("orders", 0, func(tx *fencedb.Tx) error {
					write(tx)
					return nil
				})
			}()
		}
		// The panics left the DB usable and unchanged.
		if err := db.Update("orders", 0, 4, appendEntry("a")); err != nil {
			t.Fatalf("Update after a panicking view: %v", err)
		}
		if got := journal(t, db, "orders", 0); !slices.Equal(got, []string{"a"}) {
			t.Errorf("journal = %q, want [a]", got)
		}
		// A View returns the error of fn.
		boom := errors.New("boom")
		if err := db.View("orders", 0, func(*fencedb.Tx) error { return boom }); !errors.Is(err, boom) {
			t.Errorf("View = %v, want the error of fn", err)
		}
	})

	t.Run("values are copied", func(t *testing.T) {
		db := fencedb.New()
		in := []byte("abc")
		if err := db.Update("orders", 0, 1, func(tx *fencedb.Tx) error {
			tx.Put("k", in)
			tx.Append("journal", in)
			tx.Put("empty", nil)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		in[0] = 'X'
		if err := db.View("orders", 0, func(tx *fencedb.Tx) error {
			v, ok := tx.Get("k")
			if !ok || !bytes.Equal(v, []byte("abc")) {
				t.Errorf("Get = %q, %v; want abc: Put kept the caller's slice", v, ok)
			}
			v[0] = 'Y'
			if v2, _ := tx.Get("k"); string(v2) != "abc" {
				t.Errorf("Get = %q after the caller changed a result, want abc", v2)
			}
			e := tx.Entries("journal")
			if len(e) != 1 || string(e[0]) != "abc" {
				t.Errorf("Entries = %q, want [abc]", e)
			}
			e[0][0] = 'Z'
			if again := tx.Entries("journal"); string(again[0]) != "abc" {
				t.Errorf("Entries = %q after the caller changed a result, want [abc]", again)
			}
			if v, ok := tx.Get("empty"); !ok || v == nil || len(v) != 0 {
				t.Errorf("Get(empty) = %#v, %v; want an empty, present value", v, ok)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	// Writers race to commit with their own terms while another goroutine raises the fence. Every
	// shard's history must hold non-decreasing terms, the journal one entry per commit, and the
	// sequence numbers of all commits must be exactly 1…n.
	t.Run("concurrent", func(t *testing.T) {
		const (
			shards  = 4
			writers = 8
			rounds  = 200
		)
		db := fencedb.New()
		var committed, stale atomic.Int64
		var wg sync.WaitGroup
		for w := range writers {
			wg.Go(func() {
				for r := range rounds {
					shard := (w + r) % shards
					term := core.Term(r*writers/2 + w + 1) //nolint:gosec // small positive ints; the writers interleave and overtake each other
					err := db.Update("orders", shard, term, appendEntry(fmt.Sprintf("w%d-r%d", w, r)))
					switch {
					case err == nil:
						committed.Add(1)
					case errors.Is(err, fencedb.ErrStale):
						stale.Add(1)
					default:
						t.Errorf("Update: %v", err)
						return
					}
				}
			})
		}
		wg.Go(func() {
			for r := 1; r <= rounds; r++ {
				shard := r % shards
				if err := db.Raise("orders", shard, core.Term(r*writers/2+writers)); err != nil && !errors.Is(err, fencedb.ErrStale) {
					t.Errorf("Raise: %v", err)
					return
				}
			}
		})
		wg.Wait()

		if stale.Load() == 0 || committed.Load() == 0 {
			t.Fatalf("committed %d, stale %d: want both non-zero for the test to mean something", committed.Load(), stale.Load())
		}
		var seqs []uint64
		var total int
		for shard := range shards {
			h := db.History("orders", shard)
			total += len(h)
			for i, c := range h {
				if i > 0 && c.Term < h[i-1].Term {
					t.Errorf("shard %d: term %d follows term %d", shard, c.Term, h[i-1].Term)
				}
				if i > 0 && c.Seq <= h[i-1].Seq {
					t.Errorf("shard %d: Seq %d follows Seq %d", shard, c.Seq, h[i-1].Seq)
				}
				seqs = append(seqs, c.Seq)
			}
			if got := journal(t, db, "orders", shard); len(got) != len(h) {
				t.Errorf("shard %d: %d journal entries for %d commits", shard, len(got), len(h))
			}
		}
		if int64(total) != committed.Load() {
			t.Errorf("history holds %d commits, %d Updates returned nil", total, committed.Load())
		}
		slices.Sort(seqs)
		for i, s := range seqs {
			if s != uint64(i+1) {
				t.Fatalf("sequence numbers = %v, want 1…%d", seqs, len(seqs))
			}
		}
	})
}
