// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package fencedb is the in-memory downstream of the examples: a store with one fence point per
// shard.
//
// A shard's fence is the highest term the store has seen for it. A write that carries a lower
// term fails with ErrStale and changes nothing, so a stale owner, one that still runs after
// another member took the shard, cannot commit. The check and the write happen in one critical
// section (Update), so no write slips in between them.
//
// A DB is safe for concurrent use.
package fencedb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/hmdsefi/tenure/core"
)

// ErrStale means the term of a call is below the shard's fence. Every such rejection wraps it.
var ErrStale = errors.New("fencedb: stale term")

// errZeroTerm is returned for a write with term 0, which no store grants.
var errZeroTerm = errors.New("fencedb: term 0 is not a granted term")

// Commit is one committed Update, as History lists it.
type Commit struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Term is the term the Update carried.
	Term core.Term

	// Seq numbers the commits of the whole DB, from 1, in commit order.
	Seq uint64
}

// DB is an in-memory downstream with one fence per (space, shard). Make one with New; the zero
// value is not usable.
type DB struct {
	mu     sync.RWMutex
	shards map[shardKey]*shardState
	seq    uint64
}

type shardKey struct {
	space string
	shard int
}

// shardState is the committed state of one shard.
type shardState struct {
	fence   core.Term
	kv      map[string][]byte
	logs    map[string][][]byte
	history []Commit
}

// New returns an empty DB. Every fence is 0 until it is raised.
func New() *DB {
	return &DB{shards: make(map[shardKey]*shardState)}
}

// stateFor returns the state of a shard, creating it. The caller holds the write lock.
func (db *DB) stateFor(space string, shard int) *shardState {
	k := shardKey{space, shard}
	s := db.shards[k]
	if s == nil {
		s = &shardState{kv: make(map[string][]byte), logs: make(map[string][][]byte)}
		db.shards[k] = s
	}
	return s
}

func staleError(space string, shard int, term, fence core.Term) error {
	return fmt.Errorf("%w: %s/%d at term %d, fence %d", ErrStale, space, shard, term, fence)
}

// Raise sets the fence of the shard to term. It returns an error that wraps ErrStale and changes
// nothing when term is below the fence; raising the fence to its own value is fine. Term 0 is
// not a granted term and is an error.
func (db *DB) Raise(space string, shard int, term core.Term) error {
	if term == 0 {
		return errZeroTerm
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	s := db.stateFor(space, shard)
	if term < s.fence {
		return staleError(space, shard, term, s.fence)
	}
	s.fence = term
	return nil
}

// Update runs fn on a transaction over the shard's state, under the fence of term. When term is
// below the fence, Update returns an error that wraps ErrStale without calling fn. Otherwise fn
// stages its writes on the Tx: when fn returns an error, the staged writes are dropped and
// Update returns that error; when it returns nil they are committed in one step, the fence is
// raised to term, and one Commit is appended to the shard's History. Term 0 is not a granted
// term and is an error.
//
// fn runs while the DB is locked: it must not call the DB, and must not keep the Tx.
func (db *DB) Update(space string, shard int, term core.Term, fn func(tx *Tx) error) error {
	if term == 0 {
		return errZeroTerm
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	s := db.stateFor(space, shard)
	if term < s.fence {
		return staleError(space, shard, term, s.fence)
	}
	tx := &Tx{base: s, puts: make(map[string][]byte), appends: make(map[string][][]byte)}
	if err := fn(tx); err != nil {
		return err
	}
	for k, v := range tx.puts {
		s.kv[k] = v
	}
	for log, vs := range tx.appends {
		s.logs[log] = append(s.logs[log], vs...)
	}
	db.seq++
	s.fence = term
	s.history = append(s.history, Commit{Space: space, Shard: shard, Term: term, Seq: db.seq})
	return nil
}

// View runs fn on a read-only transaction over the committed state of the shard. It does not
// look at the fence. A call of Put or Append on the Tx panics.
//
// fn runs while the DB is read-locked: it must not write to the DB, and must not keep the Tx.
func (db *DB) View(space string, shard int, fn func(tx *Tx) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return fn(&Tx{base: db.shards[shardKey{space, shard}], readOnly: true})
}

// History returns the shard's commits in order, in a new slice.
func (db *DB) History(space string, shard int) []Commit {
	db.mu.RLock()
	defer db.mu.RUnlock()
	s := db.shards[shardKey{space, shard}]
	if s == nil {
		return nil
	}
	return append([]Commit(nil), s.history...)
}

// Tx is a transaction over one shard's state, which Update and View hand to their fn. The state
// is a set of byte values by key and a set of append-only logs by name. Values go in and come
// out as copies. A Tx is valid only during the call of fn.
type Tx struct {
	base     *shardState // nil for a shard that has never been written
	readOnly bool
	puts     map[string][]byte
	appends  map[string][][]byte
}

// Get returns the value of key, including what this Tx has staged, and whether there is one.
func (tx *Tx) Get(key string) ([]byte, bool) {
	if v, ok := tx.puts[key]; ok {
		return clone(v), true
	}
	if tx.base != nil {
		if v, ok := tx.base.kv[key]; ok {
			return clone(v), true
		}
	}
	return nil, false
}

// Put stages v as the value of key. It panics on a read-only Tx.
func (tx *Tx) Put(key string, v []byte) {
	tx.mustWrite()
	tx.puts[key] = clone(v)
}

// Append stages v as the next entry of the log. It panics on a read-only Tx.
func (tx *Tx) Append(log string, v []byte) {
	tx.mustWrite()
	tx.appends[log] = append(tx.appends[log], clone(v))
}

// Entries returns the entries of the log in order, including what this Tx has staged, as copies
// in a new slice.
func (tx *Tx) Entries(log string) [][]byte {
	var out [][]byte
	if tx.base != nil {
		for _, v := range tx.base.logs[log] {
			out = append(out, clone(v))
		}
	}
	for _, v := range tx.appends[log] {
		out = append(out, clone(v))
	}
	return out
}

func (tx *Tx) mustWrite() {
	if tx.readOnly {
		panic("fencedb: write on a read-only transaction")
	}
}

// clone copies v, and keeps an empty value non-nil.
func clone(v []byte) []byte {
	return append(make([]byte, 0, len(v)), v...)
}
