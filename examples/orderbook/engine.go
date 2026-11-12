// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

const (
	// marketsSpace is the name of the space, and of the fence space of the journal in the fake
	// downstream.
	marketsSpace = "markets"

	// journalLog is the log of a shard's journal.
	journalLog = "journal"
)

var (
	// errRefusing means the engine takes no more orders: the fence rejected one of its commits,
	// or its Run has ended.
	errRefusing = errors.New("orderbook: engine refuses orders")

	// errDuplicate means the order's ID is already journaled.
	errDuplicate = errors.New("orderbook: duplicate order id")
)

// entry is one line of the journal: an order and the fills it made when it arrived.
type entry struct {
	Order order  `json:"order"`
	Fills []fill `json:"fills"`
}

// orderbook is the service that a node runs: the fake downstream that its engines journal to,
// and the engines that are published for the shards it owns.
type orderbook struct {
	db *fencedb.DB

	// raised, when not nil, is called by Run once db.Raise has accepted the term of a shard,
	// before Run reads the journal. Set it before the first Run; it is copied into every engine.
	raised func(shard int, term core.Term)

	// commit, when not nil, is called after every attempt of an engine to journal an order, with
	// the shard, the term the attempt carried, the order's ID and the error db.Update returned
	// (nil when it was committed). It runs inside the engine's critical section: it must not
	// call the engine. Set it before the first Run.
	commit func(shard int, term core.Term, id string, err error)

	mu      sync.Mutex
	engines map[engineKey]*engine
}

// engineKey names an engine: the shard and the term of the ownership it was published for.
type engineKey struct {
	shard int
	term  core.Term
}

// newOrderbook returns an orderbook that journals to db.
func newOrderbook(db *fencedb.DB) *orderbook {
	return &orderbook{db: db, engines: make(map[engineKey]*engine)}
}

// Run serves one shard of the markets space while the node owns it, as the Run of the space. It
// raises the shard's fence to the term of s first, so that an older owner can no longer commit;
// an error that wraps fencedb.ErrStale means a newer owner got there first, and Run returns it
// without reading the journal. It then rebuilds every market's book from the journaled orders,
// checks that matching them again gives the journaled fills, publishes the engine for (shard,
// term) and blocks until ctx ends, when it unpublishes the engine and returns nil. A journal
// that does not replay to its own fills is an error: nothing is published.
func (o *orderbook) Run(ctx context.Context, s *tenure.Shard) error {
	if err := o.db.Raise(marketsSpace, s.ID, s.Term); err != nil {
		return err
	}
	if o.raised != nil {
		o.raised(s.ID, s.Term)
	}

	var lines [][]byte
	if err := o.db.View(marketsSpace, s.ID, func(tx *fencedb.Tx) error {
		lines = tx.Entries(journalLog)
		return nil
	}); err != nil {
		return err
	}

	e := newEngine(o, s.ID, s.Term)
	if err := e.replay(lines); err != nil {
		return fmt.Errorf("orderbook: %s/%d at term %d: %w", marketsSpace, s.ID, s.Term, err)
	}

	k := engineKey{s.ID, s.Term}
	o.mu.Lock()
	o.engines[k] = e
	o.mu.Unlock()

	<-ctx.Done()

	o.mu.Lock()
	if o.engines[k] == e {
		delete(o.engines, k)
	}
	o.mu.Unlock()
	e.close()
	return nil
}

// engine returns the engine published for the shard at exactly this term, or nil when there is
// none: Run has not published it yet, has ended, or ran for another term.
func (o *orderbook) engine(shard int, term core.Term) *engine {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.engines[engineKey{shard, term}]
}

// engine matches the orders of one shard at one term and journals them.
type engine struct {
	o     *orderbook
	shard int
	term  core.Term

	mu       sync.Mutex
	books    map[string]*book
	ids      map[string]struct{} // IDs of the journaled orders
	refusing bool
}

func newEngine(o *orderbook, shard int, term core.Term) *engine {
	return &engine{o: o, shard: shard, term: term, books: make(map[string]*book), ids: make(map[string]struct{})}
}

// replay matches the journaled orders into empty books and compares the fills each one makes
// with the journaled fills.
func (e *engine) replay(lines [][]byte) error {
	for i, line := range lines {
		var en entry
		if err := json.Unmarshal(line, &en); err != nil {
			return fmt.Errorf("journal entry %d: %w", i, err)
		}
		if err := en.Order.validate(); err != nil {
			return fmt.Errorf("journal entry %d: %w", i, err)
		}
		if _, dup := e.ids[en.Order.ID]; dup {
			return fmt.Errorf("journal entry %d: order %q is journaled twice", i, en.Order.ID)
		}
		got := e.bookOf(en.Order.Market).match(en.Order)
		if !slices.Equal(got, en.Fills) {
			return fmt.Errorf("journal entry %d (order %q): fills differ: journaled %v, replayed %v",
				i, en.Order.ID, en.Fills, got)
		}
		e.ids[en.Order.ID] = struct{}{}
	}
	return nil
}

// bookOf returns the book of a market, making it when there is none.
func (e *engine) bookOf(market string) *book {
	b := e.books[market]
	if b == nil {
		b = new(book)
		e.books[market] = b
	}
	return b
}

// submit matches o and journals it with its fills in one db.Update at the engine's term, and
// returns the fills, never nil. The match runs on a copy of the market's book, which replaces
// the book only when the commit succeeded, so a rejected order leaves no trace.
//
// It fails with an error that wraps errInvalidOrder for an order that does not validate, with
// errRefusing once the engine refuses orders, and with errDuplicate when the ID is journaled. An
// error that wraps fencedb.ErrStale means the fence rejected the commit: the engine then refuses
// every further order.
func (e *engine) submit(o order) ([]fill, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.refusing {
		return nil, errRefusing
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	if _, dup := e.ids[o.ID]; dup {
		return nil, fmt.Errorf("%w: %s", errDuplicate, o.ID)
	}

	b := e.bookOf(o.Market).clone()
	fills := b.match(o)
	line, err := json.Marshal(entry{Order: o, Fills: fills})
	if err != nil {
		return nil, err
	}
	err = e.o.db.Update(marketsSpace, e.shard, e.term, func(tx *fencedb.Tx) error {
		tx.Append(journalLog, line)
		return nil
	})
	if e.o.commit != nil {
		e.o.commit(e.shard, e.term, o.ID, err)
	}
	if err != nil {
		if errors.Is(err, fencedb.ErrStale) {
			e.refusing = true
		}
		return nil, err
	}
	e.books[o.Market] = b
	e.ids[o.ID] = struct{}{}
	return fills, nil
}

// close makes the engine refuse every further order.
func (e *engine) close() {
	e.mu.Lock()
	e.refusing = true
	e.mu.Unlock()
}

// snapshot returns every market's book as String lists it.
func (e *engine) snapshot() map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]string, len(e.books))
	for m, b := range e.books {
		out[m] = b.String()
	}
	return out
}
