// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package orderbook is the orderbook's shared code: the books, the engine that matches a shard's
// orders and journals them under the shard's term, the journal they go to, and the HTTP server
// that takes orders.
//
// A shard's new owner raises the shard's fence in the journal before it reads it, so a stale
// owner, one that still runs after another member took the shard, can no longer journal an
// order. Both orderbook programs, the in-process demo and the one on Kubernetes, run the same
// Orderbook; the package exists so that they can share it.
package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

// SpaceName is the name of the markets space, and of the fence space of the journal.
const SpaceName = "markets"

// appendTimeout bounds one Append of an engine. The engine holds its lock meanwhile, and the
// append must not end with the request that asked for it: an order whose commit was cut off
// half way would leave the client unsure whether it is journaled.
const appendTimeout = 10 * time.Second

var (
	// errRefusing means the engine takes no more orders: the fence rejected one of its commits,
	// or its Run has ended.
	errRefusing = errors.New("orderbook: engine refuses orders")

	// errDuplicate means the order's ID is already journaled.
	errDuplicate = errors.New("orderbook: duplicate order id")
)

// Entry is one line of the journal: an order and the fills it made when it arrived.
type Entry struct {
	Order Order  `json:"order"`
	Fills []Fill `json:"fills"`
}

// Orderbook is the service that a node runs: the journal that its engines write to, and the
// engines that are published for the shards it owns. Make one with New.
type Orderbook struct {
	j Journal

	// Raised, when not nil, is called by Run once the journal has accepted the term of a shard
	// as its fence, before Run reads the journal. Set it before the first Run.
	Raised func(shard int, term core.Term)

	// Commit, when not nil, is called after every attempt of an engine to journal an order, with
	// the shard, the term the attempt carried, the order's ID and the error the journal's
	// Append returned (nil when it was committed). It runs inside the engine's critical
	// section: it must not call the engine. Set it before the first Run.
	Commit func(shard int, term core.Term, id string, err error)

	mu      sync.Mutex
	engines map[engineKey]*Engine
}

// engineKey names an engine: the shard and the term of the ownership it was published for.
type engineKey struct {
	shard int
	term  core.Term
}

// New returns an Orderbook that journals to j.
func New(j Journal) *Orderbook {
	return &Orderbook{j: j, engines: make(map[engineKey]*Engine)}
}

// Run serves one shard of the markets space while the node owns it, as the Run of the space. It
// raises the shard's fence to the term of s first, so that an older owner can no longer commit;
// an error that wraps fencedb.ErrStale means a newer owner got there first, and Run returns it
// without reading the journal. It then rebuilds every market's book from the journaled orders,
// checks that matching them again gives the journaled fills, publishes the engine for (shard,
// term) and blocks until ctx ends, when it unpublishes the engine and returns nil. A journal
// that does not replay to its own fills is an error: nothing is published.
func (o *Orderbook) Run(ctx context.Context, s *tenure.Shard) error {
	if err := o.j.Raise(ctx, s.ID, s.Term); err != nil {
		return err
	}
	if o.Raised != nil {
		o.Raised(s.ID, s.Term)
	}

	lines, err := o.j.Entries(ctx, s.ID)
	if err != nil {
		return err
	}

	e := newEngine(o, s.ID, s.Term)
	if err := e.replay(lines); err != nil {
		return fmt.Errorf("orderbook: %s/%d at term %d: %w", SpaceName, s.ID, s.Term, err)
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
	e.Close()
	return nil
}

// Engine returns the engine published for the shard at exactly this term, or nil when there is
// none: Run has not published it yet, has ended, or ran for another term.
func (o *Orderbook) Engine(shard int, term core.Term) *Engine {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.engines[engineKey{shard, term}]
}

// Replay checks a shard's journal lines, as Entries returns them: every line is one valid order
// with an ID that no earlier line has, and matching the orders again, in order, into empty books
// gives the fills each line holds.
func Replay(lines [][]byte) error {
	return newEngine(nil, 0, 0).replay(lines)
}

// Engine matches the orders of one shard at one term and journals them.
type Engine struct {
	o     *Orderbook
	shard int
	term  core.Term

	mu       sync.Mutex
	books    map[string]*book
	ids      map[string]struct{} // IDs of the journaled orders
	refusing bool
	unsure   bool // an Append failed without saying whether it committed
}

func newEngine(o *Orderbook, shard int, term core.Term) *Engine {
	return &Engine{o: o, shard: shard, term: term, books: make(map[string]*book), ids: make(map[string]struct{})}
}

// replay matches the journaled orders into empty books and compares the fills each one makes
// with the journaled fills.
func (e *Engine) replay(lines [][]byte) error {
	for i, line := range lines {
		var en Entry
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
func (e *Engine) bookOf(market string) *book {
	b := e.books[market]
	if b == nil {
		b = new(book)
		e.books[market] = b
	}
	return b
}

// submit matches o and journals it with its fills in one Append at the engine's term, and
// returns the fills, never nil. The match runs on a copy of the market's book, which replaces
// the book only when the commit succeeded, so a rejected order leaves no trace.
//
// It fails with an error that wraps errInvalidOrder for an order that does not validate, with
// errRefusing once the engine refuses orders, and with errDuplicate when the ID is journaled. An
// error that wraps fencedb.ErrStale means the fence rejected the commit: the engine then refuses
// every further order. Any other error of Append leaves it unknown whether the order is
// journaled (a remote journal's answer can be lost), so the engine reads its journal again
// before it takes the next order, and refuses orders with errRefusing until that read works.
func (e *Engine) submit(o Order) ([]Fill, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.refusing {
		return nil, errRefusing
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	if e.unsure {
		if err := e.reread(); err != nil {
			return nil, fmt.Errorf("%w: read the journal again: %w", errRefusing, err)
		}
	}
	if _, dup := e.ids[o.ID]; dup {
		return nil, fmt.Errorf("%w: %s", errDuplicate, o.ID)
	}

	b := e.bookOf(o.Market).clone()
	fills := b.match(o)
	line, err := json.Marshal(Entry{Order: o, Fills: fills})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
	err = e.o.j.Append(ctx, e.shard, e.term, line)
	cancel()
	if e.o.Commit != nil {
		e.o.Commit(e.shard, e.term, o.ID, err)
	}
	if err != nil {
		if errors.Is(err, fencedb.ErrStale) {
			e.refusing = true
		} else {
			e.unsure = true
		}
		return nil, err
	}
	e.books[o.Market] = b
	e.ids[o.ID] = struct{}{}
	return fills, nil
}

// reread rebuilds the books and the IDs from the journal, after an Append whose outcome is
// unknown. A journal that no longer replays makes the engine refuse orders for good.
func (e *Engine) reread() error {
	ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
	lines, err := e.o.j.Entries(ctx, e.shard)
	cancel()
	if err != nil {
		return err
	}
	fresh := newEngine(e.o, e.shard, e.term)
	if err := fresh.replay(lines); err != nil {
		e.refusing = true
		return err
	}
	e.books, e.ids, e.unsure = fresh.books, fresh.ids, false
	return nil
}

// Close makes the engine refuse every further order.
func (e *Engine) Close() {
	e.mu.Lock()
	e.refusing = true
	e.mu.Unlock()
}

// snapshot returns every market's book as String lists it.
func (e *Engine) snapshot() map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]string, len(e.books))
	for m, b := range e.books {
		out[m] = b.String()
	}
	return out
}
