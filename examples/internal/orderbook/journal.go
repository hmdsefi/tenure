// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orderbook

import (
	"context"
	"errors"
	"fmt"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

// journalLog is the log of a shard's journal in a fencedb.DB.
const journalLog = "journal"

// ErrLength means an Append expected the shard's journal to hold another number of lines than it
// does, and appended nothing.
var ErrLength = errors.New("orderbook: the journal's length is not the expected one")

// Journal keeps the journal of every shard of the markets space behind a fence per shard: the
// highest term it has seen for the shard. An engine journals each order it matches under its
// shard's term, so an engine whose term is below the fence can journal nothing.
type Journal interface {
	// Raise sets the fence of the shard to term. It returns an error that wraps
	// fencedb.ErrStale, and leaves the fence as it was, when term is below the fence.
	Raise(ctx context.Context, shard int, term core.Term) error

	// Entries returns the shard's journal lines in order.
	Entries(ctx context.Context, shard int) ([][]byte, error)

	// Append adds line to the shard's journal under term as its line number at, counted from 0,
	// which also raises the fence to term. It returns an error that wraps fencedb.ErrStale, and
	// appends nothing, when term is below the fence, and one that wraps ErrLength, and appends
	// nothing, when the journal does not hold exactly at lines. Any other error may leave the
	// line appended or not.
	Append(ctx context.Context, shard int, term core.Term, at int, line []byte) error
}

// DBJournal is a Journal kept in a fencedb.DB, in its fence space SpaceName. Make one with
// NewDBJournal.
type DBJournal struct {
	db *fencedb.DB
}

// NewDBJournal returns a Journal kept in db.
func NewDBJournal(db *fencedb.DB) *DBJournal {
	return &DBJournal{db: db}
}

// Raise calls db.Raise.
func (j *DBJournal) Raise(_ context.Context, shard int, term core.Term) error {
	return j.db.Raise(SpaceName, shard, term)
}

// Entries reads the shard's journal in a db.View.
func (j *DBJournal) Entries(_ context.Context, shard int) ([][]byte, error) {
	var lines [][]byte
	err := j.db.View(SpaceName, shard, func(tx *fencedb.Tx) error {
		lines = tx.Entries(journalLog)
		return nil
	})
	return lines, err
}

// Append appends line in one db.Update at term, when the journal holds at lines.
func (j *DBJournal) Append(_ context.Context, shard int, term core.Term, at int, line []byte) error {
	return j.db.Update(SpaceName, shard, term, func(tx *fencedb.Tx) error {
		if n := len(tx.Entries(journalLog)); n != at {
			return fmt.Errorf("%w: %s/%d holds %d lines, the append expects %d", ErrLength, SpaceName, shard, n, at)
		}
		tx.Append(journalLog, line)
		return nil
	})
}
