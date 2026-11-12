// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package signing

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	// ErrConflict means the chain rejected a transaction whose nonce is below the wallet's next
	// one: the nonce was used already.
	ErrConflict = errors.New("signing: nonce conflict")

	// ErrGap means the chain rejected a transaction whose nonce is above the wallet's next one:
	// a nonce is missing.
	ErrGap = errors.New("signing: nonce gap")
)

// Report is what a FakeChain saw.
type Report struct {
	// Accepted is the number of transactions the chain accepted.
	Accepted int `json:"accepted"`

	// Conflicts are the rejected transactions whose nonce was below the wallet's next one.
	Conflicts []Tx `json:"conflicts"`

	// Gaps are the rejected transactions whose nonce was above the wallet's next one.
	Gaps []Tx `json:"gaps"`
}

// FakeChain is a Chain that keeps the next nonce of each wallet, from 0. It accepts a
// transaction with that nonce and rejects, and records, every other. It is safe for concurrent
// use; make one with NewChain.
type FakeChain struct {
	mu        sync.Mutex
	next      map[string]uint64
	accepted  int
	conflicts []Tx
	gaps      []Tx
}

// NewChain returns an empty FakeChain.
func NewChain() *FakeChain {
	return &FakeChain{next: make(map[string]uint64)}
}

// Broadcast accepts tx when its nonce is the wallet's next one. A lower nonce fails with an
// error that wraps ErrConflict, a higher one with an error that wraps ErrGap; the transaction is
// recorded and the wallet's next nonce stays.
func (c *FakeChain) Broadcast(_ context.Context, tx Tx) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := c.next[tx.Wallet]
	switch {
	case tx.Nonce == next:
		c.next[tx.Wallet] = next + 1
		c.accepted++
		return nil
	case tx.Nonce < next:
		c.conflicts = append(c.conflicts, tx)
		return fmt.Errorf("%w: %s nonce %d, next %d", ErrConflict, tx.Wallet, tx.Nonce, next)
	default:
		c.gaps = append(c.gaps, tx)
		return fmt.Errorf("%w: %s nonce %d, next %d", ErrGap, tx.Wallet, tx.Nonce, next)
	}
}

// Report returns what the chain has seen so far, in copies.
func (c *FakeChain) Report() Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Report{
		Accepted:  c.accepted,
		Conflicts: slices.Clone(c.conflicts),
		Gaps:      slices.Clone(c.gaps),
	}
}
