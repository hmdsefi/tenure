// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bubble

import (
	"sync"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// fence is the downstream system every Run writes to: for each shard, of a fenced space or not,
// the largest term it has been raised to. Raising keeps the largest term; a write is accepted
// when its term is at least the raised one, and a write never raises. It is the event mode's
// fence model, under a mutex.
type fence struct {
	mu     sync.Mutex
	raised map[shardKey]core.Term

	// onAccepted hears of every accepted write while mu is held, so that it hears of them in the
	// order the fence accepted them.
	onAccepted func(check.Write)
}

// shardKey names one shard.
type shardKey struct {
	space string
	shard int
}

func newFence(onAccepted func(check.Write)) *fence {
	return &fence{raised: make(map[shardKey]core.Term), onAccepted: onAccepted}
}

// raise raises the shard's fence to term, unless it is higher already, and returns the fence's
// term after the call.
func (f *fence) raise(space string, shard int, term core.Term) core.Term {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := shardKey{space, shard}
	if term > f.raised[k] {
		f.raised[k] = term
	}
	return f.raised[k]
}

// write reports whether the fence accepts a write of holder in term: whether term is at least
// the shard's raised term.
func (f *fence) write(holder, space string, shard int, term core.Term) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if term < f.raised[shardKey{space, shard}] {
		return false
	}
	if f.onAccepted != nil {
		f.onAccepted(check.Write{Space: space, Shard: shard, Term: term, Holder: holder})
	}
	return true
}
