// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bubble

import (
	"context"
	"fmt"
	"sync"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// errCut is the error of every call through a cut memberStore. It wraps
// context.DeadlineExceeded: to the node, a cut store looks like one whose calls time out.
var errCut = fmt.Errorf("bubble: store cut: %w", context.DeadlineExceeded)

// gateFunc gates one call of a member to the store or the fence: it runs call, or refuses it with
// an error and does not run it. op names the call, such as "store.Acquire" or "fence.write". The
// default gate runs every call; a faultline process gates each call through Process.Do.
type gateFunc func(member, op string, call func()) error

// runGate is the default gate: it runs every call.
func runGate(_, _ string, call func()) error {
	call()
	return nil
}

// memberStore is one member's view of the shared memstore: a tenure.Store, tenure.SlotStore and
// tenure.Watcher that forwards every call, through the harness's gate, unless it is cut. Every
// incarnation of the member uses the same memberStore.
//
// After every forwarded call it lets the harness read the store's record writes (syncGrants), so
// the checker hears of a grant before the call returns to the node. It also keeps the term of
// every successful Acquire with the holder that made it, from which the harness tells which
// holder a Run of this member serves.
type memberStore struct {
	h      *harness
	member string
	mem    *memstore.Store

	mu       sync.Mutex // held across the store call of Acquire, see Acquire
	cut      bool
	watches  map[int]context.CancelFunc // the open watches, cancelled by Cut
	nextW    int
	acquired map[termKey]string // (space, shard, term) -> holder, for this member's grants
}

// termKey names one term of one shard.
type termKey struct {
	space string
	shard int
	term  core.Term
}

var (
	_ tenure.Store     = (*memberStore)(nil)
	_ tenure.SlotStore = (*memberStore)(nil)
	_ tenure.Watcher   = (*memberStore)(nil)
)

func newMemberStore(h *harness, member string) *memberStore {
	return &memberStore{
		h:        h,
		member:   member,
		mem:      h.store,
		watches:  make(map[int]context.CancelFunc),
		acquired: make(map[termKey]string),
	}
}

// Cut cuts the member off from the store: every later call fails with errCut without reaching
// the store, and every open watch channel is closed.
func (s *memberStore) Cut() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cut = true
	for id, cancel := range s.watches {
		cancel()
		delete(s.watches, id)
	}
}

// Heal ends a Cut.
func (s *memberStore) Heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cut = false
}

// isCut reports whether the member is cut off.
func (s *memberStore) isCut() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cut
}

// holderOf returns the holder this member acquired the term with, and whether it did. It waits
// for an Acquire whose store call is in progress.
func (s *memberStore) holderOf(space string, shard int, term core.Term) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.acquired[termKey{space, shard, term}]
	return h, ok
}

// do runs one call of the store: ctx's error when ctx is done, errCut when the member is cut,
// the gate's error when the gate refuses the call, and the call's own error otherwise. After a
// call that ran, the harness reads the store's record writes.
func (s *memberStore) do(ctx context.Context, op string, call func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isCut() {
		return errCut
	}
	var err error
	if gerr := s.h.gate(s.member, "store."+op, func() { err = call() }); gerr != nil {
		return gerr
	}
	s.h.syncGrants()
	return err
}

// KeepAlive forwards to the store.
func (s *memberStore) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	return s.do(ctx, "KeepAlive", func() error { return s.mem.KeepAlive(ctx, holder, timing) })
}

// Acquire forwards to the store and keeps a granted term with holder.
func (s *memberStore) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	var term core.Term
	err := s.do(ctx, "Acquire", func() error {
		// mu is held across the store's call: the store delivers the grant to the node's own
		// watch before it returns, and the node may start the shard's Run from that record
		// before this call returns; holderOf then waits for the term to be kept.
		s.mu.Lock()
		defer s.mu.Unlock()
		var err error
		term, err = s.mem.Acquire(ctx, holder, space, shard)
		if err == nil {
			s.acquired[termKey{space, shard, term}] = holder
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return term, nil
}

// Release forwards to the store.
func (s *memberStore) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	return s.do(ctx, "Release", func() error { return s.mem.Release(ctx, holder, space, shard, term) })
}

// Confirm forwards to the store.
func (s *memberStore) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	var rec tenure.Holding
	err := s.do(ctx, "Confirm", func() error {
		var err error
		rec, err = s.mem.Confirm(ctx, holder, space, shard, term)
		return err
	})
	if err != nil {
		return tenure.Holding{}, err
	}
	return rec, nil
}

// CloseSession forwards to the store.
func (s *memberStore) CloseSession(ctx context.Context, holder string) error {
	return s.do(ctx, "CloseSession", func() error { return s.mem.CloseSession(ctx, holder) })
}

// Holdings forwards to the store.
func (s *memberStore) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	var recs []tenure.Holding
	err := s.do(ctx, "Holdings", func() error {
		var err error
		recs, err = s.mem.Holdings(ctx, space)
		return err
	})
	if err != nil {
		return nil, err
	}
	return recs, nil
}

// ClaimSlot forwards to the store.
func (s *memberStore) ClaimSlot(ctx context.Context, holder string) (int, error) {
	slot := -1
	err := s.do(ctx, "ClaimSlot", func() error {
		var err error
		slot, err = s.mem.ClaimSlot(ctx, holder)
		return err
	})
	if err != nil {
		return -1, err
	}
	return slot, nil
}

// ReleaseSlot forwards to the store.
func (s *memberStore) ReleaseSlot(ctx context.Context, holder string, slot int) error {
	return s.do(ctx, "ReleaseSlot", func() error { return s.mem.ReleaseSlot(ctx, holder, slot) })
}

// Slots forwards to the store.
func (s *memberStore) Slots(ctx context.Context) (int, []string, error) {
	var (
		count   int
		holders []string
	)
	err := s.do(ctx, "Slots", func() error {
		var err error
		count, holders, err = s.mem.Slots(ctx)
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	return count, holders, nil
}

// SetSlots forwards to the store.
func (s *memberStore) SetSlots(ctx context.Context, count int) error {
	return s.do(ctx, "SetSlots", func() error { return s.mem.SetSlots(ctx, count) })
}

// WatchHolders opens a watch of the store under a context of its own, which a Cut cancels: the
// store then closes the channel, as a store closes a watch it cannot serve.
func (s *memberStore) WatchHolders(ctx context.Context) (<-chan tenure.Holding, error) {
	var ch <-chan tenure.Holding
	err := s.do(ctx, "WatchHolders", func() error {
		wctx, cancel := context.WithCancel(ctx)
		s.mu.Lock()
		if s.cut {
			// Cut between do's check and the call: the watch would outlive the cut.
			s.mu.Unlock()
			cancel()
			return errCut
		}
		id := s.nextW
		s.nextW++
		s.watches[id] = cancel
		s.mu.Unlock()
		var err error
		ch, err = s.mem.WatchHolders(wctx)
		if err != nil {
			s.forget(id)
			return err
		}
		// The watch ends with the caller's ctx too; forget its cancel then.
		context.AfterFunc(wctx, func() { s.forget(id) })
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// forget cancels and drops the watch id, if it is still open.
func (s *memberStore) forget(id int) {
	s.mu.Lock()
	cancel, ok := s.watches[id]
	delete(s.watches, id)
	s.mu.Unlock()
	if ok {
		cancel()
	}
}
