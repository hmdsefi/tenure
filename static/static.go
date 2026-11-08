// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package static is a membership over a fixed list of members, for tests, local development and
// fleets whose members are known in advance.
//
// Every handle made by one New call, and by Share on its handles, sees the same group. Changes
// reach every watcher at once, inside the call that makes them. The package starts no goroutine
// and arms no timer.
package static

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Membership is one handle on a static group. It implements tenure.Membership and
// tenure.PropagationEstimator. A handle that has not joined still watches the group. Make one
// with New or Share; the zero value is not usable.
//
// The methods are safe for concurrent use.
type Membership struct {
	g *group

	// The fields below are guarded by g.mu.
	self    string // ID this handle joined with; "" before Join
	closed  bool
	watches map[*watch]struct{}
}

// group is the state every handle of one New call shares.
type group struct {
	mu      sync.Mutex
	members []core.Member // sorted by ID; owned by the group, never shared with a view
	version uint64        // Version of the current view
	joined  map[string]*Membership
	handles map[*Membership]struct{} // handles with open watches
}

// watch is one channel returned by Watch.
type watch struct {
	ch   chan core.View
	stop func() bool // stops the context.AfterFunc that closes ch
}

// New returns a handle on a new group that lists members. It checks the members in order and
// returns the first failure: a member that is not valid, or one whose ID an earlier member has.
// The group keeps its own copy of members, Meta maps included. The listed members are in every
// view from the start, before any handle joins; the first view has Version 1.
func New(members []core.Member) (*Membership, error) {
	seen := make(map[string]struct{}, len(members))
	list := make([]core.Member, 0, len(members))
	for i, m := range members {
		if err := m.Validate(); err != nil {
			return nil, fmt.Errorf("static: member %d: %w", i, err)
		}
		if _, dup := seen[m.ID]; dup {
			return nil, fmt.Errorf("static: member %d: duplicate ID %q", i, m.ID)
		}
		seen[m.ID] = struct{}{}
		list = append(list, copyMember(m))
	}
	slices.SortFunc(list, func(a, b core.Member) int { return strings.Compare(a.ID, b.ID) })
	g := &group{
		members: list,
		joined:  make(map[string]*Membership),
		handles: make(map[*Membership]struct{}),
	}
	g.version = 1
	return &Membership{g: g}, nil
}

// Share returns a new handle on the same group as m. The new handle has its own joined state and
// its own channels; it shares the members and the views with m. It is open even when m has
// left.
func (m *Membership) Share() *Membership {
	return &Membership{g: m.g}
}

// Join adds self to the group, or replaces the listed member with the same ID, and delivers the
// new view when member data changes. It checks, in order: ctx, that m has not left
// (tenure.ErrClosed), that self is valid, that m has not joined already, and that no other
// handle of the group has joined with self.ID. The group keeps its own copy of self.Meta.
func (m *Membership) Join(ctx context.Context, self core.Member) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := m.g
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.closed {
		return tenure.ErrClosed
	}
	if err := self.Validate(); err != nil {
		return fmt.Errorf("static: %w", err)
	}
	switch {
	case m.self != "":
		return errors.New("static: already joined")
	case g.joined[self.ID] != nil:
		return fmt.Errorf("static: member %q already joined", self.ID)
	}
	m.self = self.ID
	g.joined[self.ID] = m
	next := slices.Clone(g.members)
	i, found := slices.BinarySearchFunc(next, self.ID, func(e core.Member, id string) int {
		return strings.Compare(e.ID, id)
	})
	if found {
		next[i] = copyMember(self)
	} else {
		next = slices.Insert(next, i, copyMember(self))
	}
	g.update(next)
	return nil
}

// Watch returns a channel that holds the current view and receives each later one. The channel
// holds one view; a newer view replaces one not yet received. It is closed when ctx is done and
// when m leaves. Watch returns tenure.ErrClosed after Leave.
func (m *Membership) Watch(ctx context.Context) (<-chan core.View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g := m.g
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.closed {
		return nil, tenure.ErrClosed
	}
	w := &watch{ch: make(chan core.View, 1)}
	w.ch <- g.view()
	if m.watches == nil {
		m.watches = make(map[*watch]struct{})
	}
	m.watches[w] = struct{}{}
	g.handles[m] = struct{}{}
	w.stop = context.AfterFunc(ctx, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if _, ok := m.watches[w]; ok {
			m.unwatch(w)
		}
	})
	return w.ch, nil
}

// SetDraining sets Draining of the member m joined with, and delivers the new view when the value
// changes. It checks, in order: ctx, that m has not left (tenure.ErrClosed), and that m has
// joined.
func (m *Membership) SetDraining(ctx context.Context, draining bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := m.g
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case m.closed:
		return tenure.ErrClosed
	case m.self == "":
		return errors.New("static: not joined")
	}
	i := g.index(m.self)
	if g.members[i].Draining == draining {
		return nil
	}
	next := slices.Clone(g.members)
	next[i].Draining = draining
	g.update(next)
	return nil
}

// Leave removes the member m joined with, delivers the view without it, and then closes every
// channel of m. Join, SetDraining and Watch on m then return tenure.ErrClosed. Leave returns
// ctx.Err() when ctx is done, and nil otherwise, also before Join and when called again.
func (m *Membership) Leave(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := m.g
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.closed {
		return nil
	}
	if m.self != "" {
		i := g.index(m.self)
		delete(g.joined, m.self)
		g.update(slices.Delete(slices.Clone(g.members), i, i+1))
		m.self = ""
	}
	for w := range m.watches {
		w.stop()
		m.unwatch(w)
	}
	m.closed = true
	return nil
}

// Propagation returns 0: a change reaches every view inside the call that makes it.
func (m *Membership) Propagation() time.Duration {
	return 0
}

// unwatch closes w and forgets it. The caller holds m.g.mu, and w is one of m's watches.
func (m *Membership) unwatch(w *watch) {
	close(w.ch)
	delete(m.watches, w)
	if len(m.watches) == 0 {
		delete(m.g.handles, m)
	}
}

// index returns the index of the member with id, which the group lists. The caller holds g.mu.
func (g *group) index(id string) int {
	i, _ := slices.BinarySearchFunc(g.members, id, func(e core.Member, id string) int {
		return strings.Compare(e.ID, id)
	})
	return i
}

// view returns the current view with its own copy of the members, Meta maps included, so no two
// channels share a map. The caller holds g.mu.
func (g *group) view() core.View {
	return core.View{Version: g.version, Members: copyMembers(g.members)}
}

// update makes next, sorted by ID, the group's member list. When member data changed, it moves
// to the next Version and delivers the new view to every open channel: a view not yet received
// is replaced. The caller holds g.mu.
func (g *group) update(next []core.Member) {
	if slices.EqualFunc(g.members, next, sameData) {
		return
	}
	g.members = next
	g.version++
	for h := range g.handles {
		for w := range h.watches {
			// Only the group sends, under g.mu, so after the drain the send does not block.
			select {
			case <-w.ch:
			default:
			}
			w.ch <- g.view()
		}
	}
}

// sameData reports whether a and b hold the same member data: Slot is ignored and Meta is
// compared by content.
func sameData(a, b core.Member) bool {
	return a.ID == b.ID && a.Addr == b.Addr && a.Draining == b.Draining &&
		maps.Equal(a.Meta, b.Meta) && a.JoinAt.Equal(b.JoinAt) && a.LeaveAt.Equal(b.LeaveAt)
}

// copyMember returns m with its own copy of m.Meta.
func copyMember(m core.Member) core.Member {
	m.Meta = maps.Clone(m.Meta)
	return m
}

// copyMembers returns a copy of members, Meta maps included, or nil when members is empty.
func copyMembers(members []core.Member) []core.Member {
	if len(members) == 0 {
		return nil
	}
	out := make([]core.Member, len(members))
	for i, m := range members {
		out[i] = copyMember(m)
	}
	return out
}
