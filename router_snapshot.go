// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"strconv"
	"strings"

	"github.com/hmdsefi/tenure/core"
)

// routerSnapshot is what a Router's lookups read (RTR-200): an immutable value that the Router
// replaces as a whole, so a lookup sees one view and the tables built from it. Nothing in a
// published snapshot ever changes.
type routerSnapshot struct {
	// version numbers the snapshot: 0 for the one before the first view, then one more for each.
	version uint64

	// closed reports that Close has been called: lookups return ErrClosed.
	closed bool

	// view is the applied view the tables come from, nil before the first.
	view *core.View

	// members holds the member each resolvable placement key routes to, as Node.Route returns
	// it: the view's member, with Slot set to the slot for a slot key.
	members []Member

	// tables holds each space's table, in the Router's order of spaces.
	tables []routeTable
}

// routeTable is one space's table in a snapshot.
type routeTable struct {
	// owners holds, by shard, the index in members of the shard's member, or -1 when no member
	// can serve it.
	owners []int32

	// errs holds, by shard, the error of a shard with no member, or is nil when every shard has
	// one. A lookup returns a pointer into it, which allocates nothing.
	errs []shardError

	// mismatch is the *SpaceMismatchError of a space whose shard count a member disagrees with,
	// which every lookup of the space returns, or nil (RTR-510).
	mismatch error
}

// shardError is the error of a lookup of a shard that no member can serve. It matches
// ErrNoMembers and reads as Node.Route's, "tenure: no members: <space>/<shard>: <detail>". The
// text is built when asked for, so that a lookup that returns it allocates nothing.
type shardError struct {
	space  string
	shard  int
	detail string
}

// Error returns "tenure: no members: <space>/<shard>: <detail>".
func (e *shardError) Error() string {
	return ErrNoMembers.Error() + ": " + e.space + "/" + strconv.Itoa(e.shard) + ": " + e.detail
}

// Unwrap returns ErrNoMembers.
func (e *shardError) Unwrap() error {
	return ErrNoMembers
}

// route returns the member that serves shard of the space at index space, or the shard's error.
func (s *routerSnapshot) route(space, shard int) (Member, error) {
	t := &s.tables[space]
	if t.mismatch != nil {
		return Member{}, t.mismatch
	}
	if i := t.owners[shard]; i >= 0 {
		return s.members[i], nil
	}
	return Member{}, &t.errs[shard]
}

// firstSnapshot returns the snapshot before the first view: version 0, and every shard with the
// detail "no view yet".
func (r *Router) firstSnapshot() *routerSnapshot {
	s := &routerSnapshot{tables: make([]routeTable, len(r.spaces))}
	for i, h := range r.spaces {
		t := &s.tables[i]
		t.mismatch = mismatch(h.name, h.shards, s.members)
		t.owners = make([]int32, h.shards)
		t.errs = make([]shardError, h.shards)
		for shard := range t.owners {
			t.owners[shard] = -1
			t.errs[shard] = shardError{space: h.name, shard: shard, detail: "no view yet"}
		}
	}
	return s
}

// keyRoute is where one placement key routes: the index in the snapshot's members, or -1 and
// the reason no member serves it.
type keyRoute struct {
	member int32
	detail string
}

// buildSnapshot returns the snapshot of the applied view and the Assigner's assignments, with
// the given version. It resolves each placement key to a member once, as Node.Route does for
// each lookup, then maps each shard through its key: O(keys + shards) for each space, with no
// call of Balanced. The caller holds r.mu, and a view has been applied.
func (r *Router) buildSnapshot(version uint64) *routerSnapshot {
	v := r.view
	s := &routerSnapshot{version: version, view: v, tables: make([]routeTable, len(r.spaces))}

	byID := make(map[string]int, len(v.Members))
	for i, m := range v.Members {
		byID[m.ID] = i
	}
	keys := r.assigner.Keys()
	routes := make(map[string]keyRoute, len(keys))
	for _, key := range keys {
		id, slot := key, -1
		if v.Slots > 0 {
			slot = slotIndex(key)
			h := v.SlotHolders[slot]
			if h == "" {
				routes[key] = keyRoute{member: -1, detail: key + " is vacant"}
				continue
			}
			id = holderID(h)
		}
		i, ok := byID[id]
		if !ok {
			routes[key] = keyRoute{member: -1, detail: id + " is not in the view"}
			continue
		}
		m := v.Members[i]
		if slot >= 0 {
			m.Slot = slot
		}
		routes[key] = keyRoute{member: int32(len(s.members))} //nolint:gosec // at most one member per key, far below 2^31
		s.members = append(s.members, m)
	}

	for i, h := range r.spaces {
		t := &s.tables[i]
		t.mismatch = mismatch(h.name, h.shards, s.members)
		t.owners = make([]int32, h.shards)
		for shard, key := range r.assigner.Assignment(h.name) {
			kr := keyRoute{member: -1, detail: "no placement keys"}
			if key != "" {
				kr = routes[key]
			}
			t.owners[shard] = kr.member
			if kr.member >= 0 {
				continue
			}
			if t.errs == nil {
				t.errs = make([]shardError, h.shards)
			}
			t.errs[shard] = shardError{space: h.name, shard: shard, detail: kr.detail}
		}
	}
	return s
}

// slotIndex returns the slot of a slot placement key, "slot-<i>". PlacementKeys makes every key
// of a view with slots one of these.
func slotIndex(key string) int {
	s, err := strconv.Atoi(strings.TrimPrefix(key, "slot-"))
	if err != nil {
		panic("tenure: router: placement key " + strconv.Quote(key) + " names no slot")
	}
	return s
}
