// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// routes is the routing snapshot that Route and Local read (RUN-400): an immutable value that the
// node replaces as a whole through n.snap, so a reader sees one view and the assignments
// computed from it, and never waits for the loop.
type routes struct {
	// view is the published merged view the assignments come from, the pointer n.view held when
	// the snapshot was made; nil before the first view.
	view *core.View

	// byID maps each member ID of view to the member's index in view.Members.
	byID map[string]int

	// assign holds each space's Machine.Assignment, by space name: the placement key of each
	// shard, "" for a shard with none. The machine never changes a slice it has returned.
	assign map[string][]string

	// held is the slot the machine holds after the step the snapshot was made in, -1 for none,
	// and heldKey its placement key, "" for none; self is the member's own ID. Route resolves
	// heldKey to self: the machine holds the slot from a ClaimSlot result, which a view from a
	// later read is still to show.
	held    int
	heldKey string
	self    string

	// closed reports that Close has stopped every goroutine of the node, right before it
	// returns: Route and Local return ErrClosed.
	closed bool
}

// Route returns the member that serves key in space: the member to which the node's latest view
// assigns key's shard. The shard is the space's ShardOf(key), or core.KeyShard(key, Shards) when
// ShardOf is nil. Route answers from the state the node has published and never waits for the
// node, so right after a view change it may still answer from the view before. The Meta map of
// the result is shared with the node's view and must not be modified.
//
// After Close, Route returns ErrClosed. It fails for a space the node does not have, such as
// `tenure: unknown space "carts"`, and for a ShardOf result outside the space's shards, such as
// `tenure: orders: ShardOf("k") = 9 is outside [0, 8)`. When no member can serve the shard, it
// returns an error that matches ErrNoMembers and names the shard, such as
// "tenure: no members: orders/3: no placement keys" when no member of the view is eligible.
//
// When the group has slots, shards are placed on slots: the member that serves a slot is the
// one whose ID is the ID part of the slot's holder, and the Slot of the result is that slot. A
// vacant slot gives "tenure: no members: orders/3: slot-1 is vacant", and a holder whose member
// is not in the view gives "tenure: no members: orders/3: n4 is not in the view".
func (n *Node) Route(space, key string) (Member, error) {
	r, _, shard, err := n.locate(space, key)
	if err != nil {
		return Member{}, err
	}
	return r.route(space, shard)
}

// Local returns this member's Shard for key in space while the member owns it, that is while the
// Shard's Check returns nil. The shard of key is the one Route takes. Otherwise Local returns a
// *NotOwnerError for the shard, whose Route is the member Route names for key, or the zero Member
// when Route finds none. After Close, for an unknown space and for a ShardOf result outside the
// space's shards, Local fails as Route does. Local takes no lock and never waits for the node.
func (n *Node) Local(space, key string) (*Shard, error) {
	r, sp, shard, err := n.locate(space, key)
	if err != nil {
		return nil, err
	}
	if sh := sp.local[shard].Load(); sh.Check() == nil {
		return sh, nil
	}
	to, _ := r.route(space, shard)
	return nil, &NotOwnerError{Space: space, Shard: shard, Route: to}
}

// locate runs Route's first three steps (RUN-400): the published snapshot unless the node is
// closed, the space, and key's shard in it.
func (n *Node) locate(space, key string) (*routes, *spaceShards, int, error) {
	r := n.snap.Load()
	if r.closed {
		return nil, nil, 0, ErrClosed
	}
	sp, ok := n.shards[space]
	if !ok {
		return nil, nil, 0, fmt.Errorf("tenure: unknown space %q", space)
	}
	shard, err := sp.shardOf(key)
	if err != nil {
		return nil, nil, 0, err
	}
	return r, sp, shard, nil
}

// shardOf returns key's shard in the space: ShardOf(key), which must lie in [0, Shards), or
// core.KeyShard(key, Shards) when ShardOf is nil.
func (sp *spaceShards) shardOf(key string) (int, error) {
	s := sp.space
	if s.ShardOf == nil {
		return core.KeyShard(key, s.Shards), nil
	}
	shard := s.ShardOf(key)
	if shard < 0 || shard >= s.Shards {
		return 0, fmt.Errorf("tenure: %s: ShardOf(%q) = %d is outside [0, %d)", s.Name, key, shard, s.Shards)
	}
	return shard, nil
}

// route runs Route's last two steps (RUN-400): the shard's placement key from the assignment, and
// the member whose ID it is or, for a slot key slot-<i>, the member whose ID is the ID part of
// the slot's holder, with Slot i. A failure wraps ErrNoMembers.
func (r *routes) route(space string, shard int) (Member, error) {
	var key string
	if a := r.assign[space]; a != nil {
		key = a[shard]
	}
	if key == "" {
		return Member{}, noMembers(space, shard, "no placement keys")
	}
	id, slot := key, -1
	if s, ok := r.slotOf(key); ok {
		switch h := r.view.SlotHolders[s]; {
		case key == r.heldKey:
			id = r.self
		case h == "":
			return Member{}, noMembers(space, shard, key+" is vacant")
		default:
			id = holderID(h)
		}
		slot = s
	}
	i, ok := r.byID[id]
	if !ok {
		return Member{}, noMembers(space, shard, id+" is not in the view")
	}
	m := r.view.Members[i]
	if slot >= 0 {
		m.Slot = slot
	}
	return m, nil
}

// slotOf returns the slot that key names, "slot-<i>" with i one of the view's slots, and false
// for a key that names none: every key without slots.
func (r *routes) slotOf(key string) (int, bool) {
	if r.view == nil || r.view.Slots == 0 {
		return 0, false
	}
	digits, ok := strings.CutPrefix(key, "slot-")
	if !ok {
		return 0, false
	}
	s, err := strconv.Atoi(digits)
	if err != nil || s < 0 || s >= r.view.Slots {
		return 0, false
	}
	return s, true
}

// noMembers returns "tenure: no members: <space>/<shard>: <detail>", wrapping ErrNoMembers.
func noMembers(space string, shard int, detail string) error {
	return fmt.Errorf("%w: %s/%s: %s", ErrNoMembers, space, strconv.Itoa(shard), detail)
}

// newRoutes returns the snapshot of view v, nil before the first view, with the assignments
// assign. byID is v's member index when the caller has it, else nil, and is then built.
func newRoutes(v *core.View, byID map[string]int, assign map[string][]string) *routes {
	if byID == nil {
		byID = make(map[string]int)
		if v != nil {
			for i, m := range v.Members {
				byID[m.ID] = i
			}
		}
	}
	return &routes{view: v, byID: byID, assign: assign, held: -1}
}

// closeRoutes publishes a closed snapshot, so that Route and Local return ErrClosed from now on.
func (n *Node) closeRoutes() {
	r := *n.snap.Load()
	r.closed = true
	n.snap.Store(&r)
}

// publishRoutes publishes a new routing snapshot after a step that changed the published view,
// the assignment of any space or the held slot (RUN-400). A recompute stores a new assignment
// slice (MCH-340), and so does assignments, so comparing slices by their first element's address
// finds every change, including the first step of a new machine. Only the loop calls it, under n.stepMu.
func (l *loop) publishRoutes() {
	n := l.n
	cur := n.snap.Load()
	if cur == nil {
		cur = &routes{} // a node made without Start, as the loop's internal tests make one
	}
	v := n.view.Load()
	held := l.status.Slot
	changed := cur.view != v || cur.held != held
	for _, sp := range n.cfg.Spaces {
		if !sameSlice(l.assignment(sp.Name), cur.assign[sp.Name]) {
			changed = true
			break
		}
	}
	if !changed {
		return
	}
	assign := make(map[string][]string, len(n.cfg.Spaces))
	for _, sp := range n.cfg.Spaces {
		assign[sp.Name] = l.assignment(sp.Name)
	}
	var byID map[string]int
	if cur.view == v {
		byID = cur.byID
	}
	r := newRoutes(v, byID, assign)
	r.self, r.held = n.cfg.Self.ID, held
	if held >= 0 {
		r.heldKey = "slot-" + strconv.Itoa(held)
	}
	n.snap.Store(r)
}

// assignment returns the assignment of space that routing follows: the one the loop computed for
// a view the machine did not apply, else the machine's.
func (l *loop) assignment(space string) []string {
	if l.assign != nil {
		return l.assign[space]
	}
	return l.m.Assignment(space)
}

// viewApplied reports whether the step rec, of a ViewChanged of v, applied v: the machine's
// session was neither closing nor closed, so it took views, and its view version moved to v's.
func viewApplied(rec stepRecord, v core.View) bool {
	switch rec.Before.Session {
	case core.SessionStateClosing, core.SessionStateClosed:
		return false
	}
	return rec.After.ViewVersion == v.Version
}

// assignments returns each space's assignment for view v at the wall-clock time wall, by space
// name, as the machine computes it when it applies v: the placement keys of v, then Balanced over
// them. It takes no slot as expired, since a machine that no longer applies views no longer marks
// slots.
func assignments(v core.View, wall time.Time, spaces []Space) map[string][]string {
	keys, _ := core.PlacementKeys(v, wall.Round(0), nil)
	out := make(map[string][]string, len(spaces))
	for _, sp := range spaces {
		a := make([]string, sp.Shards)
		for shard, k := range core.Balanced(sp.Name, sp.Shards, keys) {
			if k >= 0 {
				a[shard] = keys[k]
			}
		}
		out[sp.Name] = a
	}
	return out
}

// sameSlice reports whether a and b are the same slice: the same length and, unless empty, the
// same first element.
func sameSlice(a, b []string) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}
