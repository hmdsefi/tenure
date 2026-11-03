// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"fmt"
	"strings"
	"time"
)

// Member is one process taking part in a group.
//
// Write Member values with keyed fields: later versions add fields.
type Member struct {
	// ID identifies the member across restarts, for example its pod name. It is non-empty
	// and contains neither '/' nor a zero byte.
	ID string

	// Addr is where callers reach the member, for example "10.0.0.7:8080". core does not
	// interpret it.
	Addr string

	// Meta holds string labels. core does not interpret or modify it; a Member copied by a
	// core function shares the map.
	Meta map[string]string

	// Draining reports that the member is handing its work over before it leaves.
	Draining bool

	// Slot is the slot the member holds in a view with slots, or -1 for none. In a view
	// without slots it has no meaning. The runtime sets it when it merges slot data into a
	// view.
	Slot int

	// JoinAt and LeaveAt bound, in wall-clock time, when the member takes part in
	// placement: from JoinAt, inclusive, to LeaveAt, exclusive. A zero time is no bound.
	JoinAt  time.Time
	LeaveAt time.Time
}

// Validate reports whether m can take part in a group: ID is non-empty and contains
// neither '/' nor a zero byte, Slot is -1 or greater, and LeaveAt is after JoinAt when both
// are set. The error wraps ErrInvalidMember.
func (m Member) Validate() error {
	detail, named := m.check()
	switch {
	case detail == "":
		return nil
	case named:
		return fmt.Errorf("%w %q: %s", ErrInvalidMember, m.ID, detail)
	default:
		return fmt.Errorf("%w: %s", ErrInvalidMember, detail)
	}
}

// check runs COR-010's checks in order. It returns the detail of the first failure, or ""
// when m is valid, and whether the error message names the member's ID before the detail
// (the detail of an ID failure already contains the ID).
func (m Member) check() (detail string, named bool) {
	switch {
	case m.ID == "":
		return "empty ID", false
	case strings.IndexByte(m.ID, '/') >= 0:
		return fmt.Sprintf("ID %q contains '/'", m.ID), false
	case strings.IndexByte(m.ID, 0) >= 0:
		return fmt.Sprintf("ID %q contains a zero byte", m.ID), false
	case m.Slot < -1:
		return fmt.Sprintf("Slot %d is below -1", m.Slot), true
	case !m.JoinAt.IsZero() && !m.LeaveAt.IsZero() && !m.LeaveAt.After(m.JoinAt):
		return "LeaveAt is not after JoinAt", true
	}
	return "", false
}

// View is a membership snapshot as one member sees it.
type View struct {
	// Version increases with every new snapshot on the same member. core functions other
	// than the ownership machine ignore it.
	Version uint64

	// Members holds the members sorted by ID, with distinct IDs. Callers must not modify
	// it.
	Members []Member

	// Slots is the group's slot count; 0 means the group has no slots.
	Slots int

	// SlotHolders has one entry per slot: the holder string of the slot's holder, or ""
	// when the slot is vacant.
	SlotHolders []string
}

// Validate reports whether v is well formed: every member is valid, members are sorted by
// ID with distinct IDs, Slots is not negative, SlotHolders has Slots entries, and in a view
// with slots every member's Slot is -1 or a held slot that no other member claims. The
// error wraps ErrInvalidView.
func (v View) Validate() error {
	for i, m := range v.Members {
		if detail, _ := m.check(); detail != "" {
			return fmt.Errorf("%w: Members[%d]: %s", ErrInvalidView, i, detail)
		}
	}
	for i := 1; i < len(v.Members); i++ {
		if v.Members[i].ID <= v.Members[i-1].ID {
			return fmt.Errorf("%w: Members[%d]: ID %q does not sort after %q",
				ErrInvalidView, i, v.Members[i].ID, v.Members[i-1].ID)
		}
	}
	if v.Slots < 0 {
		return fmt.Errorf("%w: Slots %d is negative", ErrInvalidView, v.Slots)
	}
	if len(v.SlotHolders) != v.Slots {
		return fmt.Errorf("%w: %d SlotHolders for %d slots", ErrInvalidView, len(v.SlotHolders), v.Slots)
	}
	if v.Slots == 0 {
		return nil
	}
	// claimedBy[s] is 1 + the index of the first member holding slot s, 0 if none.
	claimedBy := make([]int, v.Slots)
	for i, m := range v.Members {
		switch {
		case m.Slot >= v.Slots:
			return fmt.Errorf("%w: Members[%d]: Slot %d out of range for %d slots",
				ErrInvalidView, i, m.Slot, v.Slots)
		case m.Slot < 0:
			continue
		case v.SlotHolders[m.Slot] == "":
			return fmt.Errorf("%w: Members[%d]: Slot %d is vacant", ErrInvalidView, i, m.Slot)
		case claimedBy[m.Slot] != 0:
			return fmt.Errorf("%w: Members[%d]: Slot %d already held by %q",
				ErrInvalidView, i, m.Slot, v.Members[claimedBy[m.Slot]-1].ID)
		}
		claimedBy[m.Slot] = i + 1
	}
	return nil
}

// Eligible returns the members of v that may be given shards or keys at the wall-clock
// time at: members that are not draining, whose [JoinAt, LeaveAt) window contains at (a
// zero bound is open), and that are not standbys (members without a slot in a view with
// slots). The result is a new slice in ID order, nil when no member is eligible.
func Eligible(v View, at time.Time) []Member {
	n := 0
	for i := range v.Members {
		if eligible(&v.Members[i], v.Slots, at) {
			n++
		}
	}
	if n == 0 {
		return nil
	}
	out := make([]Member, 0, n)
	for i := range v.Members {
		if eligible(&v.Members[i], v.Slots, at) {
			out = append(out, v.Members[i])
		}
	}
	return out
}

// eligible reports whether m meets COR-040's four conditions at time at.
func eligible(m *Member, slots int, at time.Time) bool {
	return !m.Draining &&
		(m.JoinAt.IsZero() || !at.Before(m.JoinAt)) &&
		(m.LeaveAt.IsZero() || at.Before(m.LeaveAt)) &&
		(slots == 0 || m.Slot >= 0)
}
