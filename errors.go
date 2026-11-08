// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"strconv"
)

var (
	// ErrNoSlot means the store has no free slot for the holder.
	ErrNoSlot = errors.New("tenure: no free slot")

	// ErrSessionLost means the holder's session has ended for good.
	ErrSessionLost = errors.New("tenure: session lost")

	// ErrClosed means the value has been closed, for example a Membership after Leave, and
	// takes no further calls.
	ErrClosed = errors.New("tenure: closed")

	// ErrNotOwner means this member does not own the shard. Every error of a Shard that is no
	// longer owned, and every *NotOwnerError, matches it with errors.Is.
	ErrNotOwner = errors.New("tenure: not owner")

	// ErrNoMembers means no member can serve the key, for example because no member of the
	// group is eligible.
	ErrNoMembers = errors.New("tenure: no members")

	// ErrDraining means the node is draining, or has drained, and refuses the call.
	ErrDraining = errors.New("tenure: draining")
)

// NotOwnerError reports that this member does not own a shard, and names the member a request
// for the shard should go to instead.
type NotOwnerError struct {
	// Space and Shard name the shard.
	Space string
	Shard int

	// Route is the member that serves the shard, or the zero Member when none is known.
	Route Member
}

// Error returns text such as "tenure: not owner of orders/3: route to n2 (10.0.0.7:8080)". The
// address is left out when Route has none, and the text ends in "no route" when Route has no ID.
func (e *NotOwnerError) Error() string {
	msg := "tenure: not owner of " + e.Space + "/" + strconv.Itoa(e.Shard) + ": "
	switch {
	case e.Route.ID == "":
		return msg + "no route"
	case e.Route.Addr == "":
		return msg + "route to " + e.Route.ID
	}
	return msg + "route to " + e.Route.ID + " (" + e.Route.Addr + ")"
}

// Is reports whether target is ErrNotOwner, so that errors.Is(err, ErrNotOwner) holds for a
// *NotOwnerError.
func (e *NotOwnerError) Is(target error) bool {
	return target == ErrNotOwner
}
