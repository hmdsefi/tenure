// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// Membership tells a member which members its group has. A member announces itself with Join,
// follows the group with Watch, marks itself as draining with SetDraining and leaves with Leave.
//
// Every method returns ctx.Err() itself when ctx is done, and then changes nothing. After Leave,
// Join, SetDraining and Watch return ErrClosed.
type Membership interface {
	// Join adds self to the group, or replaces the member with the same ID. It returns an error
	// that wraps core.ErrInvalidMember when self is not valid, and an error when the Membership
	// has joined already. The Membership keeps its own copy of self.Meta: no view shares the map
	// passed in.
	Join(ctx context.Context, self core.Member) error

	// Watch returns a channel that holds the current view and receives each later one. A view
	// lists the members sorted by ID. A new view, with the next Version, comes only when member
	// data changes: a change of Slot alone, or a Meta with the same content, makes none. The
	// channel holds one view, and a newer view replaces one not yet received. The channel is
	// closed when ctx is done and when Leave is called.
	Watch(ctx context.Context) (<-chan core.View, error)

	// SetDraining sets Draining of the member that joined. A draining member stays in the views.
	// Setting the value it already has makes no new view.
	SetDraining(ctx context.Context, draining bool) error

	// Leave removes the member that joined from the group, delivers the view without it, and
	// then closes every channel that Watch returned.
	Leave(ctx context.Context) error
}

// PropagationEstimator is implemented by a Membership that can tell how long a change takes to
// reach every member.
type PropagationEstimator interface {
	// Propagation returns how long a change of one member takes to appear in the views of every
	// member; 0 means at once.
	Propagation() time.Duration
}
