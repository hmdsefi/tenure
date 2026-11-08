// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import "errors"

var (
	// ErrNoSlot means the store has no free slot for the holder.
	ErrNoSlot = errors.New("tenure: no free slot")

	// ErrSessionLost means the holder's session has ended for good.
	ErrSessionLost = errors.New("tenure: session lost")

	// ErrClosed means the value has been closed, for example a Membership after Leave, and
	// takes no further calls.
	ErrClosed = errors.New("tenure: closed")
)
