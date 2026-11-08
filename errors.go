// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import "errors"

var (
	// ErrNoSlot means the store has no free slot for the holder.
	ErrNoSlot = errors.New("tenure: no free slot")

	// ErrSessionLost means the holder's session has ended for good.
	ErrSessionLost = errors.New("tenure: session lost")
)
