// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import "errors"

// Errors wrapped by the Validate methods. The text of each returned error starts with the
// text of the error it wraps.
var (
	ErrInvalidMember = errors.New("core: invalid member")
	ErrInvalidView   = errors.New("core: invalid view")
	ErrInvalidTiming = errors.New("core: invalid timing")
)
