// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"testing"
)

// TestSessionEnds runs SIM-600's session-end row under each rule.
func TestSessionEnds(t *testing.T) {
	runRow(t, rowNamed(t, "TestSessionEnds"), nil)
}
