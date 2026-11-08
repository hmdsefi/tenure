// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"testing"
)

func TestErrClosedValue(t *testing.T) {
	if got, want := ErrClosed.Error(), "tenure: closed"; got != want {
		t.Fatalf("ErrClosed.Error() = %q, want %q", got, want)
	}
	for _, other := range []error{ErrNoSlot, ErrSessionLost} {
		if errors.Is(ErrClosed, other) || errors.Is(other, ErrClosed) {
			t.Errorf("errors.Is(ErrClosed, %q) = true, want distinct values", other)
		}
	}
}
