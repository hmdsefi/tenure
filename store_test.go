// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestHeldErrorMessage(t *testing.T) {
	const h = "m1/00000000000000000000000000000001"
	var err error = &HeldError{Holder: h, RetryAfter: 2 * time.Second}
	if got, want := err.Error(), "tenure: held by "+h+", retry after 2s"; got != want {
		t.Fatalf("HeldError.Error() = %q, want %q", got, want)
	}

	wrapped := fmt.Errorf("acquire a/3: %w", err)
	var held *HeldError
	if !errors.As(wrapped, &held) {
		t.Fatalf("errors.As(%q, *HeldError) = false, want true", wrapped)
	}
	if held != err {
		t.Fatalf("errors.As found %p, want %p", held, err)
	}
}

func TestStoreErrorValues(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrNoSlot", ErrNoSlot, "tenure: no free slot"},
		{"ErrSessionLost", ErrSessionLost, "tenure: session lost"},
	}
	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("%s.Error() = %q, want %q", tt.name, got, tt.want)
		}
	}
	if errors.Is(ErrNoSlot, ErrSessionLost) || errors.Is(ErrSessionLost, ErrNoSlot) {
		t.Errorf("ErrNoSlot and ErrSessionLost match each other, want distinct values")
	}
}
