// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"math"
	"testing"
	"time"
)

func TestATCOR04NextTerm(t *testing.T) {
	tests := []struct {
		name string
		prev Term
		wall time.Time
		want Term
	}{
		{"wall wins over zero", 0, time.UnixMilli(1_700_000_000_000), 1_700_000_000_000},
		{"prev+1 wins over wall", 1_800_000_000_000, time.UnixMilli(1_700_000_000_000), 1_800_000_000_001},
		{"epoch counts as 0", 41, time.Unix(0, 0), 42},
		{"zero time counts as 0", 0, time.Time{}, 1},
		{"equal prev and wall", 5, time.UnixMilli(5), 6},
		{"wall above prev+1", 5, time.UnixMilli(7), 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NextTerm(tt.prev, tt.wall); got != tt.want {
				t.Fatalf("NextTerm(%d, %v) = %d, want %d", tt.prev, tt.wall, got, tt.want)
			}
		})
	}

	t.Run("overflow panics", func(t *testing.T) {
		defer func() {
			r := recover()
			err, ok := r.(error)
			if !ok {
				t.Fatalf("panic value = %#v, want an error", r)
			}
			if got, want := err.Error(), "core: NextTerm: term overflow"; got != want {
				t.Fatalf("panic text = %q, want %q", got, want)
			}
		}()
		NextTerm(math.MaxUint64, t0)
		t.Fatal("NextTerm did not panic")
	})
}

// COR-050: a time before 1970 counts as 0 and never lowers the result.
func TestNextTermBeforeEpoch(t *testing.T) {
	if got, want := NextTerm(7, time.UnixMilli(-1)), Term(8); got != want {
		t.Fatalf("NextTerm(7, 1ms before epoch) = %d, want %d", got, want)
	}
	if got := NextTerm(0, time.UnixMilli(-1_000_000)); got != 1 {
		t.Fatalf("NextTerm(0, before epoch) = %d, want 1", got)
	}
}

// COR-050: the largest prev that does not overflow returns the largest term.
func TestNextTermLargest(t *testing.T) {
	if got, want := NextTerm(math.MaxUint64-1, t0), Term(math.MaxUint64); got != want {
		t.Fatalf("NextTerm(MaxUint64-1, t0) = %d, want %d", got, want)
	}
}
