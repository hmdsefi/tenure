// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"hash/fnv"
	"strconv"
	"testing"
)

// refFNV returns the 64-bit FNV-1a hash of the concatenation of parts, from hash/fnv.
func refFNV(parts ...[]byte) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum64()
}

func TestATCOR08KeyShard(t *testing.T) {
	t.Run("internal", func(t *testing.T) {
		if got := fnv1a64(""); got != 0xcbf29ce484222325 {
			t.Errorf(`fnv1a64("") = %#x, want 0xcbf29ce484222325`, got)
		}
		if got := fmix64(fnv1a64("")); got != 0xefd01f60ba992926 {
			t.Errorf(`fmix64(fnv1a64("")) = %#x, want 0xefd01f60ba992926`, got)
		}
		for i := range 1000 {
			s := "k" + strconv.Itoa(i)
			if got, want := fnv1a64(s), refFNV([]byte(s)); got != want {
				t.Errorf("fnv1a64(%q) = %#x, want %#x", s, got, want)
			}
		}
		for b := range 256 {
			s := string([]byte{byte(b)})
			if got, want := fnv1a64(s), refFNV([]byte(s)); got != want {
				t.Errorf("fnv1a64(%q) = %#x, want %#x", s, got, want)
			}
		}
	})

	t.Run("values", func(t *testing.T) {
		tests := []struct {
			key  string
			n    int
			want int
		}{
			{"", 1, 0},
			{"", 64, 38},
			{"alice", 3, 2},
			{"alice", 64, 52},
			{"alice", 100, 36},
			{"bob", 64, 32},
			{"bob", 100, 52},
			{"market-42", 16, 9},
			{"market-42", 64, 57},
			{"0x7f3a", 8, 5},
		}
		for _, tt := range tests {
			if got := KeyShard(tt.key, tt.n); got != tt.want {
				t.Errorf("KeyShard(%q, %d) = %d, want %d", tt.key, tt.n, got, tt.want)
			}
		}
	})

	t.Run("panics", func(t *testing.T) {
		tests := []struct {
			n    int
			want string
		}{
			{0, "core: KeyShard: n is 0, want at least 1"},
			{-1, "core: KeyShard: n is -1, want at least 1"},
		}
		for _, tt := range tests {
			func() {
				defer func() {
					r := recover()
					err, ok := r.(error)
					if !ok {
						t.Errorf("KeyShard(%q, %d) panicked with %v (%T), want an error", "a", tt.n, r, r)
						return
					}
					if err.Error() != tt.want {
						t.Errorf("KeyShard(%q, %d) panicked with %q, want %q", "a", tt.n, err, tt.want)
					}
				}()
				KeyShard("a", tt.n)
			}()
		}
	})

	t.Run("doubling", func(t *testing.T) {
		for i := range 10000 {
			k := "k" + strconv.Itoa(i)
			for _, n := range []int{1, 3, 64, 1000} {
				s, d := KeyShard(k, n), KeyShard(k, 2*n)
				if d != s && d != s+n {
					t.Fatalf("KeyShard(%q, %d) = %d, KeyShard(%q, %d) = %d; want %d or %d", k, n, s, k, 2*n, d, s, s+n)
				}
			}
		}
	})
}

func TestATCOR09Score(t *testing.T) {
	tests := []struct {
		namespace, key, id string
		want               uint64
	}{
		// lockstep: Score("n1", "account", "alice")
		{"account", "alice", "n1", 0x15f291e4c62474b8},
		// lockstep: Score("n2", "account", "alice")
		{"account", "alice", "n2", 0x5e829a2c09af183a},
		// lockstep: Score("n3", "account", "alice")
		{"account", "alice", "n3", 0xcf88b1a7b3958a97},
		// lockstep: Score("n1", "account", "bob")
		{"account", "bob", "n1", 0x1b0dcc414ef9762a},
		// lockstep: Score("n2", "account", "bob")
		{"account", "bob", "n2", 0x27851a5466ea7100},
		// lockstep: Score("n3", "account", "bob")
		{"account", "bob", "n3", 0xfd113a94823d3b6e},
		// lockstep: Score("", "", "")
		{"", "", "", 0xdb01c3104e99566d},
	}
	for _, tt := range tests {
		if got := Score(tt.namespace, tt.key, tt.id); got != tt.want {
			t.Errorf("Score(%q, %q, %q) = %#x, want %#x", tt.namespace, tt.key, tt.id, got, tt.want)
		}
	}

	zero := []byte{0}
	for _, ns := range []string{"", "account", "counter"} {
		for i := range 100 {
			key := "k" + strconv.Itoa(i)
			for j := range 10 {
				id := "n" + strconv.Itoa(j)
				want := fmix64(refFNV([]byte(ns), zero, []byte(key), zero, []byte(id)))
				if got := Score(ns, key, id); got != want {
					t.Fatalf("Score(%q, %q, %q) = %#x, want %#x", ns, key, id, got, want)
				}
			}
		}
	}
}
