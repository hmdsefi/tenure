// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !race

// The matrix tests run one goroutine over many inputs; the race detector only slows them.

package core

import (
	"slices"
	"strconv"
	"testing"
)

// scoreClosure is the Score closure of ASG-090 for space.
func scoreClosure(space string) func(s int, k string) uint64 {
	return func(s int, k string) uint64 { return Score(space, strconv.Itoa(s), k) }
}

func TestATASG04Properties(t *testing.T) {
	propertyMatrix(func(space string, n int, keys []string) {
		got := Balanced(space, n, keys)
		if len(got) != n || slices.Contains(got, -1) {
			t.Errorf("Balanced(%q, %d, m0 … m%d) = %v, want %d assigned shards", space, n, len(keys)-1, got, n)
			return
		}
		m := len(keys)
		q, r := n/m, n%m
		full := 0
		for i, c := range shardCounts(got, m) {
			switch c {
			case q + 1:
				full++
			case q:
			default:
				t.Errorf("Balanced(%q, %d, m0 … m%d): %s holds %d shards, want %d or %d", space, n, m-1, keys[i], c, q, q+1)
			}
		}
		if full != r {
			t.Errorf("Balanced(%q, %d, m0 … m%d): %d keys hold %d shards, want %d", space, n, m-1, full, q+1, r)
		}
	})
}

func TestATASG06Seam(t *testing.T) {
	propertyMatrix(func(space string, n int, keys []string) {
		if got, want := Balanced(space, n, keys), balancedScored(n, keys, scoreClosure(space)); !slices.Equal(got, want) {
			t.Errorf("Balanced(%q, %d, m0 … m%d) = %v, balancedScored with Score = %v", space, n, len(keys)-1, got, want)
		}
	})
}

func TestATASG09Differential(t *testing.T) {
	check := func(space string, n int, keys []string) {
		t.Helper()
		if got, want := Balanced(space, n, keys), balancedReference(space, n, keys); !slices.Equal(got, want) {
			t.Errorf("Balanced(%q, %d, %v) = %v, balancedReference = %v", space, n, keys, got, want)
		}
	}
	propertyMatrix(check)
	for sp := range 20 {
		for _, n := range []int{1, 16, 1024} {
			for _, m := range []int{1, 2, 3, 5, 10, 33, 100} {
				check("s"+strconv.Itoa(sp), n, seqKeys("slot-", m))
			}
		}
	}
	for _, v := range []struct {
		space string
		n     int
		keys  []string
	}{
		{"orderbook", 8, []string{"a", "b", "c"}},
		{"orderbook", 8, []string{"c", "a", "b"}},
		{"orderbook", 10, []string{"m1", "m2", "m3"}},
		{"s", 2, []string{"a", "b", "c"}},
		{"orderbook", 16, seqKeys("slot-", 4)},
		{"orderbook", 6, []string{"a", "b", "c"}},
		{"orderbook", 7, []string{"a", "b", "c"}},
		{"wallets", 5, []string{"a", "b", "c"}},
		{"bench", 1024, seqKeys("m", 10)},
		{"orderbook", 64, []string{"pod-0", "pod-1", "pod-2"}},
	} {
		check(v.space, v.n, v.keys)
	}
}
