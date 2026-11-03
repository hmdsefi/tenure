// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"strconv"
	"testing"
)

// benchMembers returns m members with IDs n0 … n(m-1).
func benchMembers(m int) []Member {
	ms := make([]Member, m)
	for i := range ms {
		ms[i] = Member{ID: "n" + strconv.Itoa(i), Slot: -1}
	}
	return ms
}

func BenchmarkRank(b *testing.B) {
	for _, m := range []int{3, 10, 100, 1000} {
		ms := benchMembers(m)
		for _, n := range []int{1, 3} {
			b.Run("m="+strconv.Itoa(m)+"/n="+strconv.Itoa(n), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if got := Rank("account", "k1", ms, n); len(got) != n {
						b.Fatalf("Rank returned %d members, want %d", len(got), n)
					}
				}
			})
		}
	}
}

func BenchmarkRendezvous(b *testing.B) {
	for _, m := range []int{3, 10, 100, 1000} {
		p := NewRendezvous(benchMembers(m))
		b.Run("m="+strconv.Itoa(m), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if got := p.Rank("account", "k1", 1); len(got) != 1 {
					b.Fatalf("Rank returned %d members, want 1", len(got))
				}
			}
		})
	}
}
