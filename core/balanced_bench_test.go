// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"strconv"
	"testing"
)

func BenchmarkBalanced(b *testing.B) {
	for _, n := range []int{64, 1024, 4096, 16384, 65536} {
		for _, m := range []int{1, 3, 10, 100, 1000} {
			if n*m > 1<<24 {
				continue // outside ASG-100's measured envelope
			}
			keys := seqKeys("m", m)
			b.Run("n="+strconv.Itoa(n)+"/m="+strconv.Itoa(m), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if got := Balanced("bench", n, keys); len(got) != n {
						b.Fatalf("Balanced returned %d shards, want %d", len(got), n)
					}
				}
			})
		}
	}
}

func BenchmarkBalancedMovement(b *testing.B) {
	for _, n := range []int{64, 1024, 4096} {
		for _, m := range []int{3, 10, 100} {
			keys := seqKeys("m", m+1)
			b.Run("n="+strconv.Itoa(n)+"/m="+strconv.Itoa(m), func(b *testing.B) {
				b.ReportAllocs()
				var base, joined, left []int
				for b.Loop() {
					base, joined, left = balancedMovementRuns(n, m, keys)
				}
				movedJoin, toNew, movedLeave, fromLeft := balancedMovement(m, keys, base, joined, left)
				b.ReportMetric(float64(movedJoin), "moved/join")
				b.ReportMetric(float64(toNew), "to-new/join")
				b.ReportMetric(float64(movedLeave), "moved/leave")
				b.ReportMetric(float64(fromLeft), "from-left/leave")
			})
		}
	}
}

func BenchmarkPlacementKeys(b *testing.B) {
	const m = 1000
	b.Run("m=1000", func(b *testing.B) {
		v := View{Members: make([]Member, m)}
		for i := range v.Members {
			// Zero-padded IDs keep the members sorted by ID.
			v.Members[i] = Member{ID: "pod-" + strconv.Itoa(10000+i), Slot: -1, Draining: i%4 == 1}
		}
		b.ReportAllocs()
		for b.Loop() {
			if keys, _ := PlacementKeys(v, t0, nil); len(keys) == 0 {
				b.Fatal("no placement keys")
			}
		}
	})
	b.Run("slots=1000", func(b *testing.B) {
		v := View{Slots: m, SlotHolders: make([]string, m)}
		expired := make([]bool, m)
		for i := range v.SlotHolders {
			if i%2 == 0 {
				v.SlotHolders[i] = "pod-" + strconv.Itoa(10000+i) + "/1"
			} else {
				expired[i] = i%4 == 1
			}
		}
		b.ReportAllocs()
		for b.Loop() {
			if keys, _ := PlacementKeys(v, t0, expired); len(keys) == 0 {
				b.Fatal("no placement keys")
			}
		}
	})
}
