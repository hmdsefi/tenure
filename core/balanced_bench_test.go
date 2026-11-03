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
			b.Run("n="+strconv.Itoa(n)+"/m="+strconv.Itoa(m), func(b *testing.B) {
				b.ReportAllocs()
				var movedJoin, toNew, movedLeave, fromLeft int
				for b.Loop() {
					movedJoin, toNew, movedLeave, fromLeft = balancedMovement(n, m)
				}
				b.ReportMetric(float64(movedJoin), "moved/join")
				b.ReportMetric(float64(toNew), "to-new/join")
				b.ReportMetric(float64(movedLeave), "moved/leave")
				b.ReportMetric(float64(fromLeft), "from-left/leave")
			})
		}
	}
}
