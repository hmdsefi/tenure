// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkEligible(b *testing.B) {
	const m = 1000
	v := View{Members: make([]Member, m)}
	for i := range v.Members {
		// Zero-padded IDs keep the members sorted by ID.
		v.Members[i] = Member{ID: "pod-" + strconv.Itoa(10000+i), Slot: -1}
		switch i % 4 {
		case 1:
			v.Members[i].Draining = true
		case 2:
			v.Members[i].JoinAt = t0.Add(-time.Hour)
			v.Members[i].LeaveAt = t0.Add(time.Hour)
		}
	}
	b.Run("m=1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got := Eligible(v, t0); len(got) == 0 {
				b.Fatal("no eligible members")
			}
		}
	})
}
