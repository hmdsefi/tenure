// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
	mc "github.com/hmdsefi/tenure/internal/machinecases"
)

// benchShards and benchKeys are the sizes the machine's benchmarks cover: a shard count from one
// small space to one large one, and the members a view gives those shards to.
var (
	benchShards = []int{64, 1024, 4096}
	benchKeys   = []int{3, 10, 100}
)

// benchMachine returns a machine of member m1 whose n shards, in one space, have all gone through
// Boot, so every step walks owned shards. It returns the reading of its last step.
func benchMachine(b *testing.B, n int) (*core.Machine, time.Duration) {
	b.Helper()
	m := core.NewMachine(mc.C(mc.S("s", n)))
	var now time.Duration
	for _, s := range mc.Boot(1, mc.S("s", n)) {
		m.Step(s.Input)
		now = m.Status().Now
	}
	return m, now
}

func BenchmarkMachineTick(b *testing.B) {
	for _, n := range benchShards {
		b.Run("shards="+strconv.Itoa(n), func(b *testing.B) {
			m, now := benchMachine(b, n)
			before := m.Assignment("s")
			tick := core.Tick{Now: now}
			b.ReportAllocs()
			for b.Loop() {
				if got := m.Step(tick); len(got) != 1 {
					b.Fatalf("Step(Tick) = %v, want only a Wake", got)
				}
			}
			b.StopTimer()
			if &m.Assignment("s")[0] != &before[0] {
				b.Fatal("a plain Tick recomputed the assignment")
			}
		})
	}
}

func BenchmarkMachineViewChanged(b *testing.B) {
	for _, n := range benchShards {
		for _, keys := range benchKeys {
			b.Run("shards="+strconv.Itoa(n)+"/keys="+strconv.Itoa(keys), func(b *testing.B) {
				m, now := benchMachine(b, n)
				ids := make([]string, keys)
				ids[0] = "m1"
				for i := 1; i < keys; i++ {
					ids[i] = "m" + strconv.Itoa(1000+i)
				}
				in := core.ViewChanged{Now: now, View: mc.Vm(2, ids...), Wall: mc.Wall(now)}
				before := m.Assignment("s")
				b.ReportAllocs()
				for b.Loop() {
					// The same version applies and recomputes every time.
					m.Step(in)
				}
				b.StopTimer()
				if &m.Assignment("s")[0] == &before[0] {
					b.Fatal("the view did not recompute the assignment")
				}
			})
		}
	}
}

func BenchmarkMachineStatus(b *testing.B) {
	for _, n := range benchShards {
		b.Run("shards="+strconv.Itoa(n), func(b *testing.B) {
			m, _ := benchMachine(b, n)
			b.ReportAllocs()
			for b.Loop() {
				if st := m.Status(); len(st.Spaces[0].Shards) != n {
					b.Fatalf("Status has %d shards, want %d", len(st.Spaces[0].Shards), n)
				}
			}
		})
	}
}
