// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import "testing"

func BenchmarkTreeKey(b *testing.B) {
	tree := Tree{Name: "tq", Read: 4096, Write: 4096, Degree: 20}
	for _, c := range []struct {
		name string
		i    int
	}{{"i=0", 0}, {"i=4095", 4095}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tree.Key(c.i)
			}
		})
	}
}

func BenchmarkTreeChildren(b *testing.B) {
	tree := Tree{Name: "tq", Read: 4096, Write: 4096, Degree: 20}
	b.ReportAllocs()
	for b.Loop() {
		tree.Children(0)
	}
}

func BenchmarkTreePick(b *testing.B) {
	tree := Tree{Name: "tq", Read: 4096, Write: 1000, Degree: 20}
	b.ReportAllocs()
	var r uint64
	for b.Loop() {
		r += 0x9e3779b97f4a7c15
		tree.Pick(r)
	}
}
