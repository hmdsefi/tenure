// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import "testing"

var (
	benchTreeStr  string
	benchTreeInts []int
)

func BenchmarkTreeKey(b *testing.B) {
	tree := Tree{Name: "tq", Read: 4096, Write: 4096, Degree: 20}
	for _, c := range []struct {
		name string
		i    int
	}{{"i=0", 0}, {"i=4095", 4095}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchTreeStr = tree.Key(c.i)
			}
		})
	}
}

func BenchmarkTreeChildren(b *testing.B) {
	tree := Tree{Name: "tq", Read: 4096, Write: 4096, Degree: 20}
	b.ReportAllocs()
	for b.Loop() {
		benchTreeInts = tree.Children(0)
	}
}
