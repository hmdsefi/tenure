// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"strconv"
	"strings"
	"testing"
)

// benchKey returns a key of size bytes.
func benchKey(size int) string {
	return strings.Repeat("k", size-1) + "0"
}

func BenchmarkScore(b *testing.B) {
	for _, size := range []int{8, 64, 256} {
		key := benchKey(size)
		b.Run("len="+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			var sink uint64
			for b.Loop() {
				sink += Score("account", key, "n1")
			}
			_ = sink
		})
	}
}

func BenchmarkKeyShard(b *testing.B) {
	for _, size := range []int{8, 64, 256} {
		key := benchKey(size)
		b.Run("len="+strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			var sink int
			for b.Loop() {
				sink += KeyShard(key, 64)
			}
			_ = sink
		})
	}
}
