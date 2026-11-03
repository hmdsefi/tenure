// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"hash/fnv"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// balancedReference is the sort-based algorithm of ASG-020 to ASG-040, kept verbatim and never
// changed (ASG-120): every (shard, key) pair is scored with Score(space, strconv.Itoa(shard),
// key), the pairs are sorted by score descending, key ascending and shard ascending, and they
// are taken greedily while the shard is unassigned and the key is under its cap. It assumes
// n >= 1 and distinct keys.
func balancedReference(space string, n int, keys []string) []int {
	out := make([]int, n)
	for s := range out {
		out[s] = -1
	}
	if len(keys) == 0 {
		return out
	}
	type pair struct {
		score uint64
		key   string
		index int // position of key in keys
		shard int
	}
	var pairs []pair
	for s := range n {
		for i, k := range keys {
			pairs = append(pairs, pair{score: Score(space, strconv.Itoa(s), k), key: k, index: i, shard: s})
		}
	}
	slices.SortStableFunc(pairs, func(a, b pair) int {
		if c := cmp.Compare(b.score, a.score); c != 0 {
			return c
		}
		if c := strings.Compare(a.key, b.key); c != 0 {
			return c
		}
		return cmp.Compare(a.shard, b.shard)
	})
	m := len(keys)
	q, r := n/m, n%m
	counts := make([]int, m)
	full := 0
	for _, p := range pairs {
		if out[p.shard] != -1 {
			continue
		}
		limit := q
		if full < r {
			limit = q + 1
		}
		if counts[p.index] >= limit {
			continue
		}
		out[p.shard] = p.index
		counts[p.index]++
		if counts[p.index] == q+1 {
			full++
		}
	}
	return out
}

// seqKeys returns prefix0 … prefix(m-1).
func seqKeys(prefix string, m int) []string {
	keys := make([]string, m)
	for i := range keys {
		keys[i] = prefix + strconv.Itoa(i)
	}
	return keys
}

// balancedDigest is the digest of AT-ASG-02: the 64-bit FNV-1a hash of the entries of r in
// decimal joined by ",".
func balancedDigest(r []int) uint64 {
	parts := make([]string, len(r))
	for i, v := range r {
		parts[i] = strconv.Itoa(v)
	}
	h := fnv.New64a()
	h.Write([]byte(strings.Join(parts, ",")))
	return h.Sum64()
}

// shardCounts returns how many shards of r each index in [0, m) holds.
func shardCounts(r []int, m int) []int {
	counts := make([]int, m)
	for _, i := range r {
		counts[i]++
	}
	return counts
}

// propertyMatrix calls f for every space, n and M of AT-ASG-04's matrix, with keys m0 … m(M-1).
func propertyMatrix(f func(space string, n int, keys []string)) {
	for sp := range 20 {
		space := "s" + strconv.Itoa(sp)
		for _, n := range []int{1, 2, 3, 7, 64, 100, 1000} {
			for _, m := range []int{1, 2, 3, 5, 10, 33, 100} {
				f(space, n, seqKeys("m", m))
			}
		}
	}
}

func TestATASG01Shapes(t *testing.T) {
	tests := []struct {
		space string
		n     int
		keys  []string
		want  []int  // nil: check in instead
		in    []int  // with want nil and panic empty: every entry is one of these
		panic string // the panic text, or empty
	}{
		{space: "orderbook", n: 1, keys: []string{"a"}, want: []int{0}},
		{space: "orderbook", n: 3, keys: nil, want: []int{-1, -1, -1}},
		{space: "orderbook", n: 5, keys: []string{"a"}, want: []int{0, 0, 0, 0, 0}},
		{space: "x", n: 4, keys: []string{"", "a"}, in: []int{0, 1}},
		{space: "orderbook", n: 0, keys: []string{"a"}, panic: "core: Balanced: n is 0, want at least 1"},
		{space: "x", n: -1, keys: []string{"a"}, panic: "core: Balanced: n is -1, want at least 1"},
		{space: "x", n: 4, keys: []string{"b", "a", "b", "a"}, panic: `core: Balanced: duplicate key "a"`},
		{space: "x", n: 0, keys: []string{"a", "a"}, panic: "core: Balanced: n is 0, want at least 1"},
	}
	for _, tt := range tests {
		name := "Balanced(" + strconv.Quote(tt.space) + ", " + strconv.Itoa(tt.n) + ", " + strings.Join(tt.keys, ",") + ")"
		before := slices.Clone(tt.keys)
		if tt.panic != "" {
			if got := treePanicText(t, func() { Balanced(tt.space, tt.n, tt.keys) }); got != tt.panic {
				t.Errorf("%s panics with %q, want %q", name, got, tt.panic)
			}
		} else {
			got := Balanced(tt.space, tt.n, tt.keys)
			if len(got) != tt.n {
				t.Errorf("%s has length %d, want %d", name, len(got), tt.n)
			}
			if tt.want != nil && !slices.Equal(got, tt.want) {
				t.Errorf("%s = %v, want %v", name, got, tt.want)
			}
			for _, v := range got {
				if tt.in != nil && !slices.Contains(tt.in, v) {
					t.Errorf("%s = %v, want entries in %v", name, got, tt.in)
					break
				}
			}
		}
		if !slices.Equal(tt.keys, before) {
			t.Errorf("%s changed keys from %q to %q", name, before, tt.keys)
		}
	}
}

func TestATASG02Golden(t *testing.T) {
	tests := []struct {
		space    string
		n        int
		keys     []string
		want     []int
		uncapped []int // first-choice rendezvous, for the vectors that exercise the cap
	}{
		{"orderbook", 8, []string{"a", "b", "c"}, []int{0, 1, 2, 1, 0, 1, 0, 2}, nil},
		{"orderbook", 8, []string{"c", "a", "b"}, []int{1, 2, 0, 2, 1, 2, 1, 0}, nil},
		{"orderbook", 10, []string{"m1", "m2", "m3"}, []int{0, 0, 2, 2, 0, 1, 2, 1, 2, 1}, nil},
		{"s", 2, []string{"a", "b", "c"}, []int{1, 0}, nil},
		{"orderbook", 16, []string{"slot-0", "slot-1", "slot-2", "slot-3"}, []int{2, 3, 3, 1, 1, 3, 0, 2, 3, 2, 0, 2, 1, 0, 1, 0}, nil},
		{"orderbook", 6, []string{"a", "b", "c"}, []int{0, 1, 2, 1, 0, 2}, []int{0, 1, 2, 1, 0, 1}},
		{"orderbook", 7, []string{"a", "b", "c"}, []int{0, 1, 2, 1, 0, 1, 2}, []int{0, 1, 2, 1, 0, 1, 0}},
		{"wallets", 5, []string{"a", "b", "c"}, []int{2, 0, 1, 2, 0}, []int{2, 0, 2, 2, 2}},
	}
	for _, tt := range tests {
		name := "Balanced(" + strconv.Quote(tt.space) + ", " + strconv.Itoa(tt.n) + ", " + strings.Join(tt.keys, ",") + ")"
		if got := Balanced(tt.space, tt.n, tt.keys); !slices.Equal(got, tt.want) {
			t.Errorf("%s = %v, want %v", name, got, tt.want)
		}
		if tt.uncapped == nil {
			continue
		}
		first := make([]int, tt.n)
		for s := range first {
			id := Rank(tt.space, strconv.Itoa(s), members(tt.keys...), 1)[0].ID
			first[s] = slices.Index(tt.keys, id)
		}
		if !slices.Equal(first, tt.uncapped) {
			t.Errorf("first choices of %s = %v, want %v", name, first, tt.uncapped)
		}
	}

	digests := []struct {
		space  string
		n      int
		keys   []string
		digest uint64
		counts []int
	}{
		{"bench", 1024, seqKeys("m", 10), 0xcc81f9c881db10a3, []int{103, 103, 102, 102, 102, 102, 102, 102, 103, 103}},
		{"orderbook", 64, []string{"pod-0", "pod-1", "pod-2"}, 0xc5af605cefae4619, []int{21, 22, 21}},
	}
	for _, tt := range digests {
		got := Balanced(tt.space, tt.n, tt.keys)
		if d := balancedDigest(got); d != tt.digest {
			t.Errorf("digest of Balanced(%q, %d, %v) = %#016x, want %#016x", tt.space, tt.n, tt.keys, d, tt.digest)
		}
		if c := shardCounts(got, len(tt.keys)); !slices.Equal(c, tt.counts) {
			t.Errorf("counts of Balanced(%q, %d, %v) = %v, want %v", tt.space, tt.n, tt.keys, c, tt.counts)
		}
	}
}

// TestBalancedScore checks the allocation-free score function of Balanced against
// Score(space, strconv.Itoa(s), key) (ASG-090), with shards in an order that changes the
// shard between calls and repeats it.
func TestBalancedScore(t *testing.T) {
	shards := []int{0, 9, 10, 99, 100, 12345, math.MaxInt32, -1, -100, 0, 0, 12345, 9}
	keys := []string{"", "a", "m0", "slot-3", "x\x00y", "pod-0"}
	for _, space := range []string{"", "orderbook", "bench", "a\x00b"} {
		score := balancedScore(space)
		for _, s := range shards {
			for _, key := range keys {
				if got, want := score(s, key), Score(space, strconv.Itoa(s), key); got != want {
					t.Errorf("balancedScore(%q)(%d, %q) = %#016x, want Score = %#016x", space, s, key, got, want)
				}
			}
		}
		for i := range shards { // the shard changes on every call
			s, key := shards[i], keys[i%len(keys)]
			if got, want := score(s, key), Score(space, strconv.Itoa(s), key); got != want {
				t.Errorf("balancedScore(%q)(%d, %q) after shard %d = %#016x, want Score = %#016x", space, s, key, shards[max(i-1, 0)], got, want)
			}
		}
	}
}

func TestATASG03Ties(t *testing.T) {
	seven := func(int, string) uint64 { return 7 }
	byShard := func(s int, _ string) uint64 { return uint64(100 - s) } //nolint:gosec // s is below 100
	tests := []struct {
		name  string
		n     int
		keys  []string
		score func(int, string) uint64
		want  []int
	}{
		{"all 7, c a b", 5, []string{"c", "a", "b"}, seven, []int{1, 1, 2, 2, 0}},
		{"all 7, b a", 3, []string{"b", "a"}, seven, []int{1, 1, 0}},
		{"100-s, c a b", 5, []string{"c", "a", "b"}, byShard, []int{1, 1, 2, 2, 0}},
	}
	for _, tt := range tests {
		if got := balancedScored(tt.n, tt.keys, tt.score); !slices.Equal(got, tt.want) {
			t.Errorf("%s: balancedScored(%d, %q) = %v, want %v", tt.name, tt.n, tt.keys, got, tt.want)
		}
	}
}

func TestATASG05KeyOrder(t *testing.T) {
	permute := map[string]func([]string) []string{
		"reverse": func(k []string) []string { p := slices.Clone(k); slices.Reverse(p); return p },
		"rotate 1": func(k []string) []string {
			return append(slices.Clone(k[1:]), k[:1]...)
		},
		"rotate M/2": func(k []string) []string {
			h := len(k) / 2
			return append(slices.Clone(k[h:]), k[:h]...)
		},
		"evens then odds": func(k []string) []string {
			var p []string
			for i := 0; i < len(k); i += 2 {
				p = append(p, k[i])
			}
			for i := 1; i < len(k); i += 2 {
				p = append(p, k[i])
			}
			return p
		},
	}
	names := []string{"reverse", "rotate 1", "rotate M/2", "evens then odds"}
	propertyMatrix(func(space string, n int, keys []string) {
		if len(keys) > 10 {
			return
		}
		got := Balanced(space, n, keys)
		if again := Balanced(space, n, keys); !slices.Equal(again, got) {
			t.Errorf("Balanced(%q, %d, %v) = %v, then %v", space, n, keys, got, again)
		}
		for _, name := range names {
			perm := permute[name](keys)
			pg := Balanced(space, n, perm)
			for s := range n {
				if keys[got[s]] != perm[pg[s]] {
					t.Errorf("Balanced(%q, %d, %v) with keys %s: shard %d goes to %s, want %s", space, n, keys, name, s, perm[pg[s]], keys[got[s]])
					break
				}
			}
		}
	})
}

// balancedMovement returns, for Balanced("bench", n, m0 … m(M-1)), how many shards move when
// m<M> joins and how many of them move to it, and how many move when m(M-1) leaves and how
// many of them move from it (ASG-110).
func balancedMovement(n, m int) (movedJoin, toNew, movedLeave, fromLeft int) {
	keys := seqKeys("m", m+1)
	base, joined, left := keys[:m], keys, keys[:m-1]
	b := Balanced("bench", n, base)
	j := Balanced("bench", n, joined)
	l := Balanced("bench", n, left)
	for s := range n {
		if joined[j[s]] != base[b[s]] {
			movedJoin++
			if j[s] == m {
				toNew++
			}
		}
		if l[s] < 0 || left[l[s]] != base[b[s]] {
			movedLeave++
			if b[s] == m-1 {
				fromLeft++
			}
		}
	}
	return movedJoin, toNew, movedLeave, fromLeft
}

// countMoved returns the number of shards whose key differs between results a and b over
// keys.
func countMoved(keys []string, a, b []int) int {
	moved := 0
	for s := range a {
		if keys[a[s]] != keys[b[s]] {
			moved++
		}
	}
	return moved
}

func TestBalancedMovement(t *testing.T) {
	for _, n := range []int{64, 1024, 4096} {
		for _, m := range []int{3, 10, 100} {
			movedJoin, toNew, movedLeave, fromLeft := balancedMovement(n, m)
			keys := seqKeys("m", m)
			base, again := Balanced("bench", n, keys), Balanced("bench", n, keys)
			if same := countMoved(keys, base, again); same != 0 {
				t.Errorf("n=%d m=%d: recomputing with the same keys moves %d shards, want 0", n, m, same)
			}
			held := shardCounts(base, m)[m-1]
			if fromLeft != held {
				t.Errorf("n=%d m=%d: removing m%d moves %d of its %d shards, want all", n, m, m-1, fromLeft, held)
			}
			t.Logf("n=%d m=%d: moved/join %d, to-new/join %d, moved/leave %d, from-left/leave %d",
				n, m, movedJoin, toNew, movedLeave, fromLeft)
		}
	}
}
