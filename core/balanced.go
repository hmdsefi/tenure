// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Balanced assigns the n shards of space to placement keys and returns, for each shard
// 0…n-1, the index in keys of its key; every entry is -1 when keys is empty. Every key gets
// ⌊n/m⌋ or ⌈n/m⌉ shards for m keys, and the result depends only on space, n and the set of
// keys: reordering keys only relabels the indices.
//
// Each (shard, key) pair is scored with Score(space, strconv.Itoa(shard), key). Pairs are
// taken greedily by descending score (ties: key ascending, then shard ascending) when the
// shard is still unassigned and the key is under its cap: ⌈n/m⌉ while fewer than n mod m
// keys have reached ⌈n/m⌉, and ⌊n/m⌋ after that.
//
// Balanced takes time O(n·m·log(n·m)) and memory O(n·m); compute it once per view change,
// not per lookup. It does not modify keys. Balanced panics if n < 1 or keys contains a
// duplicate.
func Balanced(space string, n int, keys []string) []int {
	if n < 1 {
		panic(fmt.Errorf("core: Balanced: n is %d, want at least 1", n))
	}
	if dup, ok := smallestDuplicate(keys); ok {
		panic(fmt.Errorf("core: Balanced: duplicate key %q", dup))
	}
	return balancedScored(n, keys, balancedScore(space))
}

// smallestDuplicate returns the smallest key, in byte-wise order, that keys holds more than
// once, and whether there is one (ASG-010).
func smallestDuplicate(keys []string) (string, bool) {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[i-1] {
			return sorted[i], true
		}
	}
	return "", false
}

// balancedScore returns the score function Balanced passes to balancedScored: it equals
// Score(space, strconv.Itoa(shard), key) (ASG-090) without allocating. It hashes space, the
// shard's digits and the zero bytes once per shard and reuses that state while consecutive
// calls name the same shard, as balancedScored's calls do (ASG §11). strconv.Itoa allocates
// for shard >= 100; the digits go into an array local to the call instead, so the number of
// allocations does not depend on n (ASG-100).
func balancedScore(space string) func(shard int, key string) uint64 {
	var (
		last   int
		prefix uint64
		cached bool
	)
	return func(shard int, key string) uint64 {
		if !cached || shard != last {
			var digits [20]byte
			prefix = scorePrefix(space, strconv.AppendInt(digits[:0], int64(shard), 10))
			last, cached = shard, true
		}
		return scoreFrom(prefix, key)
	}
}

// balancedPair is one (shard, key) pair of the greedy pass: its score, the key's rank in
// byte-wise key order and the shard. The indices fit int32 for any n and M up to 2^31-1
// (ASG §11).
type balancedPair struct {
	score uint64
	key   int32
	shard int32
}

// balancedScored is Balanced with score in place of Score and without its panics (ASG-090):
// it sorts the pairs in pair order (ASG-030) and takes them greedily under the caps
// (ASG-040). It allocates the result and, with keys, one slice for the key order and
// counts and one for the pairs (ASG-100). The key order and counts share a slice so that it
// is at least 48 bytes from three keys on: the compiler keeps a slice of at most 32 bytes
// that does not escape on the stack, so tiny inputs may allocate less, never more.
func balancedScored(n int, keys []string, score func(shard int, key string) uint64) []int {
	out := make([]int, n)
	for s := range out {
		out[s] = -1
	}
	m := len(keys)
	if m == 0 {
		return out
	}

	// order[j] is the index in keys of the key of rank j in byte-wise order. A pair carries
	// the rank, so the pair comparison reads integers only. counts[j] is the number of
	// shards the key of rank j holds. Both share one allocation.
	scratch := make([]int, 2*m)
	order, counts := scratch[:m], scratch[m:]
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return strings.Compare(keys[a], keys[b]) })

	pairs := make([]balancedPair, 0, n*m)
	for s := range n {
		for j, i := range order {
			pairs = append(pairs, balancedPair{
				score: score(s, keys[i]),
				key:   int32(j), //nolint:gosec // j < len(keys); wraps only past 2^31-1 keys
				shard: int32(s), //nolint:gosec // s < n; wraps only past 2^31-1 shards
			})
		}
	}
	//tenure:totalorder score desc, key asc, shard asc
	slices.SortFunc(pairs, func(a, b balancedPair) int {
		if c := cmp.Compare(b.score, a.score); c != 0 {
			return c
		}
		if c := cmp.Compare(a.key, b.key); c != 0 {
			return c
		}
		return cmp.Compare(a.shard, b.shard)
	})

	q, r := n/m, n%m
	full, assigned := 0, 0
	for _, p := range pairs {
		if out[p.shard] != -1 {
			continue
		}
		limit := q
		if full < r {
			limit = q + 1
		}
		if counts[p.key] >= limit {
			continue
		}
		out[p.shard] = order[p.key]
		counts[p.key]++
		if counts[p.key] == q+1 {
			full++
		}
		if assigned++; assigned == n {
			break // every later pair has an assigned shard
		}
	}
	return out
}
