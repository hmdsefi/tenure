// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import "fmt"

// FNV-1a 64-bit parameters.
const (
	fnvOffset64 = 0xcbf29ce484222325
	fnvPrime64  = 0x100000001b3
)

// fnv1a64 returns the 64-bit FNV-1a hash of the bytes of s (COR-090).
func fnv1a64(s string) uint64 {
	return fnvString(fnvOffset64, s)
}

// fnvString folds the bytes of s into the FNV-1a state h.
func fnvString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}

// fnvZero folds one zero byte into the FNV-1a state h.
func fnvZero(h uint64) uint64 {
	return h * fnvPrime64 // h ^ 0 == h
}

// fmix64 is MurmurHash3's 64-bit finalizer (COR-090).
func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// KeyShard returns the shard in 0…n-1 that key belongs to: the 64-bit FNV-1a hash of key,
// passed through MurmurHash3's 64-bit finalizer, modulo n. Doubling n splits shard s into s
// and s+n. KeyShard panics if n < 1.
func KeyShard(key string, n int) int {
	if n < 1 {
		panic(fmt.Errorf("core: KeyShard: n is %d, want at least 1", n))
	}
	return int(fmix64(fnv1a64(key)) % uint64(n)) //nolint:gosec // the remainder is below n, which is an int
}

// Score returns the rendezvous score of memberID for key in namespace: MurmurHash3's 64-bit
// finalizer applied to the 64-bit FNV-1a hash of namespace, a zero byte, key, a zero byte
// and memberID. It is the score lockstep's virtual actors use, with namespace as the actor
// kind. Score does not allocate.
func Score(namespace, key, memberID string) uint64 {
	h := fnvString(fnvOffset64, namespace)
	h = fnvZero(h)
	h = fnvString(h, key)
	h = fnvZero(h)
	h = fnvString(h, memberID)
	return fmix64(h)
}
