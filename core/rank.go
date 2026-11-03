// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// Rank returns the first n members of members for key in namespace, best first: by Score
// descending, then by ID ascending, then by position in members. It ranks every member it
// is given, whatever its other fields; callers filter first. The result is a new slice of
// min(n, len(members)) copies, nil when n <= 0 or members is empty. Rank does not modify
// members.
func Rank(namespace, key string, members []Member, n int) []Member {
	return rankScored(members, n, func(id string) uint64 { return Score(namespace, key, id) })
}

// scoredIndex is one member's score and its position in the members being ranked.
type scoredIndex struct {
	score uint64
	index int
}

// rankScored returns the first min(n, len(members)) members in placement order for score:
// score descending, then ID ascending, then index ascending (COR-130 to COR-150). It reads
// only ID, never panics, and allocates the result plus, for n > 1, one scratch slice
// (COR-160).
func rankScored(members []Member, n int, score func(id string) uint64) []Member {
	if n <= 0 || len(members) == 0 {
		return nil
	}
	if n == 1 {
		best, bestScore := 0, score(members[0].ID)
		for i := 1; i < len(members); i++ {
			s := score(members[i].ID)
			if s > bestScore || s == bestScore && members[i].ID < members[best].ID {
				best, bestScore = i, s
			}
		}
		return []Member{members[best]}
	}
	scratch := make([]scoredIndex, len(members))
	for i := range members {
		scratch[i] = scoredIndex{score: score(members[i].ID), index: i}
	}
	//tenure:totalorder score desc, ID asc, index asc
	slices.SortFunc(scratch, func(a, b scoredIndex) int {
		if c := cmp.Compare(b.score, a.score); c != 0 {
			return c
		}
		if c := strings.Compare(members[a.index].ID, members[b.index].ID); c != 0 {
			return c
		}
		return cmp.Compare(a.index, b.index)
	})
	out := make([]Member, min(n, len(members)))
	for i := range out {
		out[i] = members[scratch[i].index]
	}
	return out
}

// Placement places keys on one fixed list of members. Build one per member list, for
// example once per view, and reuse it for every lookup. A Placement is immutable and safe
// for concurrent use.
type Placement interface {
	// Rank returns the first min(n, p) members for key in namespace, best first, as a new
	// slice, where p is the number of members the placement places (an implementation that
	// gives several members one place documents it); nil when n <= 0 or p is 0. For a <= b,
	// Rank(namespace, key, a) is a prefix of Rank(namespace, key, b), so the first choice
	// never depends on n.
	Rank(namespace, key string, n int) []Member
}

// rendezvous is the Placement NewRendezvous returns: a copy of its members sorted by ID
// (COR-190, COR-200).
type rendezvous struct {
	members []Member
}

// Rank returns Rank(namespace, key, p.members, n).
func (p rendezvous) Rank(namespace, key string, n int) []Member {
	return Rank(namespace, key, p.members, n)
}

// NewRendezvous returns the Placement over members that ranks with Rank: its Rank method
// returns Rank(namespace, key, members, n). It copies members, so later changes to the
// slice do not affect it. NewRendezvous panics if two members share an ID.
func NewRendezvous(members []Member) Placement {
	order := make([]int, len(members))
	for i := range order {
		order[i] = i
	}
	//tenure:totalorder ID asc, index asc
	slices.SortFunc(order, func(a, b int) int {
		if c := strings.Compare(members[a].ID, members[b].ID); c != 0 {
			return c
		}
		return cmp.Compare(a, b)
	})
	var sorted []Member
	if len(members) > 0 {
		sorted = make([]Member, len(members))
	}
	for i, j := range order {
		sorted[i] = members[j]
		if i > 0 && sorted[i].ID == sorted[i-1].ID {
			panic(fmt.Errorf("core: NewRendezvous: duplicate member ID %q", sorted[i].ID))
		}
	}
	return rendezvous{members: sorted}
}
