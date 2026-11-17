// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// spacesMetaKey is the Meta key under which a node advertises its spaces (RTR-500).
const spacesMetaKey = reservedMetaPrefix + "spaces"

// ErrSpaceMismatch is wrapped by the error a Router's lookups return for a space whose shard
// count differs from the one a member of the group advertises: see SpaceMismatchError.
var ErrSpaceMismatch = errors.New("tenure: space mismatch")

// SpaceMismatchError is the error of a Router's lookups in a space whose shard count differs
// from the count that a member the Router routes to advertises for it. Its table would send
// requests to the wrong members, so the Router fails the space's lookups until no such member
// disagrees. It wraps ErrSpaceMismatch.
//
// A member advertises the shard counts of its spaces in its member record when its node
// starts; members that advertise nothing, such as those of older versions, are not checked.
// Nor can ShardOf be: a Router's ShardOf must be the members' own.
type SpaceMismatchError struct {
	// Space is the space's name.
	Space string

	// Shards is the Router's shard count for the space.
	Shards int

	// Member is the ID of a member that advertises another count, and MemberShards that count.
	Member       string
	MemberShards int
}

// Error returns text such as
// "tenure: space mismatch: orders has 8 shards here and 16 on member n2".
func (e *SpaceMismatchError) Error() string {
	return ErrSpaceMismatch.Error() + ": " + e.Space + " has " + strconv.Itoa(e.Shards) +
		" shards here and " + strconv.Itoa(e.MemberShards) + " on member " + e.Member
}

// Unwrap returns ErrSpaceMismatch.
func (e *SpaceMismatchError) Unwrap() error {
	return ErrSpaceMismatch
}

// advertised returns self with Meta["tenure.spaces"] set to the spaces' names and shard counts,
// or self unchanged when there are no spaces. The Meta map is a new one, so self's is not
// modified.
func advertised(self Member, spaces []Space) Member {
	if len(spaces) == 0 {
		return self
	}
	sorted := slices.Clone(spaces)
	slices.SortFunc(sorted, func(a, b Space) int { return strings.Compare(a.Name, b.Name) })
	parts := make([]string, len(sorted))
	for i, sp := range sorted {
		parts[i] = spaceNameEscaper.Replace(sp.Name) + ":" + strconv.Itoa(sp.Shards)
	}
	meta := maps.Clone(self.Meta)
	if meta == nil {
		meta = make(map[string]string, 1)
	}
	meta[spacesMetaKey] = strings.Join(parts, ",")
	self.Meta = meta
	return self
}

// spaceNameEscaper and spaceNameUnescaper write and read the characters that the
// "tenure.spaces" format uses.
var (
	spaceNameEscaper   = strings.NewReplacer("%", "%25", ",", "%2C", ":", "%3A")
	spaceNameUnescaper = strings.NewReplacer("%25", "%", "%2C", ",", "%3A", ":")
)

// advertisedShards returns the shard count that the "tenure.spaces" value adv gives space, and
// whether it names the space. Entries it cannot read are skipped.
func advertisedShards(adv, space string) (int, bool) {
	for entry := range strings.SplitSeq(adv, ",") {
		i := strings.LastIndexByte(entry, ':')
		if i < 0 || spaceNameUnescaper.Replace(entry[:i]) != space {
			continue
		}
		n, err := strconv.Atoi(entry[i+1:])
		if err != nil {
			continue
		}
		return n, true
	}
	return 0, false
}

// mismatch returns the error of a space whose shard count is shards when a member of members
// advertises another count for it, the first such member in members' order, or nil when none
// does (RTR-510, RTR-520).
func mismatch(space string, shards int, members []Member) error {
	for _, m := range members {
		adv, ok := m.Meta[spacesMetaKey]
		if !ok {
			continue
		}
		if n, ok := advertisedShards(adv, space); ok && n != shards {
			return &SpaceMismatchError{Space: space, Shards: shards, Member: m.ID, MemberShards: n}
		}
	}
	return nil
}
