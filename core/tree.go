// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrInvalidTree is wrapped by the errors Tree.Validate and Tree.ValidateChange return.
var ErrInvalidTree = errors.New("core: invalid tree")

// Tree is the topology of one hot key split into partitions. Partition 0 is the root and
// its key is Name, so a Tree with Read = Write = 1 behaves exactly like the unpartitioned
// key Name. Partition i > 0 has key Name + "/" + i and parent (i-1)/Degree, the formula
// Temporal's matching service uses for task queue partitions.
//
// Writers pick a partition in 0…Write-1 and readers poll 0…Read-1. To remove partitions,
// lower Write first, wait until every member uses the new value and the dropped partitions
// are empty, then lower Read. ValidateChange checks each step.
//
// A partition with no local match forwards its work to Parent(i); the root keeps it. What
// counts as a match is up to the caller.
type Tree struct {
	// Name is the key being split, and partition 0's key. It is non-empty and does not
	// end in "/" followed by a decimal number without leading zeros, so that keys of
	// different trees never collide.
	Name string

	// Read is the number of partitions readers poll: 0…Read-1. Read >= Write.
	Read int

	// Write is the number of partitions writers pick from: 0…Write-1. Write >= 1.
	Write int

	// Degree is the number of children per partition. Degree >= 1.
	Degree int
}

// Validate reports whether t is usable: Name is non-empty and does not end in "/" followed
// by a decimal number without leading zeros, Write >= 1, Read >= Write and Degree >= 1. The
// error wraps ErrInvalidTree.
func (t Tree) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("%w: empty Name", ErrInvalidTree)
	}
	if suffix := partitionSuffix(t.Name); suffix != "" {
		return fmt.Errorf("%w: Name %q ends in partition suffix %q", ErrInvalidTree, t.Name, suffix)
	}
	if t.Write < 1 {
		return fmt.Errorf("%w %q: Write %d is below 1", ErrInvalidTree, t.Name, t.Write)
	}
	if t.Read < t.Write {
		return fmt.Errorf("%w %q: Read %d is below Write %d", ErrInvalidTree, t.Name, t.Read, t.Write)
	}
	if t.Degree < 1 {
		return fmt.Errorf("%w %q: Degree %d is below 1", ErrInvalidTree, t.Name, t.Degree)
	}
	return nil
}

// partitionSuffix returns the partition suffix name ends in, "/" followed by a decimal
// number from 1 up without leading zeros (PTR §3), or "" if it has none.
func partitionSuffix(name string) string {
	slash := strings.LastIndexByte(name, '/')
	if slash < 0 {
		return ""
	}
	digits := name[slash+1:]
	if digits == "" || digits[0] == '0' {
		return ""
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return ""
		}
	}
	return name[slash:]
}

// Key returns partition i's key: Name for the root and Name + "/" + i otherwise. It does
// not check i against Read, so a member with an older Tree value can still name a
// partition. Key panics if i < 0.
func (t Tree) Key(i int) string {
	if i < 0 {
		panic(fmt.Errorf("core: Tree.Key: partition %d is negative", i))
	}
	if i == 0 {
		return t.Name
	}
	// One allocation: the digits live in a stack array, and the builder is grown to the
	// exact length of the key.
	var buf [20]byte
	digits := strconv.AppendInt(buf[:0], int64(i), 10)
	var b strings.Builder
	b.Grow(len(t.Name) + 1 + len(digits))
	b.WriteString(t.Name)
	b.WriteByte('/')
	b.Write(digits)
	return b.String()
}

// IsRoot reports whether i is the root, partition 0. It panics if i < 0.
func (t Tree) IsRoot(i int) bool {
	if i < 0 {
		panic(fmt.Errorf("core: Tree.IsRoot: partition %d is negative", i))
	}
	return i == 0
}

// Parent returns the partition that partition i forwards to, (i-1)/Degree. Parent(i) < i,
// so forwarding always reaches the root, and any range 0…P-1 contains the parent of each of
// its partitions. Parent panics if i < 1 or Degree < 1.
func (t Tree) Parent(i int) int {
	if i < 1 {
		panic(fmt.Errorf("core: Tree.Parent: partition %d has no parent", i))
	}
	if t.Degree < 1 {
		panic(fmt.Errorf("core: Tree.Parent: Degree %d is below 1", t.Degree))
	}
	return (i - 1) / t.Degree
}

// Children returns, in ascending order, the partitions in 0…Read-1 whose parent is i, or
// nil if there are none. It panics if i < 0 or Degree < 1.
func (t Tree) Children(i int) []int {
	if i < 0 {
		panic(fmt.Errorf("core: Tree.Children: partition %d is negative", i))
	}
	if t.Degree < 1 {
		panic(fmt.Errorf("core: Tree.Children: Degree %d is below 1", t.Degree))
	}
	// Test i against the last partition that can have a child before multiplying, so no
	// product or sum below overflows.
	if t.Read <= 1 || i > (t.Read-2)/t.Degree {
		return nil
	}
	first := i*t.Degree + 1 // at most Read-1
	n := t.Degree           // the count, without computing first + Degree
	if t.Read-1-first < t.Degree-1 {
		n = t.Read - first
	}
	children := make([]int, n)
	for k := range children {
		children[k] = first + k
	}
	return children
}
