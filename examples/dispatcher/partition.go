// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// forwardAfter is how long a poll on a partition other than the root waits for a task before
// it withdraws and polls the partition's parent instead.
const forwardAfter = 50 * time.Millisecond

// ErrPartition means a task or a poll named a partition that is not one of the tree's read
// partitions. Nothing was changed.
var ErrPartition = errors.New("dispatcher: no such partition")

// Task is the unit of work the dispatcher matches with a poll.
type Task struct {
	ID      string `json:"id"`
	Payload string `json:"payload,omitempty"`
}

// Forwarder carries a task or a poll to another partition: the parent of the one that has no
// local match. The gRPC layer implements it by routing to the partition's owner; a test
// implements it in process.
//
// AddTask returns nil only when some partition has accepted the task. When it returns an error,
// the task may or may not have been accepted, except when the error matches ErrNotAccepted:
// then no partition has it and the caller may send it again.
type Forwarder interface {
	AddTask(ctx context.Context, partition int, t Task) error
	Poll(ctx context.Context, partition int) (*Task, error)
}

// Partitions holds the state of the partitions a member serves, over the member's current tree,
// and matches tasks with polls. The root buffers the tasks no poll is waiting for; every other
// partition buffers nothing and forwards. All methods are safe for concurrent use.
type Partitions struct {
	fwd Forwarder

	mu      sync.Mutex
	tree    core.Tree
	parts   map[int]*partition
	matches map[int]int
}

// partition is the state of one partition, guarded by Partitions.mu.
type partition struct {
	buffer  []Task    // the root's tasks that no poll waited for, oldest first
	waiters []*waiter // polls waiting here, oldest first
}

// waiter is one poll waiting on a partition. A task is handed to it by sending on task while the
// partition's lock is held and removing the waiter from the partition in the same step; a
// withdrawn waiter is no longer on the partition, so it never gets a task.
type waiter struct {
	task chan Task
}

// NewPartitions returns the state of a member that serves partitions of tree and forwards
// through fwd. It panics when the tree is not valid.
func NewPartitions(tree core.Tree, fwd Forwarder) *Partitions {
	if err := tree.Validate(); err != nil {
		panic(fmt.Sprintf("dispatcher: %v", err))
	}
	return &Partitions{
		fwd:     fwd,
		tree:    tree,
		parts:   make(map[int]*partition),
		matches: make(map[int]int),
	}
}

// Tree returns the member's current tree.
func (p *Partitions) Tree() core.Tree {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tree
}

// SetTree makes next the member's tree. It fails, and changes nothing, unless core.Tree's
// ValidateChange accepts the move from the current tree.
func (p *Partitions) SetTree(next core.Tree) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.tree.ValidateChange(next); err != nil {
		return err
	}
	p.tree = next
	return nil
}

// Matches returns how many tasks have met a poll on partition i here: a task added while a poll
// waited, a poll that took a task the root buffered, and a task a forwarded add handed to one.
func (p *Partitions) Matches(i int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.matches[i]
}

// Buffered returns how many tasks the root holds, 0 for any other partition.
func (p *Partitions) Buffered(i int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if part, ok := p.parts[i]; ok {
		return len(part.buffer)
	}
	return 0
}

// part returns partition i's state, creating it. The caller holds p.mu.
func (p *Partitions) part(i int) *partition {
	part, ok := p.parts[i]
	if !ok {
		part = &partition{}
		p.parts[i] = part
	}
	return part
}

// checked returns the current tree when partition i is one of its read partitions.
func (p *Partitions) checked(i int) (core.Tree, error) {
	tree := p.Tree()
	if i < 0 || i >= tree.Read {
		return tree, fmt.Errorf("%w: %d is not in 0..%d", ErrPartition, i, tree.Read-1)
	}
	return tree, nil
}

// AddTask adds t at partition i. A poll waiting on i takes it. Otherwise the root buffers it and
// any other partition forwards it to its parent at once.
func (p *Partitions) AddTask(ctx context.Context, i int, t Task) error {
	tree, err := p.checked(i)
	if err != nil {
		return err
	}
	p.mu.Lock()
	part := p.part(i)
	if len(part.waiters) > 0 {
		w := part.waiters[0]
		part.waiters = part.waiters[1:]
		w.task <- t // capacity 1, and a waiter is handed one task at most
		p.matches[i]++
		p.mu.Unlock()
		return nil
	}
	if tree.IsRoot(i) {
		part.buffer = append(part.buffer, t)
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()
	return p.fwd.AddTask(ctx, tree.Parent(i), t)
}

// Poll waits on partition i for a task until ctx ends. The root takes a buffered task at once
// and otherwise waits for ctx's deadline, and then returns no task and no error. Any other
// partition waits forwardAfter, then withdraws and polls its parent with what is left of ctx.
// A task that was handed to the poll is returned even when ctx ended in the meantime.
func (p *Partitions) Poll(ctx context.Context, i int) (*Task, error) {
	tree, err := p.checked(i)
	if err != nil {
		return nil, err
	}
	root := tree.IsRoot(i)

	p.mu.Lock()
	part := p.part(i)
	if root && len(part.buffer) > 0 {
		t := part.buffer[0]
		part.buffer = part.buffer[1:]
		p.matches[i]++
		p.mu.Unlock()
		return &t, nil
	}
	w := &waiter{task: make(chan Task, 1)}
	part.waiters = append(part.waiters, w)
	p.mu.Unlock()

	var timeout <-chan time.Time
	if !root {
		timer := time.NewTimer(forwardAfter)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case t := <-w.task:
		return &t, nil
	case <-ctx.Done():
	case <-timeout:
	}

	// Withdraw. Handing a task and removing the waiter happen under the lock, so either the
	// waiter is still here and nothing will be handed to it, or the task is already in w.task.
	p.mu.Lock()
	if idx := indexOf(part.waiters, w); idx >= 0 {
		part.waiters = append(part.waiters[:idx], part.waiters[idx+1:]...)
		p.mu.Unlock()
	} else {
		p.mu.Unlock()
		t := <-w.task
		return &t, nil
	}

	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, nil
		}
		return nil, ctx.Err()
	}
	t, err := p.fwd.Poll(ctx, tree.Parent(i))
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil, nil // the deadline ended the parent's wait: an empty answer, not an error
	}
	return t, err
}

// indexOf returns the position of w in ws, or -1.
func indexOf(ws []*waiter, w *waiter) int {
	for i, x := range ws {
		if x == w {
			return i
		}
	}
	return -1
}
