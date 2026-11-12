// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// testLimit is how long one of these tests may run; the issue allows 60 s with -race.
const testLimit = 55 * time.Second

// deliveryTest starts a cluster of three members over tree and returns it with a load of tasks
// tasks and a context that ends after testLimit. The cluster is closed with the test.
func deliveryTest(t *testing.T, tree core.Tree, tasks int, opts ...clusterOption) (context.Context, *cluster, *work) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testLimit)
	t.Cleanup(cancel)
	c, err := startCluster(ctx, tree, memberCount, opts...)
	if err != nil {
		t.Fatalf("startCluster: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), setupTimeout)
		defer cancel()
		if err := c.Close(cctx); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return ctx, c, newWork(c, tree, tasks, 1)
}

// checkExactlyOnce fails unless the load delivered each of its tasks, and none twice. A second
// delivery of a task fails the run itself, so a run that returned nil saw none.
func checkExactlyOnce(t *testing.T, w *work) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	var missing []string
	for n := range w.tasks {
		id := fmt.Sprintf("t-%05d", n)
		if _, ok := w.seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d of %d tasks never delivered, first %v", len(missing), w.tasks, missing[:min(len(missing), 5)])
	}
	if len(w.seen) != w.tasks {
		t.Fatalf("%d tasks delivered, want %d: some task ID that was never added came out", len(w.seen), w.tasks)
	}
}

// TestDispatcherDelivery is AT-EXM-19 on the program's tree: 4 producers and 6 pollers over
// {8, 8}, forwarding to parents, deliver every task exactly once.
func TestDispatcherDelivery(t *testing.T) {
	ctx, _, w := deliveryTest(t, tasksTree, 1000)
	if err := w.run(ctx); err != nil {
		t.Fatal(err)
	}
	checkExactlyOnce(t, w)
}

// TestDispatcherUnpartitioned is the same load on one partition, the root, where nothing
// forwards.
func TestDispatcherUnpartitioned(t *testing.T) {
	tree := core.Tree{Name: "tasks", Read: 1, Write: 1, Degree: 3}
	ctx, c, w := deliveryTest(t, tree, 500)
	if err := w.run(ctx); err != nil {
		t.Fatal(err)
	}
	checkExactlyOnce(t, w)
	matches := 0
	for _, s := range c.servers {
		matches += s.Partitions().Matches(0)
	}
	if matches != 500 {
		t.Fatalf("the root matched %d tasks, want 500", matches)
	}
}

// TestDispatcherScaleDown moves every member from {8, 8} to {8, 4} and, once partitions 4 to 7
// hold nothing, to {4, 4}, with the load running. ValidateChange accepts each step and refuses
// the shortcut. Every task is delivered exactly once.
func TestDispatcherScaleDown(t *testing.T) {
	const tasks = 600
	from := tasksTree
	mid := core.Tree{Name: "tasks", Read: 8, Write: 4, Degree: 3}
	to := core.Tree{Name: "tasks", Read: 4, Write: 4, Degree: 3}
	for _, step := range [][2]core.Tree{{from, mid}, {mid, to}} {
		if err := step[0].ValidateChange(step[1]); err != nil {
			t.Fatalf("ValidateChange(%v, %v): %v", step[0], step[1], err)
		}
	}
	if err := from.ValidateChange(to); err == nil {
		t.Fatalf("ValidateChange(%v, %v) accepted dropping partitions that may still be written", from, to)
	}

	ctx, c, w := deliveryTest(t, from, tasks)

	// A member that has not applied a change may not skip a step.
	if err := c.servers[0].Partitions().SetTree(to); err == nil {
		t.Fatal("SetTree skipped the first step")
	}
	if got := c.servers[0].Partitions().Tree(); got != from {
		t.Fatalf("a refused SetTree left the tree %v, want %v", got, from)
	}

	// The producers hold at one third and at two thirds of the tasks while the tree changes.
	first, second := tasks/3, 2*tasks/3
	stage1, stage2 := make(chan struct{}), make(chan struct{})
	w.gate = func(ctx context.Context, n int) error {
		var open chan struct{}
		switch {
		case n >= second:
			open = stage2
		case n >= first:
			open = stage1
		default:
			return nil
		}
		select {
		case <-open:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var runErr error
	ended := make(chan struct{})
	go func() {
		runErr = w.run(ctx)
		close(ended)
	}()
	wait := func(what string, cond func() bool) {
		t.Helper()
		for !cond() {
			select {
			case <-ended:
				t.Fatalf("the load ended while waiting for %s: %v", what, runErr)
			case <-ctx.Done():
				t.Fatalf("timed out waiting for %s", what)
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	setAll := func(next core.Tree) {
		t.Helper()
		for i, s := range c.servers {
			if err := s.Partitions().SetTree(next); err != nil {
				t.Fatalf("member %d: SetTree(%v): %v", i+1, next, err)
			}
		}
	}

	// {8, 8} to {8, 4}: every task so far is delivered, then the writes narrow.
	wait("the first third", func() bool { return w.delivered() == first })
	setAll(mid)
	w.setTree(mid)
	close(stage1)

	// {8, 4} to {4, 4}: the polls move off partitions 4 to 7, those partitions hold nothing,
	// and only then do the members drop them.
	wait("the second third", func() bool { return w.delivered() == second })
	w.setTree(to)
	wait("partitions 4 to 7 to empty", func() bool {
		if w.busy(to.Read) {
			return false
		}
		for _, s := range c.servers {
			for i := to.Read; i < mid.Read; i++ {
				if s.Partitions().Buffered(i) != 0 {
					return false
				}
			}
		}
		return true
	})
	setAll(to)
	close(stage2)

	<-ended
	if runErr != nil {
		t.Fatal(runErr)
	}
	checkExactlyOnce(t, w)
	for i, s := range c.servers {
		if got := s.Partitions().Tree(); got != to {
			t.Fatalf("member %d ended on %v, want %v", i+1, got, to)
		}
	}
}

// TestDispatcherStaleView gives n1 its own membership list, n1 and n2, while n2 and n3 see all
// three. The members disagree about who owns some partitions; producers and pollers enter through
// every member and try again after a route that did not settle, and every task is still
// delivered exactly once.
func TestDispatcherStaleView(t *testing.T) {
	ctx, c, w := deliveryTest(t, tasksTree, 300, withView(0, 0, 1))

	// The test means nothing if the views place every partition alike.
	differ := 0
	for i := range tasksTree.Read {
		key := tasksTree.Key(i)
		stale, err := c.nodes[0].Resolver(namespace).Lookup(key)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := c.nodes[1].Resolver(namespace).Lookup(key)
		if err != nil {
			t.Fatal(err)
		}
		if stale.ID != fresh.ID {
			differ++
		}
	}
	if differ == 0 {
		t.Fatal("n1 and n2 place every partition alike: the stale view changes nothing")
	}

	if err := w.run(ctx); err != nil {
		t.Fatal(err)
	}
	checkExactlyOnce(t, w)
}
