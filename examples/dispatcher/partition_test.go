// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// hop is one forward: the partition it went to and the time since the test began.
type hop struct {
	partition int
	at        time.Duration
}

// loopback is a Forwarder for one member that serves every partition: a forward goes straight
// to the same Partitions, and is recorded.
type loopback struct {
	start time.Time
	parts *Partitions

	mu    sync.Mutex
	adds  []hop
	polls []hop
}

func newLoopback(tree core.Tree) *loopback {
	l := &loopback{start: time.Now()}
	l.parts = NewPartitions(tree, l)
	return l
}

func (l *loopback) AddTask(ctx context.Context, partition int, t Task) error {
	l.mu.Lock()
	l.adds = append(l.adds, hop{partition, time.Since(l.start)})
	l.mu.Unlock()
	return l.parts.AddTask(ctx, partition, t)
}

func (l *loopback) Poll(ctx context.Context, partition int) (*Task, error) {
	l.mu.Lock()
	l.polls = append(l.polls, hop{partition, time.Since(l.start)})
	l.mu.Unlock()
	return l.parts.Poll(ctx, partition)
}

func (l *loopback) hops() (adds, polls []hop) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.adds), slices.Clone(l.polls)
}

// TestATEXM17Forwarding checks AT-EXM-17: a poll on partition 4 forwards to 1 after 50 ms and
// to 0 after 100 ms, and then receives a task added at partition 7. The steps are written from
// the issue's text.
func TestATEXM17Forwarding(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLoopback(tasksTree)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		type answer struct {
			task *Task
			err  error
		}
		got := make(chan answer, 1)
		go func() {
			task, err := l.parts.Poll(ctx, 4)
			got <- answer{task, err}
		}()

		time.Sleep(25 * time.Millisecond)
		if _, polls := l.hops(); len(polls) != 0 {
			t.Fatalf("after 25 ms the poll forwarded: %v", polls)
		}
		time.Sleep(50 * time.Millisecond) // 75 ms
		if _, polls := l.hops(); !slices.Equal(polls, []hop{{1, 50 * time.Millisecond}}) {
			t.Fatalf("after 75 ms the forwards are %v, want one to partition 1 at 50 ms", polls)
		}
		time.Sleep(50 * time.Millisecond) // 125 ms
		want := []hop{{1, 50 * time.Millisecond}, {0, 100 * time.Millisecond}}
		if _, polls := l.hops(); !slices.Equal(polls, want) {
			t.Fatalf("after 125 ms the forwards are %v, want %v", polls, want)
		}

		// 7's parent is 2, whose parent is the root, where the poll waits.
		if err := l.parts.AddTask(ctx, 7, Task{ID: "t-1", Payload: "p"}); err != nil {
			t.Fatalf("AddTask(7): %v", err)
		}
		adds, _ := l.hops()
		if wantAdds := []hop{{2, 125 * time.Millisecond}, {0, 125 * time.Millisecond}}; !slices.Equal(adds, wantAdds) {
			t.Fatalf("task forwards are %v, want %v", adds, wantAdds)
		}
		a := <-got
		if a.err != nil || a.task == nil || a.task.ID != "t-1" {
			t.Fatalf("poll = %+v, %v; want task t-1", a.task, a.err)
		}
		if l.parts.Matches(0) != 1 || l.parts.Buffered(0) != 0 {
			t.Fatalf("root: %d matches, %d buffered; want 1 and 0", l.parts.Matches(0), l.parts.Buffered(0))
		}
	})
}

func TestPartitionsRoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newLoopback(tasksTree)
		ctx := t.Context()

		// The root buffers; a poll on it takes the oldest buffered task at once.
		for _, id := range []string{"a", "b"} {
			if err := l.parts.AddTask(ctx, 0, Task{ID: id}); err != nil {
				t.Fatal(err)
			}
		}
		// A task added at a leaf with no poll is forwarded up to the root.
		if err := l.parts.AddTask(ctx, 5, Task{ID: "c"}); err != nil {
			t.Fatal(err)
		}
		if n := l.parts.Buffered(0); n != 3 {
			t.Fatalf("root buffers %d tasks, want 3", n)
		}
		if n := l.parts.Buffered(5); n != 0 {
			t.Fatalf("a leaf buffers %d tasks, want 0", n)
		}
		var ids []string
		for range 3 {
			task, err := l.parts.Poll(ctx, 0)
			if err != nil || task == nil {
				t.Fatalf("Poll(0) = %v, %v", task, err)
			}
			ids = append(ids, task.ID)
		}
		if want := []string{"a", "b", "c"}; !slices.Equal(ids, want) {
			t.Fatalf("polled %v, want %v", ids, want)
		}

		// A root poll with nothing to take waits for its deadline and then answers with no task
		// and no error.
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		start := time.Now()
		task, err := l.parts.Poll(pctx, 0)
		if task != nil || err != nil || time.Since(start) != 5*time.Second {
			t.Fatalf("empty root poll = %v, %v after %v; want nil, nil after 5s", task, err, time.Since(start))
		}
	})
}

func TestPartitionsRange(t *testing.T) {
	l := newLoopback(tasksTree)
	ctx := t.Context()
	for _, p := range []int{-1, 8} {
		if err := l.parts.AddTask(ctx, p, Task{ID: "x"}); !errors.Is(err, ErrPartition) {
			t.Errorf("AddTask(%d) = %v, want ErrPartition", p, err)
		}
		if _, err := l.parts.Poll(ctx, p); !errors.Is(err, ErrPartition) {
			t.Errorf("Poll(%d) = %v, want ErrPartition", p, err)
		}
	}
}

func TestPartitionsSetTree(t *testing.T) {
	l := newLoopback(tasksTree)
	p := l.parts
	if err := p.SetTree(core.Tree{Name: "tasks", Read: 8, Write: 4, Degree: 3}); err != nil {
		t.Fatalf("lower Write: %v", err)
	}
	// Read may not drop below the old Write, and the name may not change.
	if err := p.SetTree(core.Tree{Name: "tasks", Read: 3, Write: 3, Degree: 3}); !errors.Is(err, core.ErrInvalidTree) {
		t.Fatalf("Read 3 below Write 4: %v, want ErrInvalidTree", err)
	}
	if err := p.SetTree(core.Tree{Name: "other", Read: 8, Write: 4, Degree: 3}); !errors.Is(err, core.ErrInvalidTree) {
		t.Fatalf("rename: %v, want ErrInvalidTree", err)
	}
	if got := p.Tree().Write; got != 4 {
		t.Fatalf("Write = %d after refused changes, want 4", got)
	}
	if err := p.SetTree(core.Tree{Name: "tasks", Read: 4, Write: 4, Degree: 3}); err != nil {
		t.Fatalf("lower Read: %v", err)
	}
	if err := p.AddTask(t.Context(), 5, Task{ID: "x"}); !errors.Is(err, ErrPartition) {
		t.Fatalf("AddTask(5) after Read 4: %v, want ErrPartition", err)
	}
}

// TestPartitionsConserve races adds against polls that give up and forward at random times: every
// task must come out exactly once, none handed to a withdrawn poll and none lost in a forward.
func TestPartitionsConserve(t *testing.T) {
	const tasks = 300
	l := newLoopback(tasksTree)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var (
		mu  sync.Mutex
		got = make(map[string]int)
	)
	record := func(task *Task) {
		mu.Lock()
		defer mu.Unlock()
		got[task.ID]++
	}
	var pollers sync.WaitGroup
	for i := range 6 {
		pollers.Go(func() {
			rng := rand.New(rand.NewPCG(7, uint64(i))) //nolint:gosec // seeded on purpose
			for ctx.Err() == nil {
				// Deadlines around forwardAfter, so a poll often ends while a task is handed to it.
				pctx, stop := context.WithTimeout(ctx, time.Duration(30+rng.IntN(60))*time.Millisecond)
				task, err := l.parts.Poll(pctx, tasksTree.PickRead(rng.Uint64()))
				stop()
				if err != nil && ctx.Err() == nil {
					t.Errorf("Poll: %v", err)
					return
				}
				if task != nil {
					record(task)
				}
			}
		})
	}
	rng := rand.New(rand.NewPCG(7, 100)) //nolint:gosec // seeded on purpose
	for n := range tasks {
		task := Task{ID: fmt.Sprintf("t-%d", n)}
		if err := l.parts.AddTask(ctx, tasksTree.Pick(rng.Uint64()), task); err != nil {
			t.Fatalf("AddTask: %v", err)
		}
		if n%50 == 0 {
			time.Sleep(time.Duration(rng.IntN(70)) * time.Millisecond)
		}
	}
	deadline := time.After(20 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == tasks {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("%d of %d tasks delivered", n, tasks)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	pollers.Wait()
	for id, n := range got {
		if n != 1 {
			t.Errorf("task %s delivered %d times", id, n)
		}
	}
}
