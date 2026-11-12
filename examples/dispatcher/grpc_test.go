// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hmdsefi/tenure"
)

// lookupFunc is a Lookuper that is a function.
type lookupFunc func(key string) (tenure.Member, error)

func (f lookupFunc) Lookup(key string) (tenure.Member, error) { return f(key) }

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestATEXM16Routing checks AT-EXM-16. The steps are written from the issue's text: a call to a
// member that does not own the partition is answered FailedPrecondition with the owner's address
// in the tenure-route trailer; the client follows it and succeeds; two members that name each
// other end after 2 resends with an error; the JSON codec round-trips the messages.
func TestATEXM16Routing(t *testing.T) {
	ctx := testContext(t)

	t.Run("route to the owner", func(t *testing.T) {
		c, err := startCluster(ctx, tasksTree, 3)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(ctx) })

		// Partition 0's owner by node 0's resolver; the members share one static group, so
		// every member names the same owner.
		owner, err := c.nodes[0].Resolver(namespace).Lookup(tasksTree.Key(0))
		if err != nil {
			t.Fatal(err)
		}
		ownerIdx, other := -1, -1
		for i, a := range c.addrs {
			if a == owner.Addr {
				ownerIdx = i
			} else if other < 0 {
				other = i
			}
		}
		if ownerIdx < 0 {
			t.Fatalf("owner %s is not a member address", owner.Addr)
		}

		// A call straight to a member that is not the owner is refused with the route.
		cc, err := grpc.NewClient(c.addrs[other],
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.CallContentSubtype(codecName)))
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Close()
		var trailer metadata.MD
		err = cc.Invoke(ctx, addTaskMethod, &AddTaskRequest{Partition: 0, Task: Task{ID: "t-1"}},
			new(AddTaskResponse), grpc.Trailer(&trailer))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("call to a non-owner: %v, want FailedPrecondition", err)
		}
		if got := trailer.Get("tenure-route"); !reflect.DeepEqual(got, []string{owner.Addr}) {
			t.Fatalf("tenure-route = %v, want [%s]", got, owner.Addr)
		}
		for i, s := range c.servers {
			if n := s.Partitions().Buffered(0); n != 0 {
				t.Fatalf("after the refused call node %d buffers %d tasks", i, n)
			}
		}

		// The client follows the route and succeeds, for a task and for a poll.
		if err := c.client.AddTask(ctx, c.addrs[other], AddTaskRequest{Partition: 0, Task: Task{ID: "t-1"}}); err != nil {
			t.Fatalf("AddTask through a non-owner: %v", err)
		}
		if n := c.servers[ownerIdx].Partitions().Buffered(0); n != 1 {
			t.Fatalf("the owner buffers %d tasks, want 1", n)
		}
		task, err := c.client.Poll(ctx, c.addrs[other], PollRequest{Partition: 0})
		if err != nil || task == nil || task.ID != "t-1" {
			t.Fatalf("Poll through a non-owner = %v, %v; want t-1", task, err)
		}
	})

	t.Run("members that name each other", func(t *testing.T) {
		var calls atomic.Int32
		count := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			calls.Add(1)
			return h(ctx, req)
		})
		var lns [2]net.Listener
		var lc net.ListenConfig
		for i := range lns {
			ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			lns[i] = ln
		}
		ids := [2]string{"a", "b"}
		addrs := [2]string{lns[0].Addr().String(), lns[1].Addr().String()}
		for i := range lns {
			peer := tenure.Member{ID: ids[1-i], Addr: addrs[1-i]}
			s := NewServer(ids[i], lookupFunc(func(string) (tenure.Member, error) { return peer, nil }), tasksTree, nil)
			g := grpc.NewServer(count)
			s.Register(g)
			go func() { _ = g.Serve(lns[i]) }()
			t.Cleanup(g.Stop)
		}
		client := NewClient()
		t.Cleanup(func() { _ = client.Close() })

		err := client.AddTask(ctx, addrs[0], AddTaskRequest{Partition: 0, Task: Task{ID: "t-1"}})
		if !errors.Is(err, ErrRoute) || !errors.Is(err, ErrNotAccepted) {
			t.Fatalf("AddTask = %v, want ErrRoute, which is ErrNotAccepted", err)
		}
		if got := calls.Load(); got != 1+maxResends {
			t.Fatalf("the members answered %d calls, want %d: one send and %d resends", got, 1+maxResends, maxResends)
		}
		calls.Store(0)
		if _, err := client.Poll(ctx, addrs[0], PollRequest{Partition: 0}); !errors.Is(err, ErrRoute) {
			t.Fatalf("Poll = %v, want ErrRoute", err)
		}
		if got := calls.Load(); got != 1+maxResends {
			t.Fatalf("the members answered %d poll calls, want %d", got, 1+maxResends)
		}
	})

	t.Run("json codec", func(t *testing.T) {
		RegisterCodec()
		RegisterCodec() // again: the Once makes it harmless
		codec := encoding.GetCodec(codecName)
		if codec == nil || codec.Name() != "json" {
			t.Fatalf("the codec registered as %q is %v", codecName, codec)
		}
		for _, msg := range []any{
			&AddTaskRequest{Partition: 7, Task: Task{ID: "t-9", Payload: "héllo \"x\""}},
			&AddTaskResponse{},
			&PollRequest{Partition: 3},
			&PollResponse{Task: &Task{ID: "t-2"}},
			&PollResponse{},
		} {
			data, err := codec.Marshal(msg)
			if err != nil {
				t.Fatalf("Marshal(%+v): %v", msg, err)
			}
			if !json.Valid(data) {
				t.Fatalf("Marshal(%+v) = %q, not JSON", msg, data)
			}
			back := reflect.New(reflect.TypeOf(msg).Elem()).Interface()
			if err := codec.Unmarshal(data, back); err != nil {
				t.Fatalf("Unmarshal(%q): %v", data, err)
			}
			if !reflect.DeepEqual(back, msg) {
				t.Fatalf("round trip of %+v gave %+v", msg, back)
			}
		}
	})
}

// TestPollEndsBeforeDeadline checks that a root poll stops waiting a margin before its caller's
// deadline, so that a task is never handed to a caller that has already given up: the margin is
// pollMargin, and at most a quarter of the time left.
func TestPollEndsBeforeDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		self := tenure.Member{ID: "a"}
		s := NewServer("a", lookupFunc(func(string) (tenure.Member, error) { return self, nil }), tasksTree, nil)
		for _, tc := range []struct{ deadline, want time.Duration }{
			{5 * time.Second, 5*time.Second - pollMargin},
			{200 * time.Millisecond, 150 * time.Millisecond},
		} {
			ctx, cancel := context.WithTimeout(t.Context(), tc.deadline)
			start := time.Now()
			resp, err := s.poll(ctx, &PollRequest{Partition: 0})
			cancel()
			if err != nil || resp == nil || resp.Task != nil {
				t.Fatalf("poll = %+v, %v; want an empty answer", resp, err)
			}
			if got := time.Since(start); got != tc.want {
				t.Errorf("a poll with a %v deadline ended after %v, want %v", tc.deadline, got, tc.want)
			}
		}
	})
}
