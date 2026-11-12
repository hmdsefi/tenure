// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command dispatcher matches tasks with polls on a partition tree, over gRPC.
//
//	go run ./dispatcher [-tasks 1000] [-seed 1]
//
// It starts three tenure members in one process. They have no store and no spaces: they only
// join a static group and place the partitions of one hot key, tasks, with its Resolver. The
// tree has 8 read and 8 write partitions with 3 children each, so partition 0 is the root,
// partitions 1 to 3 are its children, and 4 to 6 and 7 hang below 1 and 2. Each member serves a
// gRPC service, dispatcher.Dispatcher, written by hand with a JSON codec instead of generated
// code; a member that is not the owner of a partition answers FailedPrecondition and names the
// owner in the tenure-route trailer, and the caller follows it.
//
// A task added at a partition goes to a poll waiting there. When none waits, the root keeps the
// task and every other partition forwards it to its parent. A poll on a partition other than the
// root waits 50 ms and then asks the parent instead; a poll on the root waits for its 5 s
// deadline. The tasks are fake: each is an ID and a short payload.
//
// Four producers add the tasks at random write partitions, and six pollers poll random read
// partitions until every task is delivered. The program prints one line for each partition,
//
//	partition <i>: <n> matches
//
// the number of tasks that met a poll there, and then
//
//	tasks delivered: <n>
//
// The flags are the number of tasks (-tasks, default 1000) and the seed of the random choices
// (-seed, default 1). The program exits 0 when every task was delivered exactly once.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/static"
)

const (
	memberCount = 3
	producers   = 4
	pollers     = 6

	// namespace is the resolver namespace of the members.
	namespace = "dispatcher"

	// pollDeadline is how long a poll waits on the root.
	pollDeadline = 5 * time.Second

	// setupTimeout bounds the start and the stop of the members.
	setupTimeout = 30 * time.Second

	// addAttempts bounds the sends of one task that the members refused, one after the other.
	addAttempts = 100
)

// tasksTree is the tree of the example.
var tasksTree = core.Tree{Name: "tasks", Read: 8, Write: 8, Degree: 3}

func main() {
	os.Exit(exitCode())
}

// exitCode runs the program and returns its exit code.
func exitCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "dispatcher:", err)
		return 1
	}
	return 0
}

// execute is main with its context, arguments and output.
func execute(ctx context.Context, args []string, out io.Writer) (err error) {
	fs := flag.NewFlagSet("dispatcher", flag.ContinueOnError)
	tasks := fs.Int("tasks", 1000, "number of tasks to deliver")
	seed := fs.Uint64("seed", 1, "seed of the random choices")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tasks < 1 {
		return fmt.Errorf("-tasks must be at least 1, got %d", *tasks)
	}

	sctx, cancel := context.WithTimeout(ctx, setupTimeout)
	c, err := startCluster(sctx, tasksTree, memberCount)
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), setupTimeout)
		defer cancel()
		if cerr := c.Close(cctx); cerr != nil && err == nil {
			err = cerr
		}
	}()

	w := &work{c: c, tree: tasksTree, tasks: *tasks, seed: *seed, done: make(chan struct{}), seen: make(map[string]struct{})}
	if err := w.run(ctx); err != nil {
		return err
	}

	for i := range tasksTree.Read {
		matches := 0
		for _, s := range c.servers {
			matches += s.Partitions().Matches(i)
		}
		fmt.Fprintf(out, "partition %d: %d matches\n", i, matches)
	}
	fmt.Fprintf(out, "tasks delivered: %d\n", w.delivered())
	return nil
}

// work is the load of the program: producers that add tasks and pollers that take them.
type work struct {
	c     *cluster
	tree  core.Tree
	tasks int
	seed  uint64

	done chan struct{} // closed when every task has been delivered

	mu   sync.Mutex
	seen map[string]struct{}
}

func (w *work) delivered() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.seen)
}

// deliver records that a poll received t. A task received twice is an error.
func (w *work) deliver(t *Task) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, dup := w.seen[t.ID]; dup {
		return fmt.Errorf("task %s delivered twice", t.ID)
	}
	w.seen[t.ID] = struct{}{}
	if len(w.seen) == w.tasks {
		close(w.done)
	}
	return nil
}

// run starts the producers and the pollers and returns when every task was delivered, or at the
// first failure. The producers and the pollers have ended when it returns.
func (w *work) run(ctx context.Context) error {
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	var wg sync.WaitGroup
	errc := make(chan error, producers+pollers) // each goroutine sends at most one error
	for i := range producers {
		wg.Go(func() {
			if err := w.produce(lctx, i); err != nil {
				errc <- err
			}
		})
	}
	for i := range pollers {
		wg.Go(func() {
			if err := w.poll(lctx, producers+i); err != nil {
				errc <- err
			}
		})
	}
	var err error
	select {
	case <-w.done:
	case err = <-errc:
	case <-ctx.Done():
		err = ctx.Err()
	}
	stop()
	wg.Wait()
	return err
}

// produce is producer i: it adds tasks i, i+producers, … at random write partitions, entering
// through a random member.
func (w *work) produce(ctx context.Context, i int) error {
	rng := rand.New(rand.NewPCG(w.seed, uint64(i))) //nolint:gosec // seeded on purpose: the same flags make the same load
	for n := i; n < w.tasks; n += producers {
		t := Task{ID: fmt.Sprintf("t-%05d", n), Payload: fmt.Sprintf("job %d", n)}
		req := AddTaskRequest{Partition: w.tree.Pick(rng.Uint64()), Task: t}
		addr := w.c.addrs[rng.IntN(len(w.c.addrs))]
		if err := add(ctx, w.c.client, addr, req); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("add %s: %w", t.ID, err)
		}
	}
	return nil
}

// taskAdder sends an AddTask call; *Client is one.
type taskAdder interface {
	AddTask(ctx context.Context, addr string, req AddTaskRequest) error
}

// add sends req, and sends it again only after an answer that says the task was not accepted:
// after any other error the task may be somewhere already, and a second send could duplicate it.
func add(ctx context.Context, c taskAdder, addr string, req AddTaskRequest) error {
	var err error
	for range addAttempts {
		err = c.AddTask(ctx, addr, req)
		if !errors.Is(err, ErrNotAccepted) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return err
}

// poll is poller i: it polls random read partitions until the load ends.
func (w *work) poll(ctx context.Context, i int) error {
	rng := rand.New(rand.NewPCG(w.seed, uint64(i))) //nolint:gosec // seeded on purpose: the same flags make the same load
	for ctx.Err() == nil {
		addr := w.c.addrs[rng.IntN(len(w.c.addrs))]
		pctx, cancel := context.WithTimeout(ctx, pollDeadline)
		t, err := w.c.client.Poll(pctx, addr, PollRequest{Partition: w.tree.PickRead(rng.Uint64())})
		cancel()
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil && errors.Is(err, ErrNotAccepted):
			time.Sleep(10 * time.Millisecond) // no owner yet, or the views differ: ask again
		case err != nil:
			return fmt.Errorf("poll: %w", err)
		case t != nil:
			if err := w.deliver(t); err != nil {
				return err
			}
		}
	}
	return nil
}

// cluster is a group of members in one process: each has a node, a gRPC server and a listener.
type cluster struct {
	nodes     []*tenure.Node
	servers   []*Server
	grpcs     []*grpc.Server
	listeners []net.Listener
	addrs     []string
	clients   []*Client // the members' own clients, for forwarding
	client    *Client   // the program's client

	closeOnce sync.Once
	closeErr  error
}

// startCluster starts n members that place the partitions of tree with their resolvers. Every
// listener is open before the group is formed, so each member's address is known. ctx bounds the
// start only.
func startCluster(ctx context.Context, tree core.Tree, n int) (*cluster, error) {
	c := &cluster{client: NewClient()}
	members := make([]core.Member, n)
	var lc net.ListenConfig
	for i := range members {
		ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
		if err != nil {
			c.abort()
			return nil, fmt.Errorf("listen: %w", err)
		}
		c.listeners = append(c.listeners, ln)
		c.addrs = append(c.addrs, ln.Addr().String())
		members[i] = core.Member{ID: fmt.Sprintf("n%d", i+1), Addr: ln.Addr().String(), Slot: -1}
	}
	group, err := static.New(members)
	if err != nil {
		c.abort()
		return nil, fmt.Errorf("group: %w", err)
	}
	for i, self := range members {
		node, err := tenure.Start(ctx, tenure.Config{Self: self, Membership: group.Share()})
		if err != nil {
			c.abort()
			return nil, fmt.Errorf("start %s: %w", self.ID, err)
		}
		c.nodes = append(c.nodes, node)
		cl := NewClient()
		c.clients = append(c.clients, cl)
		s := NewServer(self.ID, node.Resolver(namespace), tree, cl)
		c.servers = append(c.servers, s)
		g := grpc.NewServer()
		s.Register(g)
		c.grpcs = append(c.grpcs, g)
		go func() { _ = g.Serve(c.listeners[i]) }() // Serve ends with Stop; a broken listener fails the calls
	}
	return c, nil
}

func (c *cluster) abort() {
	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()
	_ = c.Close(ctx)
}

// Close stops the servers, the clients and the members, and returns the first error. It is safe
// to call again.
func (c *cluster) Close(ctx context.Context) error {
	c.closeOnce.Do(func() { c.closeErr = c.close(ctx) })
	return c.closeErr
}

func (c *cluster) close(ctx context.Context) error {
	var errs []error
	for _, g := range c.grpcs {
		g.Stop()
	}
	for _, cl := range c.clients {
		errs = append(errs, cl.Close())
	}
	errs = append(errs, c.client.Close())
	for _, n := range c.nodes {
		errs = append(errs, n.Close(ctx))
	}
	for _, ln := range c.listeners {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
