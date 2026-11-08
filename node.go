// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// incarnationBytes is the number of random bytes that name one session of a member.
const incarnationBytes = 16

// Node is a running member of a group, as Start returns it. Its methods are safe for concurrent
// use.
type Node struct {
	cfg    Config    // checkConfig's result: defaults applied, Spaces and Self.Meta copied
	holder string    // Self.ID + "/" + the incarnation in lowercase hex
	epoch  time.Time // the node's monotonic origin, taken after the incarnation is drawn
	obs    observer

	// ctx is the internal context, context.WithCancel(context.WithoutCancel(Start's ctx)):
	// every goroutine and every call after Start runs under it, and Close cancels it.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // every goroutine of the node

	view  atomic.Pointer[core.View] // the published merged view; nil before the first
	first chan struct{}             // closed when the watcher has taken the first view
	ended chan struct{}             // closed when the watcher returns

	// The watcher's own state: the Version of the latest view it took, once it took one.
	offered        bool
	offeredVersion uint64

	// With a store: the session, the executor of the machine's store calls, and the loop that
	// drives the machine. The loop is the only goroutine that steps the machine.
	sessionOpen bool // Start's KeepAlive succeeded
	exec        *executor
	inbox       *inbox
	lp          *loop
	stepMu      sync.Mutex    // held while the loop carries a step out
	deadline    atomic.Int64  // the session deadline in ns since epoch; only the loop writes it
	machineDone chan struct{} // closed when the machine's Status first reports Done

	// The shard table: each space's shards by name, kept for the node's life, across machines.
	// sups counts the supervisor goroutines, which Close and failStart wait for after wg.
	shards map[string]*spaceShards
	sups   sync.WaitGroup

	mu        sync.Mutex
	left      bool          // leave has been called: the watcher calls Watch no more
	watchCall chan struct{} // open while the watcher's Watch call is in flight, else nil
	drainDone chan struct{} // nil before the first Drain or Close; closed when the drain completes
	closing   bool          // Close has been called
	closed    bool          // Close has returned

	resMu     sync.Mutex
	resolvers []*Resolver // every open Resolver that Resolver returned
	resClosed bool        // Close has closed the resolvers: Resolver returns closed ones
}

// Start starts a node for cfg and returns it once the member has joined the group, the node
// holds a valid view of it and, with a Store, the member's session is open. It fails with an
// error that starts with "tenure: invalid config: " when cfg is not valid, and otherwise with the
// step that failed, such as "tenure: join: " and the error of Join, or "tenure: open session: "
// and the error of the store's KeepAlive. A failed Start closes the session again if it opened
// one, leaves the group again if it joined, and leaves no goroutine behind.
//
// ctx bounds Start only: Start's own calls of the membership and the store use it, and the node
// keeps running after ctx ends, until Close. Start does not wait for the shards of Spaces: the
// node acquires and runs them in the background, never under ctx.
func Start(ctx context.Context, cfg Config) (*Node, error) {
	cfg, err := checkConfig(cfg)
	if err != nil {
		return nil, err
	}

	var inc [incarnationBytes]byte
	if _, err := io.ReadFull(cfg.Rand, inc[:]); err != nil {
		return nil, fmt.Errorf("tenure: draw incarnation: %w", err)
	}
	n := &Node{
		cfg:         cfg,
		holder:      cfg.Self.ID + "/" + hex.EncodeToString(inc[:]),
		epoch:       time.Now(),
		obs:         nopObserver{},
		first:       make(chan struct{}),
		ended:       make(chan struct{}),
		inbox:       newInbox(),
		machineDone: make(chan struct{}),
		shards:      newShardTable(cfg.Spaces),
	}
	n.obs.start(n.holder)

	if err := cfg.Membership.Join(ctx, cfg.Self); err != nil {
		err = fmt.Errorf("tenure: join: %w", err)
		n.obs.startFailed(ctx, err)
		return nil, err
	}
	n.ctx, n.cancel = context.WithCancel(context.WithoutCancel(ctx))
	// Watch runs on the internal context, which outlives ctx; ctx ending during the call cancels
	// it, so that ctx still bounds Start.
	stop := context.AfterFunc(ctx, n.cancel)
	ch, err := cfg.Membership.Watch(n.ctx)
	switch {
	case !stop():
		return nil, n.failStart(ctx, fmt.Errorf("tenure: watch: %w", ctx.Err()))
	case err != nil:
		return nil, n.failStart(ctx, fmt.Errorf("tenure: watch: %w", err))
	case ch == nil:
		return nil, n.failStart(ctx, errors.New("tenure: watch: nil channel"))
	}
	n.wg.Go(func() { n.watchViews(ch) })

	select {
	case <-n.first:
	case <-n.ended:
		return nil, n.failStart(ctx, fmt.Errorf("tenure: watch: %w", ErrClosed))
	case <-ctx.Done():
		return nil, n.failStart(ctx, fmt.Errorf("tenure: first view: %w", ctx.Err()))
	}
	if cfg.Store != nil {
		if err := n.openSession(ctx); err != nil {
			return nil, n.failStart(ctx, err)
		}
	}
	return n, nil
}

// openSession runs Start's steps with a store (RUN-060 steps 4, 5 and 7): it reads the slots
// when the store is a SlotStore, opens the session with KeepAlive, and starts the loop with a new
// machine. The loop's first steps are SessionRenewed with KeepAlive's SentAt and ViewChanged with
// the first merged view; openSession returns once both have been carried out, or when ctx ends
// first. Every call uses ctx.
func (n *Node) openSession(ctx context.Context) error {
	_, first := n.inbox.take()
	if ss, ok := n.cfg.Store.(SlotStore); ok {
		// The views get the slot data with slots (1.5-12); this read only checks the store.
		if _, _, err := ss.Slots(ctx); err != nil {
			return fmt.Errorf("tenure: read slots: %w", err)
		}
	}
	sentAt := time.Since(n.epoch)
	if err := n.cfg.Store.KeepAlive(ctx, n.holder, n.cfg.Timing); err != nil {
		return fmt.Errorf("tenure: open session: %w", err)
	}
	n.sessionOpen = true

	const gen = 1
	m := core.NewMachine(core.MachineConfig{
		Holder: n.holder,
		Member: n.cfg.Self.ID,
		Spaces: spaceSpecs(n.cfg.Spaces),
		Timing: n.cfg.Timing,
	})
	n.exec = newExecutor(n.ctx, n.cfg.Store, n.cfg.Timing, n.epoch, n.obs.callFailed)
	hook, _ := ctx.Value(stepHookKey{}).(func(stepRecord))
	n.lp = newLoop(n, m, gen, hook)
	steps := []loopInput{
		{in: core.SessionRenewed{SentAt: sentAt}, sentWall: n.epoch.Add(sentAt)},
		{in: core.ViewChanged{View: *first}},
	}
	started := make(chan struct{})
	n.wg.Go(func() { n.lp.run(steps, started) })
	select {
	case <-started:
		return nil
	case <-ctx.Done():
	}
	select {
	case <-started: // first steps carried out at the instant ctx ended win
		return nil
	default:
		return fmt.Errorf("tenure: start: %w", ctx.Err())
	}
}

// spaceSpecs returns the machine's description of spaces, in the same order.
func spaceSpecs(spaces []Space) []core.SpaceSpec {
	specs := make([]core.SpaceSpec, len(spaces))
	for i, sp := range spaces {
		specs[i] = core.SpaceSpec{Name: sp.Name, Shards: sp.Shards, Fenced: sp.Fenced}
	}
	return specs
}

// failStart runs RUN-070's cleanup after Join succeeded and returns err. Once the session is
// open it first stops the loop and the executor, waits for every Run the loop may have started
// to return, and closes the session with a fresh context bounded by RetryPeriod. Then it calls
// Leave with a fresh context bounded by RetryPeriod, cancels the internal context and waits for
// every goroutine.
func (n *Node) failStart(ctx context.Context, err error) error {
	fresh := context.WithoutCancel(ctx)
	if n.sessionOpen {
		n.cancel()
		n.wait()
		n.closeSession(fresh)
	}
	n.leave(fresh)
	n.cancel()
	n.wait()
	n.obs.startFailed(ctx, err)
	return err
}

// wait waits for every goroutine of the node: first the loop and the others, then the
// supervisors, which the loop may start until it returns.
func (n *Node) wait() {
	n.wg.Wait()
	n.sups.Wait()
}

// closeSession calls CloseSession with ctx bounded by RetryPeriod. A failure is reported and
// otherwise ignored: the store then expires the session.
func (n *Node) closeSession(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, n.cfg.Timing.RetryPeriod)
	defer cancel()
	if err := n.cfg.Store.CloseSession(ctx, n.holder); err != nil {
		n.obs.callFailed(callResult{act: core.CloseSession{}, err: err})
	}
}

// Drain hands the member's work over: it marks the member as draining in the group and, with a
// Store, stops the Run of every shard the member owns, releases each shard once its Run has
// returned, and closes the member's session. It returns nil once all of that is done. The first
// call of Drain or Close starts the drain; later and concurrent calls wait for the same drain.
// When ctx ends first, Drain returns ctx's error and the drain goes on. After Close, Drain
// returns ErrClosed.
func (n *Node) Drain(ctx context.Context) error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ErrClosed
	}
	if n.drainDone == nil {
		// Under n.mu, so that Close cannot wait for the goroutines before this one is added.
		n.drainDone = make(chan struct{})
		n.obs.draining()
		n.wg.Go(n.drain)
	}
	done := n.drainDone
	n.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
	case <-n.ctx.Done():
	}
	select {
	case <-done: // a drain that completed at the same instant wins
		return nil
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrClosed // Close stopped the node before the drain completed
}

// drain is the drain goroutine (RUN-500). With a store it first posts Drain to the loop, so the
// machine hands everything over and closes the session. It calls SetDraining(true) with a
// context bounded by RetryPeriod, and again RetryPeriod after each failed attempt started, until
// one succeeds. The drain completes once that call has succeeded and, with a store, the
// machine's Status reports Done. It ends early when the internal context is done.
func (n *Node) drain() {
	if n.cfg.Store != nil {
		n.inbox.post(core.Drain{})
	}
	if !n.setDraining() {
		return
	}
	if n.cfg.Store != nil {
		select {
		case <-n.machineDone:
		case <-n.ctx.Done():
			return
		}
	}
	n.obs.drained()
	close(n.drainDone)
}

// setDraining calls SetDraining(true) until a call succeeds, as drain says, and reports whether
// one did before the internal context was done.
func (n *Node) setDraining() bool {
	period := n.cfg.Timing.RetryPeriod
	for {
		start := time.Now()
		ctx, cancel := context.WithTimeout(n.ctx, period)
		err := n.cfg.Membership.SetDraining(ctx, true)
		cancel()
		if err == nil {
			return true
		}
		if n.ctx.Err() != nil {
			return false
		}
		n.obs.membershipFailed("SetDraining", err)
		t := time.NewTimer(time.Until(start.Add(period)))
		select {
		case <-t.C:
		case <-n.ctx.Done():
			t.Stop()
			return false
		}
	}
}

// Close drains the node as Drain does, leaves the group, and stops every goroutine of the node.
// It returns nil once all of that is done; later calls return ErrClosed. When ctx ends before
// the drain completes, Close does not leave the group: it stops the node and returns an error
// that wraps ctx's error.
func (n *Node) Close(ctx context.Context) error {
	n.mu.Lock()
	if n.closing {
		n.mu.Unlock()
		return ErrClosed
	}
	n.closing = true
	n.mu.Unlock()

	reason := "drained"
	err := n.Drain(ctx)
	if err == nil {
		n.leave(ctx)
	} else {
		reason, err = "deadline", fmt.Errorf("tenure: close: %w", err)
	}
	n.closeResolvers()
	n.cancel()
	n.wait()

	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	n.obs.closed(ctx, reason)
	return err
}

// Resolver returns a Resolver that ranks keys in namespace over the node's views, the same views
// the node itself follows, with no watch of its own. It holds the node's current view at once,
// and places keys with core.NewRendezvous. Close of the node closes it; after Close, Resolver
// returns a closed Resolver, whose lookups return ErrClosed.
func (n *Node) Resolver(namespace string) *Resolver {
	r := newResolver(namespace, core.NewRendezvous)
	n.resMu.Lock()
	defer n.resMu.Unlock()
	if n.resClosed {
		_ = r.Close()
		return r
	}
	if v := n.view.Load(); v != nil {
		r.update(*v)
	}
	r.onClose = n.dropResolver
	n.resolvers = append(n.resolvers, r)
	return r
}

// feedResolvers gives v, a view the node publishes, to every open Resolver of the node.
func (n *Node) feedResolvers(v core.View) {
	n.resMu.Lock()
	defer n.resMu.Unlock()
	for _, r := range n.resolvers {
		r.update(v)
	}
}

// dropResolver removes r, which has been closed, from the node's resolvers.
func (n *Node) dropResolver(r *Resolver) {
	n.resMu.Lock()
	defer n.resMu.Unlock()
	n.resolvers = slices.DeleteFunc(n.resolvers, func(x *Resolver) bool { return x == r })
}

// closeResolvers closes every Resolver of the node (RUN-540), and makes Resolver return closed
// ones from now on.
func (n *Node) closeResolvers() {
	n.resMu.Lock()
	n.resClosed = true
	rs := n.resolvers
	n.resolvers = nil
	n.resMu.Unlock()
	for _, r := range rs {
		_ = r.Close()
	}
}
