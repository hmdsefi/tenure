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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// incarnationBytes is the number of random bytes that name one session of a member.
const incarnationBytes = 16

// Node is a running member of a group, as Start returns it. Its methods are safe for concurrent
// use.
//
// When the store reports the member's session lost, the node stops every shard it owns with a
// "lost" error, waits for every Run to return, and then goes on with a new incarnation: a new
// holder and a new session, under which it acquires its shards again. It stays in the group
// meanwhile, and keeps its watches.
type Node struct {
	cfg Config // checkConfig's result: defaults applied, Spaces and Self.Meta copied

	// holder is Self.ID + "/" + the incarnation in lowercase hex. After Start only the loop
	// changes it, under stepMu, when it starts a new incarnation (RUN-180).
	holder string

	epoch time.Time // the node's monotonic origin, taken after the incarnation is drawn
	obs   observer

	// ctx is the internal context, context.WithCancel(context.WithoutCancel(Start's ctx)):
	// every goroutine and every call after Start runs under it, and Close cancels it.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // every goroutine of the node

	view  atomic.Pointer[core.View] // the published merged view; nil before the first
	snap  atomic.Pointer[routes]    // the routing snapshot; never nil once Start has made n
	first chan struct{}             // closed when the watcher has taken the first view
	ended chan struct{}             // closed when the watcher returns

	// merge merges the latest membership view and slot data into the views the node steps and
	// publishes; refresh reads the slot data of a SlotStore once the session is open, else nil.
	merge   merger
	refresh *refresher

	// With a store: the session, the executor of the machine's store calls, and the loop that
	// drives the machine. The loop is the only goroutine that steps the machine.
	sessionOpen bool // Start's KeepAlive succeeded
	exec        *executor
	inbox       *inbox
	lp          *loop
	stepMu      sync.Mutex    // held while the loop carries a step out
	halted      bool          // a forced Close stopped driving the machine; guarded by stepMu
	deadline    atomic.Int64  // the session deadline in ns since epoch; only the loop writes it
	machineDone chan struct{} // closed when a machine is done and no new incarnation follows it

	// Holder observations (RUN-170, RUN-175): the freshness filter, which only the loop uses, the
	// readings that the store watch and the resyncs hand to the loop, and the resync requests.
	fresh    *freshness
	readings *readings
	resync   chan struct{} // buffer 1

	// The shard table: each space's shards by name, kept for the node's life, across machines.
	// sups counts the supervisor goroutines, which Close and failStart wait for after wg. runMu
	// guards each space's inRun.
	shards map[string]*spaceShards
	sups   sync.WaitGroup
	runMu  sync.Mutex

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

	holder, err := drawHolder(cfg.Self.ID, cfg.Rand)
	if err != nil {
		return nil, fmt.Errorf("tenure: draw incarnation: %w", err)
	}
	n := &Node{
		cfg:         cfg,
		holder:      holder,
		epoch:       time.Now(),
		obs:         nopObserver{},
		first:       make(chan struct{}),
		ended:       make(chan struct{}),
		inbox:       newInbox(),
		machineDone: make(chan struct{}),
		shards:      newShardTable(cfg.Spaces),
	}
	n.merge.post = n.postView
	n.snap.Store(newRoutes(nil, nil, nil))
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

// openSession runs Start's steps with a store (RUN-060 steps 4, 5 and 7): it reads and seeds
// the slots when the store is a SlotStore, opens the session with KeepAlive, and starts the loop
// with a new machine and, with a SlotStore, the slot refresher. The loop's first steps are
// SessionRenewed with KeepAlive's SentAt and ViewChanged with the latest merged view, which holds
// the slots read; openSession returns once both have been carried out, or when ctx ends first.
// Every call uses ctx. With spaces it also starts the resync goroutine and, when the store is a
// Watcher, the store watch (RUN-170).
func (n *Node) openSession(ctx context.Context) error {
	ss, slots := n.cfg.Store.(SlotStore)
	var readAt time.Time
	if slots {
		var err error
		if readAt, err = n.readSlots(ctx, ss); err != nil {
			return err
		}
	}
	_, first := n.inbox.take()
	sentAt := time.Since(n.epoch)
	if err := n.cfg.Store.KeepAlive(ctx, n.holder, n.cfg.Timing); err != nil {
		return fmt.Errorf("tenure: open session: %w", err)
	}
	n.sessionOpen = true

	const gen = 1
	m := n.newMachine(n.holder)
	n.exec = newExecutor(n.ctx, n.cfg.Store, n.cfg.Timing, n.epoch, n.obs.callFailed)
	n.fresh = newFreshness(n.holder, n.cfg.Spaces, n.exec.confirmStarted)
	n.readings = newReadings()
	n.resync = make(chan struct{}, 1)
	hook, _ := ctx.Value(stepHookKey{}).(func(stepRecord))
	n.lp = newLoop(n, m, gen, hook)
	steps := []loopInput{
		{in: core.SessionRenewed{SentAt: sentAt}, sentWall: n.epoch.Add(sentAt)},
		{in: core.ViewChanged{View: *first}},
	}
	started := make(chan struct{})
	if slots {
		n.refresh = newRefresher(n, ss) // before the loop, which asks it for reads
	}
	n.wg.Go(func() { n.lp.run(steps, started) })
	if slots {
		n.wg.Go(func() { n.refresh.run(readAt) })
	}
	if len(n.cfg.Spaces) > 0 {
		n.wg.Go(n.resyncHolders)
		if w, ok := n.cfg.Store.(Watcher); ok {
			n.wg.Go(func() { n.watchHolders(w) })
		}
	}
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

// readSlots reads the slot count and holders for Start (RUN-060 step 4, RUN-230) and hands them
// to the merger. When the stored count is 0 and Config.Slots is not, it seeds the count with
// SetSlots and reads again; a stored count that differs from a positive Config.Slots is reported
// and kept. It returns the start of the last read, from which the refresher's period runs.
func (n *Node) readSlots(ctx context.Context, ss SlotStore) (time.Time, error) {
	at := time.Now()
	count, holders, err := readSlotData(ctx, ss)
	if err != nil {
		return at, fmt.Errorf("tenure: read slots: %w", err)
	}
	want := n.cfg.Slots
	if count == 0 && want > 0 {
		if err := ss.SetSlots(ctx, want); err != nil {
			return at, fmt.Errorf("tenure: seed slots: %w", err)
		}
		at = time.Now()
		if count, holders, err = readSlotData(ctx, ss); err != nil {
			return at, fmt.Errorf("tenure: seed slots: %w", err)
		}
	}
	if want > 0 && count > 0 && count != want {
		n.obs.slotCountMismatch(want, count)
	}
	// Stamp 0: Start's reads come before every call of the executor, and no slot result is
	// stepped before them.
	n.merge.slots(0, count, holders)
	return at, nil
}

// drawHolder reads an incarnation of incarnationBytes bytes from r and returns the holder of
// member id with it: id, "/" and the incarnation in lowercase hex.
func drawHolder(id string, r io.Reader) (string, error) {
	var inc [incarnationBytes]byte
	if _, err := io.ReadFull(r, inc[:]); err != nil {
		return "", err
	}
	return id + "/" + hex.EncodeToString(inc[:]), nil
}

// newMachine returns a new machine of the node's member and spaces for holder.
func (n *Node) newMachine(holder string) *core.Machine {
	return core.NewMachine(core.MachineConfig{
		Holder: holder,
		Member: n.cfg.Self.ID,
		Spaces: spaceSpecs(n.cfg.Spaces),
		Timing: n.cfg.Timing,
	})
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
// returned, and closes the member's session. It returns nil once all of that is done. When the
// session is lost during the drain, there is nothing left to release or close: Drain returns nil
// once every Run has returned, and the node starts no new incarnation. The first call of Drain
// or Close starts the drain; later and concurrent calls wait for the same drain.
//
// When ctx ends first, Drain returns an error that wraps ctx's error and says what is left, and
// the drain goes on: a later Drain waits for it again. The error names the shards not released
// yet, those whose Run has not returned or whose release is still out, by space and then shard,
// as in "tenure: drain: context deadline exceeded (not released: orders/3, orders/5)". When every
// shard is released but the session is not closed yet, it ends in "(session not closed)". After
// Close, Drain returns ErrClosed.
func (n *Node) Drain(ctx context.Context) error {
	n.mu.Lock()
	if n.closed || (n.closing && n.drainDone == nil) {
		// After Close, or during a Close that skipped the drain (RUN-540).
		n.mu.Unlock()
		return ErrClosed
	}
	done := n.startDrainLocked()
	n.mu.Unlock()

	if n.awaitDrain(ctx, done) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("tenure: drain: %w%s", err, n.undrained())
	}
	return ErrClosed // Close stopped the node before the drain completed
}

// startDrainLocked starts the drain unless the first Drain or Close has already started it, and
// returns the channel closed when it completes. The caller holds n.mu, so that Close cannot wait
// for the goroutines before the drain goroutine is added.
func (n *Node) startDrainLocked() chan struct{} {
	if n.drainDone == nil {
		n.drainDone = make(chan struct{})
		n.obs.draining()
		n.wg.Go(n.drain)
	}
	return n.drainDone
}

// awaitDrain waits until done, the drain's completion, is closed, ctx ends or the internal context
// is done, and reports whether the drain completed. A drain that completes at the instant ctx
// ends wins. A nil done never completes.
func (n *Node) awaitDrain(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
	case <-n.ctx.Done():
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// undrained returns RUN-510's account of what a drain that has not completed has left, as the
// end of Drain's deadline error; see undrainedLocked.
func (n *Node) undrained() string {
	n.stepMu.Lock()
	defer n.stepMu.Unlock()
	return n.undrainedLocked()
}

// undrainedLocked returns " (not released: <space>/<shard>, …)" for the shards still published
// or with a Release queued or in flight, by space name and then shard (RUN-510); else
// " (session not closed)" while the machine is not done; else "", as without a store, where the
// drain waits for SetDraining alone. The caller holds n.stepMu, under which the loop publishes,
// unpublishes and queues Release in one step, so a shard between its RunStopped and its Release
// is never missed. A shard whose Release failed and that the machine is yet to release again is
// neither published nor out, and is not listed.
func (n *Node) undrainedLocked() string {
	if n.exec == nil {
		return ""
	}
	out := n.exec.releasing()
	names := make([]string, 0, len(n.shards))
	for name := range n.shards {
		names = append(names, name)
	}
	slices.Sort(names)
	var left []string
	for _, name := range names {
		sp := n.shards[name]
		for id := range sp.local {
			if sp.local[id].Load() != nil || out[laneKey{kind: laneShard, space: name, shard: id}] {
				left = append(left, name+"/"+strconv.Itoa(id))
			}
		}
	}
	if len(left) > 0 {
		return " (not released: " + strings.Join(left, ", ") + ")"
	}
	select {
	case <-n.machineDone:
		return ""
	default:
		return " (session not closed)"
	}
}

// drain is the drain goroutine (RUN-500). With a store it first posts Drain to the loop, so the
// machine hands everything over and closes the session. It calls SetDraining(true) with a
// context bounded by RetryPeriod, and again RetryPeriod after each failed attempt started, until
// one succeeds. The drain completes once that call has succeeded and, with a store, the loop has
// closed n.machineDone: the machine's session is closed, or it was lost and the machine is done
// with every Run returned. After a loss the loop starts no new incarnation once a drain has been
// requested, and nothing closes the lost session (RUN-190). It ends early when the internal
// context is done or a forced Close has halted the node.
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
	// Under stepMu, so that the drain completes either before a forced Close's halt or not at all.
	n.stepMu.Lock()
	defer n.stepMu.Unlock()
	if n.halted {
		return
	}
	n.obs.drained()
	close(n.drainDone)
}

// drainRequested reports whether Drain or Close has started the drain.
func (n *Node) drainRequested() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.drainDone != nil
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
// It returns nil once all of that is done. Later calls return ErrClosed, and so do Route, Local,
// Drain and the lookups of every Resolver the node returned; a Shard the node handed to Run
// keeps returning its stop error.
//
// When ctx ends before the drain completes, or is done already when Close is called, Close
// neither closes the member's session nor leaves the group: the store expires the session, so no
// other member takes one of the shards while a Run of this member may still act on it. A ctx
// done at the call does not start the drain at all. Close then stops every shard the member
// still owns, with a stop error ending in "closed", cancels the shard's Run with that error,
// stops the node and returns an error that wraps ctx's error and says what the drain had left,
// as Drain does, such as "tenure: close: context deadline exceeded (not released: orders/3)". It
// does not wait for a Run that has not returned: that Run's supervisor ends when it returns, and
// makes no call of its own then.
//
// No call of the membership or the store starts after Close has returned. Close waits for the
// calls in flight, whose context it cancels, so a call that does not return once its context
// is done holds Close up.
func (n *Node) Close(ctx context.Context) error {
	n.mu.Lock()
	if n.closing {
		n.mu.Unlock()
		return ErrClosed
	}
	n.closing = true
	done := n.drainDone // the drain a Drain started, if any: a ctx done already starts none
	if ctx.Err() == nil {
		done = n.startDrainLocked()
	}
	n.mu.Unlock()

	if !n.awaitDrain(ctx, done) {
		return n.forceClose(ctx)
	}
	n.leave(ctx)
	n.closeResolvers()
	n.cancel()
	n.wait()
	n.closeRoutes()
	n.markClosed()
	n.obs.closed(ctx, "drained")
	return nil
}

// forceClose is RUN-530's path, for a Close whose ctx ended before the drain completed (MCH O7).
// halt stops driving the machine and stops the published shards; then come observer.closed with
// "deadline", the resolvers closed, the internal context canceled, and a wait for every goroutine
// but the supervisors whose Run has not returned. Neither CloseSession nor Leave is called. It
// returns Close's error.
func (n *Node) forceClose(ctx context.Context) error {
	detail := n.halt()
	n.obs.closed(ctx, "deadline")
	n.closeResolvers()
	n.cancel()
	n.wg.Wait() // the loop, and with it every lane, has returned: no supervisor starts any more
	n.waitIdleSupervisors()
	n.closeRoutes()
	n.markClosed()
	return fmt.Errorf("tenure: close: %w%s", ctx.Err(), detail)
}

// halt stops driving the machine: from now on the loop carries out no step, the executor starts
// no store call (executor.halt), and the drain does not complete. In the same instant it takes
// what the drain has left (undrainedLocked), which it returns, and stops every published shard:
// with reason "closed" while the member owns it, reported to observer.shardClosed, and otherwise
// with the error it already has, or "expired" past the session deadline. It cancels each shard's
// Run with that error, before the internal context, so that Run sees the stop error as its
// context's cause (RUN-320).
func (n *Node) halt() string {
	n.stepMu.Lock()
	defer n.stepMu.Unlock()
	detail := n.undrainedLocked()
	n.halted = true
	if n.exec != nil {
		n.exec.halt() // no queued call, such as a CloseSession behind a KeepAlive, goes out
	}
	for _, sp := range n.shards {
		for id := range sp.local {
			sh, r := sp.local[id].Load(), sp.runs[id]
			if sh == nil || r.sh != sh {
				continue
			}
			var err error
			if sh.Check() == nil {
				err = sh.stop(reasonClosed)
				n.obs.shardClosed(sh)
			} else {
				err = sh.stop(reasonExpired) // the error it has, or the deadline's
			}
			r.cancel(err)
		}
	}
	return detail
}

// reasonClosed is the stop reason of a shard that a Close whose ctx ended first stopped.
const reasonClosed = "closed"

// gateRun returns the Run of sp for shard id, wrapped so that the node knows whether a Run of
// the shard is in progress (sp.inRun): forceClose waits for the shard's supervisors only while
// none is. A call whose ctx is done already does not start Run, so no Run starts after
// waitIdleSupervisors has looked. At most one Run of a shard runs at a time (RUN-300).
func (n *Node) gateRun(sp *spaceShards, id int) func(context.Context, *Shard) error {
	run := sp.space.Run
	return func(ctx context.Context, sh *Shard) error {
		n.runMu.Lock()
		if ctx.Err() != nil {
			n.runMu.Unlock()
			return context.Cause(ctx)
		}
		sp.inRun[id] = true
		n.runMu.Unlock()
		defer func() {
			n.runMu.Lock()
			sp.inRun[id] = false
			n.runMu.Unlock()
		}()
		return run(ctx, sh)
	}
}

// waitIdleSupervisors waits, once the internal context is done and the loop has returned, for
// the supervisors of every shard with no Run in progress (RUN-530, RUN-550). They end at once:
// their contexts are done, and the earlier supervisors of the shard, for which each waits, have
// no Run in progress either. A shard whose Run has not returned keeps its supervisors, which end
// once that Run returns.
func (n *Node) waitIdleSupervisors() {
	var wait []chan struct{}
	n.runMu.Lock()
	for _, sp := range n.shards {
		for id, r := range sp.runs {
			if r.done != nil && !sp.inRun[id] {
				wait = append(wait, r.done)
			}
		}
	}
	n.runMu.Unlock()
	for _, done := range wait {
		<-done
	}
}

// markClosed records that Close has stopped the node.
func (n *Node) markClosed() {
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
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
