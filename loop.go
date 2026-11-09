// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// stepRecord is what one step of the loop did. The loop hands it to the observer and to the
// test hook after the step's actions have been executed.
type stepRecord struct {
	// Gen is the generation of the machine that took the step.
	Gen uint64

	// Now is the reading the input carries: time.Since(epoch) when the loop read it, or for a
	// Tick the reading of the wake it steps.
	Now time.Duration

	// Input is the input stepped, with Now set.
	Input core.Input

	// Before and After are the machine's Status before and after the step. Before is the
	// previous step's After.
	Before, After core.Status

	// Actions are the actions the step returned, in order.
	Actions []core.Action

	// SentWall is the wall time matching SentAt, epoch.Add(SentAt), for an input that answers
	// a KeepAlive, also a failed one. It is zero otherwise.
	SentWall time.Time

	// Err is the error of the store call the input answers, nil otherwise.
	Err error
}

// stepHookKey is the context key of the step hook: a func(stepRecord) that Start reads from its
// ctx and the loop calls after each step's observer call. Only tests set it.
type stepHookKey struct{}

// loopInput is one input for the loop to step, with the facts about it that the step record
// reports.
type loopInput struct {
	in       core.Input
	sentWall time.Time
	err      error

	// sh is the shard whose supervisor ended, for the RunStopped it posted: once the step is
	// carried out, the loop unpublishes sh if it is still the shard's published one.
	sh *Shard
}

// inbox holds the inputs that goroutines other than the loop hand to it: the latest merged view,
// of which only the latest counts, and posted inputs such as Drain and RunStopped, stepped in
// post order. It is safe for concurrent use.
type inbox struct {
	mu     sync.Mutex
	view   *core.View
	inputs []loopInput
	ready  chan struct{} // buffer 1: a value means the inbox may hold something
}

// newInbox returns an empty inbox.
func newInbox() *inbox {
	return &inbox{ready: make(chan struct{}, 1)}
}

// postView replaces the view waiting in b with v.
func (b *inbox) postView(v core.View) {
	b.mu.Lock()
	b.view = &v
	b.mu.Unlock()
	b.notify()
}

// post adds in after the inputs waiting in b.
func (b *inbox) post(in core.Input) {
	b.postInput(loopInput{in: in})
}

// postStopped adds RunStopped for sh, whose supervisor has ended, after the inputs waiting in b.
func (b *inbox) postStopped(sh *Shard) {
	b.postInput(loopInput{in: core.RunStopped{Space: sh.Space, Shard: sh.ID, Term: sh.Term}, sh: sh})
}

// postInput adds in after the inputs waiting in b.
func (b *inbox) postInput(in loopInput) {
	b.mu.Lock()
	b.inputs = append(b.inputs, in)
	b.mu.Unlock()
	b.notify()
}

// notify lets the loop know that b holds something.
func (b *inbox) notify() {
	select {
	case b.ready <- struct{}{}:
	default:
	}
}

// take empties b and returns the posted inputs in post order and the latest view, nil when none
// is waiting.
func (b *inbox) take() ([]loopInput, *core.View) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ins, v := b.inputs, b.view
	b.inputs, b.view = nil, nil
	return ins, v
}

// loop is the loop goroutine's state (RUN-100): the machine, of which the loop is the only
// caller, and its wake timer. Only the loop goroutine touches it once run has started.
//
// A lost session is final for its machine. Once the machine is done after the loss, the loop
// starts a new incarnation (RUN-180): a new holder, the next generation, a new machine and a new
// session, keeping the membership, the watches, the shard table and the freshness floors. A drain
// requested by then completes instead (RUN-190).
type loop struct {
	n    *Node
	m    *core.Machine
	gen  uint64
	hook func(stepRecord)

	status core.Status // the machine's Status after the latest step
	done   bool        // n.machineDone has been closed

	// recoverDue reports that the machine is done after a lost session and no drain has been
	// requested: run starts a new incarnation once the input at hand has been carried out.
	recoverDue bool

	// opening reports that the machine of a new incarnation waits for the answer to its first
	// KeepAlive, which it steps first; until then the loop steps nothing else into it.
	opening bool

	// unrenewed reports that no renewal of a new incarnation's session has been accepted yet: the
	// first one makes a resync of every space due (RUN-170). Start's machine has none.
	unrenewed bool

	// bump is the number of new incarnations opened so far. Each opening steps the published view
	// with its Version raised by one (open), and every later merged view is stepped with its
	// Version raised by bump, so the Versions the loop steps and publishes keep rising across
	// incarnations, however many come between two views of the membership.
	bump uint64

	// opened is when the latest new incarnation sent its first KeepAlive, zero before the first:
	// the next one draws no sooner than RetryPeriod later (draw).
	opened time.Time

	// assign, when not nil, is the assignment of each space that the loop computed itself for the
	// latest stepped view, which the machine did not apply: a machine whose session is closing or
	// closed takes no view, so its Assignment stays with an older one. The routing snapshot then
	// takes assign instead (RUN-200, the latest view wins).
	assign map[string][]string

	timer  *time.Timer
	armed  bool          // the timer is set for wakeAt and its value not yet received
	wakeAt time.Duration // the reading the timer is set for
}

// newLoop returns the loop of n for machine m of generation gen. hook may be nil.
func newLoop(n *Node, m *core.Machine, gen uint64, hook func(stepRecord)) *loop {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return &loop{n: n, m: m, gen: gen, hook: hook, status: m.Status(), timer: t}
}

// run is the loop goroutine. It steps first, closes started, and then steps every input that
// reaches it, until the internal context is done. After the input that leaves a lost machine
// done, it starts a new incarnation. While a new machine waits for its first KeepAlive's answer,
// views, posted inputs and readings wait. On return it waits for the executor's lanes.
func (l *loop) run(first []loopInput, started chan<- struct{}) {
	n := l.n
	defer n.exec.wait()
	defer l.timer.Stop()
	for _, in := range first {
		l.step(in)
	}
	close(started)
	for {
		if l.recoverDue {
			l.recover()
		}
		var inbox, rds <-chan struct{}
		if !l.opening {
			inbox, rds = n.inbox.ready, n.readings.ready
		}
		select {
		case <-n.ctx.Done():
			return
		case <-l.timer.C:
			l.armed = false
			l.tickUntil(time.Since(n.epoch))
			l.rearm()
		case <-n.exec.ready():
			for _, r := range n.exec.take() {
				l.deliver(r)
				l.slotResult(r)
			}
		case <-rds:
			for _, rd := range n.readings.take() {
				l.observe(rd)
			}
		case <-inbox:
			ins, v := n.inbox.take()
			for _, in := range ins {
				l.step(in)
			}
			if v != nil && !n.merge.stale(v.Version) {
				v.Version += l.bump
				l.step(loopInput{in: core.ViewChanged{View: *v}})
			}
		}
	}
}

// step steps in at the current reading, after a Tick for every wake at or before it
// (tick-first, RUN-120), so the machine sees a deadline pass before anything that arrives at or
// after it.
func (l *loop) step(in loopInput) {
	now := time.Since(l.n.epoch)
	l.tickUntil(now)
	in.in = l.at(in.in, now)
	l.apply(in, now)
}

// tickUntil steps Tick{Now: w} for each wake w of the machine at or before now, in order.
func (l *loop) tickUntil(now time.Duration) {
	for {
		w, ok := l.m.NextWake()
		if !ok || w > now {
			return
		}
		l.apply(loopInput{in: core.Tick{Now: w}}, w)
	}
}

// apply steps in, whose Now is set to now, and carries the step out (RUN-100): under n.stepMu it
// executes the actions in order, publishes a stepped view, computes the view's assignments itself
// when the machine did not apply the view, publishes the routing snapshot when the view or an
// assignment changed, records a stepped view for SetSlots (n.stepped), unpublishes the shard of a
// stepped RunStopped that is still published, and reports the step to the observer and the hook.
// A step that loses the session first drops the lost machine's calls in the executor, before any
// of that, and runs lose once it is carried out. apply then re-arms the timer. The first time
// the machine is done, it closes n.machineDone, unless the machine is done after a lost session
// and no drain has been requested: then a new incarnation is due instead.
func (l *loop) apply(in loopInput, now time.Duration) {
	n := l.n
	acts := l.m.Step(in.in)
	rec := stepRecord{
		Gen:      l.gen,
		Now:      now,
		Input:    in.in,
		Before:   l.status,
		After:    l.m.Status(),
		Actions:  acts,
		SentWall: in.sentWall,
		Err:      in.err,
	}
	l.status = rec.After
	lost := rec.Before.Session != core.SessionStateLost && rec.After.Session == core.SessionStateLost
	if lost {
		// Before anything else of the step, so that no call of the lost holder goes out once the
		// step is done (MCH O7).
		n.exec.dropOlder(l.gen + 1)
	}

	n.stepMu.Lock()
	if n.halted { // a forced Close: the machine is no longer driven (RUN-530)
		n.stepMu.Unlock()
		return
	}
	for _, a := range acts {
		l.execute(a)
	}
	if v, ok := in.in.(core.ViewChanged); ok {
		n.publish(v.View)
		l.assign = nil
		if !viewApplied(rec, v.View) {
			l.assign = assignments(v.View, v.Wall, n.cfg.Spaces)
		}
	}
	l.publishRoutes()
	if v, ok := in.in.(core.ViewChanged); ok {
		// After its routes, so that SetSlots returns once Route follows the view. The merger's
		// Version of the view is its own less bump.
		n.stepped.record(v.View.Version-l.bump, v.View.Slots)
	}
	if in.sh != nil {
		n.shards[in.sh.Space].local[in.sh.ID].CompareAndSwap(in.sh, nil)
	}
	n.obs.step(rec)
	if l.hook != nil {
		l.hook(rec)
	}
	n.stepMu.Unlock()

	if lost {
		l.lose(now)
	}
	l.rearm()
	if l.status.Done && !l.done && !l.recoverDue {
		if l.status.Session == core.SessionStateLost && !n.drainRequested() {
			l.recoverDue = true
		} else {
			l.finish()
		}
	}
}

// finish closes n.machineDone: the machine is done and no new incarnation follows it.
func (l *loop) finish() {
	l.done = true
	close(l.n.machineDone)
}

// lose runs right after the step that lost the session. That step has already dropped the queued
// calls of this machine and discarded the results of its calls in flight, as soon as the machine
// returned from it, before its actions and its report: one session lane serves every generation,
// and no call of the lost holder, such as a Release or a CloseSession, may go out once the step
// is done. The machine still waits for the answer to each Acquire it sent, and that answer is now
// discarded, so lose steps Acquired with no grant at now for every acquiring shard in its place;
// after a loss that only ends the wait. A record that the discarded call granted names the lost
// holder, so it is takeable.
func (l *loop) lose(now time.Duration) {
	var owed []core.Acquired
	for _, sp := range l.status.Spaces {
		for i, sh := range sp.Shards {
			if sh.State == core.ShardStateAcquiring {
				owed = append(owed, core.Acquired{Space: sp.Name, Shard: i, Now: now})
			}
		}
	}
	for _, a := range owed {
		l.apply(loopInput{in: a}, now)
	}
}

// recover starts a new incarnation (RUN-180) once the machine is done after a lost session. It
// waits for every supervisor in the shard table to end, so that no Run of an earlier machine is
// left; draws a new incarnation from Rand (draw); makes the new holder the node's and reports the
// previous one to the observer; moves to the next generation; clears the session deadline; starts
// the freshness state of a new machine, which keeps the floors; makes the new machine; and sends
// KeepAlive for the new holder through the executor. The new machine steps that KeepAlive's
// answer first, then the latest merged view (open). A drain requested by the time of a draw
// completes instead (RUN-190). The internal context ending stops recover.
func (l *loop) recover() {
	n := l.n
	l.recoverDue = false
	for _, sp := range n.shards {
		for _, r := range sp.runs {
			if r.done == nil {
				continue
			}
			select {
			case <-r.done:
			case <-n.ctx.Done():
				return
			}
		}
	}
	holder, ok := l.draw()
	if !ok {
		return
	}

	n.stepMu.Lock()
	prev := n.holder
	n.holder = holder
	n.stepMu.Unlock()
	n.obs.newIncarnation(prev)

	l.gen++
	n.exec.dropOlder(l.gen) // done at the loss already; repeated, it changes nothing
	n.deadline.Store(0)
	n.fresh.newMachine(holder)
	if l.assign == nil {
		// Routing keeps the lost machine's assignment until the new machine has taken a view.
		l.assign = make(map[string][]string, len(n.cfg.Spaces))
		for _, sp := range n.cfg.Spaces {
			l.assign[sp.Name] = l.m.Assignment(sp.Name)
		}
	}
	l.m = n.newMachine(holder)
	l.status = l.m.Status()
	l.opening, l.unrenewed = true, true
	l.opened = time.Now()
	n.exec.do(l.gen, holder, core.KeepAlive{})
	l.rearm()
}

// draw returns a new holder drawn from Rand, as Start draws one. After a failed read it reads
// again RetryPeriod after the start of the failed one. The first read comes no sooner than
// RetryPeriod after the previous new incarnation's KeepAlive, so a store that ends each new
// session at once gets at most one KeepAlive per RetryPeriod, as a live session does. Before each
// read it checks for a drain request: when there is one, it completes the drain with finish and
// returns false, as it does when the internal context ends.
func (l *loop) draw() (string, bool) {
	n := l.n
	period := n.cfg.Timing.RetryPeriod
	next := time.Now()
	if !l.opened.IsZero() {
		next = l.opened.Add(period)
	}
	for {
		if d := time.Until(next); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-t.C:
			case <-n.ctx.Done():
				t.Stop()
				return "", false
			}
		}
		if n.drainRequested() {
			l.finish()
			return "", false
		}
		at := time.Now()
		holder, err := drawHolder(n.cfg.Self.ID, n.cfg.Rand)
		if err == nil {
			return holder, true
		}
		next = at.Add(period)
	}
}

// open steps r, the answer to the first KeepAlive of a new incarnation's machine, and then the
// latest merged view with the Version after it, so that the view is newer than every view the
// node has published; bump grows by one, so that later views stay above it. Until then the
// machine has stepped nothing. The view's step moves routing to the new machine's assignment.
func (l *loop) open(r callResult) {
	l.opening = false
	for _, in := range r.inputs {
		l.step(loopInput{in: in, sentWall: r.sentWall, err: r.err})
	}
	l.renewed()
	v := *l.n.view.Load()
	v.Version++
	l.bump++
	l.step(loopInput{in: core.ViewChanged{View: v}})
}

// renewed makes a resync of every space due once the first renewal of a new incarnation's
// session has been accepted (RUN-170). The resync waits for the Confirm calls queued so far, as
// one after failed renewals does.
func (l *loop) renewed() {
	if l.unrenewed && l.status.LastFailure == core.CauseNone && l.status.Session != core.SessionStateOpening {
		l.unrenewed = false
		f := l.n.fresh
		f.wait, f.resyncAt = true, f.issued
	}
}

// execute carries out one action (RUN-130). Store calls go to the executor, which never blocks;
// ExtendDeadline raises the session deadline that unfenced shards check; StartRun and StopRun
// start and stop a shard's supervisor; Degrade and Restore change nothing on the shard, so a
// fenced run keeps going through an outage; Wake needs nothing, since the timer follows NextWake.
// Any other action panics.
func (l *loop) execute(act core.Action) {
	n := l.n
	switch a := act.(type) {
	case core.KeepAlive, core.CloseSession, core.Acquire, core.Release, core.ClaimSlot, core.ReleaseSlot:
		n.exec.do(l.gen, n.holder, act)
	case core.ExtendDeadline:
		n.raiseDeadline(a.Deadline)
	case core.StartRun:
		n.startRun(a)
	case core.StopRun:
		n.stopRun(a)
	case core.Confirm:
		l.confirm(a)
	case core.Degrade, core.Restore, core.Wake:
	default:
		panic(fmt.Sprintf("tenure: unknown action %T", act))
	}
}

// slotResult follows up the result r of a ClaimSlot or ReleaseSlot call once its inputs have been
// stepped (RUN-210, MCH O8). A SlotClaimed or SlotReleased raises the merger's floor to the end of
// the call, so that no slot data read before it reaches the machine again, and every result asks
// for a slot read. Other results need nothing.
func (l *loop) slotResult(r callResult) {
	n := l.n
	switch r.act.(type) {
	case core.ClaimSlot, core.ReleaseSlot:
	default:
		return
	}
	for _, in := range r.inputs {
		switch in.(type) {
		case core.SlotClaimed, core.SlotReleased:
			n.merge.raise(r.end)
		}
	}
	if n.refresh != nil {
		n.refresh.refresh()
	}
}

// raiseDeadline sets the session deadline that unfenced shards check to d, unless it is later
// already. Only the loop writes it.
func (n *Node) raiseDeadline(d time.Duration) {
	if int64(d) > n.deadline.Load() {
		n.deadline.Store(int64(d))
	}
}

// spaceShards is the node's table of one space's shards. The node keeps it for its whole life,
// across machines, so that the supervisor a new machine starts for a shard waits for the one an
// earlier machine started (RUN-300).
type spaceShards struct {
	space Space

	// local holds each shard's published Shard, read without a lock: the loop stores the new
	// Shard when it starts the run, and clears it once it has stepped the RunStopped of that
	// run, unless a later run of the shard has replaced it.
	local []atomic.Pointer[Shard]

	// runs holds each shard's latest supervisor, guarded by n.stepMu.
	runs []shardRun

	// inRun reports, for each shard, that a Run of it is in progress; guarded by n.runMu.
	inRun []bool
}

// shardRun is the latest supervisor of one shard.
type shardRun struct {
	sh     *Shard                  // the Shard of its run; nil before the shard's first run
	cancel context.CancelCauseFunc // cancels its context with a cause
	done   chan struct{}           // closed when it has returned
}

// newShardTable returns the shard table of spaces, by name.
func newShardTable(spaces []Space) map[string]*spaceShards {
	t := make(map[string]*spaceShards, len(spaces))
	for _, sp := range spaces {
		t[sp.Name] = &spaceShards{
			space: sp,
			local: make([]atomic.Pointer[Shard], sp.Shards),
			runs:  make([]shardRun, sp.Shards),
			inRun: make([]bool, sp.Shards),
		}
	}
	return t
}

// startRun carries out a StartRun (RUN-130): it raises the session deadline to the action's,
// makes the run's Shard, publishes it, and starts its supervisor on a fresh context that StopRun
// cancels. The supervisor first waits for the shard's previous supervisor, of this machine or of
// an earlier one (RUN-300). It reports each return of Run to the observer, and posts the run's
// RunStopped to the loop once it has stopped, so that the machine releases the shard only after
// Run has returned. The caller holds n.stepMu.
func (n *Node) startRun(a core.StartRun) {
	n.raiseDeadline(a.Deadline)
	sp := n.shards[a.Space]
	sh := newShard(a.Space, a.Shard, a.Term, sp.space.Fenced, n.epoch, &n.deadline)
	ctx, cancel := context.WithCancelCause(n.ctx)
	prev := sp.runs[a.Shard].done
	done := make(chan struct{})
	sp.runs[a.Shard] = shardRun{sh: sh, cancel: cancel, done: done}
	sp.local[a.Shard].Store(sh)

	run, timing := n.gateRun(sp, a.Shard), n.cfg.Timing
	onReturn := func(err error, count int, backoff time.Duration) {
		n.obs.runReturned(sh, err, count, backoff)
	}
	n.sups.Go(func() {
		defer close(done)
		supervise(ctx, sh, run, prev, timing, onReturn, func() { n.inbox.postStopped(sh) })
		cancel(nil) // ctx is done already: this only lets it go
	})
}

// stopRun carries out a StopRun (RUN-130, RUN-320): it stops the run's Shard with the reason, and
// only then cancels the run's context with the Shard's stop error, so that a Run that sees its
// context done gets that error from Check as well. A StopRun for a term the shard does not run
// changes nothing. The caller holds n.stepMu.
func (n *Node) stopRun(a core.StopRun) {
	r := n.shards[a.Space].runs[a.Shard]
	if r.sh == nil || r.sh.Term != a.Term {
		return
	}
	r.cancel(r.sh.stop(a.Reason.String()))
}

// rearm sets the timer for the machine's next wake, or stops it when none is armed.
func (l *loop) rearm() {
	w, ok := l.m.NextWake()
	switch {
	case !ok:
		if l.armed {
			l.timer.Stop()
			l.armed = false
		}
	case !l.armed || w != l.wakeAt:
		l.wakeAt, l.armed = w, true
		l.timer.Reset(w - time.Since(l.n.epoch))
	}
}

// at returns in with Now set to now; a ViewChanged also gets Wall, epoch.Add(now). It panics
// for a value that is not one of the machine's inputs.
func (l *loop) at(in core.Input, now time.Duration) core.Input {
	switch v := in.(type) {
	case core.Tick:
		v.Now = now
		return v
	case core.ViewChanged:
		v.Now, v.Wall = now, l.n.epoch.Add(now)
		return v
	case core.SessionRenewed:
		v.Now = now
		return v
	case core.SessionFailed:
		v.Now = now
		return v
	case core.SessionClosed:
		v.Now = now
		return v
	case core.Acquired:
		v.Now = now
		return v
	case core.AcquireFailed:
		v.Now = now
		return v
	case core.Released:
		v.Now = now
		return v
	case core.RunStopped:
		v.Now = now
		return v
	case core.HolderChanged:
		v.Now = now
		return v
	case core.SlotClaimed:
		v.Now = now
		return v
	case core.SlotReleased:
		v.Now = now
		return v
	case core.Drain:
		v.Now = now
		return v
	}
	panic(fmt.Sprintf("tenure: unknown input %T", in))
}
