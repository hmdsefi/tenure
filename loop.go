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
type loop struct {
	n    *Node
	m    *core.Machine
	gen  uint64
	hook func(stepRecord)

	status core.Status // the machine's Status after the latest step
	done   bool        // n.machineDone has been closed

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
// reaches it, until the internal context is done. It then waits for the executor's lanes.
func (l *loop) run(first []loopInput, started chan<- struct{}) {
	n := l.n
	defer n.exec.wait()
	defer l.timer.Stop()
	for _, in := range first {
		l.step(in)
	}
	close(started)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-l.timer.C:
			l.armed = false
			l.tickUntil(time.Since(n.epoch))
			l.rearm()
		case <-n.exec.ready():
			for _, r := range n.exec.take() {
				for _, in := range r.inputs {
					l.step(loopInput{in: in, sentWall: r.sentWall, err: r.err})
				}
				l.slotResult(r)
			}
		case <-n.inbox.ready:
			ins, v := n.inbox.take()
			for _, in := range ins {
				l.step(in)
			}
			if v != nil && !n.merge.stale(v.Version) {
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
// assignment changed, unpublishes the shard of a stepped RunStopped that is still
// published, and reports the step to the observer and the hook. It then re-arms the timer and,
// the first time the machine is done, closes n.machineDone.
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

	n.stepMu.Lock()
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
	if in.sh != nil {
		n.shards[in.sh.Space].local[in.sh.ID].CompareAndSwap(in.sh, nil)
	}
	n.obs.step(rec)
	if l.hook != nil {
		l.hook(rec)
	}
	n.stepMu.Unlock()

	l.rearm()
	if rec.After.Done && !l.done {
		l.done = true
		close(n.machineDone)
	}
}

// execute carries out one action (RUN-130). Store calls go to the executor, which never blocks;
// ExtendDeadline raises the session deadline that unfenced shards check; StartRun and StopRun
// start and stop a shard's supervisor; Wake needs nothing, since the timer follows NextWake. Any
// other action panics.
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
	case core.Wake:
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

	run, timing := sp.space.Run, n.cfg.Timing
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
