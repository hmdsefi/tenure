// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// execPermits is the number of store calls other than session calls that may be in flight at
// once, over every lane and every withPermit caller (RUN-140).
const execPermits = 16

// laneKind names the record a lane serializes calls on.
type laneKind int

const (
	laneSession laneKind = iota // KeepAlive, CloseSession
	laneSlot                    // ClaimSlot, ReleaseSlot
	laneShard                   // Acquire, Release, Confirm of one (space, shard)
)

// laneKey identifies a lane: the session, the slot record, or one shard record.
type laneKey struct {
	kind  laneKind
	space string
	shard int
}

// execCall is one machine action to run on a lane for the machine of generation gen. tag is
// the tag doConfirm gave a Confirm, 0 for every other call.
type execCall struct {
	gen    uint64
	holder string
	act    core.Action
	tag    uint64
}

// confirmMark is the latest Confirm call that went out on a shard lane: the tag doConfirm gave
// it, and its start stamp.
type confirmMark struct {
	tag, start uint64
}

// lane runs the calls on one record in FIFO order, one at a time. It exists, with its
// goroutine, only while it has a call queued or in flight.
type lane struct {
	key   laneKey
	queue []execCall
	cur   execCall // the call taken from the queue, while busy
	busy  bool
}

// callResult is the outcome of one store call.
type callResult struct {
	// gen is the generation of the machine whose action this was.
	gen uint64

	// act is the machine action that was run, nil for a call that runs no machine action.
	act core.Action

	// op and space name a call that runs no machine action, for the observer: op is "Holdings",
	// with the space read, or "WatchHolders" for the store watch (RUN-170).
	op, space string

	// inputs are RUN-150's machine inputs for the outcome, with Now 0: the loop sets Now when
	// it steps them. Empty when the machine repeats the call itself.
	inputs []core.Input

	// holding is the record a successful Confirm returned. The loop steps it through the
	// freshness filter (RUN-175) in place of the HolderChanged in inputs.
	holding Holding

	// err is the store's error.
	err error

	// start and end are node-wide stamps taken right before the call went out and right after
	// it returned. Every stamp is unique and later stamps are larger.
	start, end uint64

	// sentWall is the wall time matching SentAt of a KeepAlive, epoch.Add(SentAt).
	sentWall time.Time
}

// executor runs machine actions against the store on per-record lanes, so the loop never
// blocks on the store (RUN-130, RUN-140, RUN-160). Results are delivered in end-stamp order
// through ready and take.
type executor struct {
	ctx    context.Context // the internal context every call's context derives from
	store  Store
	slots  SlotStore // the store as a SlotStore, nil when it is not one
	timing core.Timing
	epoch  time.Time
	failed func(callResult)

	permits chan struct{}
	stamps  atomic.Uint64
	readyc  chan struct{}
	wg      sync.WaitGroup

	mu       sync.Mutex
	lanes    map[laneKey]*lane
	results  []callResult
	floor    uint64                  // calls and results of older generations are dropped
	closed   bool                    // wait was called: do queues nothing more
	confirms map[laneKey]confirmMark // the latest Confirm call that went out, by shard lane
}

// newExecutor returns an executor whose calls derive their contexts from ctx and send t with
// KeepAlive. SentAt readings are time.Since(epoch). failed, when not nil, is called on the lane
// with every result whose error came from a call other than Acquire and KeepAlive, also when
// the result itself is discarded by dropOlder.
func newExecutor(ctx context.Context, store Store, t core.Timing, epoch time.Time,
	failed func(callResult)) *executor {
	if failed == nil {
		failed = func(callResult) {}
	}
	slots, _ := store.(SlotStore)
	return &executor{
		ctx:      ctx,
		store:    store,
		slots:    slots,
		timing:   t,
		epoch:    epoch,
		failed:   failed,
		permits:  make(chan struct{}, execPermits),
		readyc:   make(chan struct{}, 1),
		lanes:    make(map[laneKey]*lane),
		confirms: make(map[laneKey]confirmMark),
	}
}

// laneOf returns the lane of a store action. It panics for an action that is not a store call.
func laneOf(act core.Action) laneKey {
	switch a := act.(type) {
	case core.KeepAlive, core.CloseSession:
		return laneKey{kind: laneSession}
	case core.ClaimSlot, core.ReleaseSlot:
		return laneKey{kind: laneSlot}
	case core.Acquire:
		return laneKey{kind: laneShard, space: a.Space, shard: a.Shard}
	case core.Release:
		return laneKey{kind: laneShard, space: a.Space, shard: a.Shard}
	case core.Confirm:
		return laneKey{kind: laneShard, space: a.Space, shard: a.Shard}
	}
	panic(fmt.Sprintf("tenure: not a store action %T", act))
}

// do queues act, an action of the machine of generation gen holding the session holder, on its
// lane, and reports whether it was queued. It never blocks on the store. It drops act (RUN-130)
// when gen is older than the latest dropOlder, when the executor is shut down, when act is a
// KeepAlive and one is already queued, and when act is a Release, ReleaseSlot, ClaimSlot or
// CloseSession equal to one queued or in flight. Acquire and Confirm are never dropped. It
// panics for an action that is not a store call, and for a slot action when the store is not a
// SlotStore.
func (e *executor) do(gen uint64, holder string, act core.Action) bool {
	return e.enqueue(execCall{gen: gen, holder: holder, act: act})
}

// doConfirm queues a as do does and returns the tag it gives the call, a fresh stamp, also when
// the call is dropped. confirmStarted reports the tag once the call has gone out, so the loop can
// tell when the shard's latest Confirm started (RUN-175, MCH O5).
func (e *executor) doConfirm(gen uint64, holder string, a core.Confirm) (tag uint64, queued bool) {
	tag = e.stamp()
	return tag, e.enqueue(execCall{gen: gen, holder: holder, act: a, tag: tag})
}

// confirmStarted returns the tag and the start stamp of the latest Confirm call that went out
// for the shard, or zeros when none has.
func (e *executor) confirmStarted(space string, shard int) (tag, start uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.confirms[laneKey{kind: laneShard, space: space, shard: shard}]
	return m.tag, m.start
}

// enqueue queues c on its lane as do says, and reports whether it was queued.
func (e *executor) enqueue(c execCall) bool {
	key := laneOf(c.act)
	if key.kind == laneSlot && e.slots == nil {
		panic(fmt.Sprintf("tenure: %T needs a SlotStore", c.act))
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.ctx.Err() != nil || c.gen < e.floor {
		return false
	}
	l := e.lanes[key]
	if l != nil && coalesced(l, c) {
		return false
	}
	if l == nil {
		l = &lane{key: key}
		e.lanes[key] = l
		e.wg.Add(1)
		go e.run(l)
	}
	l.queue = append(l.queue, c)
	return true
}

// coalesced reports whether c is dropped because of a call already on l (RUN-130, MCH O6).
func coalesced(l *lane, c execCall) bool {
	switch c.act.(type) {
	case core.KeepAlive:
		for _, q := range l.queue {
			if q == c {
				return true
			}
		}
	case core.Release, core.ReleaseSlot, core.ClaimSlot, core.CloseSession:
		if l.busy && l.cur == c {
			return true
		}
		for _, q := range l.queue {
			if q == c {
				return true
			}
		}
	}
	return false
}

// dropOlder drops every queued call of a machine older than generation gen and discards the
// results of such calls, both those not yet taken and those still in flight. Calls of older
// generations passed to do later are dropped too.
func (e *executor) dropOlder(gen uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if gen <= e.floor {
		return
	}
	e.floor = gen
	for _, l := range e.lanes {
		q := l.queue[:0]
		for _, c := range l.queue {
			if c.gen >= gen {
				q = append(q, c)
			}
		}
		clear(l.queue[len(q):])
		l.queue = q
	}
	rs := e.results[:0]
	for _, r := range e.results {
		if r.gen >= gen {
			rs = append(rs, r)
		}
	}
	clear(e.results[len(rs):])
	e.results = rs
}

// releasing returns the lanes of the shards with a Release queued or in flight, for the account
// of a drain that has not completed (RUN-510). A Release is in flight from the moment its lane
// takes it until its result is out.
func (e *executor) releasing() map[laneKey]bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	isRelease := func(c execCall) bool {
		_, ok := c.act.(core.Release)
		return ok
	}
	out := make(map[laneKey]bool)
	for key, l := range e.lanes {
		if key.kind == laneShard && (l.busy && isRelease(l.cur) || slices.ContainsFunc(l.queue, isRelease)) {
			out[key] = true
		}
	}
	return out
}

// ready returns a channel that receives a value when results may be waiting for take.
func (e *executor) ready() <-chan struct{} { return e.readyc }

// take returns the results delivered since the last take, in end-stamp order, or nil.
func (e *executor) take() []callResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	rs := e.results
	e.results = nil
	return rs
}

// stamp returns a new node-wide stamp, larger than every start and end stamp taken before.
// Readers outside the lanes, such as Holdings and Slots, take one right before their call.
func (e *executor) stamp() uint64 { return e.stamps.Add(1) }

// withPermit runs f while holding one of the permits the lanes' non-session calls share, for
// store reads such as Holdings and Slots. It waits for a free permit, and returns false without
// running f when the internal context ends first.
func (e *executor) withPermit(f func()) bool {
	select {
	case e.permits <- struct{}{}:
	case <-e.ctx.Done():
		return false
	}
	defer func() { <-e.permits }()
	if e.ctx.Err() != nil {
		return false
	}
	f()
	return true
}

// wait stops do from queuing calls and waits until every lane goroutine has returned. Once the
// internal context is done, lanes drop their queued calls, which get no result, and return after
// the call in flight, so no store call of a lane is out when wait returns. A store call that does
// not return once its context is done holds wait up, since the executor cannot end it. wait does
// not wait for withPermit callers.
func (e *executor) wait() {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.wg.Wait()
}

// run is a lane's goroutine: it runs the lane's calls until the queue is empty or the internal
// context is done, and then removes the lane.
func (e *executor) run(l *lane) {
	defer e.wg.Done()
	for {
		c, ok := e.next(l)
		if !ok {
			return
		}
		// failed runs after exec has given the permit back, so a slow callback holds none.
		if r, ok := e.exec(l, c); ok && r.err != nil {
			switch c.act.(type) {
			case core.Acquire, core.KeepAlive:
			default:
				e.failed(r)
			}
		}
	}
}

// next takes the next call of l, or removes l and returns false when there is none to run.
func (e *executor) next(l *lane) (execCall, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	l.busy, l.cur = false, execCall{} // also after a call that never went out
	if len(l.queue) == 0 || e.ctx.Err() != nil {
		l.queue = nil
		delete(e.lanes, l.key)
		return execCall{}, false
	}
	c := l.queue[0]
	l.queue[0] = execCall{}
	l.queue = l.queue[1:]
	l.busy, l.cur = true, c
	return c, true
}

// exec runs c on its lane: it takes a permit unless c is a session call, runs the store call
// with a RetryPeriod timeout, and delivers the result. It returns the result, and false when the
// call never went out.
func (e *executor) exec(l *lane, c execCall) (callResult, bool) {
	if l.key.kind != laneSession {
		select {
		case e.permits <- struct{}{}:
		case <-e.ctx.Done():
			return callResult{}, false
		}
		defer func() { <-e.permits }()
	}
	e.mu.Lock()
	stale := c.gen < e.floor
	e.mu.Unlock()
	if stale || e.ctx.Err() != nil {
		return callResult{}, false
	}

	ctx, cancel := context.WithTimeout(e.ctx, e.timing.RetryPeriod)
	r := callResult{gen: c.gen, act: c.act}
	var sentAt time.Duration
	if _, ok := c.act.(core.KeepAlive); ok {
		sentAt = time.Since(e.epoch)
		r.sentWall = e.epoch.Add(sentAt)
	}
	r.start = e.stamp()
	if c.tag != 0 {
		e.mu.Lock()
		e.confirms[l.key] = confirmMark{tag: c.tag, start: r.start}
		e.mu.Unlock()
	}
	o := e.invoke(ctx, c)
	cancel()

	r.err, r.holding = o.err, o.holding
	r.inputs = resultInputs(c.act, o, sentAt)
	e.mu.Lock()
	r.end = e.stamp()
	// The call is no longer in flight once its result is out: an equal call queued from here
	// on, while failed still runs, is not dropped.
	l.busy, l.cur = false, execCall{}
	if c.gen >= e.floor {
		e.results = append(e.results, r)
		select {
		case e.readyc <- struct{}{}:
		default:
		}
	}
	e.mu.Unlock()
	return r, true
}

// outcome is what a store call returned.
type outcome struct {
	term    core.Term
	slot    int
	holding Holding
	err     error
}

// invoke makes the store call of c.
func (e *executor) invoke(ctx context.Context, c execCall) outcome {
	var o outcome
	switch a := c.act.(type) {
	case core.KeepAlive:
		o.err = e.store.KeepAlive(ctx, c.holder, e.timing)
	case core.CloseSession:
		o.err = e.store.CloseSession(ctx, c.holder)
	case core.Acquire:
		o.term, o.err = e.store.Acquire(ctx, c.holder, a.Space, a.Shard)
	case core.Release:
		o.err = e.store.Release(ctx, c.holder, a.Space, a.Shard, a.Term)
	case core.Confirm:
		o.holding, o.err = e.store.Confirm(ctx, c.holder, a.Space, a.Shard, a.Term)
	case core.ClaimSlot:
		o.slot, o.err = e.slots.ClaimSlot(ctx, c.holder)
	case core.ReleaseSlot:
		o.err = e.slots.ReleaseSlot(ctx, c.holder, a.Slot)
	}
	return o
}

// resultInputs maps the outcome of a store call to machine inputs as RUN-150's table says, with
// Now 0. sentAt is the SentAt reading of a KeepAlive. An ErrSessionLost from any call other than
// KeepAlive and CloseSession adds SessionFailed{CauseLost}; a call whose other errors leave the
// repeat to the machine maps to no input (MCH O3).
func resultInputs(act core.Action, o outcome, sentAt time.Duration) []core.Input {
	lost := errors.Is(o.err, ErrSessionLost)
	sessionLost := core.SessionFailed{Cause: core.CauseLost}
	switch a := act.(type) {
	case core.KeepAlive:
		switch {
		case o.err == nil:
			return []core.Input{core.SessionRenewed{SentAt: sentAt}}
		case lost:
			return []core.Input{sessionLost}
		}
		return []core.Input{core.SessionFailed{Cause: core.CauseUnreachable}}
	case core.CloseSession:
		// MCH §11's driver duty: a lost or unknown session is closed, since the machine
		// ignores SessionFailed while closing.
		if o.err == nil || lost {
			return []core.Input{core.SessionClosed{}}
		}
	case core.Acquire:
		var held *HeldError
		switch {
		case o.err == nil && o.term > 0:
			return []core.Input{core.Acquired{Space: a.Space, Shard: a.Shard, Term: o.term}}
		case errors.As(o.err, &held):
			return []core.Input{core.AcquireFailed{Space: a.Space, Shard: a.Shard,
				Holder: held.Holder, RetryAfter: held.RetryAfter}}
		case lost:
			return []core.Input{core.Acquired{Space: a.Space, Shard: a.Shard}, sessionLost}
		}
		return []core.Input{core.Acquired{Space: a.Space, Shard: a.Shard}}
	case core.Release:
		switch {
		case o.err == nil:
			return []core.Input{core.Released{Space: a.Space, Shard: a.Shard, Term: a.Term}}
		case lost:
			return []core.Input{sessionLost}
		}
	case core.Confirm:
		switch {
		case o.err == nil:
			return []core.Input{core.HolderChanged{Space: a.Space, Shard: a.Shard,
				Holder: o.holding.Holder, Term: o.holding.Term}}
		case lost:
			return []core.Input{sessionLost}
		}
	case core.ClaimSlot:
		switch {
		case o.err == nil && o.slot >= 0:
			return []core.Input{core.SlotClaimed{Slot: o.slot}}
		case lost:
			return []core.Input{sessionLost}
		}
	case core.ReleaseSlot:
		switch {
		case o.err == nil:
			return []core.Input{core.SlotReleased{Slot: a.Slot}}
		case lost:
			return []core.Input{sessionLost}
		}
	}
	return nil
}
