// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// ringSize is the number of events a node keeps for RecentEvents.
const ringSize = 256

// The reasons of EventNodeClosed.
const (
	closedDrained  = "drained"
	closedDeadline = "deadline"
)

// eventLog is a node's event log (OBS-200, OBS-210). emit numbers and stamps each event under
// mu, keeps it in a ring of the newest ringSize events and appends it to an unbounded FIFO queue;
// it never waits for anything but mu, so emitting never blocks the loop. One delivery goroutine,
// started by start, takes the queue's events one at a time, in Seq order, and hands each to the
// logger and then to OnEvent, outside mu. stop ends the delivery (OBS-220): no OnEvent call
// starts once it has returned.
type eventLog struct {
	member  string
	logger  *slog.Logger // Config.Logger; nil: no log records
	onEvent func(Event)  // Config.OnEvent; nil: none

	mu        sync.Mutex
	work      *sync.Cond      // the delivery goroutine waits on it for the queue or stop
	progress  *sync.Cond      // waitDelivered and stop wait on it for the delivery goroutine
	seq       uint64          // the Seq of the newest event, 0 before the first
	ring      [ringSize]Event // event Seq at index (Seq-1) % ringSize
	queue     []queued        // emitted and not yet taken by the delivery goroutine, oldest first
	holder    string          // the node's holder, noted with each event
	recording bool            // the delivery goroutine has taken an event and writes its record
	delivered uint64          // the Seq of the newest event whose delivery has returned
	started   bool            // start has started the delivery goroutine
	sealed    bool            // emitLast has emitted the node's last event: emit does nothing more
	stopped   bool            // stop has been called: emit and the delivery goroutine do nothing more
}

// queued is an event waiting for delivery, with the holder the node had when it was emitted.
type queued struct {
	e      Event
	holder string
}

// newEventLog returns the event log of member, delivering to logger and onEvent, either of which
// may be nil. Its delivery goroutine does not run before start.
func newEventLog(member string, logger *slog.Logger, onEvent func(Event)) *eventLog {
	l := &eventLog{member: member, logger: logger, onEvent: onEvent}
	l.work = sync.NewCond(&l.mu)
	l.progress = sync.NewCond(&l.mu)
	return l
}

// start notes holder as the node's holder and starts the delivery goroutine, once. The goroutine
// is not one of the node's: it runs until stop and returns once a delivery in progress then has
// returned.
func (l *eventLog) start(holder string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holder = holder
	if l.started || l.stopped {
		return
	}
	l.started = true
	go l.run()
}

// setHolder notes holder as the node's holder from the next event on.
func (l *eventLog) setHolder(holder string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holder = holder
}

// emit gives e the next Seq and the current wall-clock time in UTC, without a monotonic reading,
// keeps it in the ring and queues it for delivery with the current holder. It returns e's Seq. It
// waits for nothing but the log's lock. After emitLast or stop it does nothing and returns 0, so
// nothing is emitted after node.closed or once Close has returned.
func (l *eventLog) emit(e Event) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.emitLocked(e)
}

// emitLast emits e as emit does, as the node's last event: later emits do nothing. A forced Close
// emits node.closed while some of the node's goroutines still run; their events would follow it.
func (l *eventLog) emitLast(e Event) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.emitLocked(e)
	l.sealed = true
	return seq
}

// emitLocked is emit; the caller holds l.mu.
func (l *eventLog) emitLocked(e Event) uint64 {
	if l.stopped || l.sealed {
		return 0
	}
	l.seq++
	e.Seq = l.seq
	e.Time = time.Now().UTC() // UTC strips the monotonic reading
	l.ring[(e.Seq-1)%ringSize] = e
	l.queue = append(l.queue, queued{e: e, holder: l.holder})
	l.work.Signal()
	return e.Seq
}

// last returns the Seq of the newest event, 0 before the first.
func (l *eventLog) last() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// recent returns a copy of the ring: the newest events, at most ringSize, oldest first.
func (l *eventLog) recent() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := min(l.seq, ringSize) // the ring holds events 1 to seq, the newest ringSize of them
	first := l.seq - n        // the Seq before the oldest kept event
	out := make([]Event, n)
	for i := range n {
		out[i] = l.ring[(first+i)%ringSize]
	}
	return out
}

// waitDelivered waits until the delivery of event seq has returned, stop has been called or ctx
// is done.
func (l *eventLog) waitDelivered(ctx context.Context, seq uint64) {
	stop := context.AfterFunc(ctx, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.progress.Broadcast()
	})
	defer stop()
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.delivered < seq && !l.stopped && ctx.Err() == nil {
		l.progress.Wait()
	}
}

// stop ends the delivery: the queued events are dropped, though they stay in the ring, later
// emits do nothing, and the delivery goroutine returns once a delivery in progress has returned.
// A delivery is in progress once the goroutine has taken its event from the queue: stop waits
// while it writes the event's log record, but not for the event's OnEvent call. So once stop has
// returned, the goroutine is waiting, gone, or calling OnEvent with the one event it took before
// stop was called: no OnEvent call with any other event starts. Calling it again does nothing.
func (l *eventLog) stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = true
	l.queue = nil
	l.work.Broadcast()
	l.progress.Broadcast()
	for l.recording {
		l.progress.Wait()
	}
}

// run is the delivery goroutine. It has no recover: a panic of OnEvent or of the logger's
// handler ends the process.
func (l *eventLog) run() {
	var d deliverer
	for {
		l.mu.Lock()
		for len(l.queue) == 0 && !l.stopped {
			l.work.Wait()
		}
		if l.stopped {
			l.mu.Unlock()
			return
		}
		q := l.queue[0]
		l.queue[0] = queued{}
		l.queue = l.queue[1:]
		if len(l.queue) == 0 {
			l.queue = nil // let a backlog's array go
		}
		l.recording = true
		l.mu.Unlock()

		d.record(l, q)

		l.mu.Lock()
		l.recording = false
		l.progress.Broadcast() // for a stop waiting for the record
		l.mu.Unlock()

		if l.onEvent != nil {
			l.onEvent(q.e)
		}

		l.mu.Lock()
		l.delivered = q.e.Seq
		l.progress.Broadcast()
		l.mu.Unlock()
	}
}

// deliverer is the delivery goroutine's state: the node's logger with the member and the holder
// of the last record, kept while the holder stays the same.
type deliverer struct {
	logger *slog.Logger
	holder string
}

// record writes q's log record, at its kind's level, with its kind's message and attributes, on
// the logger with the member and q's holder. Without a logger it does nothing.
func (d *deliverer) record(l *eventLog, q queued) {
	if l.logger == nil || !q.e.Kind.known() {
		return
	}
	if d.logger == nil || d.holder != q.holder {
		d.logger = l.logger.With("member", l.member, "holder", q.holder)
		d.holder = q.holder
	}
	k := &kinds[q.e.Kind]
	d.logger.LogAttrs(context.Background(), k.level, k.msg, eventAttrs(q.e)...)
}

// logObserver is the node's observer: it emits the node's events to its event log, those of the
// loop's steps through its step collector. Its methods for events that later tasks add do nothing
// yet.
type logObserver struct {
	n     *Node
	steps *stepCollector // guarded by n.stepMu

	// drainStart is the monotonic reading at node.draining. draining writes it before it starts
	// the drain goroutine, which calls drained.
	drainStart time.Time
}

var _ observer = (*logObserver)(nil)

// newLogObserver returns the observer of n, whose Config and event log are set.
func newLogObserver(n *Node) *logObserver {
	return &logObserver{n: n, steps: newStepCollector(n.cfg.Spaces, n.events.emit)}
}

func (o *logObserver) start(holder string) { o.n.events.start(holder) }

// startFailed waits for the delivery of every event emitted so far, or for ctx, and then stops
// the delivery. A failed Start emits no node.closed (OBS-220).
func (o *logObserver) startFailed(ctx context.Context, _ error) {
	l := o.n.events
	l.waitDelivered(ctx, l.last())
	l.stop()
}

// view emits view.changed for a view the node publishes without a store. With a store the loop
// has stepped the view, and step emits it in the step's order.
func (o *logObserver) view(v core.View) {
	if o.n.cfg.Store == nil {
		o.n.events.emit(viewChanged(v))
	}
}

// step emits the events of one step of the loop, in OBS-110's order.
func (o *logObserver) step(rec stepRecord) { o.steps.collect(rec) }

// shardClosed emits shard.stopping with reason "closed" for a shard that a forced Close stopped.
func (o *logObserver) shardClosed(sh *Shard) { o.steps.closed(sh, time.Since(o.n.epoch)) }

// slotCountMismatch emits slot.count_mismatch: Start keeps the stored count, which differs from
// Config.Slots.
func (o *logObserver) slotCountMismatch(configured, stored int) {
	o.n.events.emit(Event{Kind: EventSlotCountMismatch, Shard: -1, Slot: -1, Count: stored, Configured: configured})
}

// noSlot emits slot.standby for a ClaimSlot that found no free slot, once per standby period.
func (o *logObserver) noSlot() { o.steps.noSlot() }

// newIncarnation notes the node's new holder, which the loop has set, for the events from now on,
// and then emits session.restarted with the previous holder prev.
func (o *logObserver) newIncarnation(prev string) {
	o.n.events.setHolder(o.n.holder)
	o.steps.restarted(prev)
}

func (o *logObserver) draining() {
	o.drainStart = time.Now()
	o.steps.draining.Store(true)
	o.n.events.emit(Event{Kind: EventNodeDraining, Shard: -1, Slot: -1})
}

func (o *logObserver) drained() {
	o.n.events.emit(Event{Kind: EventNodeDrained, Shard: -1, Slot: -1, Duration: time.Since(o.drainStart)})
}

// closed emits node.closed with reason, as the node's last event: the log drops any event emitted
// after it. After a completed drain it is Close's last step: it waits
// for node.closed's delivery, or for ctx, and stops the delivery. After a deadline Close stops
// the delivery itself, once it has stopped the node.
func (o *logObserver) closed(ctx context.Context, reason string) {
	l := o.n.events
	seq := l.emitLast(Event{Kind: EventNodeClosed, Shard: -1, Slot: -1, Reason: reason})
	if reason == closedDrained {
		l.waitDelivered(ctx, seq)
		l.stop()
	}
}

// RecentEvents returns a copy of the node's newest events, at most 256, oldest first. It holds
// events the node emitted but did not deliver to Config.OnEvent, such as those Close dropped. It
// may be called at any time, from OnEvent too, and after Close.
func (n *Node) RecentEvents() []Event { return n.events.recent() }
