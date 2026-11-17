// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"log/slog"
	"maps"
	"strconv"
	"sync"
	"time"
)

// RouterEventKind says what a RouterEvent reports. Its String method returns the kind's name,
// such as "router.view", which is also the kind's name in log records. Each kind's doc comment
// lists the RouterEvent fields it fills besides Seq, Time and Kind; the others hold their unused
// values (see RouterEvent).
type RouterEventKind uint8

// RouterEventView ("router.view") is emitted when the Router applies a view: Version is the new
// snapshot's version, ViewVersion the view's and Count its number of members.
const RouterEventView RouterEventKind = 1

// RouterEventRebuilt ("router.rebuilt") is emitted for each space of each new snapshot: Version
// is the snapshot's, Space the space, Count the number of its shards that route to another
// member than in the previous snapshot, Balanced whether the assignment was recomputed, because
// the placement keys changed, rather than only mapped to new members, and Duration how long
// building the snapshot took.
const RouterEventRebuilt RouterEventKind = 2

// RouterEventStale ("router.stale") is emitted when the Router becomes stale: the channel of
// its watch of its Membership closes, with Err "tenure: watch channel closed", or the
// Membership reports itself stale through a method Stale() bool, which the Router reads every
// RetryPeriod and after each view, with Err "tenure: membership reports stale". The Router stays
// stale while the Watch calls that follow fail or the Membership reports itself stale, and
// emits no second router.stale meanwhile. Router.Stale reports both at once.
const RouterEventStale RouterEventKind = 3

// RouterEventFresh ("router.fresh") is emitted when a stale Router is no longer stale: it has
// applied a view of a new watch, and its Membership, if it reports staleness, last reported
// itself fresh. Version is the current snapshot's version.
const RouterEventFresh RouterEventKind = 4

// RouterEventCorrected ("router.corrected") is emitted when Router.Correct records a correction:
// Version is the snapshot's version, Space and Shard the shard, and Addr the address.
const RouterEventCorrected RouterEventKind = 5

// RouterEventMismatch ("router.mismatch") is emitted when a snapshot marks a space whose shard
// count a member disagrees with, and it was not marked before: Version is the snapshot's, Space
// the space, and Err the *SpaceMismatchError its lookups return.
const RouterEventMismatch RouterEventKind = 6

// RouterEventMatched ("router.matched") is emitted when a snapshot clears the mark of a space:
// Version is the snapshot's and Space the space.
const RouterEventMatched RouterEventKind = 7

// RouterEventClosed ("router.closed") is the last event of a Router, emitted by Close.
const RouterEventClosed RouterEventKind = 8

// routerEventNames are the kinds' names, by kind.
var routerEventNames = [...]string{
	RouterEventView:      "router.view",
	RouterEventRebuilt:   "router.rebuilt",
	RouterEventStale:     "router.stale",
	RouterEventFresh:     "router.fresh",
	RouterEventCorrected: "router.corrected",
	RouterEventMismatch:  "router.mismatch",
	RouterEventMatched:   "router.matched",
	RouterEventClosed:    "router.closed",
}

// String returns the kind's name, such as "router.view", or text such as "RouterEventKind(0)"
// for a value that is not a kind.
func (k RouterEventKind) String() string {
	if k >= RouterEventView && int(k) < len(routerEventNames) {
		return routerEventNames[k]
	}
	return "RouterEventKind(" + strconv.Itoa(int(k)) + ")"
}

// RouterEvent is one thing that happened on a Router, as WithRouterEvents receives it. Each kind
// fills the fields its doc comment lists; a field the kind does not use holds its unused value:
// -1 for Shard, and the zero value for the others.
type RouterEvent struct {
	// Seq is the event's number on its Router: 1 for the first event, then one more for each
	// event, without gaps.
	Seq uint64

	// Time is the wall-clock time, in UTC, at which the Router emitted the event.
	Time time.Time

	// Kind says what the event reports.
	Kind RouterEventKind

	// Version is the version of the Router's snapshot, or 0 when unused.
	Version uint64

	// ViewVersion is the version of the view, or 0 when unused.
	ViewVersion uint64

	// Space is the space the event is about, or "".
	Space string

	// Shard is the shard the event is about, or -1.
	Shard int

	// Addr is a member's address, or "".
	Addr string

	// Count is a number, such as members or shards moved, or 0.
	Count int

	// Balanced reports that the assignment was recomputed.
	Balanced bool

	// Duration is a length of time, or 0.
	Duration time.Duration

	// Err is the error the event reports, or nil.
	Err error
}

// routerEvents numbers a Router's events and hands them to the logger and the callback on a
// goroutine of its own, in order, so that emitting never waits for either (RTR-600): it appends
// to an unbounded queue under mu. With neither a logger nor a callback it keeps nothing.
type routerEvents struct {
	logger *slog.Logger
	f      func(RouterEvent)
	now    func() time.Time

	mu      sync.Mutex
	work    *sync.Cond
	seq     uint64
	queue   []RouterEvent
	stopped bool          // close has been called: emit does nothing and run returns once the queue is empty
	done    chan struct{} // closed when run returns; nil when there is no delivery
}

// newRouterEvents returns the event delivery of a Router and starts its goroutine when there is a
// logger or a callback.
func newRouterEvents(logger *slog.Logger, f func(RouterEvent), now func() time.Time) *routerEvents {
	e := &routerEvents{logger: logger, f: f, now: now}
	e.work = sync.NewCond(&e.mu)
	if logger != nil || f != nil {
		e.done = make(chan struct{})
		go e.run()
	}
	return e
}

// emit numbers ev, stamps it and queues it. It waits for nothing but mu.
func (e *routerEvents) emit(ev RouterEvent) {
	if e.done == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return
	}
	e.seq++
	ev.Seq = e.seq
	ev.Time = e.now().UTC()
	e.queue = append(e.queue, ev)
	e.work.Signal()
}

// close emits last as the final event, then waits until every queued event has been delivered
// and the goroutine has returned. Later calls do nothing more than wait.
func (e *routerEvents) close(last RouterEvent) {
	if e.done == nil {
		return
	}
	e.emit(last)
	e.mu.Lock()
	e.stopped = true
	e.work.Signal()
	e.mu.Unlock()
	<-e.done
}

// wait waits until the delivery goroutine, if any, has returned.
func (e *routerEvents) wait() {
	if e.done != nil {
		<-e.done
	}
}

// run is the delivery goroutine.
func (e *routerEvents) run() {
	defer close(e.done)
	for {
		e.mu.Lock()
		for len(e.queue) == 0 && !e.stopped {
			e.work.Wait()
		}
		if len(e.queue) == 0 {
			e.mu.Unlock()
			return
		}
		ev := e.queue[0]
		e.queue[0] = RouterEvent{}
		e.queue = e.queue[1:]
		e.mu.Unlock()
		e.deliver(ev)
	}
}

// deliver hands ev to the logger, then to the callback.
func (e *routerEvents) deliver(ev RouterEvent) {
	if e.logger != nil {
		level := slog.LevelInfo
		switch ev.Kind {
		case RouterEventStale, RouterEventMismatch:
			level = slog.LevelWarn
		case RouterEventCorrected, RouterEventRebuilt:
			level = slog.LevelDebug
		}
		e.logger.LogAttrs(context.Background(), level, ev.Kind.String(), routerEventAttrs(ev)...)
	}
	if e.f != nil {
		e.f(ev)
	}
}

// routerEventAttrs returns the log attributes of ev: seq, then each field the kind fills.
func routerEventAttrs(ev RouterEvent) []slog.Attr {
	attrs := []slog.Attr{slog.Uint64("seq", ev.Seq)}
	if ev.Version != 0 {
		attrs = append(attrs, slog.Uint64("version", ev.Version))
	}
	if ev.ViewVersion != 0 {
		attrs = append(attrs, slog.Uint64("view_version", ev.ViewVersion))
	}
	if ev.Space != "" {
		attrs = append(attrs, slog.String("space", ev.Space))
	}
	if ev.Shard >= 0 {
		attrs = append(attrs, slog.Int("shard", ev.Shard))
	}
	if ev.Addr != "" {
		attrs = append(attrs, slog.String("addr", ev.Addr))
	}
	switch ev.Kind {
	case RouterEventView, RouterEventRebuilt:
		attrs = append(attrs, slog.Int("count", ev.Count))
	}
	if ev.Kind == RouterEventRebuilt {
		attrs = append(attrs, slog.Bool("balanced", ev.Balanced), slog.Duration("duration", ev.Duration))
	}
	if ev.Err != nil {
		attrs = append(attrs, slog.String("err", ev.Err.Error()))
	}
	return attrs
}

// RouterTable is a consistent copy of a Router's snapshot, as Router.Table returns it.
type RouterTable struct {
	// Taken is the wall-clock time, in UTC, at which the copy was taken.
	Taken time.Time

	// Version is the snapshot's version, 0 before the first view.
	Version uint64

	// Stale and Closed report what Router.Stale reported and whether Close had been called.
	Stale  bool
	Closed bool

	// ViewVersion is the version of the view the snapshot comes from, Members its members, Slots
	// its slot count and SlotHolders its slot holders; all zero before the first view.
	ViewVersion uint64
	Members     []Member
	Slots       int
	SlotHolders []string

	// Spaces holds each space's table, in the order NewRouter was given the spaces.
	Spaces []RouterSpaceTable
}

// RouterSpaceTable is one space of a RouterTable.
type RouterSpaceTable struct {
	// Name and Shards are the space's.
	Name   string
	Shards int

	// Mismatch is the text of the space's *SpaceMismatchError, or "" when no member disagrees.
	Mismatch string

	// Routes holds each shard's route, by shard.
	Routes []RouterRoute
}

// RouterRoute is one shard's route in a RouterTable.
type RouterRoute struct {
	// Member is the ID of the member the snapshot routes the shard to, or "" for none, Addr its
	// address and Slot its slot, -1 without one.
	Member string
	Addr   string
	Slot   int

	// Err is the text of the error a lookup of the shard returns without a correction, or "".
	Err string

	// Corrected is the address of the shard's live correction, or "" for none.
	Corrected string
}

// Table returns a consistent copy of the Router's snapshot: its version, whether the Router is
// stale or closed, the members of its view, and for each space the route of each shard and its
// live correction. The copy shares nothing with the Router. Table does not wait for the builder.
func (r *Router) Table() RouterTable {
	s := r.snap.Load()
	now := r.opts.now()
	t := RouterTable{
		Taken:   now.UTC(),
		Version: s.version,
		Stale:   r.Stale(),
		Closed:  s.closed,
		Spaces:  make([]RouterSpaceTable, len(r.spaces)),
	}
	if v := s.view; v != nil {
		t.ViewVersion, t.Slots = v.Version, v.Slots
		t.Members = make([]Member, len(v.Members))
		for i, m := range v.Members {
			m.Meta = maps.Clone(m.Meta)
			t.Members[i] = m
		}
		t.SlotHolders = append([]string(nil), v.SlotHolders...)
	}
	for i, h := range r.spaces {
		tab := &s.tables[i]
		st := RouterSpaceTable{Name: h.name, Shards: h.shards, Routes: make([]RouterRoute, h.shards)}
		if tab.mismatch != nil {
			st.Mismatch = tab.mismatch.Error()
		}
		for shard := range h.shards {
			route := RouterRoute{Slot: -1}
			if m := tab.owners[shard]; m >= 0 {
				mem := s.members[m]
				route.Member, route.Addr, route.Slot = mem.ID, mem.Addr, mem.Slot
			} else {
				route.Err = tab.errs[shard].Error()
			}
			if c := h.corrections[shard].Load(); c != nil && c.version == s.version && now.Sub(c.at) < r.opts.correctionTTL {
				route.Corrected = c.member.Addr
			}
			st.Routes[shard] = route
		}
		t.Spaces[i] = st
	}
	return t
}
