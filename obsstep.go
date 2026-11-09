// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// stepCollector turns each step of the loop into the node's events, in OBS-110's order, so that
// the events about one shard always follow its transitions. It keeps what it needs to know about
// each shard across steps and across machines. Its methods run under n.stepMu: collect from
// observer.step, on the loop goroutine, and closed from observer.shardClosed.
//
// The order of one step's events, each group from its own method:
//
//  1. session.restarted, for the first step of a new incarnation;
//  2. the input's events: view.changed and slot.count_changed (viewEvents), session.expired, the
//     input's own event (inputEvents), and the shards that left releasing (releasedEvents);
//  3. one event per executed action, in Step's order (actionEvents);
//  4. slot.claimed or slot.released;
//  5. shard.stuck.
//
// An input the machine ignores emits nothing but its view.changed and slot.count_changed
// (ignored). slot.standby comes from noSlot, outside the steps.
type stepCollector struct {
	emit   func(Event) uint64
	shards map[string][]shardTrack // by space name, indexed by shard

	// slots is the slot count of the latest slot.count_changed, 0 before the first.
	slots int

	// standby reports that slot.standby was emitted and the node has held no slot since.
	standby bool

	// draining reports that Drain or Close has started the drain. observer.draining sets it,
	// under n.mu rather than n.stepMu.
	draining atomic.Bool
}

// shardTrack is what the collector keeps about one shard.
type shardTrack struct {
	// stopTerm is the term of the latest run told to stop, 0 for none; stop is why, a stop
	// reason's name, and stopAt the loop's reading at its shard.stopping (or at the event that
	// takes its place). shard.released takes stop for the term the run held, and
	// shard.handoff_completed measures from stopAt.
	stopTerm core.Term
	stop     string
	stopAt   time.Duration

	// releasing is the term shard.releasing was emitted for and that has not been released yet,
	// 0 for none: a Release repeated for the same term emits nothing.
	releasing core.Term
}

// newStepCollector returns the collector of a node with spaces, which hands each event to emit.
func newStepCollector(spaces []Space, emit func(Event) uint64) *stepCollector {
	c := &stepCollector{emit: emit, shards: make(map[string][]shardTrack, len(spaces))}
	for _, sp := range spaces {
		c.shards[sp.Name] = make([]shardTrack, sp.Shards)
	}
	return c
}

// track returns the collector's state of a shard.
func (c *stepCollector) track(space string, shard int) *shardTrack {
	return &c.shards[space][shard]
}

// collect emits the events of one step, in the order stepCollector lists.
func (c *stepCollector) collect(rec stepRecord) {
	// Slot: session.restarted. A new incarnation's first step follows observer.newIncarnation,
	// which may emit it there, before this step's events.
	c.viewEvents(rec)
	if ignored(rec) {
		return
	}
	// Slot: session.expired, once per deadline, at the first step at or past it.
	c.inputEvents(rec)
	c.releasedEvents(rec)
	c.actionEvents(rec)
	c.slotEvents(rec)
	// Slot: shard.stuck, for each shard whose Stuck turned true.
}

// ignored reports that the machine ignored the input of rec: the step changed nothing that Status
// shows but the clock, and returned no action but a Wake. A failed renewal the machine accepts is
// not ignored even so: a second failure with the same cause changes nothing that Status shows, but
// session.renew_failed and session.recovered's count report each one.
func ignored(rec stepRecord) bool {
	if f, ok := rec.Input.(core.SessionFailed); ok && acceptsFailure(rec.Before, f.Cause) {
		return false
	}
	for _, a := range rec.Actions {
		if _, ok := a.(core.Wake); !ok {
			return false
		}
	}
	return sameStatus(rec.Before, rec.After)
}

// acceptsFailure reports whether a machine whose Status is st accepts a SessionFailed with cause:
// one with a named cause, while the session has not ended.
func acceptsFailure(st core.Status, cause core.Cause) bool {
	switch st.Session {
	case core.SessionStateLost, core.SessionStateClosing, core.SessionStateClosed:
		return false
	}
	return cause == core.CauseUnreachable || cause == core.CauseLost
}

// sameStatus reports whether a and b are equal but for Now.
func sameStatus(a, b core.Status) bool {
	if a.Session != b.Session || a.Deadline != b.Deadline || a.LastFailure != b.LastFailure ||
		a.Done != b.Done || a.Draining != b.Draining || a.Slot != b.Slot || a.ViewVersion != b.ViewVersion {
		return false
	}
	return slices.EqualFunc(a.Spaces, b.Spaces, func(x, y core.SpaceStatus) bool {
		return x.Name == y.Name && x.Fenced == y.Fenced && slices.Equal(x.Shards, y.Shards)
	})
}

// viewEvents emits view.changed for a stepped ViewChanged: the node has published its view, also
// when the machine took no view, as while the session closes. slot.count_changed follows when the
// view's slot count differs from the last one it reported, 0 before the first.
func (c *stepCollector) viewEvents(rec stepRecord) {
	v, ok := rec.Input.(core.ViewChanged)
	if !ok {
		return
	}
	c.emit(viewChanged(v.View))
	if v.View.Slots != c.slots {
		c.slots = v.View.Slots
		c.emit(Event{Kind: EventSlotCountChanged, Shard: -1, Slot: -1, Count: c.slots})
	}
}

// viewChanged returns the view.changed event of v.
func viewChanged(v core.View) Event {
	return Event{Kind: EventViewChanged, Shard: -1, Slot: -1, Version: v.Version, Count: len(v.Members)}
}

// inputEvents emits the input's own event: session.opened at the incarnation's first accepted
// renewal, session.closed when the session closes, and shard.acquire_failed when an acquiring
// shard takes an AcquireFailed.
func (c *stepCollector) inputEvents(rec stepRecord) {
	switch in := rec.Input.(type) {
	case core.SessionRenewed:
		// The deadline is 0 until the first accepted renewal, and each machine is a new
		// incarnation.
		if rec.Before.Deadline == 0 && rec.After.Deadline != 0 {
			c.emit(Event{Kind: EventSessionOpened, Shard: -1, Slot: -1})
		}
		// Slot: session.renewed_late and session.recovered.
	case core.SessionFailed:
		// Slot: session.renew_failed and session.lost.
	case core.SessionClosed:
		if rec.Before.Session != core.SessionStateClosed && rec.After.Session == core.SessionStateClosed {
			c.emit(Event{Kind: EventSessionClosed, Shard: -1, Slot: -1})
		}
	case core.AcquireFailed:
		if sh, ok := shardIn(rec.Before, in.Space, in.Shard); ok && sh.State == core.ShardStateAcquiring {
			e := shardEvent(EventShardAcquireFailed, in.Space, in.Shard, 0)
			e.Holder, e.Duration = in.Holder, in.RetryAfter
			if held := (*HeldError)(nil); !errors.As(rec.Err, &held) {
				e.Err = rec.Err
			}
			c.emit(e)
		}
	}
}

// releasedEvents emits shard.released for each shard that the step moved out of releasing the
// term it was releasing, by space name and then shard, and shard.handoff_completed after it when
// the run that held the term was stopped as moved or draining.
func (c *stepCollector) releasedEvents(rec stepRecord) {
	for i, sp := range rec.Before.Spaces {
		if i >= len(rec.After.Spaces) {
			return
		}
		after := rec.After.Spaces[i].Shards
		for id, b := range sp.Shards {
			if b.State != core.ShardStateReleasing || id >= len(after) {
				continue
			}
			if a := after[id]; a.State == core.ShardStateReleasing && a.Term == b.Term {
				continue
			}
			c.released(sp.Name, id, b.Term, rec.Now)
		}
	}
}

// released emits shard.released for term of a shard at the loop's reading now: its Reason is the
// stop reason of the run that held term, "" when no run did, and a moved or draining run's
// handoff completes with it.
func (c *stepCollector) released(space string, shard int, term core.Term, now time.Duration) {
	t := c.track(space, shard)
	e := shardEvent(EventShardReleased, space, shard, term)
	if t.stopTerm == term {
		e.Reason = t.stop
	}
	if t.releasing == term {
		t.releasing = 0
	}
	c.emit(e)
	switch e.Reason {
	case core.StopMoved.String(), core.StopDraining.String():
		e.Kind, e.Duration = EventShardHandoffCompleted, now-t.stopAt
		c.emit(e)
	}
}

// actionEvents emits one event per executed action, in Step's order.
func (c *stepCollector) actionEvents(rec stepRecord) {
	for _, act := range rec.Actions {
		switch a := act.(type) {
		case core.Acquire:
			c.emit(shardEvent(EventShardAcquiring, a.Space, a.Shard, 0))
		case core.StartRun:
			c.emit(shardEvent(EventShardAcquired, a.Space, a.Shard, a.Term))
		case core.StopRun:
			c.stopRun(a, rec.Now)
		case core.Release:
			if t := c.track(a.Space, a.Shard); t.releasing != a.Term {
				t.releasing = a.Term
				c.emit(shardEvent(EventShardReleasing, a.Space, a.Shard, a.Term))
			}
		case core.Degrade:
			// Slot: shard.degraded.
		case core.Restore:
			// Slot: shard.recovered.
		}
	}
}

// stopRun notes the StopRun a of a run at the loop's reading now and emits shard.stopping for a
// run stopped as moved or draining.
func (c *stepCollector) stopRun(a core.StopRun, now time.Duration) {
	c.stopped(a.Space, a.Shard, a.Term, a.Reason.String(), now)
	switch a.Reason {
	case core.StopMoved, core.StopDraining:
		e := shardEvent(EventShardStopping, a.Space, a.Shard, a.Term)
		e.Reason = a.Reason.String()
		c.emit(e)
	case core.StopExpired:
		c.emit(shardEvent(EventShardExpired, a.Space, a.Shard, a.Term))
	case core.StopLost:
		c.emit(shardEvent(EventShardLost, a.Space, a.Shard, a.Term))
	}
}

// slotEvents emits slot.released when the step ended the hold of the slot the node held, and
// slot.claimed when it made the node hold a slot (OBS-130). A release that the step's SlotReleased
// answered is "draining" once Drain or Close has started the drain, else "retired"; one without
// it, as when another holder took the slot or the session was lost, is "lost".
func (c *stepCollector) slotEvents(rec stepRecord) {
	before, after := rec.Before.Slot, rec.After.Slot
	if before == after {
		return
	}
	if before >= 0 {
		e := Event{Kind: EventSlotReleased, Shard: -1, Slot: before, Reason: slotLost}
		if r, ok := rec.Input.(core.SlotReleased); ok && r.Slot == before {
			e.Reason = slotRetired
			if c.draining.Load() {
				e.Reason = slotDraining
			}
		}
		c.emit(e)
	}
	if after >= 0 {
		c.standby = false
		c.emit(Event{Kind: EventSlotClaimed, Shard: -1, Slot: after})
	}
}

// The reasons of slot.released.
const (
	slotLost     = "lost"
	slotDraining = "draining"
	slotRetired  = "retired"
)

// noSlot emits slot.standby for a ClaimSlot that found no free slot, the first since the node
// started or last held a slot.
func (c *stepCollector) noSlot() {
	if !c.standby {
		c.standby = true
		c.emit(Event{Kind: EventSlotStandby, Shard: -1, Slot: -1})
	}
}

// closed emits shard.stopping with reason "closed" for sh, which a Close whose ctx ended first
// stopped at the reading now (RUN-530).
func (c *stepCollector) closed(sh *Shard, now time.Duration) {
	c.stopped(sh.Space, sh.ID, sh.Term, reasonClosed, now)
	e := shardEvent(EventShardStopping, sh.Space, sh.ID, sh.Term)
	e.Reason = reasonClosed
	c.emit(e)
}

// stopped notes that the run of term of a shard was told to stop for reason at the reading now.
func (c *stepCollector) stopped(space string, shard int, term core.Term, reason string, now time.Duration) {
	t := c.track(space, shard)
	t.stopTerm, t.stop, t.stopAt = term, reason, now
}

// shardEvent returns an event of kind about a shard and term; it names no slot.
func shardEvent(kind EventKind, space string, shard int, term core.Term) Event {
	return Event{Kind: kind, Space: space, Shard: shard, Term: term, Slot: -1}
}
