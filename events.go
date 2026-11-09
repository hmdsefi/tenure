// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// EventKind says what an Event reports. Its String method returns the kind's name, such as
// "shard.acquired", which is also the kind's name in log records. Each kind's doc comment lists
// the Event fields it fills besides Seq, Time and Kind; the node sets the fields a kind does not
// use to their unused values (see Event).
type EventKind uint8

// EventViewChanged ("view.changed") is emitted when the node takes a new merged view of the
// membership: Version is the view's version and Count its number of members.
const EventViewChanged EventKind = 1

// EventSessionOpened ("session.opened") is emitted when the store first accepts a renewal of
// a session incarnation, which makes the session live.
const EventSessionOpened EventKind = 2

// EventSessionRenewFailed ("session.renew_failed") is emitted when a renewal of the session
// fails: Err is the error of the KeepAlive call.
const EventSessionRenewFailed EventKind = 3

// EventSessionRenewedLate ("session.renewed_late") is emitted when a renewal is accepted
// after the session deadline passed: Duration is how long after the deadline the node took
// the renewal.
const EventSessionRenewedLate EventKind = 4

// EventSessionRecovered ("session.recovered") is emitted right after
// EventSessionRenewedLate: Count is the number of failed renewals since the last accepted
// one.
const EventSessionRecovered EventKind = 5

// EventSessionExpired ("session.expired") is emitted once per session deadline, when the
// node steps an input at or after that deadline without an accepted renewal, unless the
// session was lost or is closing by then.
const EventSessionExpired EventKind = 6

// EventSessionLost ("session.lost") is emitted at the first sign that a session incarnation
// has ended for good: Err is the error of the call that reported it.
const EventSessionLost EventKind = 7

// EventSessionClosed ("session.closed") is emitted when the node has closed its session,
// after it released its shards on Drain or Close.
const EventSessionClosed EventKind = 8

// EventShardAcquiring ("shard.acquiring") is emitted when the node starts an attempt to
// acquire a shard: Space and Shard name the shard.
const EventShardAcquiring EventKind = 9

// EventShardAcquireFailed ("shard.acquire_failed") is emitted when an attempt to acquire a
// shard fails, either because another holder has it (a HeldError) or because the Acquire call
// failed with another error, as when the store is unreachable: Space and Shard name the shard,
// Holder is the holder that has it ("" when not known, as after a failed call), Duration is
// the wait before the next attempt that the failure asks for, the HeldError's RetryAfter or
// else the RetryPeriod of Config.Timing, and Err is the call's error, or nil when the shard is
// held by another holder. A node that drains makes no next attempt. A call that fails with
// ErrSessionLost emits this event, then EventSessionLost.
const EventShardAcquireFailed EventKind = 10

// EventShardAcquired ("shard.acquired") is emitted when the node has acquired a shard and
// starts its Run: Space and Shard name the shard and Term is the acquired term.
const EventShardAcquired EventKind = 11

// EventShardDegraded ("shard.degraded") is emitted when the session deadline passes in a
// space with fencing: the shard's Run keeps running on its term. Space, Shard and Term name
// the shard and its term.
const EventShardDegraded EventKind = 12

// EventShardRecovered ("shard.recovered") is emitted when the store confirms a degraded
// shard's term after the session recovered. Space, Shard and Term name the shard and its
// unchanged term.
const EventShardRecovered EventKind = 13

// EventShardExpired ("shard.expired") is emitted when the node stops a shard's Run because
// the session deadline passed in a space without fencing. Space, Shard and Term name the
// shard and its term.
const EventShardExpired EventKind = 14

// EventShardLost ("shard.lost") is emitted when the node stops a shard's Run because it no
// longer holds the shard: another holder has the shard, another holder took the slot the
// shard belongs to, the node gave up a held slot that the views showed vacant too long, or
// the session has ended for good. Space, Shard and Term name the shard and its term.
const EventShardLost EventKind = 15

// EventShardStopping ("shard.stopping") is emitted when the node stops a shard's Run to hand
// the shard over, or because Close gives up on the drain: Space, Shard and Term name the shard
// and its term, and Reason is "moved" (the assignment gives the shard to another member),
// "draining" (the node drains) or "closed" (Close's context ended before the drain completed;
// the node then stops without releasing the shard).
const EventShardStopping EventKind = 16

// EventShardStuck ("shard.stuck") is emitted once per run when a stopped Run has not returned
// in time: Space, Shard and Term name the shard and its term, and Reason is the reason the
// run was stopped.
const EventShardStuck EventKind = 17

// EventShardReleasing ("shard.releasing") is emitted once per term when the node starts to
// release a shard's term in the store: Space, Shard and Term name the shard and the term.
const EventShardReleasing EventKind = 18

// EventShardReleased ("shard.released") is emitted when the node has released a shard's
// term, or has let go of it without a release once its stopped Run returned because the store
// no longer names the node for it or the session was lost: Space, Shard and Term name the
// shard and the term, and Reason is the reason the run that held the term was stopped, or ""
// when no run held it.
const EventShardReleased EventKind = 19

// EventShardHandoffCompleted ("shard.handoff_completed") is emitted right after
// EventShardReleased when the run was stopped because the shard moved or the node drains:
// Space, Shard, Term and Reason are those of EventShardReleased, and Duration is the time
// from the run's EventShardStopping to the release.
const EventShardHandoffCompleted EventKind = 20

// EventSlotClaimed ("slot.claimed") is emitted when the node starts to hold a slot: Slot is
// the slot.
const EventSlotClaimed EventKind = 21

// EventSlotReleased ("slot.released") is emitted when the node stops holding a slot: Slot is
// the slot, and Reason is "draining" (the node drains), "retired" (the slot count no longer
// includes the slot) or "lost" (the node lost the slot without releasing it, as when another
// holder took it or the session was lost).
const EventSlotReleased EventKind = 22

// EventSlotStandby ("slot.standby") is emitted when the node finds no free slot for the first
// time since Start or since it last held a slot, and waits as a standby.
const EventSlotStandby EventKind = 23

// EventSlotCountChanged ("slot.count_changed") is emitted right after EventViewChanged when
// the view's slot count differs from the previous view's, taken as 0 before the first view:
// Count is the new count.
const EventSlotCountChanged EventKind = 24

// EventSlotCountMismatch ("slot.count_mismatch") is emitted by Start when Config.Slots is
// above 0 and the store already holds another slot count, which the node keeps: Count is the
// stored count and Configured is Config.Slots.
const EventSlotCountMismatch EventKind = 25

// EventRunReturned ("run.returned") is emitted when a shard's Run returns while the node
// still owns the shard: Space, Shard and Term name the shard and its term, Count is the
// number of consecutive returns, Duration is how long the node waits before it starts Run
// again, and Err is the error Run returned, or nil.
const EventRunReturned EventKind = 26

// EventStoreFailed ("store.failed") is emitted when a call the node makes to the Store on its
// own fails, or when the store's watch ends: Reason is the name of the Store method, such as
// "Release" or "WatchHolders", Err is the error, and Space, Shard and Slot are the call's
// space, shard and slot when the call has them. Failed Acquire and KeepAlive calls have their
// own events, a ClaimSlot that finds no free slot is not a failure, and an error that Start or
// Node.SetSlots returns emits no event.
const EventStoreFailed EventKind = 27

// EventMembershipFailed ("membership.failed") is emitted when a Watch, SetDraining or Leave
// call of the Membership fails, or when the watch channel closes before Close calls Leave:
// Reason is the name of the Membership method, such as "Watch", and Err is the error. A failed
// Join is Start's error and emits no event.
const EventMembershipFailed EventKind = 28

// EventNodeDraining ("node.draining") is emitted when the first Drain or Close starts the
// node's drain.
const EventNodeDraining EventKind = 29

// EventNodeDrained ("node.drained") is emitted when the drain has completed: Duration is the
// time since EventNodeDraining.
const EventNodeDrained EventKind = 30

// EventNodeClosed ("node.closed") is the last event of a node, emitted when Close finishes:
// Reason is "drained" when the drain completed or "deadline" when Close's context ended
// first.
const EventNodeClosed EventKind = 31

// EventSessionRestarted ("session.restarted") is emitted when the node starts a new session
// incarnation after the previous one was lost, before the new incarnation's other events:
// Holder is the previous incarnation's holder.
const EventSessionRestarted EventKind = 32

// String returns the kind's name, such as "shard.acquired", or text such as "EventKind(0)" for
// a value that is not a kind.
func (k EventKind) String() string {
	if k.known() {
		return kinds[k].name
	}
	return "EventKind(" + strconv.Itoa(int(k)) + ")"
}

// known reports whether k is one of the kinds in the kinds table.
func (k EventKind) known() bool {
	return k >= EventViewChanged && int(k) < len(kinds)
}

// Event is one thing that happened on a node, such as a change of the view, the session or a
// shard's ownership, as Config.OnEvent receives it. Each kind fills the fields its doc comment
// lists; a field the kind does not use holds its unused value: "" for Space, -1 for Shard and
// Slot, and the zero value for the others.
type Event struct {
	// Seq is the event's number on its node: 1 for the first event, then one more for each
	// event, without gaps.
	Seq uint64

	// Time is the wall-clock time, in UTC, at which the node emitted the event.
	Time time.Time

	// Kind says what the event reports.
	Kind EventKind

	// Space is the space of the shard or store call the event is about, or "" when unused.
	Space string

	// Shard is the index of the shard in Space, or -1 when unused.
	Shard int

	// Term is the shard's term, or 0 when unused.
	Term core.Term

	// Slot is the slot the event is about, or -1 when unused.
	Slot int

	// Reason says why, or which call: a stop reason such as "moved", a method name such as
	// "Release", or "" when unused.
	Reason string

	// Holder is a holder string other than the node's current one: the holder that has a shard,
	// or the previous holder of the node. It is "" when unused.
	Holder string

	// Duration is a length of time, such as a wait before a retry or the time a handoff took,
	// or 0 when unused.
	Duration time.Duration

	// Version is the version of the membership view, or 0 when unused.
	Version uint64

	// Count is a number, such as the number of members, of failed renewals or of slots, or 0
	// when unused.
	Count int

	// Configured is the slot count of Config.Slots, or 0 when unused.
	Configured int

	// Err is the error the event reports, or nil.
	Err error
}

// eventField names the Event field an attribute is read from.
type eventField uint8

const (
	fieldSpace eventField = iota
	fieldShard
	fieldTerm
	fieldSlot
	fieldReason
	fieldHolder
	fieldDuration
	fieldVersion
	fieldCount
	fieldConfigured
	fieldErr
)

// attrSpec is one attribute of a kind: its key and the Event field it is read from. The error
// attribute is present only when Err is not nil. When opt is true, a space, shard or slot
// attribute is present only when the field is not unused ("" or -1); otherwise it is always
// present, even with the unused value.
type attrSpec struct {
	key   string
	field eventField
	opt   bool
}

// kindInfo is what the kinds table holds for one kind: its name, the level and message of its
// log records, and its attributes in order.
type kindInfo struct {
	name  string
	level slog.Level
	msg   string
	attrs []attrSpec
}

// The attributes shared by several kinds.
var (
	attrSpace  = attrSpec{key: "space", field: fieldSpace}
	attrShard  = attrSpec{key: "shard", field: fieldShard}
	attrTerm   = attrSpec{key: "term", field: fieldTerm}
	attrSlot   = attrSpec{key: "slot", field: fieldSlot}
	attrReason = attrSpec{key: "reason", field: fieldReason}
	attrOp     = attrSpec{key: "op", field: fieldReason}
	attrTook   = attrSpec{key: "took", field: fieldDuration}
	attrError  = attrSpec{key: "error", field: fieldErr}
)

// kinds is the table of event kinds, indexed by EventKind; index 0 is not a kind. The message
// is "tenure: " and the name with "." and "_" turned into spaces. String, the event log and the
// debughttp drift test read it.
var kinds = [...]kindInfo{
	EventViewChanged: {
		name: "view.changed", level: slog.LevelInfo, msg: "tenure: view changed",
		attrs: []attrSpec{{key: "version", field: fieldVersion}, {key: "members", field: fieldCount}},
	},
	EventSessionOpened: {
		name: "session.opened", level: slog.LevelInfo, msg: "tenure: session opened",
	},
	EventSessionRenewFailed: {
		name: "session.renew_failed", level: slog.LevelWarn, msg: "tenure: session renew failed",
		attrs: []attrSpec{attrError},
	},
	EventSessionRenewedLate: {
		name: "session.renewed_late", level: slog.LevelWarn, msg: "tenure: session renewed late",
		attrs: []attrSpec{{key: "late_by", field: fieldDuration}},
	},
	EventSessionRecovered: {
		name: "session.recovered", level: slog.LevelInfo, msg: "tenure: session recovered",
		attrs: []attrSpec{{key: "failures", field: fieldCount}},
	},
	EventSessionExpired: {
		name: "session.expired", level: slog.LevelError, msg: "tenure: session expired",
	},
	EventSessionLost: {
		name: "session.lost", level: slog.LevelError, msg: "tenure: session lost",
		attrs: []attrSpec{attrError},
	},
	EventSessionClosed: {
		name: "session.closed", level: slog.LevelInfo, msg: "tenure: session closed",
	},
	EventShardAcquiring: {
		name: "shard.acquiring", level: slog.LevelDebug, msg: "tenure: shard acquiring",
		attrs: []attrSpec{attrSpace, attrShard},
	},
	EventShardAcquireFailed: {
		name: "shard.acquire_failed", level: slog.LevelDebug, msg: "tenure: shard acquire failed",
		attrs: []attrSpec{
			attrSpace, attrShard, {key: "holder", field: fieldHolder},
			{key: "retry_after", field: fieldDuration}, attrError,
		},
	},
	EventShardAcquired: {
		name: "shard.acquired", level: slog.LevelInfo, msg: "tenure: shard acquired",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm},
	},
	EventShardDegraded: {
		name: "shard.degraded", level: slog.LevelWarn, msg: "tenure: shard degraded",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm},
	},
	EventShardRecovered: {
		name: "shard.recovered", level: slog.LevelInfo, msg: "tenure: shard recovered",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm},
	},
	EventShardExpired: {
		name: "shard.expired", level: slog.LevelError, msg: "tenure: shard expired",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm},
	},
	EventShardLost: {
		name: "shard.lost", level: slog.LevelError, msg: "tenure: shard lost",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm},
	},
	EventShardStopping: {
		name: "shard.stopping", level: slog.LevelInfo, msg: "tenure: shard stopping",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm, attrReason},
	},
	EventShardStuck: {
		name: "shard.stuck", level: slog.LevelWarn, msg: "tenure: shard stuck",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm, attrReason},
	},
	EventShardReleasing: {
		name: "shard.releasing", level: slog.LevelDebug, msg: "tenure: shard releasing",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm},
	},
	EventShardReleased: {
		name: "shard.released", level: slog.LevelInfo, msg: "tenure: shard released",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm, attrReason},
	},
	EventShardHandoffCompleted: {
		name: "shard.handoff_completed", level: slog.LevelInfo, msg: "tenure: shard handoff completed",
		attrs: []attrSpec{attrSpace, attrShard, attrTerm, attrReason, attrTook},
	},
	EventSlotClaimed: {
		name: "slot.claimed", level: slog.LevelInfo, msg: "tenure: slot claimed",
		attrs: []attrSpec{attrSlot},
	},
	EventSlotReleased: {
		name: "slot.released", level: slog.LevelInfo, msg: "tenure: slot released",
		attrs: []attrSpec{attrSlot, attrReason},
	},
	EventSlotStandby: {
		name: "slot.standby", level: slog.LevelInfo, msg: "tenure: slot standby",
	},
	EventSlotCountChanged: {
		name: "slot.count_changed", level: slog.LevelInfo, msg: "tenure: slot count changed",
		attrs: []attrSpec{{key: "slots", field: fieldCount}},
	},
	EventSlotCountMismatch: {
		name: "slot.count_mismatch", level: slog.LevelWarn, msg: "tenure: slot count mismatch",
		attrs: []attrSpec{{key: "stored", field: fieldCount}, {key: "configured", field: fieldConfigured}},
	},
	EventRunReturned: {
		name: "run.returned", level: slog.LevelWarn, msg: "tenure: run returned",
		attrs: []attrSpec{
			attrSpace, attrShard, attrTerm, {key: "restarts", field: fieldCount},
			{key: "restart_in", field: fieldDuration}, attrError,
		},
	},
	EventStoreFailed: {
		name: "store.failed", level: slog.LevelWarn, msg: "tenure: store failed",
		attrs: []attrSpec{
			attrOp,
			{key: "space", field: fieldSpace, opt: true},
			{key: "shard", field: fieldShard, opt: true},
			{key: "slot", field: fieldSlot, opt: true},
			attrError,
		},
	},
	EventMembershipFailed: {
		name: "membership.failed", level: slog.LevelWarn, msg: "tenure: membership failed",
		attrs: []attrSpec{attrOp, attrError},
	},
	EventNodeDraining: {
		name: "node.draining", level: slog.LevelInfo, msg: "tenure: node draining",
	},
	EventNodeDrained: {
		name: "node.drained", level: slog.LevelInfo, msg: "tenure: node drained",
		attrs: []attrSpec{attrTook},
	},
	EventNodeClosed: {
		name: "node.closed", level: slog.LevelInfo, msg: "tenure: node closed",
		attrs: []attrSpec{attrReason},
	},
	EventSessionRestarted: {
		name: "session.restarted", level: slog.LevelInfo, msg: "tenure: session restarted",
		attrs: []attrSpec{{key: "previous", field: fieldHolder}},
	},
}

// eventAttrs returns the log attributes of e, in its kind's order: strings as strings, int fields
// as int64, Term and Version as uint64, durations as durations and Err as its error value. The
// error attribute is left out when Err is nil, and an optional space, shard or slot attribute
// when its field holds the unused value. It returns nil for a value that is not a kind.
func eventAttrs(e Event) []slog.Attr {
	if !e.Kind.known() {
		return nil
	}
	specs := kinds[e.Kind].attrs
	attrs := make([]slog.Attr, 0, len(specs))
	for _, a := range specs {
		if v, ok := a.value(e); ok {
			attrs = append(attrs, slog.Attr{Key: a.key, Value: v})
		}
	}
	return attrs
}

// value returns the attribute's value in e, and false when the attribute is absent.
func (a attrSpec) value(e Event) (slog.Value, bool) {
	switch a.field {
	case fieldSpace:
		return slog.StringValue(e.Space), !a.opt || e.Space != ""
	case fieldShard:
		return slog.IntValue(e.Shard), !a.opt || e.Shard >= 0
	case fieldTerm:
		return slog.Uint64Value(uint64(e.Term)), true
	case fieldSlot:
		return slog.IntValue(e.Slot), !a.opt || e.Slot >= 0
	case fieldReason:
		return slog.StringValue(e.Reason), true
	case fieldHolder:
		return slog.StringValue(e.Holder), true
	case fieldDuration:
		return slog.DurationValue(e.Duration), true
	case fieldVersion:
		return slog.Uint64Value(e.Version), true
	case fieldCount:
		return slog.IntValue(e.Count), true
	case fieldConfigured:
		return slog.IntValue(e.Configured), true
	case fieldErr:
		return slog.AnyValue(e.Err), e.Err != nil
	}
	return slog.Value{}, false
}
