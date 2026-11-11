// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// storeModel is the simulated store as a state machine: the sessions of the holders, the shard
// records, the slot records and the slot count, and the view of who takes part. It has no
// network, no timer and no clock: handle answers one request and expire lets time pass, each
// with the store's clock readings as arguments, and what a call changed is left as effects in
// an outbox that the caller takes with takeEffects.
//
// Under the Server rule the model answers as memstore does for the same requests and the same
// readings: sessions, grants, held shards, releases, read-only confirmations and slots. The
// store's clock ends a session that was not renewed for the LeaseDuration of its own timing,
// and acquireReq and claimSlotReq take what such a session holds. memstore's Slots is expire
// at the reading followed by currentView, or currentView alone at a slot count of 0, where
// memstore reads no clock; memstore's DeleteRecord has no counterpart. Where
// memstore removes a slot record, the model keeps one with no holder at a new slot version,
// which no reply of the Server rule shows.
//
// Under the Observer rule the store's clock never ends the session of anyone but the sender of
// a request: a member that wants a shard reads the record (getShardReq), reads the session of
// the record's holder (getSessionReq), decides on its own clock, and writes with casReq, which
// succeeds only while the record still has the version the member read. A confirmReq is a
// write there: it gives the record a new version, so a casReq decided before it fails. Slots go
// the same way, with getSlotReq and casSlotReq. An acquireReq or a claimSlotReq, which members
// do not send under this rule, takes only what an ended session holds.
//
// Under both rules a session ends for good when it is closed, when another session of the same
// member opens, when a request that needs it arrives at or after its expiry on the store's
// clock, and, with endAfter, that long after it opened. An ended session's records stay as
// they are until someone takes them; only closing a session clears them.
//
// The store's time is the largest monotonic reading it was given by a request that looks at
// sessions (keepAliveReq, acquireReq, confirmReq, claimSlotReq, casReq, casSlotReq) or by
// expire, so it never moves back. The other requests look at no session and read no time, as
// in memstore.
//
// The model checks no argument: it takes every request as well formed, which the requests of
// the simulated members are. A holder is not empty and has a member ID before its first "/",
// a space is not empty, a shard, a slot and a slot count are not negative, a term is not 0,
// and a timing is valid. It panics only for a request of a type it does not know.
//
// Nothing here depends on the iteration order of a map: every reply, effect and view is built
// in sorted order. A storeModel is not safe for concurrent use.
type storeModel struct {
	rule Takeover
	// endAfter, when positive, ends every session that long after it opened.
	endAfter time.Duration
	// mutation is the scenario's Mutation: GrantHeld and ReadConfirm plant their bugs here in
	// a build with the tag tenure_mutants (mutants_on.go), and no build without it plants any.
	mutation Mutation

	// now is the store's time: the largest reading seen. seen reports that there has been one.
	now  time.Duration
	seen bool

	// version counts the writes of shard records, and slotVersion those of slot records.
	version     uint64
	slotVersion uint64

	// sessions holds every holder the store has heard of, also those whose session has ended,
	// so that an ended session never reopens. alive holds the holders whose session has not
	// ended, sorted.
	sessions map[string]*storeSession
	alive    []string

	// opened maps a member ID to the holder whose session of that member opened last.
	opened map[string]string

	// records holds the shard records by space and shard. A record is never removed.
	records map[string]map[int]tenure.Holding

	// slots holds the slot records by slot, also those at or above slotCount, the retired
	// slots. A record is never removed: a released slot keeps a record with no holder.
	slots     map[int]slotRecord
	slotCount int

	// view is the latest view. Its Version counts the views so far.
	view core.View

	// out is the outbox: the effects since the last takeEffects, in order.
	out []effect
}

// storeSession is one holder's session.
type storeSession struct {
	holder string
	// timing is the timing of the latest keepAliveReq that opened or renewed the session.
	timing core.Timing
	// opened and renewed are the store's time when the session opened and at its latest renewal.
	opened  time.Duration
	renewed time.Duration
	// version is 1 for a session that was opened, and grows by one with every renewal.
	version uint64
	// ended reports that the session has ended for good, and cause says why.
	ended bool
	cause endCause
}

// endCause says why a session ended.
type endCause string

const (
	// causeClosed: a closeSessionReq named the session.
	causeClosed endCause = "closed"
	// causeExpired: the store's clock saw no renewal for the session's LeaseDuration. It happens
	// under the Server rule only, and never at a request of the session's own holder.
	causeExpired endCause = "expired"
	// causeReplaced: another session of the same member opened.
	causeReplaced endCause = "replaced"
	// causeLate: a request of the session's own holder that needs the session arrived at or
	// after its expiry on the store's clock.
	causeLate endCause = "late"
	// causeEndAfter: the session had been open for the store's endAfter.
	causeEndAfter endCause = "end_after"
)

// effect is something a call of the model changed or decided, for the store's node to act on:
// a record to emit, a push to send, an observation for the checks. The effects of a call are
// in the order they happened, and a viewEffect, when the call changed the view, is the last.
type effect interface {
	isEffect()
}

// sessionOp is what happened to a session.
type sessionOp string

// The session operations.
const (
	sessionOpen  sessionOp = "open"
	sessionRenew sessionOp = "renew"
	sessionEnd   sessionOp = "end"
	sessionClose sessionOp = "close"
)

// sessionEffect reports that a session opened, was renewed, ended by itself or was closed. A
// closeSessionReq gives one sessionClose whatever the session was before, also for a holder
// that never had one.
type sessionEffect struct {
	Op     sessionOp
	Holder string
	// Version is the session's version after the operation, 0 for a holder that never opened
	// one.
	Version uint64
	// Cause says why the session ended. It is set for sessionEnd only.
	Cause endCause
}

// grantEffect reports a grant of a shard, with what the store knew when it decided. A
// recordEffect of the same record follows it.
type grantEffect struct {
	// Record is the record after the grant: the grantee, the new term and the new version.
	Record tenure.Holding
	// Prev is the holder the record named before the grant: "" for none, and the grantee itself
	// when it took the shard again. With no Prev, the next three fields are zero.
	Prev string
	// Ended reports that Prev's session had ended for a reason other than causeExpired: it was
	// closed, replaced, late or cut by endAfter, or Prev never had one. A session the store's
	// clock expired is not reported as ended: Unseen then shows the whole Lease.
	Ended bool
	// Unseen is how long the deciding clock had seen Prev's session without a renewal: the
	// store's time since the session's latest renewal for an acquireReq, and the Unseen the
	// acquirer sent for a casReq.
	Unseen time.Duration
	// Lease is the LeaseDuration of Prev's session.
	Lease time.Duration
}

// releaseEffect reports a releaseReq.
type releaseEffect struct {
	// Holder and Term are the sender and the term it named.
	Holder string
	Term   core.Term
	// Record is the record after the request, with only Space and Shard set when there is none.
	Record tenure.Holding
	// Cleared reports that the request cleared the record's holder. A recordEffect of the same
	// record then follows.
	Cleared bool
}

// confirmEffect reports a confirmReq that the store answered with the record.
type confirmEffect struct {
	// Holder and Term are the sender and the term it named.
	Holder string
	Term   core.Term
	// Record is the record after the request, with only Space and Shard set when there is none.
	Record tenure.Holding
	// Wrote reports that the request gave the record a new version, which only the Observer
	// rule does. A recordEffect of the same record then follows.
	Wrote bool
}

// observeOp is one of the requests of a takeover under the Observer rule.
type observeOp string

// The observe operations.
const (
	observeGetShard   observeOp = "get_shard"
	observeGetSlot    observeOp = "get_slot"
	observeGetSession observeOp = "get_session"
	// observeCAS is a casReq or a casSlotReq; OfSlot in the effect tells them apart.
	observeCAS observeOp = "cas"
)

// observeResult is how such a request ended.
type observeResult string

// The observe results.
const (
	// observeOK: a read, or a compare-and-swap that wrote.
	observeOK observeResult = "ok"
	// observeStale: the record no longer had the expected version, or the slot of a casSlotReq
	// was no longer below the slot count.
	observeStale observeResult = "stale"
	// observeLost: the sender had no live session.
	observeLost observeResult = "lost"
)

// observeEffect reports a getShardReq, getSlotReq, getSessionReq, casReq or casSlotReq.
type observeEffect struct {
	Op     observeOp
	Holder string
	// OfSlot reports that the request was about a slot: a getSlotReq or a casSlotReq.
	OfSlot bool
	// Record is the shard record that was read, for a getShardReq and a casReq: for a
	// compare-and-swap, the record before the write.
	Record tenure.Holding
	// Slot is the slot record that was read, for a getSlotReq and a casSlotReq, and Count the
	// slot count then.
	Slot  slotRecord
	Count int
	// Session is the session that was read, for observeGetSession.
	Session sessionInfo
	// Expect is the version a compare-and-swap expected.
	Expect uint64
	Result observeResult
}

// slotOp is what a request asked of the slots.
type slotOp string

// The slot operations.
const (
	slotClaim   slotOp = "claim"
	slotRelease slotOp = "release"
	slotSet     slotOp = "set"
)

// slotResult is how a slot operation ended.
type slotResult string

// The slot results.
const (
	// slotClaimed: the request wrote the slot's record for the sender.
	slotClaimed slotResult = "claimed"
	// slotKept: the sender held the slot already; nothing was written.
	slotKept slotResult = "kept"
	// slotRetired: the sender holds a slot at or above the slot count, and gets none.
	slotRetired slotResult = "retired"
	// slotNone: no slot below the slot count is free.
	slotNone slotResult = "none"
	// slotCleared: the request cleared the slot's holder.
	slotCleared slotResult = "cleared"
	// slotMismatch: the slot's record does not name the sender; nothing was written.
	slotMismatch slotResult = "mismatch"
	// slotClosed: a closeSessionReq cleared the slot's holder.
	slotClosed slotResult = "closed"
	// slotCounted: the request set the slot count.
	slotCounted slotResult = "set"
)

// slotEffect reports a slot operation: a claimSlotReq or a casSlotReq that wrote (slotClaim), a
// releaseSlotReq or a slot that a closeSessionReq cleared (slotRelease), a setSlotsReq
// (slotSet). A request whose sender had no live session reports none.
type slotEffect struct {
	Op     slotOp
	Holder string
	// Slot is the slot, or -1 when the operation is about none: slotRetired, slotNone, slotSet.
	Slot int
	// Count is the slot count after the operation.
	Count  int
	Result slotResult
}

// recordEffect reports one write of a shard record: a grant, a clear or, under the Observer
// rule, a confirmation.
type recordEffect struct {
	// Record is the record after the write.
	Record tenure.Holding
}

// viewEffect reports a new view.
type viewEffect struct {
	// View is the view; the effect owns its slices.
	View core.View
	// Sessions holds the sessions that have not ended, sorted by holder.
	Sessions []sessionInfo
}

func (sessionEffect) isEffect() {}
func (grantEffect) isEffect()   {}
func (releaseEffect) isEffect() {}
func (confirmEffect) isEffect() {}
func (observeEffect) isEffect() {}
func (slotEffect) isEffect()    {}
func (recordEffect) isEffect()  {}
func (viewEffect) isEffect()    {}

// newStoreModel returns an empty store with the takeover rule, the slot count at the start and
// endAfter: when positive, the store ends every session that long after it opened. A rule that
// is not Observer counts as Server. The first view, of no members, has version 0.
func newStoreModel(rule Takeover, slots int, endAfter time.Duration) *storeModel {
	m := &storeModel{
		rule:      rule,
		endAfter:  endAfter,
		sessions:  make(map[string]*storeSession),
		opened:    make(map[string]string),
		records:   make(map[string]map[int]tenure.Holding),
		slots:     make(map[int]slotRecord),
		slotCount: slots,
	}
	m.view = m.buildView()
	return m
}

// handle answers one request. now is the reading of the store's monotonic clock and wall the
// store's wall time; only the requests that look at sessions use now, and only an acquireReq
// uses wall, for the new term. The effects of the request are added to the outbox.
func (m *storeModel) handle(now time.Duration, wall time.Time, req request) reply {
	var rep reply
	switch r := req.(type) {
	case keepAliveReq:
		rep = m.keepAlive(now, r)
	case acquireReq:
		rep = m.acquire(now, wall, r)
	case releaseReq:
		rep = m.release(r)
	case confirmReq:
		rep = m.confirm(now, r)
	case closeSessionReq:
		rep = m.closeSession(r)
	case holdingsReq:
		rep = m.holdings(r)
	case claimSlotReq:
		rep = m.claimSlot(now, r)
	case releaseSlotReq:
		rep = m.releaseSlot(r)
	case setSlotsReq:
		rep = m.setSlots(r)
	case getShardReq:
		rep = m.getShard(r)
	case getSlotReq:
		rep = m.getSlot(r)
	case getSessionReq:
		rep = m.getSession(r)
	case casReq:
		rep = m.cas(now, r)
	case casSlotReq:
		rep = m.casSlot(now, r)
	default:
		panic(fmt.Sprintf("world: store model: unknown request %T", req))
	}
	m.refreshView()
	return m.stampView(rep)
}

// stampView returns rep with the version of the latest view, for a reply to a slot request:
// the driver discards a view read before it (MCH O8).
func (m *storeModel) stampView(rep reply) reply {
	switch r := rep.(type) {
	case slotClaimedReply:
		r.View = m.view.Version
		return r
	case slotReleasedReply:
		r.View = m.view.Version
		return r
	}
	return rep
}

// expire moves the store's time to now, if that is later, and ends every session whose end has
// come: under the Server rule the sessions that were not renewed for their LeaseDuration, and
// under both rules those that have been open for endAfter. The store's node calls it when the
// timer it set for nextEnd fires.
func (m *storeModel) expire(now time.Duration) {
	m.tick(now)
	m.settle("")
	m.refreshView()
}

// nextEnd returns the earliest store time at which a session ends by itself, as expire would
// end it, and false when no session will. A session that is renewed in time ends later, so the
// caller asks again after every call of handle and expire.
func (m *storeModel) nextEnd() (time.Duration, bool) {
	var first time.Duration
	found := false
	for _, holder := range m.alive {
		at, _, ok := m.endOf(m.sessions[holder], m.rule != Observer)
		if ok && (!found || at < first) {
			first, found = at, true
		}
	}
	return first, found
}

// takeEffects returns the effects since the last call, in order, and empties the outbox.
func (m *storeModel) takeEffects() []effect {
	out := m.out
	m.out = nil
	return out
}

// currentView returns the latest view, in slices of the caller's own. Its Version is the number
// of views before it: it grows by one whenever the members, the slot count or the slot holders
// change. Members has one member per session that has not ended, sorted by ID, and with slots
// each member's Slot is the lowest slot below the count that a session of the member holds,
// else -1. SlotHolders has, for each slot below the count, the holder of the slot's record while
// its session has not ended, else "".
//
// Under the Server rule the sessions that have not ended are those the store's clock found live
// when it last looked: at the latest request that looks at sessions, or the latest expire. A
// renewal, which changes a session's version only, makes no new view, and neither does a
// replacement that keeps the member's ID and slots: the versions observers watch travel in the
// sessionEffects and in sessionList.
func (m *storeModel) currentView() core.View {
	return cloneView(m.view)
}

// sessionList returns the sessions that have not ended, sorted by holder.
func (m *storeModel) sessionList() []sessionInfo {
	if len(m.alive) == 0 {
		return nil
	}
	out := make([]sessionInfo, len(m.alive))
	for i, holder := range m.alive {
		out[i] = m.session(holder)
	}
	return out
}

// allRecords returns every shard record, also those with no holder, sorted by space and then
// by shard.
func (m *storeModel) allRecords() []tenure.Holding {
	var out []tenure.Holding
	for _, space := range slices.Sorted(maps.Keys(m.records)) {
		out = append(out, m.spaceRecords(space)...)
	}
	return out
}

// snapshot returns everything a member needs to catch up: every record, the latest view and
// the sessions that have not ended.
func (m *storeModel) snapshot() snapshotPush {
	return snapshotPush{Records: m.allRecords(), View: m.currentView(), Sessions: m.sessionList()}
}

// keepAlive opens a session for a holder the store has never heard of, which ends the session
// of the same member that opened last, or renews the sender's live session and stores the
// timing. A sender whose session has ended, also at this request, gets a lostReply.
func (m *storeModel) keepAlive(now time.Duration, r keepAliveReq) reply {
	m.tick(now)
	m.settle(r.Holder)
	s := m.sessions[r.Holder]
	if s == nil {
		member := sessionMember(r.Holder)
		if prev, ok := m.opened[member]; ok && !m.sessions[prev].ended {
			m.end(m.sessions[prev], causeReplaced)
		}
		m.opened[member] = r.Holder
		s = &storeSession{holder: r.Holder, timing: r.Timing, opened: m.now, renewed: m.now, version: 1}
		m.sessions[r.Holder] = s
		at, _ := slices.BinarySearch(m.alive, r.Holder)
		m.alive = slices.Insert(m.alive, at, r.Holder)
		m.emit(sessionEffect{Op: sessionOpen, Holder: r.Holder, Version: s.version})
		return renewedReply{replyHead: replyHead{r.ID}, Version: s.version, Opened: true}
	}
	if s.ended {
		return lostReply{replyHead{r.ID}}
	}
	s.timing = r.Timing
	s.renewed = m.now
	s.version++
	m.emit(sessionEffect{Op: sessionRenew, Holder: r.Holder, Version: s.version})
	return renewedReply{replyHead: replyHead{r.ID}, Version: s.version}
}

// acquire grants the shard to the sender when the record is absent, has no holder, names the
// sender itself or names a holder whose session has ended; the new term is core.NextTerm of the
// record's term and wall. Otherwise it answers heldReply with the RetryPeriod of the sender's
// own timing.
func (m *storeModel) acquire(now time.Duration, wall time.Time, r acquireReq) reply {
	s := m.caller(now, r.Holder)
	if s == nil {
		return lostReply{replyHead{r.ID}}
	}
	rec := m.record(r.Space, r.Shard)
	if rec.Holder != "" && rec.Holder != r.Holder && !m.gone(rec.Holder) && !m.mutation.plants(GrantHeld) {
		return heldReply{replyHead: replyHead{r.ID}, Record: rec, RetryAfter: s.timing.RetryPeriod}
	}
	unseen := time.Duration(0)
	if prev := m.sessions[rec.Holder]; prev != nil {
		unseen = m.now - prev.renewed
	}
	return grantedReply{replyHead{r.ID}, m.grant(rec, r.Holder, core.NextTerm(rec.Term, wall), unseen)}
}

// release clears the record's holder, keeping the term, when the record names the sender at
// the request's term. It looks at no session.
func (m *storeModel) release(r releaseReq) reply {
	rec := m.record(r.Space, r.Shard)
	cleared := rec.Version != 0 && rec.Holder == r.Holder && rec.Term == r.Term
	if cleared {
		rec.Holder = ""
		rec = m.write(rec)
	}
	m.emit(releaseEffect{Holder: r.Holder, Term: r.Term, Record: rec, Cleared: cleared})
	if cleared {
		m.emit(recordEffect{rec})
	}
	return releasedReply{replyHead: replyHead{r.ID}, Record: rec, Cleared: cleared}
}

// confirm answers with the shard's record. Under the Server rule it only reads. Under the
// Observer rule it gives a record that names the sender at the request's term a new version,
// and changes neither its holder nor its term: a casReq that read the record before then fails.
func (m *storeModel) confirm(now time.Duration, r confirmReq) reply {
	if m.caller(now, r.Holder) == nil {
		return lostReply{replyHead{r.ID}}
	}
	rec := m.record(r.Space, r.Shard)
	wrote := m.rule == Observer && rec.Version != 0 && rec.Holder == r.Holder && rec.Term == r.Term &&
		!m.mutation.plants(ReadConfirm)
	if wrote {
		rec = m.write(rec)
	}
	m.emit(confirmEffect{Holder: r.Holder, Term: r.Term, Record: rec, Wrote: wrote})
	if wrote {
		m.emit(recordEffect{rec})
	}
	return confirmedReply{replyHead{r.ID}, rec}
}

// closeSession ends the sender's session for good, also when it has ended or never opened, and
// clears the holder of every shard record that names the sender, space by space and shard by
// shard, and then of every slot record that names it, also of retired slots.
func (m *storeModel) closeSession(r closeSessionReq) reply {
	s := m.sessions[r.Holder]
	switch {
	case s == nil:
		s = &storeSession{holder: r.Holder, ended: true, cause: causeClosed}
		m.sessions[r.Holder] = s
	case !s.ended:
		m.markEnded(s, causeClosed)
	}
	m.emit(sessionEffect{Op: sessionClose, Holder: r.Holder, Version: s.version})

	for _, space := range slices.Sorted(maps.Keys(m.records)) {
		for _, rec := range m.spaceRecords(space) {
			if rec.Holder == r.Holder {
				rec.Holder = ""
				m.emit(recordEffect{m.write(rec)})
			}
		}
	}
	for _, slot := range slices.Sorted(maps.Keys(m.slots)) {
		if m.slots[slot].Holder == r.Holder {
			m.writeSlot(slot, "")
			m.emit(slotEffect{Op: slotRelease, Holder: r.Holder, Slot: slot, Count: m.slotCount, Result: slotClosed})
		}
	}
	return closedReply{replyHead{r.ID}}
}

// holdings answers with every record of the space. It looks at no session.
func (m *storeModel) holdings(r holdingsReq) reply {
	return holdingsReply{replyHead: replyHead{r.ID}, Space: r.Space, Records: m.spaceRecords(r.Space)}
}

// claimSlot answers with the lowest slot below the count that the sender holds, without
// writing. A sender that holds only a slot at or above the count gets none. Otherwise the
// sender gets the lowest slot below the count that has no holder or a holder whose session has
// ended, if there is one.
func (m *storeModel) claimSlot(now time.Duration, r claimSlotReq) reply {
	if m.caller(now, r.Holder) == nil {
		return lostReply{replyHead{r.ID}}
	}
	result := slotEffect{Op: slotClaim, Holder: r.Holder, Slot: -1, Count: m.slotCount, Result: slotNone}
	for _, slot := range slices.Sorted(maps.Keys(m.slots)) {
		if m.slots[slot].Holder != r.Holder {
			continue
		}
		if slot < m.slotCount {
			result.Slot, result.Result = slot, slotKept
			m.emit(result)
			return slotClaimedReply{replyHead: replyHead{r.ID}, Slot: slot}
		}
		result.Result = slotRetired
	}
	if result.Result == slotRetired {
		m.emit(result)
		return noSlotReply{replyHead{r.ID}}
	}
	for slot := range m.slotCount {
		if h := m.slots[slot].Holder; h == "" || m.gone(h) {
			m.writeSlot(slot, r.Holder)
			result.Slot, result.Result = slot, slotClaimed
			m.emit(result)
			return slotClaimedReply{replyHead: replyHead{r.ID}, Slot: slot}
		}
	}
	m.emit(result)
	return noSlotReply{replyHead{r.ID}}
}

// releaseSlot clears the slot's holder when the slot's record names the sender. It looks at no
// session.
func (m *storeModel) releaseSlot(r releaseSlotReq) reply {
	cleared := m.slots[r.Slot].Version != 0 && m.slots[r.Slot].Holder == r.Holder
	result := slotMismatch
	if cleared {
		m.writeSlot(r.Slot, "")
		result = slotCleared
	}
	m.emit(slotEffect{Op: slotRelease, Holder: r.Holder, Slot: r.Slot, Count: m.slotCount, Result: result})
	return slotReleasedReply{replyHead: replyHead{r.ID}, Slot: r.Slot, Cleared: cleared}
}

// setSlots sets the slot count and changes no slot record: the holder of a slot at or above the
// new count keeps its record. It looks at no session.
func (m *storeModel) setSlots(r setSlotsReq) reply {
	m.slotCount = r.Count
	m.emit(slotEffect{Op: slotSet, Holder: r.Holder, Slot: -1, Count: r.Count, Result: slotCounted})
	return slotsSetReply{replyHead{r.ID}, r.Count}
}

// getShard reads the shard's record. It looks at no session.
func (m *storeModel) getShard(r getShardReq) reply {
	rec := m.record(r.Space, r.Shard)
	m.emit(observeEffect{Op: observeGetShard, Holder: r.Holder, Record: rec, Result: observeOK})
	return shardReply{replyHead{r.ID}, rec}
}

// getSlot reads the slot's record and the slot count. It looks at no session.
func (m *storeModel) getSlot(r getSlotReq) reply {
	rec := m.slot(r.Slot)
	m.emit(observeEffect{
		Op: observeGetSlot, Holder: r.Holder, OfSlot: true, Slot: rec, Count: m.slotCount, Result: observeOK,
	})
	return slotReply{replyHead: replyHead{r.ID}, Record: rec, Count: m.slotCount}
}

// getSession reads the session of the holder the request names. It looks at the session of
// neither holder with the store's clock: what it answers is what the store has stored.
func (m *storeModel) getSession(r getSessionReq) reply {
	info := m.session(r.Of)
	m.emit(observeEffect{Op: observeGetSession, Holder: r.Holder, Session: info, Result: observeOK})
	return sessionReply{replyHead{r.ID}, info}
}

// cas grants the shard to the sender when the record still has the version the request
// expects, 0 for no record, whoever the record names: the sender has decided that it may take
// the shard, and the store checks only that nothing was written since. The new term is
// core.NextTerm of the record's term and the wall time of the request.
func (m *storeModel) cas(now time.Duration, r casReq) reply {
	live := m.caller(now, r.Holder) != nil
	rec := m.record(r.Space, r.Shard)
	seen := observeEffect{Op: observeCAS, Holder: r.Holder, Record: rec, Expect: r.Expect, Result: observeOK}
	switch {
	case !live:
		seen.Result = observeLost
		m.emit(seen)
		return lostReply{replyHead{r.ID}}
	case rec.Version != r.Expect:
		seen.Result = observeStale
		m.emit(seen)
		return staleReply{replyHead: replyHead{r.ID}, Record: rec}
	}
	m.emit(seen)
	return grantedReply{replyHead{r.ID}, m.grant(rec, r.Holder, core.NextTerm(rec.Term, r.Wall), r.Unseen)}
}

// casSlot gives the slot to the sender when the slot is below the slot count and its record
// still has the version the request expects, 0 for no record, whoever the record names. A slot
// that is no longer below the count is stale too: the sender read it under another count.
func (m *storeModel) casSlot(now time.Duration, r casSlotReq) reply {
	live := m.caller(now, r.Holder) != nil
	rec := m.slot(r.Slot)
	seen := observeEffect{
		Op: observeCAS, Holder: r.Holder, OfSlot: true, Slot: rec, Count: m.slotCount,
		Expect: r.Expect, Result: observeOK,
	}
	switch {
	case !live:
		seen.Result = observeLost
		m.emit(seen)
		return lostReply{replyHead{r.ID}}
	case r.Slot >= m.slotCount, rec.Version != r.Expect:
		seen.Result = observeStale
		m.emit(seen)
		return staleReply{replyHead: replyHead{r.ID}, Slot: rec, OfSlot: true}
	}
	m.emit(seen)
	m.writeSlot(r.Slot, r.Holder)
	m.emit(slotEffect{Op: slotClaim, Holder: r.Holder, Slot: r.Slot, Count: m.slotCount, Result: slotClaimed})
	return slotClaimedReply{replyHead: replyHead{r.ID}, Slot: r.Slot}
}

// tick moves the store's time to now when now is later: a reading smaller than an earlier one
// counts as the earlier one.
func (m *storeModel) tick(now time.Duration) {
	if !m.seen || now > m.now {
		m.now, m.seen = now, true
	}
}

// caller moves the store's time to now, ends the sessions whose end has come, and returns the
// session of holder, the sender of a request that needs it, or nil when the sender has no live
// session.
func (m *storeModel) caller(now time.Duration, holder string) *storeSession {
	m.tick(now)
	m.settle(holder)
	if s := m.sessions[holder]; s != nil && !s.ended {
		return s
	}
	return nil
}

// settle ends, in holder order, every session whose end has come at the store's time. caller
// is the sender of the request at hand, or "": under the Observer rule the store's clock
// judges the expiry of the caller's session only, and under both rules a caller's session that
// has expired ends as causeLate.
func (m *storeModel) settle(caller string) {
	for _, holder := range slices.Clone(m.alive) {
		s := m.sessions[holder]
		own := holder == caller
		at, cause, ok := m.endOf(s, own || m.rule != Observer)
		if !ok || m.now < at {
			continue
		}
		if own && cause == causeExpired {
			cause = causeLate
		}
		m.end(s, cause)
	}
}

// endOf returns the store time at which session s, which has not ended, ends by itself, and
// why: the earlier of its expiry, LeaseDuration after its latest renewal, when byClock says
// that the store's clock judges it, and of endAfter after it opened, when the store has one.
// It returns false when neither applies.
func (m *storeModel) endOf(s *storeSession, byClock bool) (time.Duration, endCause, bool) {
	expiry, cut := s.renewed+s.timing.LeaseDuration, s.opened+m.endAfter
	switch {
	case byClock && (m.endAfter <= 0 || expiry <= cut):
		return expiry, causeExpired, true
	case m.endAfter > 0:
		return cut, causeEndAfter, true
	default:
		return 0, "", false
	}
}

// end ends session s by itself and reports it.
func (m *storeModel) end(s *storeSession, cause endCause) {
	m.markEnded(s, cause)
	m.emit(sessionEffect{Op: sessionEnd, Holder: s.holder, Version: s.version, Cause: cause})
}

// markEnded ends session s, which has not ended.
func (m *storeModel) markEnded(s *storeSession, cause endCause) {
	s.ended, s.cause = true, cause
	if at, ok := slices.BinarySearch(m.alive, s.holder); ok {
		m.alive = slices.Delete(m.alive, at, at+1)
	}
}

// gone reports whether the session of holder has ended, or holder never had one.
func (m *storeModel) gone(holder string) bool {
	s := m.sessions[holder]
	return s == nil || s.ended
}

// session returns what the store tells of the session of holder.
func (m *storeModel) session(holder string) sessionInfo {
	s := m.sessions[holder]
	if s == nil {
		return sessionInfo{Holder: holder, Ended: true}
	}
	return sessionInfo{Holder: holder, Version: s.version, Lease: s.timing.LeaseDuration, Ended: s.ended}
}

// record returns the shard's record, with only Space and Shard set when there is none.
func (m *storeModel) record(space string, shard int) tenure.Holding {
	if rec, ok := m.records[space][shard]; ok {
		return rec
	}
	return tenure.Holding{Space: space, Shard: shard}
}

// spaceRecords returns the records of the space sorted by shard, in a new slice, nil when the
// space has none.
func (m *storeModel) spaceRecords(space string) []tenure.Holding {
	out := slices.Collect(maps.Values(m.records[space]))
	slices.SortFunc(out, func(a, b tenure.Holding) int { return cmp.Compare(a.Shard, b.Shard) })
	return out
}

// write commits one write of a shard record: it takes the next version, stamps it on rec,
// stores rec and returns it.
func (m *storeModel) write(rec tenure.Holding) tenure.Holding {
	m.version++
	rec.Version = m.version
	shards := m.records[rec.Space]
	if shards == nil {
		shards = make(map[int]tenure.Holding)
		m.records[rec.Space] = shards
	}
	shards[rec.Shard] = rec
	return rec
}

// grant writes the shard's record for holder at the term t, over rec, the record as it was, and
// reports the grant with what the store knows of the holder rec names. unseen is how long the
// deciding clock had seen that holder's session without a renewal.
func (m *storeModel) grant(rec tenure.Holding, holder string, t core.Term, unseen time.Duration) tenure.Holding {
	g := grantEffect{Prev: rec.Holder}
	if rec.Holder != "" {
		g.Ended, g.Unseen = true, unseen
		if prev := m.sessions[rec.Holder]; prev != nil {
			g.Ended = prev.ended && prev.cause != causeExpired
			g.Lease = prev.timing.LeaseDuration
		}
	}
	rec.Holder, rec.Term = holder, t
	g.Record = m.write(rec)
	m.emit(g)
	m.emit(recordEffect{g.Record})
	return g.Record
}

// slot returns the slot's record, with only Slot set when there is none.
func (m *storeModel) slot(slot int) slotRecord {
	if rec, ok := m.slots[slot]; ok {
		return rec
	}
	return slotRecord{Slot: slot}
}

// writeSlot commits one write of a slot record: the record names holder, or no holder for "",
// at the next slot version.
func (m *storeModel) writeSlot(slot int, holder string) {
	m.slotVersion++
	m.slots[slot] = slotRecord{Slot: slot, Holder: holder, Version: m.slotVersion}
}

// emit adds an effect to the outbox.
func (m *storeModel) emit(e effect) {
	m.out = append(m.out, e)
}

// refreshView builds the view again and, when the members, the slot count or the slot holders
// have changed, makes it the latest view, with the next version, and reports it.
func (m *storeModel) refreshView() {
	v := m.buildView()
	if sameView(v, m.view) {
		return
	}
	v.Version = m.view.Version + 1
	m.view = v
	m.emit(viewEffect{View: cloneView(v), Sessions: m.sessionList()})
}

// buildView returns the view of the store as it is, with the version of the latest view. Slot
// data is merged in as the runtime merges it: each member's Slot is the lowest slot whose
// holder has the member's ID, else -1, also in a view without slots.
func (m *storeModel) buildView() core.View {
	v := core.View{Version: m.view.Version, Slots: m.slotCount}
	lowest := make(map[string]int) // by member ID: 1 + its lowest slot, so that 0 means none
	if m.slotCount > 0 {
		v.SlotHolders = make([]string, m.slotCount)
		for slot := range v.SlotHolders {
			h := m.slots[slot].Holder
			if h == "" || m.gone(h) {
				continue
			}
			v.SlotHolders[slot] = h
			if id := sessionMember(h); lowest[id] == 0 {
				lowest[id] = slot + 1
			}
		}
	}
	if len(m.alive) > 0 {
		v.Members = make([]core.Member, len(m.alive))
		for i, holder := range m.alive {
			id := sessionMember(holder)
			v.Members[i] = core.Member{ID: id, Slot: lowest[id] - 1}
		}
		// Holders in order are not member IDs in order: "m1-r/…" sorts before "m1/…".
		slices.SortFunc(v.Members, func(a, b core.Member) int { return cmp.Compare(a.ID, b.ID) })
	}
	return v
}

// sameView reports whether two views of the store have the same content: the members with
// their slots, the slot count and the slot holders. It does not compare the versions.
func sameView(a, b core.View) bool {
	return a.Slots == b.Slots && slices.Equal(a.SlotHolders, b.SlotHolders) &&
		slices.EqualFunc(a.Members, b.Members, func(x, y core.Member) bool {
			return x.ID == y.ID && x.Slot == y.Slot
		})
}

// cloneView returns v with slices of its own.
func cloneView(v core.View) core.View {
	v.Members = slices.Clone(v.Members)
	v.SlotHolders = slices.Clone(v.SlotHolders)
	return v
}

// sessionMember returns the member ID of a holder: the text before its first "/", or the whole
// holder when it has none.
func sessionMember(holder string) string {
	id, _, _ := strings.Cut(holder, "/")
	return id
}
