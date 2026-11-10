// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The Observer rule on the member's side (SIM-230, SIM-310's observer rows, SIM-330's observer
// views). The store never judges another holder's session by its clock, so each driver does, as
// the Kubernetes adapter does: it watches the session versions the store pushes and takes a
// holder's session for expired once it has seen the same version for the session's
// LeaseDuration on its own monotonic clock. A takeover then reads the record, reads the
// holder's session afresh and writes with a compare-and-swap, which fails if anything wrote
// the record since the read.
//
// Observations. A driver keeps, per holder, the latest session version it saw and its own
// reading when it first saw that version (firstSeen), from the sessions of every view push and
// snapshot and from every getSession answer. A version below the one kept is an older message
// and changes nothing. A holder is observed expired when the store said its session has ended
// (a getSession answer), or when firstSeen is LeaseDuration of the member's monotonic clock in
// the past: the LeaseDuration of the holder's session as the store reported it, else the
// driver's own. The driver never observes its own holder: its machine knows its own session.
//
// Observer views (SIM-330). The view a driver steps is the store's latest view, minus every
// member all of whose observed holders are observed expired, with every slot whose holder is
// observed expired shown vacant, so that a crashed member drops out and SlotGrace starts for
// its slot. It is stepped under the driver's own version, which grows by one whenever the
// content changes: when a newer store view comes, when an observation changes it, and at the
// next expiry, for which a mono timer is kept. A store view of a version below the view
// version of the latest SlotClaimed or SlotReleased reply is not used, and no view is stepped
// while the latest one used is below it (O8).
//
// Takeovers (SIM-230). An Acquire sends getShard. A record with no holder, or naming this
// driver's holder, is written at once with cas; a record naming a holder that is not observed
// expired, whose session the driver has seen and that the latest sessions list still had, is
// answered AcquireFailed{Holder, RetryAfter: RetryPeriod}, with no more requests. Otherwise
// the driver sends getSession for the holder, and sends cas only when the session has ended or
// still has the version observed expired; else it answers AcquireFailed. The cas carries the
// record version read, the member's wall reading and the unseen time: how long the driver had
// seen the version, 0 for no holder or its own. A granted answer steps Acquired; a stale one
// steps AcquireFailed naming the holder of the record as the store has it now (Acquired with
// no term when that is this driver's holder, a grant whose reply came late); a lost one
// steps Acquired with no term and SessionFailed{CauseLost}, as under the Server rule. One
// RetryPeriod timeout, from the first request, covers the call: its timeout steps Acquired
// with no term once, and any reply after it is late.
//
// ClaimSlot goes the same way on one slot: the lowest slot below the count that the latest
// store view used shows held by this driver's holder, else the lowest that it shows vacant or
// held by a holder observed expired. With no such slot nothing is sent. getSlot reads it; a
// slot at or above the count steps nothing; one this holder holds steps SlotClaimed; a vacant
// one, or one whose holder's session getSession shows ended or unchanged, is written with
// casSlot; its slotClaimed answer steps SlotClaimed, and every other end steps nothing (the
// machine claims again every RetryPeriod). The getShard and stale answers are records the
// driver read: they go through the record filter with src reply.

// The src attributes of the Observer rule: reply for a record or view change read in an
// answer of a takeover's request, observe for a view the driver's own clock changed.
const (
	srcReply   = "reply"
	srcObserve = "observe"
)

// labelObserve is the label of the mono timer of the next observed expiry.
const labelObserve = "tenure/driver/observe"

// observation is what a driver has observed of one holder's session.
type observation struct {
	holder string
	// version is the largest session version seen, and firstSeen the member's reading when it
	// was first seen.
	version   uint64
	firstSeen time.Duration
	// lease is the LeaseDuration of the session as the store reported it; 0 when unknown.
	lease time.Duration
	// ended reports that a getSession answer said the session has ended.
	ended bool
	// unlisted reports that the latest sessions list the driver got did not have the holder:
	// its session has likely ended. It only makes a takeover read the session.
	unlisted bool
}

// observer is a driver's state under the Observer rule.
type observer struct {
	// table holds the observations, sorted by holder.
	table []observation

	// base is the latest store view used, and hasBase reports that there is one.
	base    core.View
	hasBase bool

	// shown is the content of the latest view stepped, with each member's Slot, and hasShown
	// reports that one was stepped. version is the Version of the latest view stepped.
	shown    core.View
	hasShown bool
	version  uint64

	// timer is the mono timer of the next expiry that would change the view; nil when none.
	timer *monoTimer
}

// find returns the observation of holder, nil when there is none.
func (o *observer) find(holder string) *observation {
	i, ok := slices.BinarySearchFunc(o.table, holder, func(e observation, h string) int { return strings.Compare(e.holder, h) })
	if !ok {
		return nil
	}
	return &o.table[i]
}

// note records that the session of s.Holder had s's version at the reading now, and returns
// its observation.
func (o *observer) note(s sessionInfo, now time.Duration) *observation {
	i, ok := slices.BinarySearchFunc(o.table, s.Holder, func(e observation, h string) int { return strings.Compare(e.holder, h) })
	if !ok {
		o.table = slices.Insert(o.table, i, observation{holder: s.Holder, version: s.Version, firstSeen: now})
	}
	e := &o.table[i]
	if s.Version > e.version {
		e.version, e.firstSeen = s.Version, now
	}
	if s.Lease > 0 {
		e.lease = s.Lease
	}
	if s.Ended {
		e.ended = true
	}
	return e
}

// leaseOf returns the LeaseDuration after which e expires.
func (d *driver) leaseOf(e *observation) time.Duration {
	if e.lease > 0 {
		return e.lease
	}
	return d.timing.LeaseDuration
}

// expiredAt reports whether e is observed expired at the reading now.
func (d *driver) expiredAt(e *observation, now time.Duration) bool {
	return e.ended || now-e.firstSeen >= d.leaseOf(e)
}

// observedExpired reports whether the session of holder, not this driver's, is observed
// expired now.
func (d *driver) observedExpired(holder string) bool {
	if holder == d.holder {
		return false
	}
	e := d.obs.find(holder)
	return e != nil && d.expiredAt(e, d.m.Mono())
}

// mayTake reports whether a takeover from holder, not this driver's, reads holder's session:
// it is observed expired, or the driver has no observation of it, or the latest sessions list
// did not have it.
func (d *driver) mayTake(holder string) bool {
	if holder == d.holder {
		return false
	}
	e := d.obs.find(holder)
	return e == nil || e.unlisted || d.expiredAt(e, d.m.Mono())
}

// unseen returns how long the driver has seen holder's session version unchanged, 0 when it has
// no observation of it.
func (d *driver) unseen(holder string) time.Duration {
	if e := d.obs.find(holder); e != nil {
		return d.m.Mono() - e.firstSeen
	}
	return 0
}

// observeSessions records the sessions of a view push or a snapshot: every session it has, and,
// for every other holder, that the list did not have it.
func (d *driver) observeSessions(list []sessionInfo) {
	now := d.m.Mono()
	for i := range d.obs.table {
		d.obs.table[i].unlisted = true
	}
	for _, s := range list {
		d.obs.note(s, now).unlisted = false
	}
}

// observerView returns the view the driver would step now, without its Version: the base
// view, with the members and slots its observations drop (see the comment at the top of the
// file), and each member's Slot per RUN-220.
func (d *driver) observerView() core.View {
	b := d.obs.base
	v := core.View{Slots: b.Slots, SlotHolders: slices.Clone(b.SlotHolders)}
	for s, h := range v.SlotHolders {
		if h != "" && d.observedExpired(h) {
			v.SlotHolders[s] = ""
		}
	}
	for _, m := range b.Members {
		if !d.memberExpired(m.ID) {
			v.Members = append(v.Members, m)
		}
	}
	return withSlots(v)
}

// memberExpired reports whether the member id, not this driver's, has at least one observed
// holder and every observed holder of it is observed expired.
func (d *driver) memberExpired(id string) bool {
	if id == d.m.Name {
		return false
	}
	now, found := d.m.Mono(), false
	for i := range d.obs.table {
		e := &d.obs.table[i]
		if sessionMember(e.holder) != id {
			continue
		}
		if !d.expiredAt(e, now) {
			return false
		}
		found = true
	}
	return found
}

// observerBase makes v the base view when it is newer than the base and was not read before the
// latest SlotClaimed or SlotReleased (O8), then steps the observer view if it changed.
func (d *driver) observerBase(v core.View, text, src string) {
	if (!d.obs.hasBase || v.Version > d.obs.base.Version) && v.Version >= d.slotFloor {
		d.obs.base, d.obs.hasBase = cloneView(v), true
	}
	d.showObserved(text, src)
}

// showObserved steps the observer view as ViewChanged, under the next version, when its content
// differs from the latest one stepped, and keeps the expiry timer. No view is stepped while the
// base was read before the latest slot reply (O8).
func (d *driver) showObserved(text, src string) {
	if d.stopped {
		return
	}
	if d.obs.hasBase && d.obs.base.Version >= d.slotFloor {
		v := d.observerView()
		if !d.obs.hasShown || !sameView(v, d.obs.shown) {
			d.obs.version++
			d.obs.shown, d.obs.hasShown = cloneView(v), true
			v.Version = d.obs.version
			d.step(core.ViewChanged{View: v}, text, src)
		}
	}
	d.armObserve()
}

// armObserve keeps the expiry timer at the earliest reading at which an observation that the
// base view shows would expire, and cancels it when there is none.
func (d *driver) armObserve() {
	at, ok := d.nextExpiry()
	o := d.obs
	if o.timer != nil && o.timer.pending() {
		if ok && o.timer.target() == at {
			return
		}
		o.timer.cancel()
	}
	o.timer = nil
	if ok && !d.stopped {
		o.timer = d.w.setTimer(d.m.Node, at, labelObserve, func() { d.showObserved("", srcObserve) })
	}
}

// nextExpiry returns the earliest reading at which a holder that the base view shows, as a
// member or a slot holder, is observed expired, and false when no such holder is waiting to
// expire.
func (d *driver) nextExpiry() (time.Duration, bool) {
	if !d.obs.hasBase {
		return 0, false
	}
	now := d.m.Mono()
	var first time.Duration
	found := false
	for i := range d.obs.table {
		e := &d.obs.table[i]
		if e.holder == d.holder || d.expiredAt(e, now) || !d.inBase(e.holder) {
			continue
		}
		if at := e.firstSeen + d.leaseOf(e); !found || at < first {
			first, found = at, true
		}
	}
	return first, found
}

// inBase reports whether the base view shows holder: its member, or holder as a slot holder.
func (d *driver) inBase(holder string) bool {
	b := &d.obs.base
	if slices.Contains(b.SlotHolders, holder) {
		return true
	}
	id := sessionMember(holder)
	return slices.ContainsFunc(b.Members, func(m core.Member) bool { return m.ID == id })
}

// takePhase is the request of a takeover in flight.
type takePhase uint8

const (
	takeRead    takePhase = iota // getShard or getSlot
	takeSession                  // getSession
	takeWrite                    // cas or casSlot
)

// takeover is the state of one Acquire or ClaimSlot under the Observer rule, from its first
// request to its end.
type takeover struct {
	slot  bool // a ClaimSlot; else an Acquire
	space string
	shard int
	phase takePhase

	// rec is the shard record read, for an Acquire; slotRec the slot record read, for a
	// ClaimSlot.
	rec     tenure.Holding
	slotRec slotRecord

	// final reports that the driver decided how the call ends, and decided holds the inputs it
	// steps then, in order, possibly none.
	final   bool
	decided []core.Input
}

// observerCall returns the call of an Acquire or a ClaimSlot under the Observer rule, and false
// for any other action. A ClaimSlot with no slot to claim returns a nil call: nothing goes out.
func (d *driver) observerCall(a core.Action) (*call, bool) {
	t := &takeover{}
	c := &call{act: a, onTimeout: d.expire}
	c.next = func(c *call, rep reply) bool { return d.takeNext(c, t, rep) }
	c.onReply = func(c *call, rep reply) { d.takeAnswer(c, t, rep) }
	switch a := a.(type) {
	case core.Acquire:
		t.space, t.shard = a.Space, a.Shard
		c.key, c.req = shardLane(a.Space, a.Shard), getShardReq{Space: a.Space, Shard: a.Shard}
	case core.ClaimSlot:
		s := d.slotCandidate()
		if s < 0 {
			return nil, true
		}
		t.slot = true
		c.key, c.req = laneKey{kind: laneSlot}, getSlotReq{Slot: s}
	default:
		return nil, false
	}
	return c, true
}

// slotCandidate returns the slot a ClaimSlot reads: the lowest slot below the count of the
// base view that names this driver's holder, else the lowest that is vacant there or held by a
// holder observed expired; -1 when there is none.
func (d *driver) slotCandidate() int {
	if !d.obs.hasBase {
		return -1
	}
	holders := d.obs.base.SlotHolders
	if s := slices.Index(holders, d.holder); s >= 0 {
		return s
	}
	for s, h := range holders {
		if h == "" || d.observedExpired(h) {
			return s
		}
	}
	return -1
}

// takeNext takes the answer rep to a request of the takeover t, the call c, and sends c's next
// request when there is one, reporting whether it did. When it does not, c ends: with the
// inputs t decided, or, when t decided nothing, with what rep itself means (takeAnswer).
func (d *driver) takeNext(c *call, t *takeover, rep reply) bool {
	switch t.phase {
	case takeRead:
		if t.slot {
			r, ok := rep.(slotReply)
			if !ok {
				return false
			}
			return d.slotRead(c, t, r)
		}
		r, ok := rep.(shardReply)
		if !ok {
			return false
		}
		return d.shardRead(c, t, r)
	case takeSession:
		r, ok := rep.(sessionReply)
		if !ok {
			return false
		}
		return d.sessionRead(c, t, r)
	}
	return false
}

// shardRead takes the getShard answer of an Acquire.
func (d *driver) shardRead(c *call, t *takeover, r shardReply) bool {
	rec := r.Record
	rec.Space, rec.Shard = t.space, t.shard
	t.rec = rec
	d.observe(recReading{rec: rec, src: srcReply, text: r.Describe(), call: c})
	if d.stopped {
		return false
	}
	switch h := rec.Holder; {
	case h == "" || h == d.holder:
		d.sendWrite(c, t, 0)
		return true
	case d.mayTake(h):
		t.phase = takeSession
		d.sendNext(c, getSessionReq{Of: h})
		return true
	}
	d.decide(t, d.heldBy(t, rec.Holder))
	return false
}

// slotRead takes the getSlot answer of a ClaimSlot.
func (d *driver) slotRead(c *call, t *takeover, r slotReply) bool {
	t.slotRec = r.Record
	switch h := r.Record.Holder; {
	case r.Record.Slot >= r.Count:
		d.decide(t)
	case h == d.holder:
		d.decide(t, core.SlotClaimed{Slot: r.Record.Slot})
	case h == "":
		d.sendWrite(c, t, 0)
		return true
	case d.mayTake(h):
		t.phase = takeSession
		d.sendNext(c, getSessionReq{Of: h})
		return true
	default:
		d.decide(t)
	}
	return false
}

// sessionRead takes the getSession answer of a takeover: the write goes out when the session
// has ended, or still has the version the driver has observed expired.
func (d *driver) sessionRead(c *call, t *takeover, r sessionReply) bool {
	info := r.Session
	e := d.obs.note(info, d.m.Mono())
	take := info.Ended || (e.version == info.Version && d.expiredAt(e, d.m.Mono()))
	if take {
		d.sendWrite(c, t, d.unseen(info.Holder))
	} else if t.slot {
		d.decide(t)
	} else {
		d.decide(t, d.heldBy(t, info.Holder))
	}
	d.showObserved(r.Describe(), srcReply)
	return take && !d.stopped
}

// heldBy returns the AcquireFailed of the takeover t's shard, naming holder.
func (d *driver) heldBy(t *takeover, holder string) core.Input {
	return core.AcquireFailed{Space: t.space, Shard: t.shard, Holder: holder, RetryAfter: d.timing.RetryPeriod}
}

// decide ends the takeover t with the inputs ins.
func (d *driver) decide(t *takeover, ins ...core.Input) {
	t.final, t.decided = true, ins
}

// sendWrite sends the write of the takeover t: cas over the record read, with the member's wall
// reading, or casSlot over the slot record read; unseen is the evidence it carries.
func (d *driver) sendWrite(c *call, t *takeover, unseen time.Duration) {
	t.phase = takeWrite
	if t.slot {
		d.sendNext(c, casSlotReq{Slot: t.slotRec.Slot, Expect: t.slotRec.Version, Unseen: unseen})
		return
	}
	d.sendNext(c, casReq{
		Space: t.space, Shard: t.shard, Expect: t.rec.Version, Wall: d.m.Wall(), Unseen: unseen,
	})
}

// sendNext sends req as the next request of the call c, which stays in flight on its lane under
// the timeout of its first request.
func (d *driver) sendNext(c *call, req request) {
	d.md.lastID++
	c.req = withID(req, reqHead{ID: d.md.lastID, Holder: d.holder})
	d.inFlight[d.md.lastID] = c
	d.w.fw.Net.Send(d.m.Node, d.w.store.ID(), c.req)
}

// takeAnswer steps how the takeover t, the call c, ended with the reply rep: the inputs t
// decided; for a stale answer, the record it carries through the filter, then AcquireFailed
// naming that record's holder (Acquired with no term when it is this driver's) or, for a slot,
// nothing; for any other reply, what the reply means (answer).
func (d *driver) takeAnswer(c *call, t *takeover, rep reply) {
	if t.final {
		for _, in := range t.decided {
			d.step(in, rep.Describe(), "")
		}
		return
	}
	r, ok := rep.(staleReply)
	if !ok {
		d.answer(c, rep)
		return
	}
	if t.slot {
		return
	}
	rec := r.Record
	rec.Space, rec.Shard = t.space, t.shard
	d.observe(recReading{rec: rec, src: srcReply, text: r.Describe(), call: c})
	if rec.Holder == d.holder {
		d.step(core.Acquired{Space: t.space, Shard: t.shard}, r.Describe(), "")
		return
	}
	d.step(d.heldBy(t, rec.Holder), r.Describe(), "")
}
