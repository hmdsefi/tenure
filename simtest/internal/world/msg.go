// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The messages between the members and the store. A member sends a request and the store
// answers it with one reply that carries the request's ID; the store also sends, unasked, a
// holderPush for every write of a shard record, a viewPush for every new view and a snapshot of
// everything from time to time.
//
// Every message is a plain value: a message that holds a slice owns it. Describe gives the
// text of a message in a trace: the name of the message, "#" and the request's ID where there is
// one, and the fields, with a shard as book/3, a holder in full, "-" for no holder, and every
// time and duration as seconds with nine decimals. The text is the same in every run: it prints
// no pointer, no map and no float.

// request is a message a member sends to the store.
type request interface {
	kernel.Describer

	// head returns the ID and the sender of the request.
	head() reqHead
}

// reqHead is the start of every request.
type reqHead struct {
	// ID is the sender's own number of the request. The reply carries it back.
	ID uint64
	// Holder is the holder string of the sender: the session the request is made for.
	Holder string
}

// head returns h.
func (h reqHead) head() reqHead { return h }

// text returns the start of a request's text: the name, the ID and the sender.
func (h reqHead) text(name string) string {
	return name + "#" + strconv.FormatUint(h.ID, 10) + " " + msgHolder(h.Holder)
}

// keepAliveReq opens the sender's session, or renews it and stores Timing as its timing.
type keepAliveReq struct {
	reqHead
	Timing core.Timing
}

// Describe returns, for example, "keepAlive#7 m1/0a lease=15.000000000s retry=2.000000000s".
func (r keepAliveReq) Describe() string {
	return r.text("keepAlive") + " lease=" + msgDuration(r.Timing.LeaseDuration) +
		" retry=" + msgDuration(r.Timing.RetryPeriod)
}

// acquireReq asks for the shard. Members send it under the Server rule only.
type acquireReq struct {
	reqHead
	Space string
	Shard int
}

// Describe returns, for example, "acquire#8 m1/0a book/3".
func (r acquireReq) Describe() string {
	return r.text("acquire") + " " + msgShard(r.Space, r.Shard)
}

// releaseReq clears the holder of the shard's record if the record names the sender at Term.
type releaseReq struct {
	reqHead
	Space string
	Shard int
	Term  core.Term
}

// Describe returns, for example, "release#9 m1/0a book/3 term=5".
func (r releaseReq) Describe() string {
	return r.text("release") + " " + msgShard(r.Space, r.Shard) + " term=" + msgTerm(r.Term)
}

// confirmReq asks for the shard's record as it is after every write so far. Term is the term
// the sender holds the shard at.
type confirmReq struct {
	reqHead
	Space string
	Shard int
	Term  core.Term
}

// Describe returns, for example, "confirm#10 m1/0a book/3 term=5".
func (r confirmReq) Describe() string {
	return r.text("confirm") + " " + msgShard(r.Space, r.Shard) + " term=" + msgTerm(r.Term)
}

// closeSessionReq ends the sender's session for good and clears its records.
type closeSessionReq struct {
	reqHead
}

// Describe returns, for example, "closeSession#11 m1/0a".
func (r closeSessionReq) Describe() string {
	return r.text("closeSession")
}

// holdingsReq asks for every record of the space.
type holdingsReq struct {
	reqHead
	Space string
}

// Describe returns, for example, "holdings#12 m1/0a book".
func (r holdingsReq) Describe() string {
	return r.text("holdings") + " " + r.Space
}

// claimSlotReq asks for a slot. Members send it under the Server rule only.
type claimSlotReq struct {
	reqHead
}

// Describe returns, for example, "claimSlot#13 m1/0a".
func (r claimSlotReq) Describe() string {
	return r.text("claimSlot")
}

// releaseSlotReq clears the holder of the slot's record if the record names the sender.
type releaseSlotReq struct {
	reqHead
	Slot int
}

// Describe returns, for example, "releaseSlot#14 m1/0a slot=2".
func (r releaseSlotReq) Describe() string {
	return r.text("releaseSlot") + " slot=" + strconv.Itoa(r.Slot)
}

// setSlotsReq sets the slot count.
type setSlotsReq struct {
	reqHead
	Count int
}

// Describe returns, for example, "setSlots#15 m1/0a count=3".
func (r setSlotsReq) Describe() string {
	return r.text("setSlots") + " count=" + strconv.Itoa(r.Count)
}

// getShardReq reads the shard's record. It is the first step of a takeover under the Observer
// rule.
type getShardReq struct {
	reqHead
	Space string
	Shard int
}

// Describe returns, for example, "getShard#16 m2/0b book/3".
func (r getShardReq) Describe() string {
	return r.text("getShard") + " " + msgShard(r.Space, r.Shard)
}

// getSlotReq reads the slot's record and the slot count.
type getSlotReq struct {
	reqHead
	Slot int
}

// Describe returns, for example, "getSlot#17 m2/0b slot=2".
func (r getSlotReq) Describe() string {
	return r.text("getSlot") + " slot=" + strconv.Itoa(r.Slot)
}

// getSessionReq reads the session of the holder Of: its version and whether it has ended.
type getSessionReq struct {
	reqHead
	Of string
}

// Describe returns, for example, "getSession#18 m2/0b of=m1/0a".
func (r getSessionReq) Describe() string {
	return r.text("getSession") + " of=" + msgHolder(r.Of)
}

// casReq takes the shard for the sender if the shard's record still has the version Expect; 0
// expects no record. It is the last step of a takeover under the Observer rule, and carries
// what the store cannot know there.
type casReq struct {
	reqHead
	Space string
	Shard int
	// Expect is the version of the record the sender decided on.
	Expect uint64
	// Wall is the sender's wall-clock reading. The new term is computed from it.
	Wall time.Time
	// Unseen is how long the sender's clock had seen the session of the record's holder without
	// a renewal when the sender decided. It is evidence for the checks, and the store does not
	// judge it.
	Unseen time.Duration
}

// Describe returns, for example,
// "cas#19 m2/0b book/3 expect=7 wall=41.207000000s unseen=15.000000000s", with the wall time
// counted from the simulation's epoch.
func (r casReq) Describe() string {
	return r.text("cas") + " " + msgShard(r.Space, r.Shard) +
		" expect=" + strconv.FormatUint(r.Expect, 10) +
		" wall=" + msgDuration(r.Wall.Sub(kernel.Epoch)) +
		" unseen=" + msgDuration(r.Unseen)
}

// casSlotReq claims the slot for the sender if the slot's record still has the version Expect;
// 0 expects no record. Version numbers of slot records are not those of shard records.
type casSlotReq struct {
	reqHead
	Slot int
	// Expect is the version of the slot record the sender decided on.
	Expect uint64
	// Unseen is as in casReq.
	Unseen time.Duration
}

// Describe returns, for example, "casSlot#20 m2/0b slot=2 expect=4 unseen=15.000000000s".
func (r casSlotReq) Describe() string {
	return r.text("casSlot") + " slot=" + strconv.Itoa(r.Slot) +
		" expect=" + strconv.FormatUint(r.Expect, 10) +
		" unseen=" + msgDuration(r.Unseen)
}

// reply is the store's answer to a request.
type reply interface {
	kernel.Describer

	// requestID returns the ID of the request the reply answers.
	requestID() uint64
}

// replyHead is the start of every reply.
type replyHead struct {
	// ID is the ID of the request the reply answers.
	ID uint64
}

// requestID returns h.ID.
func (h replyHead) requestID() uint64 { return h.ID }

// text returns the start of a reply's text: the name and the request's ID.
func (h replyHead) text(name string) string {
	return name + "#" + strconv.FormatUint(h.ID, 10)
}

// renewedReply answers a keepAliveReq that opened or renewed the session.
type renewedReply struct {
	replyHead
	// Version is the session's version after the request.
	Version uint64
	// Opened reports that the request opened the session.
	Opened bool
}

// Describe returns, for example, "renewed#7 version=3", and "renewed#7 version=1 opened" for
// the request that opened the session.
func (r renewedReply) Describe() string {
	s := r.text("renewed") + " version=" + strconv.FormatUint(r.Version, 10)
	if r.Opened {
		s += " opened"
	}
	return s
}

// lostReply answers a request that needs the sender's live session when the sender has none.
// The request changed no record.
type lostReply struct {
	replyHead
}

// Describe returns, for example, "lost#7".
func (r lostReply) Describe() string {
	return r.text("lost")
}

// closedReply answers a closeSessionReq.
type closedReply struct {
	replyHead
}

// Describe returns, for example, "closed#11".
func (r closedReply) Describe() string {
	return r.text("closed")
}

// grantedReply answers an acquireReq or a casReq that got the shard.
type grantedReply struct {
	replyHead
	// Record is the record after the grant: it names the sender at the new term.
	Record tenure.Holding
}

// Describe returns, for example, "granted#8 book/3 holder=m1/0a term=5 version=9".
func (r grantedReply) Describe() string {
	return r.text("granted") + " " + msgRecord(r.Record)
}

// heldReply answers an acquireReq for a shard that another live session holds.
type heldReply struct {
	replyHead
	// Record is the record as it is: it names the holder.
	Record tenure.Holding
	// RetryAfter is how long the sender should wait before it asks again: the RetryPeriod of
	// its own session.
	RetryAfter time.Duration
}

// Describe returns, for example,
// "held#8 book/3 holder=m2/0b term=4 version=6 retry=2.000000000s".
func (r heldReply) Describe() string {
	return r.text("held") + " " + msgRecord(r.Record) + " retry=" + msgDuration(r.RetryAfter)
}

// releasedReply answers a releaseReq, whether or not it cleared the record.
type releasedReply struct {
	replyHead
	// Record is the record after the request, with only Space and Shard set when there is none.
	Record tenure.Holding
	// Cleared reports that the request cleared the record's holder.
	Cleared bool
}

// Describe returns, for example, "released#9 book/3 holder=- term=5 version=10 cleared", and
// the same without "cleared" for a request that changed nothing.
func (r releasedReply) Describe() string {
	s := r.text("released") + " " + msgRecord(r.Record)
	if r.Cleared {
		s += " cleared"
	}
	return s
}

// confirmedReply answers a confirmReq.
type confirmedReply struct {
	replyHead
	// Record is the record after the request, with only Space and Shard set when there is none.
	Record tenure.Holding
}

// Describe returns, for example, "confirmed#10 book/3 holder=m1/0a term=5 version=9".
func (r confirmedReply) Describe() string {
	return r.text("confirmed") + " " + msgRecord(r.Record)
}

// holdingsReply answers a holdingsReq.
type holdingsReply struct {
	replyHead
	Space string
	// Records holds every record of the space, also those with no holder, sorted by shard. It
	// is nil when the space has none.
	Records []tenure.Holding
}

// Describe returns, for example, "holdings#12 book records=16 newest=40": the number of records
// and the largest of their versions.
func (r holdingsReply) Describe() string {
	return r.text("holdings") + " " + r.Space + " records=" + strconv.Itoa(len(r.Records)) +
		" newest=" + strconv.FormatUint(msgNewest(r.Records), 10)
}

// slotClaimedReply answers a claimSlotReq or a casSlotReq that got a slot, and a claimSlotReq
// of a sender that holds one below the slot count already.
type slotClaimedReply struct {
	replyHead
	Slot int
	// View is the version of the store's view after the request: a view of a lower version was
	// read before it. Describe leaves it out; the tenure.view records hold the versions.
	View uint64
}

// Describe returns, for example, "slotClaimed#13 slot=2".
func (r slotClaimedReply) Describe() string {
	return r.text("slotClaimed") + " slot=" + strconv.Itoa(r.Slot)
}

// noSlotReply answers a claimSlotReq when no slot is free or the sender holds a retired slot.
type noSlotReply struct {
	replyHead
}

// Describe returns, for example, "noSlot#13".
func (r noSlotReply) Describe() string {
	return r.text("noSlot")
}

// slotReleasedReply answers a releaseSlotReq, whether or not it cleared the record.
type slotReleasedReply struct {
	replyHead
	Slot int
	// Cleared reports that the request cleared the slot's holder.
	Cleared bool
	// View is as in slotClaimedReply.
	View uint64
}

// Describe returns, for example, "slotReleased#14 slot=2 cleared", and the same without
// "cleared" for a request that changed nothing.
func (r slotReleasedReply) Describe() string {
	s := r.text("slotReleased") + " slot=" + strconv.Itoa(r.Slot)
	if r.Cleared {
		s += " cleared"
	}
	return s
}

// slotsSetReply answers a setSlotsReq.
type slotsSetReply struct {
	replyHead
	Count int
}

// Describe returns, for example, "slotsSet#15 count=3".
func (r slotsSetReply) Describe() string {
	return r.text("slotsSet") + " count=" + strconv.Itoa(r.Count)
}

// shardReply answers a getShardReq.
type shardReply struct {
	replyHead
	// Record is the record, with only Space and Shard set when there is none.
	Record tenure.Holding
}

// Describe returns, for example, "shard#16 book/3 holder=m1/0a term=5 version=9".
func (r shardReply) Describe() string {
	return r.text("shard") + " " + msgRecord(r.Record)
}

// slotReply answers a getSlotReq.
type slotReply struct {
	replyHead
	// Record is the slot's record, with only Slot set when there is none.
	Record slotRecord
	// Count is the slot count.
	Count int
}

// Describe returns, for example, "slot#17 slot=2 holder=m1/0a version=4 count=5".
func (r slotReply) Describe() string {
	return r.text("slot") + " " + r.Record.text() + " count=" + strconv.Itoa(r.Count)
}

// sessionReply answers a getSessionReq.
type sessionReply struct {
	replyHead
	Session sessionInfo
}

// Describe returns, for example, "session#18 m1/0a version=3 lease=15.000000000s", with
// " ended" at the end for a session that has ended.
func (r sessionReply) Describe() string {
	return r.text("session") + " " + r.Session.text()
}

// staleReply answers a casReq or a casSlotReq whose record no longer has the expected version,
// and a casSlotReq for a slot that is no longer below the slot count. The request changed
// nothing.
type staleReply struct {
	replyHead
	// Record is the shard's record as it is, for a casReq. For a casSlotReq it is the zero
	// Holding.
	Record tenure.Holding
	// Slot is the slot's record as it is, for a casSlotReq. For a casReq it is the zero
	// slotRecord.
	Slot slotRecord
	// OfSlot reports that the reply answers a casSlotReq.
	OfSlot bool
}

// Describe returns, for example, "stale#19 book/3 holder=m1/0a term=5 version=9", and
// "stale#20 slot=2 holder=m1/0a version=4" for a slot.
func (r staleReply) Describe() string {
	if r.OfSlot {
		return r.text("stale") + " " + r.Slot.text()
	}
	return r.text("stale") + " " + msgRecord(r.Record)
}

// slotRecord is a slot record as the store reports it.
type slotRecord struct {
	// Slot is the slot.
	Slot int
	// Holder is the holder the record names; "" means the record has no holder.
	Holder string
	// Version orders the writes of slot records: each takes a version larger than that of every
	// earlier write of any slot record. 0 means there is no record. Slot versions and the
	// versions of shard records are counted apart.
	Version uint64
}

// text returns, for example, "slot=2 holder=m1/0a version=4".
func (r slotRecord) text() string {
	return "slot=" + strconv.Itoa(r.Slot) + " holder=" + msgHolder(r.Holder) +
		" version=" + strconv.FormatUint(r.Version, 10)
}

// sessionInfo is what the store tells of a session.
type sessionInfo struct {
	// Holder is the holder of the session.
	Holder string
	// Version changes with every renewal: a member that sees it unchanged has seen no renewal.
	// It is 1 for a session that was opened and never renewed, and 0 for a holder that never
	// opened one.
	Version uint64
	// Lease is the LeaseDuration of the session's timing.
	Lease time.Duration
	// Ended reports that the session has ended for good. A holder that never opened a session
	// has none to wait for, and reads as ended.
	Ended bool
}

// text returns, for example, "m1/0a version=3 lease=15.000000000s", with " ended" at the end
// for a session that has ended.
func (s sessionInfo) text() string {
	out := msgHolder(s.Holder) + " version=" + strconv.FormatUint(s.Version, 10) +
		" lease=" + msgDuration(s.Lease)
	if s.Ended {
		out += " ended"
	}
	return out
}

// holderPush tells a member of one write of a shard record.
type holderPush struct {
	// Record is the record after the write.
	Record tenure.Holding
}

// Describe returns, for example, "holder book/3 holder=m1/0a term=5 version=9".
func (p holderPush) Describe() string {
	return "holder " + msgRecord(p.Record)
}

// viewPush tells a member of a new view of the store.
type viewPush struct {
	// View is the view. Its Version is the store's count of views.
	View core.View
	// Sessions holds the sessions that have not ended, sorted by holder.
	Sessions []sessionInfo
}

// Describe returns, for example, "view version=4 members=m1,m2,m3 slots=0 sessions=3".
func (p viewPush) Describe() string {
	return "view " + msgView(p.View) + " sessions=" + strconv.Itoa(len(p.Sessions))
}

// snapshotPush tells a member of everything the store has, so that a member that missed a push
// catches up.
type snapshotPush struct {
	// Records holds every shard record, sorted by space and then by shard.
	Records []tenure.Holding
	// View is the store's latest view.
	View core.View
	// Sessions holds the sessions that have not ended, sorted by holder.
	Sessions []sessionInfo
}

// Describe returns, for example,
// "snapshot records=16 newest=40 view version=4 members=m1,m2,m3 slots=0 sessions=3": the number
// of records and the largest of their versions, then the view.
func (p snapshotPush) Describe() string {
	return "snapshot records=" + strconv.Itoa(len(p.Records)) +
		" newest=" + strconv.FormatUint(msgNewest(p.Records), 10) +
		" view " + msgView(p.View) + " sessions=" + strconv.Itoa(len(p.Sessions))
}

// msgHolder returns the holder as a message prints it: "-" for no holder.
func msgHolder(holder string) string {
	if holder == "" {
		return "-"
	}
	return holder
}

// msgShard returns the shard as <space>/<shard>, for example book/3.
func msgShard(space string, shard int) string {
	return space + "/" + strconv.Itoa(shard)
}

// msgTerm returns the term in decimal.
func msgTerm(term core.Term) string {
	return strconv.FormatUint(uint64(term), 10)
}

// msgDuration returns the duration as seconds with nine decimals, for example 15.000000000s.
func msgDuration(d time.Duration) string {
	return kernel.Time(d).String()
}

// msgRecord returns, for example, "book/3 holder=m1/0a term=5 version=9".
func msgRecord(rec tenure.Holding) string {
	return msgShard(rec.Space, rec.Shard) + " holder=" + msgHolder(rec.Holder) +
		" term=" + msgTerm(rec.Term) + " version=" + strconv.FormatUint(rec.Version, 10)
}

// msgNewest returns the largest version of the records, 0 when there are none.
func msgNewest(recs []tenure.Holding) uint64 {
	newest := uint64(0)
	for _, rec := range recs {
		newest = max(newest, rec.Version)
	}
	return newest
}

// msgView returns, for example, "version=4 members=m1,m2,m3 slots=0": the member IDs in the
// view's order.
func msgView(v core.View) string {
	ids := make([]string, len(v.Members))
	for i, m := range v.Members {
		ids[i] = m.ID
	}
	return "version=" + strconv.FormatUint(v.Version, 10) +
		" members=" + strings.Join(ids, ",") + " slots=" + strconv.Itoa(v.Slots)
}
