// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"strconv"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// The labels of the store's mono timers.
const (
	labelStoreEnd      = "tenure/store/end"
	labelStoreSnapshot = "tenure/store/snapshot"
)

// storeServer is the store's node: it serves the store model over the simulated network
// (SIM-260). Its state lives here, in the World, and not in the boot function, so the model,
// with every session, record and slot, survives the crashes of the store's node; a crash loses
// only the handler and the mono timers, which every boot installs and sets again.
//
// The model's clock is the store's monotonic clock: every request and every timer passes
// Mono(store) as the model's reading, and the store's wall clock, which has no offset, for the
// new terms of acquireReq.
//
// What the store sends, all from the store's node:
//
//   - A reply to every request, to its sender, in the event that delivered the request. A
//     message that arrives while the store is down, or paused and then crashed, is dropped by
//     the network and never answered.
//   - A holderPush for every write of a shard record, in the order of the writes.
//   - A viewPush for every new view, with the view and the sessions that have not ended. A
//     call of the model that changed a session but not the view, as a renewal does, also
//     sends a viewPush, with the latest view at its old version and the new sessions: under
//     the Observer rule the members watch the session versions, and must see a renewal
//     within one network delay, not only at the next snapshot. A member stepping views by
//     version sees nothing new in it.
//   - A snapshotPush, every RetryPeriod of the store's monotonic clock: at the readings RP,
//     2·RP, 3·RP and so on. A boot sets the timer for the next of those readings after the
//     present one, so a crash skips the snapshots the store was down for and shifts none.
//
// The handler sends the reply first, then the pushes the request caused, holder pushes before
// the view push. Every push goes to the members whose phase is Active when it is sent, in
// ascending NodeID order, also to those whose node is down, whose copy the network drops:
// the store cannot know who is up. A replacement on standby and a member that exited get
// nothing; a replacement that becomes active catches up from the next snapshot. Every
// message is built for its receiver, so no two receivers share a slice.
//
// A session ends by itself at the model's nextEnd: its expiry under the Server rule, or the
// scenario's EndAfter under both rules (SIM-225). One mono timer is kept for it; it is set
// again after every call of the model, and at every boot, so an end that came while the store
// was down happens in an event of its own as soon as the store is up again. The view push of
// such an end goes out in the timer's event.
//
// Every effect of the model becomes a record of the store (SIM §8), whose text is the
// Describe of the request that caused it, or empty for a timer: tenure.session,
// tenure.grant, tenure.release, tenure.confirm, tenure.observe, tenure.slot and tenure.view.
// A write of a record emits nothing of its own: the grant, the release, the confirmation or
// the closed session that wrote it has its record.
type storeServer struct {
	w     *World
	model *storeModel

	// onGrant, when set, is called for every grant, in the event of the grant, after its
	// tenure.grant record and before any message about it goes out. The checks of the run
	// set it to feed Checker.Grant (1.8-10).
	onGrant func(check.Grant)

	// end and snapshot are the mono timers of the present boot; nil or not pending when none
	// is set.
	end      *monoTimer
	snapshot *monoTimer
}

// newStoreServer returns the store of w, with an empty model under the scenario's rule, with
// its slot count and its EndAfter.
func newStoreServer(w *World) *storeServer {
	return &storeServer{w: w, model: newStoreModel(w.sc.Takeover, w.sc.Slots, w.sc.EndAfter)}
}

// boot runs at every boot of the store's node: it installs the handler and sets the mono
// timers for the next session end and the next snapshot. The crash before it dropped the
// timers of the last boot.
func (s *storeServer) boot(n *kernel.Node) {
	s.w.fw.Net.Handle(n, s.handle)
	s.end, s.snapshot = nil, nil
	s.armEnd()
	s.armSnapshot()
}

// handle answers one delivered message: a request of a member. Anything else fails the run.
func (s *storeServer) handle(from kernel.NodeID, payload any) {
	req, ok := payload.(request)
	if !ok {
		s.w.sim.Fail(fmt.Errorf("world: store: unexpected message %T from node %d", payload, from))
		return
	}
	n := s.w.store
	rep := s.model.handle(Mono(n), s.w.Wall(n), req)
	pushes := s.apply(req.Describe())
	s.w.fw.Net.Send(n, from, rep)
	s.push(pushes)
	s.armEnd()
}

// expire runs when the session-end timer fires.
func (s *storeServer) expire() {
	s.model.expire(Mono(s.w.store))
	s.push(s.apply(""))
	s.armEnd()
}

// grant returns the grant of e as the checker takes it.
func (e grantEffect) grant() check.Grant {
	return check.Grant{
		Space:  e.Record.Space,
		Shard:  e.Record.Shard,
		Term:   e.Record.Term,
		Holder: e.Record.Holder,
		Prev:   e.Prev,
		Ended:  e.Ended,
		Unseen: e.Unseen,
		Lease:  e.Lease,
	}
}

// storePushes is what one call of the model asks the store to push: the records written, in
// order, and the view and sessions when the view or a session changed.
type storePushes struct {
	records []holderPush
	view    *viewPush
}

// apply takes the model's effects, emits a record for each, with text, and calls onGrant for
// each grant. It returns the pushes they ask for.
func (s *storeServer) apply(text string) storePushes {
	var out storePushes
	sessions := false
	for _, e := range s.model.takeEffects() {
		switch e := e.(type) {
		case sessionEffect:
			s.w.emitSession(text, string(e.Op), e.Holder, e.Version, string(e.Cause))
			sessions = true
		case grantEffect:
			g := e.grant()
			s.w.emitGrant(text, g, e.Record.Version)
			if s.onGrant != nil {
				s.onGrant(g)
			}
		case releaseEffect:
			result := "mismatch"
			if e.Cleared {
				result = "cleared"
			}
			s.w.emitRelease(text, e.Holder, e.Term, e.Record, result)
		case confirmEffect:
			s.w.emitConfirm(text, e.Holder, e.Term, e.Record)
		case observeEffect:
			s.w.emitObserve(text, string(e.Op), e.Holder, observeRead(e), string(e.Result))
		case slotEffect:
			s.w.emitSlot(text, string(e.Op), e.Holder, e.Slot, e.Count, string(e.Result))
		case recordEffect:
			out.records = append(out.records, holderPush(e))
		case viewEffect:
			s.w.emitView(text, e.View)
			out.view = &viewPush{View: e.View, Sessions: e.Sessions}
		default:
			panic(fmt.Sprintf("world: store: unknown effect %T", e))
		}
	}
	if out.view == nil && sessions {
		out.view = &viewPush{View: s.model.currentView(), Sessions: s.model.sessionList()}
	}
	return out
}

// observeRead returns the attributes of tenure.observe for what the request read: for a shard
// record space, shard, record_holder, record_term and version; for a slot record slot,
// record_holder, version and count; for a session session, version, lease_ns and ended. A
// compare-and-swap adds expect, the version it expected.
func observeRead(e observeEffect) []kernel.Attr {
	var read []kernel.Attr
	switch {
	case e.Op == observeGetSession:
		read = []kernel.Attr{
			recAttr("session", e.Session.Holder),
			recAttr("version", recUint(e.Session.Version)),
			recAttr("lease_ns", recNanos(e.Session.Lease)),
			recAttr("ended", strconv.FormatBool(e.Session.Ended)),
		}
	case e.OfSlot:
		read = []kernel.Attr{
			recAttr("slot", strconv.Itoa(e.Slot.Slot)),
			recAttr("record_holder", e.Slot.Holder),
			recAttr("version", recUint(e.Slot.Version)),
			recAttr("count", strconv.Itoa(e.Count)),
		}
	default:
		read = []kernel.Attr{
			recAttr("space", e.Record.Space),
			recAttr("shard", strconv.Itoa(e.Record.Shard)),
			recAttr("record_holder", e.Record.Holder),
			recAttr("record_term", recTerm(e.Record.Term)),
			recAttr("version", recUint(e.Record.Version)),
		}
	}
	if e.Op == observeCAS {
		read = append(read, recAttr("expect", recUint(e.Expect)))
	}
	return read
}

// push sends the pushes: each holder push to every receiver, in the order of the writes, then
// the view push to every receiver.
func (s *storeServer) push(p storePushes) {
	to := s.receivers()
	for _, hp := range p.records {
		for _, id := range to {
			s.w.fw.Net.Send(s.w.store, id, hp)
		}
	}
	if p.view == nil {
		return
	}
	for _, id := range to {
		s.w.fw.Net.Send(s.w.store, id, viewPush{View: cloneView(p.view.View), Sessions: cloneSessions(p.view.Sessions)})
	}
}

// receivers returns the nodes that get the store's pushes and snapshots: the members whose
// phase is Active, in ascending NodeID order.
func (s *storeServer) receivers() []kernel.NodeID {
	return s.w.membersRole()
}

// armEnd sets the session-end timer for the model's nextEnd, unless it is set for that
// reading already, and cancels it when no session will end by itself.
func (s *storeServer) armEnd() {
	at, ok := s.model.nextEnd()
	if s.end != nil && s.end.pending() {
		if ok && s.end.target() == at {
			return
		}
		s.end.cancel()
	}
	s.end = nil
	if ok {
		s.end = s.w.setTimer(s.w.store, at, labelStoreEnd, s.expire)
	}
}

// armSnapshot sets the snapshot timer for the next multiple of RetryPeriod of the store's
// monotonic clock after the present reading.
func (s *storeServer) armSnapshot() {
	rp := s.w.sc.Timing.RetryPeriod
	at := (Mono(s.w.store)/rp + 1) * rp
	s.snapshot = s.w.setTimer(s.w.store, at, labelStoreSnapshot, s.sendSnapshot)
}

// sendSnapshot sends a snapshot of the model to every receiver and sets the timer for the next.
func (s *storeServer) sendSnapshot() {
	for _, id := range s.receivers() {
		s.w.fw.Net.Send(s.w.store, id, s.model.snapshot())
	}
	s.armSnapshot()
}

// cloneSessions returns the sessions in a slice of their own.
func cloneSessions(in []sessionInfo) []sessionInfo {
	if in == nil {
		return nil
	}
	return append([]sessionInfo(nil), in...)
}
