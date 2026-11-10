// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"time"

	"github.com/hmdsefi/tenure/core"
)

// shardPermits is the number of shard-lane requests a member may have in flight at once
// (SIM-310: 16 permits, as the runtime's executor has).
const shardPermits = 16

// laneKind names what a lane serializes requests on.
type laneKind uint8

const (
	laneSession laneKind = iota // keepAlive, closeSession
	laneSlot                    // claimSlot, releaseSlot, and setSlots from 1.8-14
	laneShard                   // acquire, release, confirm of one shard
)

// laneKey names a lane: the session's, the slot lane, or the lane of one shard.
type laneKey struct {
	kind  laneKind
	space string
	shard int
}

// call is one request of a member on its way through a lane: queued, in flight, or done.
type call struct {
	key laneKey
	// act is the machine action the request carries out, nil for a request the driver makes
	// on its own account, such as the holdings reads of 1.8-08 or the setSlots of 1.8-14.
	act core.Action
	// req is the request, with its ID and the sender's holder.
	req request

	// sentAt is the member's monotonic reading when the request went out, and timer the mono
	// timer of its timeout, RetryPeriod later on the member's clock. Both are set once it goes
	// out.
	sentAt time.Duration
	timer  *monoTimer

	// onReply steps what the reply means; onTimeout what the timeout means. Either may be nil.
	// The lane frees the call before either runs.
	onReply   func(c *call, rep reply)
	onTimeout func(c *call)

	// next, when set, takes a reply first, for a call of several requests: when it sends the
	// call's next request and reports true, the call stays in flight on its lane, under the
	// timer of its first request, and onReply waits for a later reply.
	next func(c *call, rep reply) bool
}

// lane holds the requests on one record: they go out one at a time, in the order they were
// queued (SIM-310).
type lane struct {
	key     laneKey
	queue   []*call
	cur     *call // the request in flight, nil when none is
	waiting bool  // a shard lane whose next request waits for a permit
}

// lanes are the lanes of one incarnation of a member (SIM-310): the session lane, the slot lane
// and one lane per shard. A request of a lane goes out when the request before it on that lane
// has been answered or has timed out, and at most shardPermits requests of the shard lanes are
// in flight at once; a shard lane whose turn comes with no free permit waits for one, and the
// waiting lanes get the permits in the order they began to wait.
//
// lanes decides only when a request goes out. send puts a request on the network, and the
// driver calls finish when the request has been answered or has timed out. Neither submit nor
// finish steps the machine, so the driver can call them while it carries out a step.
type lanes struct {
	send func(c *call)

	session, slot *lane
	shards        map[laneKey]*lane // looked up only; never ranged over
	all           []*lane           // every lane, in the order it was made
	waiting       []*lane           // the shard lanes waiting for a permit, in the order they began to wait

	inFlight int // requests in flight, over every lane
	shardOut int // requests in flight on shard lanes

	// closing reports that a CloseSession has been submitted (O7): no request but a
	// CloseSession is queued from then on. closeCall is a CloseSession that waits for the
	// requests in flight before it goes out.
	closing   bool
	closeCall *call
}

// newLanes returns empty lanes that put requests on the network with send.
func newLanes(send func(c *call)) *lanes {
	ls := &lanes{send: send, shards: make(map[laneKey]*lane)}
	ls.session = ls.newLane(laneKey{kind: laneSession})
	ls.slot = ls.newLane(laneKey{kind: laneSlot})
	return ls
}

// newLane makes the lane of key.
func (ls *lanes) newLane(key laneKey) *lane {
	l := &lane{key: key}
	ls.all = append(ls.all, l)
	return l
}

// laneOf returns the lane of key, making a shard's lane at its first request.
func (ls *lanes) laneOf(key laneKey) *lane {
	switch key.kind {
	case laneSession:
		return ls.session
	case laneSlot:
		return ls.slot
	}
	l := ls.shards[key]
	if l == nil {
		l = ls.newLane(key)
		ls.shards[key] = l
	}
	return l
}

// submit queues c on its lane, and sends it at once when the lane is free and, for a shard
// lane, a permit is. It reports whether c was queued. It drops c, and returns false:
//
//   - O6: when c is a Release, ReleaseSlot, ClaimSlot or CloseSession equal to one queued or in
//     flight on its lane, whose answer answers both, and when c is a KeepAlive while one is
//     queued, as the runtime's executor drops it;
//   - O7: when a CloseSession has been submitted and c is not one.
//
// A CloseSession that is not dropped closes the lanes (O7): it drops every queued request, and
// it goes out once every request in flight has been answered or has timed out. The machine
// returns CloseSession only when every shard is idle and it holds no slot, so no dropped request
// is an Acquire that waits for its answer.
func (ls *lanes) submit(c *call) bool {
	if !ls.admits(c) {
		return false
	}
	l := ls.laneOf(c.key)
	if _, isClose := c.act.(core.CloseSession); isClose {
		ls.closing = true
		ls.dropQueued()
		if ls.inFlight > 0 {
			ls.closeCall = c
			return true
		}
	}
	l.queue = append(l.queue, c)
	ls.pump(l)
	return true
}

// admits reports whether submit would queue c rather than drop it.
func (ls *lanes) admits(c *call) bool {
	_, isClose := c.act.(core.CloseSession)
	if ls.closing && !isClose {
		return false
	}
	return !coalesced(ls.laneOf(c.key), c) && (!isClose || ls.closeCall == nil)
}

// coalesced reports whether c is dropped because of an equal request on l (O6).
func coalesced(l *lane, c *call) bool {
	switch c.act.(type) {
	case core.KeepAlive:
		return queued(l, c.act)
	case core.Release, core.ReleaseSlot, core.ClaimSlot, core.CloseSession:
		return (l.cur != nil && l.cur.act == c.act) || queued(l, c.act)
	}
	return false
}

// queued reports whether a request for act is queued on l.
func queued(l *lane, act core.Action) bool {
	for _, q := range l.queue {
		if q.act == act {
			return true
		}
	}
	return false
}

// dropQueued drops the queued requests of every lane (O7).
func (ls *lanes) dropQueued() {
	for _, l := range ls.all {
		clear(l.queue)
		l.queue = nil
		l.waiting = false
	}
	ls.waiting = nil
}

// finish frees the lane of c, which has been answered or has timed out, and sends what may go
// out now: the requests of the lanes that waited for a permit, in the order they began to wait,
// then the next request of c's lane, and, once nothing is in flight, a CloseSession that waited
// for that. finish does nothing for a call that is not in flight.
func (ls *lanes) finish(c *call) {
	l := ls.laneOf(c.key)
	if l.cur != c {
		return
	}
	l.cur = nil
	ls.inFlight--
	if l.key.kind == laneShard {
		ls.shardOut--
	}
	for len(ls.waiting) > 0 && ls.shardOut < shardPermits {
		w := ls.waiting[0]
		ls.waiting = ls.waiting[1:]
		w.waiting = false
		ls.pump(w)
	}
	ls.pump(l)
	if ls.closeCall != nil && ls.inFlight == 0 {
		cl := ls.closeCall
		ls.closeCall = nil
		ls.session.queue = append(ls.session.queue, cl)
		ls.pump(ls.session)
	}
}

// pump sends the next request of l if l is free and, for a shard lane, a permit is; a shard
// lane with no permit begins to wait for one.
func (ls *lanes) pump(l *lane) {
	if l.cur != nil || len(l.queue) == 0 || l.waiting {
		return
	}
	if l.key.kind == laneShard {
		if ls.shardOut >= shardPermits {
			l.waiting = true
			ls.waiting = append(ls.waiting, l)
			return
		}
		ls.shardOut++
	}
	c := l.queue[0]
	l.queue[0] = nil
	l.queue = l.queue[1:]
	l.cur = c
	ls.inFlight++
	ls.send(c)
}

// pending returns the number of queued requests, over every lane, with a CloseSession that
// waits for the requests in flight.
func (ls *lanes) pending() int {
	n := 0
	for _, l := range ls.all {
		n += len(l.queue)
	}
	if ls.closeCall != nil {
		n++
	}
	return n
}
