// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"slices"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The record states a driver reads reach its machine as HolderChanged only through the filter
// in this file (SIM-335, MCH O4 and O5), whatever brought them: a holder push, a snapshot, the
// answer of a Confirm, or a holdings answer of a resync (SIM-340).
//
// O4: per shard, a record state is stepped only when its version is above the delivered one,
// the version of the latest state the machine has had for the shard: the latest HolderChanged
// stepped, or the grant of an Acquired. A Confirm's answer may repeat the delivered version:
// under the Server rule a confirmation writes nothing, and its answer must still reach a
// degraded shard.
//
// O5: while the machine's Status shows the shard degraded, a record naming this driver's
// holder at the run's term is stepped only when it was read after the shard's latest Confirm
// action went out. A Confirm's own answer counts as read when the Confirm went out. No other
// reading can be placed after the Confirm reached the store: the network keeps no order, so a
// holdings request sent after the Confirm may be served before it, and a push or a snapshot may
// have left the store before the Confirm came. So only the answer of the latest Confirm action
// is stepped there (decided for 1.8-08; the machine repeats Confirm every RetryPeriod while
// degraded and live, so a restore waits for no more than one round trip of it).
//
// Late grants (SIM-370, MCH-550): a granted reply after its timeout is dropped with
// tenure.reply_late by reply; the grant's record reaches the machine as a HolderChanged naming
// this holder through a push, a snapshot or a resync, since no Acquired delivered its version.

// srcHoldings is the src of a HolderChanged read by a resync's holdings request.
const srcHoldings = "holdings"

// shardRecord is what a driver keeps of one shard's record for the filter.
type shardRecord struct {
	// delivered is the version of the latest record state the machine has had for the shard:
	// that of the latest HolderChanged stepped, or of the record of a granted reply stepped as
	// Acquired. 0 before either. A record skipped by the filter delivers nothing.
	delivered uint64

	// confirm is the call of the shard's latest Confirm action, nil before the first. It is
	// set whether the lanes queued the call or dropped it (O7): a dropped Confirm never goes
	// out, so no answer counts as read after it.
	confirm *call
}

// recReading is one record state the driver read, on its way through the filter.
type recReading struct {
	rec  tenure.Holding
	src  string // srcPush, srcSnapshot, srcConfirm or srcHoldings
	text string // the Describe of the message that carried it
	// call is the request whose reply carried rec: a Confirm or a holdings request. nil for a
	// push or a snapshot.
	call *call
}

// resync is the state of a driver's holdings resync (SIM-340): after a SessionRenewed that
// follows one or more SessionFailed{CauseUnreachable}, holdings(space) for each space in name
// order, one at a time, each resent every RetryPeriod until answered, or until the session is
// lost or the lanes close.
type resync struct {
	running bool
	next    int  // the index in driver.spaceNames of the space being read
	again   bool // a renewal asked for a resync while this one ran: start over when it ends
}

// resyncLane returns the lane of a resync's holdings requests: a shard lane of no shard, so the
// requests go out one at a time and each takes one of the shardPermits, as the runtime's
// Holdings calls take a permit of its executor.
func resyncLane() laneKey { return laneKey{kind: laneShard, shard: -1} }

// shardRecord returns the filter state of the shard of key, made at its first use.
func (d *driver) shardRecord(key laneKey) *shardRecord {
	sr := d.records[key]
	if sr == nil {
		sr = &shardRecord{}
		d.records[key] = sr
	}
	return sr
}

// observe steps rd as HolderChanged if the filter admits it. It first steps the Tick due by
// now, so the filter sees the Status after every wake due and no Confirm action comes between
// its decision and the step.
func (d *driver) observe(rd recReading) {
	if d.stopped {
		return
	}
	d.tickUntil(d.m.Mono())
	if !d.admit(rd) {
		return
	}
	d.shardRecord(shardLane(rd.rec.Space, rd.rec.Shard)).delivered = rd.rec.Version
	d.step(core.HolderChanged{Space: rd.rec.Space, Shard: rd.rec.Shard, Holder: rd.rec.Holder, Term: rd.rec.Term}, rd.text, rd.src)
}

// admit reports whether rd is stepped. In order:
//
//  1. O4: a version below the delivered one is dropped, and so is one equal to it unless rd is
//     a Confirm's answer;
//  2. a record naming this holder while an Acquire of the shard is in flight is left to the
//     Acquire's reply, which steps the grant as Acquired, so a StartRun follows the Acquired of
//     its term; if the reply comes late, a later push, snapshot or resync has the record;
//  3. O5: while the shard is degraded, a record naming this holder at the run's term is
//     stepped only when it is the answer of the shard's latest Confirm action.
func (d *driver) admit(rd recReading) bool {
	key := shardLane(rd.rec.Space, rd.rec.Shard)
	sr := d.shardRecord(key)
	v := rd.rec.Version
	if v < sr.delivered || v == sr.delivered && rd.src != srcConfirm {
		return false
	}
	if rd.rec.Holder != d.holder {
		return true
	}
	if rd.src != srcConfirm && d.acquiring(key) {
		return false
	}
	if sh, ok := shardIn(d.status(), rd.rec.Space, rd.rec.Shard); ok &&
		sh.State == core.ShardStateDegraded && sh.Term == rd.rec.Term {
		return rd.call != nil && rd.call == sr.confirm
	}
	return true
}

// shardIn returns the shard's entry in st, and false when st has no such shard.
func shardIn(st core.Status, space string, shard int) (core.ShardStatus, bool) {
	for _, sp := range st.Spaces {
		if sp.Name == space {
			if shard < 0 || shard >= len(sp.Shards) {
				return core.ShardStatus{}, false
			}
			return sp.Shards[shard], true
		}
	}
	return core.ShardStatus{}, false
}

// noteConfirm makes c, the call of a Confirm action, the shard's latest (O5).
func (d *driver) noteConfirm(c *call) {
	d.shardRecord(c.key).confirm = c
}

// noteGrant raises the delivered version of the shard of c, an Acquire, to the version of the
// record its granted reply carries: the Acquired stepped from it gives the machine that state.
func (d *driver) noteGrant(c *call, rep grantedReply) {
	sr := d.shardRecord(c.key)
	sr.delivered = max(sr.delivered, rep.Record.Version)
}

// confirmReading returns the reading of rep, the answer of the Confirm of c.
func confirmReading(c *call, a core.Confirm, rep confirmedReply) recReading {
	rec := rep.Record
	rec.Space, rec.Shard = a.Space, a.Shard
	return recReading{rec: rec, src: srcConfirm, text: rep.Describe(), call: c}
}

// startResync starts a resync, or, while one runs, asks for another after it.
func (d *driver) startResync() {
	if d.resync.running {
		d.resync.again = true
		return
	}
	if len(d.spaceNames) == 0 {
		return
	}
	d.resync = resync{running: true}
	d.readHoldings()
}

// readHoldings sends the holdings request of the resync's present space. Once the session is
// lost, or the lanes close (O7), the resync ends: a lost machine has no use for the records, and
// a new incarnation's driver starts its own.
func (d *driver) readHoldings() {
	c := &call{
		key:       resyncLane(),
		req:       holdingsReq{Space: d.spaceNames[d.resync.next]},
		onReply:   d.holdingsAnswer,
		onTimeout: d.holdingsTimeout,
	}
	if d.status().Session == core.SessionStateLost || !d.lanes.admits(c) {
		d.resync = resync{}
		return
	}
	d.md.lastID++
	c.req = withID(c.req, reqHead{ID: d.md.lastID, Holder: d.holder})
	d.lanes.submit(c)
}

// holdingsAnswer steps each record of a holdings answer through the filter, in shard order,
// then reads the next space, or ends the resync, or starts the one asked for meanwhile.
func (d *driver) holdingsAnswer(c *call, rep reply) {
	r, ok := rep.(holdingsReply)
	if !ok {
		d.w.sim.Fail(fmt.Errorf("world: driver %s: reply %q to %q", d.m.Name, rep.Describe(), c.req.Describe()))
		return
	}
	text := r.Describe()
	for _, rec := range r.Records {
		d.observe(recReading{rec: rec, src: srcHoldings, text: text, call: c})
	}
	if d.stopped {
		return
	}
	d.resync.next++
	switch {
	case d.resync.next < len(d.spaceNames):
		d.readHoldings()
	case d.resync.again:
		d.resync = resync{running: true}
		d.readHoldings()
	default:
		d.resync = resync{}
	}
}

// holdingsTimeout sends the timed-out holdings request again, RetryPeriod after the one before
// went out.
func (d *driver) holdingsTimeout(*call) {
	d.readHoldings()
}

// sortedSpaceNames returns the names of the spaces, sorted.
func sortedSpaceNames(spaces []core.SpaceSpec) []string {
	names := make([]string, len(spaces))
	for i, sp := range spaces {
		names[i] = sp.Name
	}
	slices.Sort(names)
	return names
}
