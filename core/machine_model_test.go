// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
	mc "github.com/hmdsefi/tenure/internal/machinecases"
)

// This file holds the model driver of AT-MCH-48 (MCH §10.7). Where the chaos driver of AT-MCH-49 breaks
// the driver contract, the model driver keeps it, and drives the machine against a model store, so that
// the checks can hold the machine to the outcomes the contract promises.
//
// The driver is a discrete-event loop: a queue of events ordered by reading, then by sequence number.
// Each event is a call's answer coming due, a run reporting its end, a record change reaching the watch,
// a new view, the second holder h2 acting, the store ending a session, or the network going into or out
// of an outage. The driver keeps its side of the contract:
//
//   - O2: when the machine's NextWake comes before the next event, the driver steps a Tick at it first.
//   - O3: every call gets one answer, after a delay drawn from [0, modelAnswerMax], and the call applies
//     to the store when its answer is due. Calls on one record, on the slots or on the session apply in
//     the order they went out, as #51's executor keeps them, so no answer comes before an earlier
//     call's on the same record. A dropped answer's call applies with probability 1/2. The
//     driver still owes the machine an answer to a dropped Acquire, Acquired with term 0, and to a dropped
//     KeepAlive, SessionFailed unreachable; the machine repeats the other calls itself. A Release answers
//     Released whether or not the compare-and-swap found the record, a ReleaseSlot answers SlotReleased
//     whether or not the store found the slot held, a CloseSession answers SessionClosed whether or not the
//     store still had the session, and a claim that finds no free slot gets no answer.
//   - O4: each record change reaches the machine as a HolderChanged, per record in version order, and the
//     driver steps only versions above the one it has delivered. An Acquired with a grant, or a Released
//     whose compare-and-swap found the record, reports the version its call made, which becomes the
//     delivered one. A Confirm's answer may repeat the delivered version, so that a degraded shard can be
//     restored.
//   - O5: while Status shows a shard degraded, a record naming this holder in the run's term is stepped
//     only when its read started after the shard's latest Confirm; any other is held back until the shard
//     is no longer degraded. A watch report's read starts at the change it reports, and a Confirm's read
//     when the Confirm went out, which is all a driver can know of it.
//   - O6: the driver never repeats a call of its own.
//   - A RunStopped follows every StopRun within [0, modelStopMax], so some runs outlive RenewDeadline and
//     are marked stuck. Every answered claim is followed by a view, and a view comes every
//     [0, modelViewMax] otherwise. The store's slot count is the latest view's Slots. One Drain comes with
//     probability 3/10.
//
// The store follows architecture §9: a session ends LeaseDuration after its last renewal, every grant
// takes its term from core.NextTerm, a Release is a compare-and-swap on the holder and term, a claim takes
// the lowest free slot, and a Confirm is a read of the record. The second holder h2 has a session that
// never ends, and every [0, modelH2Max] it tries to acquire a random shard and, with probability 1/2,
// releases a random shard it holds. A third holder, h3, holds some slots for good and no shard.
//
// The original §10.7 text is lost, so these choices are this file's: answers are dropped with probability
// 1/10 outside outages and always inside one; outages last [0, modelOutageMax] and come
// [modelGoodMin, modelGoodMax] apart; AcquireFailed asks for a RetryAfter of 0, 1 s, 5 s or 20 s; an
// Acquire the store cannot grant because this member's session has ended answers Acquired with term 0;
// views are snapshots of the store when they are stepped, so they never lag it; and the Drain comes at a
// reading drawn from [0, modelDrainMax].

// The model driver's parameters.
const (
	modelAnswerMax = 3 * time.Second
	modelStopMax   = 12 * time.Second
	modelViewMax   = 20 * time.Second
	modelH2Max     = 5 * time.Second
	modelOutageMax = 30 * time.Second
	modelGoodMin   = 10 * time.Second
	modelGoodMax   = 60 * time.Second
	modelDrainMax  = 120 * time.Second

	// modelSlotMax bounds the slot counts of the model's views.
	modelSlotMax = 3

	// modelOwnBound is how long a wanted shard can wait to be owned while nothing outside the machine
	// stands in its way: the session live, the shard wanted, the member not draining, the record not
	// held by h2, and no answer about the shard dropped or held back by O5. The longest path is a run told to stop before
	// the wait began: its RunStopped within modelStopMax, the Release answered within modelAnswerMax,
	// then the Acquire answered within modelAnswerMax, and modelOwnBound leaves 2 s beyond that.
	modelOwnBound = modelStopMax + 2*modelAnswerMax + 2*time.Second

	// modelDrainBound is how long a drained member can take to close its session, counted from the
	// later of the Drain and the last answer dropped other than a KeepAlive's: a run's RunStopped, the
	// Release, the ReleaseSlot and the CloseSession, each answered within modelAnswerMax.
	modelDrainBound = modelStopMax + 3*modelAnswerMax

	// modelLossBound is how long a machine can take to be done after its session is lost: every run
	// has its StopRun in the step that accepts the loss, and its RunStopped comes within modelStopMax.
	// An Acquire still out is answered sooner.
	modelLossBound = modelStopMax
)

// modelOther is the second holder of shards, and modelSlotHolder a holder of slots only.
const (
	modelOther      = "h2"
	modelSlotHolder = "h3"
)

// modelRecord is a shard record of the model store.
type modelRecord struct {
	holder  string
	term    core.Term
	version uint64
}

// recordRead is what one read of a shard record saw: the shard's index in the driver's list, the record,
// and the driver's sequence number at the start of the read, which orders it against the machine's steps.
type recordRead struct {
	shard   int
	holder  string
	term    core.Term
	version uint64
	seq     uint64
}

// modelStore is the model store: the session of the machine's holder, the shard records, and the slots.
// h2's session never ends, and h3 holds its slots for good.
type modelStore struct {
	// alive says the machine's session has neither ended nor closed, and expiresAt is the reading at
	// which it ends unless renewed.
	alive     bool
	expiresAt time.Duration

	// records holds the shard records, by the driver's shard index.
	records []modelRecord

	// slots holds each slot's holder, "" for none, and slotCount is the latest view's Slots.
	slots     [modelSlotMax]string
	slotCount int
}

// modelKind says what a modelEvent is.
type modelKind int

const (
	// modelAnswer is a call's answer coming due: the call applies to the store and its answer is stepped.
	modelAnswer modelKind = iota

	// modelInput steps a prepared input: a RunStopped or the Drain.
	modelInput

	// modelWatch delivers a record read through the O4 and O5 filters.
	modelWatch

	// modelView steps a view of the store, and modelPeriodicView does too and schedules the next.
	modelView
	modelPeriodicView

	// modelH2 is h2 acting on the records.
	modelH2

	// modelExpire is the store ending the machine's session if it has not been renewed.
	modelExpire

	// modelNetwork starts or ends an outage.
	modelNetwork
)

// modelEvent is one event of the driver's queue.
type modelEvent struct {
	at   time.Duration
	seq  uint64
	kind modelKind

	// call, sentAt and sentSeq are a modelAnswer's call, the reading it went out at, and the sequence
	// number of the step that returned it.
	call    core.Action
	sentAt  time.Duration
	sentSeq uint64

	// in is a modelInput's input.
	in core.Input

	// read is a modelWatch's read.
	read recordRead
}

// modelStamp is the reading of a call the machine returned, and the term it named.
type modelStamp struct {
	at   time.Duration
	term core.Term
	ok   bool
}

// modelDriver is one walk of the machine against the model store.
type modelDriver struct {
	rng    *rand.Rand
	holder string
	timing core.Timing
	w      *propWalk
	st     core.Status

	now   time.Duration
	seq   uint64
	queue []modelEvent
	store modelStore

	// steps counts the steps so far, and lastIn is the input of the latest.
	steps  int
	lastIn core.Input

	// stepSeq is the sequence number of the latest step.
	stepSeq uint64

	// shards lists the config's shards by space name, then shard: a shard's index here indexes every
	// per-shard slice of the driver.
	shards []propShard

	// floor holds each record's delivered version (O4), confirmSeq the sequence number of the step that
	// returned the shard's latest Confirm (O5), held a read held back by O5, and watchAt the reading of
	// the record's latest watch delivery, so that deliveries keep version order.
	floor      []uint64
	confirmSeq []uint64
	held       []recordRead
	heldOK     []bool
	watchAt    []time.Duration

	// laneAt holds the reading of each lane's latest answer, by lane: see call.
	laneAt []time.Duration

	slotMode    int
	viewVersion uint64
	outage      bool

	checks modelChecks
	reach  *modelReach
}

// newModelDriver returns the model walk of a seed, which adds what it reaches to reach.
func newModelDriver(seed uint64, reach *modelReach) *modelDriver {
	cfg := chaosConfig()
	w := newPropWalk(cfg)
	d := &modelDriver{
		// A seeded generator, so that a seed replays its walk exactly; nothing here needs to be unpredictable.
		rng:      rand.New(rand.NewPCG(seed, 48)), //nolint:gosec // G404: a test's seeded walk.
		holder:   cfg.Holder,
		timing:   core.MachineTiming(w.m),
		w:        w,
		st:       w.m.Status(),
		slotMode: int(seed % 3),
		reach:    reach,
	}
	for _, sp := range d.st.Spaces {
		for shard := range sp.Shards {
			d.shards = append(d.shards, propShard{sp.Name, shard})
		}
	}
	n := len(d.shards)
	d.store.records = make([]modelRecord, n)
	d.floor, d.confirmSeq, d.watchAt = make([]uint64, n), make([]uint64, n), make([]time.Duration, n)
	d.held, d.heldOK = make([]recordRead, n), make([]bool, n)
	d.laneAt = make([]time.Duration, n+2)
	d.checks = newModelChecks(n)
	for s := range d.store.slots {
		if d.rng.IntN(3) == 0 {
			d.store.slots[s] = modelSlotHolder
		}
	}

	// The driver opens the session before its first step, and the open counts as a renewal.
	d.store.alive, d.store.expiresAt = true, d.timing.LeaseDuration
	d.schedule(modelEvent{at: d.store.expiresAt, kind: modelExpire})
	d.schedule(modelEvent{at: d.within(modelViewMax), kind: modelPeriodicView})
	d.schedule(modelEvent{at: d.within(modelH2Max), kind: modelH2})
	d.schedule(modelEvent{at: d.between(modelGoodMin, modelGoodMax), kind: modelNetwork})
	if d.rng.IntN(10) < 3 {
		d.schedule(modelEvent{at: d.within(modelDrainMax), kind: modelInput, in: core.Drain{}})
	}
	return d
}

// within returns a duration drawn from [0, top].
func (d *modelDriver) within(top time.Duration) time.Duration {
	return time.Duration(d.rng.Int64N(int64(top) + 1))
}

// between returns a duration drawn from [low, top].
func (d *modelDriver) between(low, top time.Duration) time.Duration {
	return low + d.within(top-low)
}

// schedule adds e to the queue after every event at or before its reading, with the next sequence number.
func (d *modelDriver) schedule(e modelEvent) {
	d.seq++
	e.seq = d.seq
	i, _ := slices.BinarySearchFunc(d.queue, e, func(a, b modelEvent) int {
		if a.at != b.at {
			if a.at < b.at {
				return -1
			}
			return 1
		}
		if a.seq < b.seq {
			return -1
		}
		return 1
	})
	d.queue = slices.Insert(d.queue, i, e)
}

// index returns the driver's index of shard k.
func (d *modelDriver) index(k propShard) int {
	return slices.Index(d.shards, k)
}

// shardStatus returns the Status of the shard at index i after the latest step.
func (d *modelDriver) shardStatus(i int) core.ShardStatus {
	sh, _ := shardOf(d.st, d.shards[i])
	return sh
}

// run steps the walk until it has made steps steps, and returns the first rule a step broke.
func (d *modelDriver) run(steps int) error {
	if err := d.step(core.Tick{Now: 0}); err != nil {
		return err
	}
	for d.steps < steps {
		if at, armed := d.w.m.NextWake(); armed && (len(d.queue) == 0 || at <= d.queue[0].at) {
			// O2: the Tick at the wake comes before anything at or after its reading.
			d.now = at
			if err := d.step(core.Tick{Now: at}); err != nil {
				return err
			}
			continue
		}
		e := d.queue[0]
		d.queue = slices.Delete(d.queue, 0, 1)
		d.now = e.at
		if err := d.handle(e); err != nil {
			return err
		}
	}
	return nil
}

// handle applies one event.
func (d *modelDriver) handle(e modelEvent) error {
	switch e.kind {
	case modelAnswer:
		return d.answer(e.call, e.sentAt, e.sentSeq)
	case modelInput:
		return d.step(withNow(e.in, d.now))
	case modelWatch:
		return d.deliver(e.read, false)
	case modelPeriodicView:
		d.schedule(modelEvent{at: d.now + d.within(modelViewMax), kind: modelPeriodicView})
		return d.view()
	case modelView:
		return d.view()
	case modelH2:
		d.schedule(modelEvent{at: d.now + d.within(modelH2Max), kind: modelH2})
		d.actAsH2()
	case modelExpire:
		d.expireSession()
	case modelNetwork:
		d.outage = !d.outage
		next := d.between(modelGoodMin, modelGoodMax)
		if d.outage {
			d.reach.outages++
			next = d.within(modelOutageMax)
		}
		d.schedule(modelEvent{at: d.now + next, kind: modelNetwork})
	}
	return nil
}

// withNow returns a prepared input with its reading set to now.
func withNow(in core.Input, now time.Duration) core.Input {
	switch v := in.(type) {
	case core.Drain:
		v.Now = now
		return v
	case core.RunStopped:
		v.Now = now
		return v
	}
	return in
}

// step steps in, runs the checkers of AT-MCH-45 to AT-MCH-47 and those of AT-MCH-48, and carries out the
// actions the machine returned.
func (d *modelDriver) step(in core.Input) error {
	d.seq++
	stepSeq := d.seq
	d.stepSeq = stepSeq
	d.steps++
	d.lastIn = in
	s, err := d.w.step(in)
	if err != nil {
		return err
	}
	d.st = s.st
	d.reach.count(s, d.holder)
	if err := d.checks.check(d, s); err != nil {
		return err
	}
	for _, a := range s.got {
		switch v := a.(type) {
		case core.KeepAlive, core.CloseSession:
			d.call(a, len(d.shards)+1)
		case core.ClaimSlot, core.ReleaseSlot:
			d.call(a, len(d.shards))
		case core.Acquire:
			d.call(a, d.index(propShard{v.Space, v.Shard}))
		case core.Release:
			d.call(a, d.index(propShard{v.Space, v.Shard}))
		case core.Confirm:
			i := d.index(propShard{v.Space, v.Shard})
			d.confirmSeq[i] = stepSeq
			d.call(a, i)
		case core.StopRun:
			d.schedule(modelEvent{
				at:   d.now + d.within(modelStopMax),
				kind: modelInput,
				in:   core.RunStopped{Space: v.Space, Shard: v.Shard, Term: v.Term},
			})
		}
	}
	// O5: a read held back goes once the shard is no longer degraded, through the filters again.
	for i := range d.shards {
		if d.heldOK[i] && d.shardStatus(i).State != core.ShardStateDegraded {
			d.heldOK[i] = false
			d.schedule(modelEvent{at: d.now, kind: modelWatch, read: d.held[i]})
		}
	}
	if _, ok := in.(core.SlotClaimed); ok {
		// The driver follows every answered claim with a view.
		d.schedule(modelEvent{at: d.now, kind: modelView})
	}
	return nil
}

// call sends a call on the given lane: one per shard record, one for the slots and one for the session.
// Its answer comes due after a delay drawn from [0, modelAnswerMax], and never before an earlier call's
// on the same lane, so that calls on one record apply in the order they went out (#51).
func (d *modelDriver) call(a core.Action, lane int) {
	at := max(d.now+d.within(modelAnswerMax), d.laneAt[lane])
	d.laneAt[lane] = at
	d.schedule(modelEvent{at: at, kind: modelAnswer, call: a, sentAt: d.now, sentSeq: d.stepSeq})
}

// dropped reports whether an answer due now is lost: one in ten outside an outage, every one inside.
func (d *modelDriver) dropped() bool {
	return d.outage || d.rng.IntN(10) == 0
}

// answer applies a call whose answer is due and steps the answer, unless it is dropped. A dropped
// answer's call applies with probability 1/2, and the machine still gets an answer to a KeepAlive or an
// Acquire: the failure the driver sees.
func (d *modelDriver) answer(call core.Action, sentAt time.Duration, sentSeq uint64) error {
	if d.dropped() {
		applied := d.rng.IntN(2) == 0
		if applied {
			d.reach.droppedApplied++
		}
		switch v := call.(type) {
		case core.KeepAlive:
			if applied {
				d.renew()
			}
			return d.step(core.SessionFailed{Now: d.now, Cause: core.CauseUnreachable})
		case core.Acquire:
			d.checks.dropped(d, v.Space, v.Shard)
			if applied {
				d.acquire(d.index(propShard{v.Space, v.Shard}))
			}
			return d.step(core.Acquired{Now: d.now, Space: v.Space, Shard: v.Shard})
		case core.Release:
			d.checks.dropped(d, v.Space, v.Shard)
			if applied {
				d.release(d.index(propShard{v.Space, v.Shard}), v.Term)
			}
		case core.Confirm:
			d.checks.dropped(d, v.Space, v.Shard)
		case core.ClaimSlot:
			d.checks.dropped(d, "", -1)
			if applied {
				d.claim()
			}
		case core.ReleaseSlot:
			d.checks.dropped(d, "", -1)
			if applied {
				d.releaseSlot(v.Slot)
			}
		case core.CloseSession:
			d.checks.dropped(d, "", -1)
			if applied {
				d.endSession()
			}
		}
		return nil
	}
	switch v := call.(type) {
	case core.KeepAlive:
		if !d.renew() {
			return d.step(core.SessionFailed{Now: d.now, Cause: core.CauseLost})
		}
		return d.step(core.SessionRenewed{Now: d.now, SentAt: sentAt})
	case core.Acquire:
		i := d.index(propShard{v.Space, v.Shard})
		switch t, holder := d.acquire(i); {
		case t != 0:
			// O4: the answer reports the record's new version, so the watch's report of it is not newer.
			d.raiseFloor(i)
			return d.step(core.Acquired{Now: d.now, Space: v.Space, Shard: v.Shard, Term: t})
		case holder != "":
			d.reach.acquireFailed++
			retryAfter := []time.Duration{0, time.Second, 5 * time.Second, 20 * time.Second}[d.rng.IntN(4)]
			return d.step(core.AcquireFailed{Now: d.now, Space: v.Space, Shard: v.Shard, Holder: holder, RetryAfter: retryAfter})
		default:
			return d.step(core.Acquired{Now: d.now, Space: v.Space, Shard: v.Shard})
		}
	case core.Release:
		i := d.index(propShard{v.Space, v.Shard})
		if d.release(i, v.Term) {
			d.raiseFloor(i)
		} else {
			d.reach.releaseMissed++
		}
		return d.step(core.Released{Now: d.now, Space: v.Space, Shard: v.Shard, Term: v.Term})
	case core.Confirm:
		d.reach.confirmReads++
		// The driver knows only when it sent the read, so the read counts as started then (O5).
		r := d.read(d.index(propShard{v.Space, v.Shard}))
		r.seq = sentSeq
		return d.deliver(r, true)
	case core.ClaimSlot:
		if s, ok := d.claim(); ok {
			return d.step(core.SlotClaimed{Now: d.now, Slot: s})
		}
		d.reach.claimsUnanswered++
		return nil
	case core.ReleaseSlot:
		d.releaseSlot(v.Slot)
		return d.step(core.SlotReleased{Now: d.now, Slot: v.Slot})
	case core.CloseSession:
		if !d.store.alive {
			d.reach.closedGone++
		}
		d.endSession()
		return d.step(core.SessionClosed{Now: d.now})
	}
	return nil
}

// raiseFloor makes the record's current version, which an answer has just reported, the delivered one
// (O4), so that the watch never steps an older state after the answer.
func (d *modelDriver) raiseFloor(i int) {
	d.floor[i] = d.store.records[i].version
	if d.heldOK[i] && d.held[i].version <= d.floor[i] {
		d.heldOK[i] = false
	}
}

// deliver steps a record read as a HolderChanged, if O4 and O5 let it through: its version must be above
// the delivered one, or equal for a Confirm's answer, and while Status shows the shard degraded a read
// naming this holder in the run's term must come after the shard's latest Confirm, or it is held back.
func (d *modelDriver) deliver(r recordRead, confirm bool) error {
	i := r.shard
	if r.version < d.floor[i] || !confirm && r.version == d.floor[i] {
		return nil
	}
	if sh := d.shardStatus(i); sh.State == core.ShardStateDegraded && r.holder == d.holder && r.term == sh.Term &&
		r.seq < d.confirmSeq[i] {
		if !d.heldOK[i] || d.held[i].version < r.version {
			d.held[i], d.heldOK[i] = r, true
		}
		d.reach.heldBack++
		d.checks.since[i] = d.now
		return nil
	}
	d.floor[i] = r.version
	if d.heldOK[i] && d.held[i].version <= r.version {
		d.heldOK[i] = false
	}
	k := d.shards[i]
	return d.step(core.HolderChanged{Now: d.now, Space: k.space, Shard: k.shard, Holder: r.holder, Term: r.term})
}

// read returns a read of the record at index i now.
func (d *modelDriver) read(i int) recordRead {
	d.seq++
	rec := d.store.records[i]
	return recordRead{shard: i, holder: rec.holder, term: rec.term, version: rec.version, seq: d.seq}
}

// setRecord changes the record at index i and sends the change to the watch, which delivers it after a
// delay drawn from [0, modelAnswerMax], and never before the record's earlier changes.
func (d *modelDriver) setRecord(i int, holder string, term core.Term) {
	rec := &d.store.records[i]
	rec.holder, rec.term = holder, term
	rec.version++
	at := max(d.now+d.within(modelAnswerMax), d.watchAt[i])
	d.watchAt[i] = at
	d.schedule(modelEvent{at: at, kind: modelWatch, read: d.read(i)})
}

// sessionAlive ends the machine's session if its time has come, and reports whether it is still alive.
func (d *modelDriver) sessionAlive() bool {
	if d.store.alive && d.now >= d.store.expiresAt {
		d.endSession()
		d.reach.expiries++
	}
	return d.store.alive
}

// renew renews the machine's session, and reports whether it was still alive.
func (d *modelDriver) renew() bool {
	if !d.sessionAlive() {
		return false
	}
	d.store.expiresAt = d.now + d.timing.LeaseDuration
	d.schedule(modelEvent{at: d.store.expiresAt, kind: modelExpire})
	return true
}

// expireSession ends the machine's session if it has not been renewed in time.
func (d *modelDriver) expireSession() {
	d.sessionAlive()
}

// endSession ends the machine's session: its records and slots are free from now on.
func (d *modelDriver) endSession() {
	if !d.store.alive {
		return
	}
	d.store.alive = false
	for i, rec := range d.store.records {
		if rec.holder == d.holder {
			d.setRecord(i, "", rec.term)
		}
	}
	for s, holder := range d.store.slots {
		if holder == d.holder {
			d.store.slots[s] = ""
		}
	}
}

// acquire applies the machine's Acquire of the record at index i: a free record, or one already in its
// name, is granted in a new term. It returns the term, or 0 and the holder that has the record, or 0 and
// "" when the machine's session has ended.
func (d *modelDriver) acquire(i int) (core.Term, string) {
	if !d.sessionAlive() {
		return 0, ""
	}
	rec := d.store.records[i]
	if rec.holder != "" && rec.holder != d.holder {
		return 0, rec.holder
	}
	t := core.NextTerm(rec.term, mc.Wall(d.now))
	d.setRecord(i, d.holder, t)
	return t, ""
}

// release applies the machine's Release of term t of the record at index i, a compare-and-swap on the
// holder and the term, and reports whether it found the record.
func (d *modelDriver) release(i int, t core.Term) bool {
	rec := d.store.records[i]
	if rec.holder != d.holder || rec.term != t {
		return false
	}
	d.setRecord(i, "", t)
	return true
}

// claim gives the machine the lowest free slot the latest view counts, and reports whether there was one.
func (d *modelDriver) claim() (int, bool) {
	if !d.sessionAlive() {
		return 0, false
	}
	for s := range d.store.slotCount {
		if d.store.slots[s] == "" {
			d.store.slots[s] = d.holder
			return s, true
		}
	}
	return 0, false
}

// releaseSlot frees slot s if the machine holds it.
func (d *modelDriver) releaseSlot(s int) {
	if s >= 0 && s < len(d.store.slots) && d.store.slots[s] == d.holder {
		d.store.slots[s] = ""
	}
}

// actAsH2 is h2's turn: it tries to acquire a random shard, and with probability 1/2 releases a random
// shard it holds.
func (d *modelDriver) actAsH2() {
	i := d.rng.IntN(len(d.shards))
	if rec := d.store.records[i]; rec.holder == "" {
		d.setRecord(i, modelOther, core.NextTerm(rec.term, mc.Wall(d.now)))
		d.checks.since[i] = d.now
		d.reach.h2Grants++
	}
	if d.rng.IntN(2) != 0 {
		return
	}
	var mine []int
	for i, rec := range d.store.records {
		if rec.holder == modelOther {
			mine = append(mine, i)
		}
	}
	if len(mine) == 0 {
		return
	}
	i = mine[d.rng.IntN(len(mine))]
	d.setRecord(i, "", d.store.records[i].term)
	d.checks.since[i] = d.now
}

// The memberships of the model's views without slots: this member alone, with others, without it, and
// draining.
var modelMembers = [][]core.Member{
	{{ID: "m1", Slot: -1}},
	{{ID: "m1", Slot: -1}, {ID: "m2", Slot: -1}},
	{{ID: "m1", Slot: -1}, {ID: "m3", Slot: -1}},
	{{ID: "m1", Slot: -1}, {ID: "m2", Slot: -1}, {ID: "m3", Slot: -1}},
	{{ID: "m2", Slot: -1}},
	{{ID: "m1", Slot: -1, Draining: true}, {ID: "m2", Slot: -1}},
}

// view steps a new view: a membership, and in a group with slots the store's slot holders. Its slot
// count becomes the store's.
func (d *modelDriver) view() error {
	d.viewVersion++
	v := core.View{Version: d.viewVersion}
	slotted := d.slotMode == 1 || d.slotMode == 2 && d.rng.IntN(2) == 0
	if slotted {
		v.Members = []core.Member{{ID: "m1", Slot: -1}, {ID: "m2", Slot: -1}}
		v.Slots = 1 + d.rng.IntN(modelSlotMax)
		v.SlotHolders = slices.Clone(d.store.slots[:v.Slots])
	} else {
		v.Members = slices.Clone(modelMembers[d.rng.IntN(len(modelMembers))])
	}
	d.store.slotCount = v.Slots
	return d.step(core.ViewChanged{Now: d.now, View: v, Wall: mc.Wall(d.now)})
}

// modelChecks is what the checks of AT-MCH-48 keep across the steps of a walk.
type modelChecks struct {
	// The readings of the latest KeepAlive, ClaimSlot and CloseSession, of each slot's latest
	// ReleaseSlot, and of each shard's latest Release, Confirm and StopRun, with the term it named.
	keepAlive, claim, closeSession modelStamp
	slotRelease                    [modelSlotMax]modelStamp
	release, confirm, stop         []modelStamp

	// liveSince is the reading of the first step of the session's current live streak.
	liveSince time.Duration

	// since holds, by shard, the latest reading at which something outside the machine kept the shard
	// from being owned (see modelOwnBound).
	since []time.Duration

	// lostAt is the reading of the step that accepted a lost session, drainAt of the step that accepted
	// the Drain, and droppedAt of the latest answer dropped other than a KeepAlive's.
	lostAt, drainAt, droppedAt time.Duration
	lost, drained              bool
}

// newModelChecks returns the checks of a walk of a machine with n shards.
func newModelChecks(n int) modelChecks {
	return modelChecks{
		release: make([]modelStamp, n),
		confirm: make([]modelStamp, n),
		stop:    make([]modelStamp, n),
		since:   make([]time.Duration, n),
	}
}

// dropped notes an answer dropped now: about the named shard, or about no shard when shard is -1.
func (c *modelChecks) dropped(d *modelDriver, space string, shard int) {
	c.droppedAt = d.now
	if shard >= 0 {
		c.since[d.index(propShard{space, shard})] = d.now
	}
}

// check runs the checks of AT-MCH-48 on a step, after the checkers of AT-MCH-45 to AT-MCH-47:
//
//  1. The store: while the session is live, the store has it, and the session deadline comes at least
//     LeaseDuration - RenewDeadline before the store would end it. An owned shard, or a degraded one while
//     the session is live, has its record in this holder's name at its term or a later one. A held slot
//     is this holder's in the store until a ReleaseSlot of it goes out.
//  2. Wakes and deadlines: while live, the machine wakes by the deadline. A run told to stop is stuck
//     exactly from RenewDeadline after its StopRun, and the machine wakes by then. An idle wanted shard of
//     a live member that does not drain waits for a RetryAt ahead, and the machine wakes by it.
//  3. Repeats on cadence: until the session ends a KeepAlive goes every RetryPeriod; a releasing shard's
//     Release, a degraded shard's Confirm while live, a standby's ClaimSlot, a give-back's ReleaseSlot,
//     the held slot's once its release is under way and its shards are released, and the CloseSession
//     while closing each went out less than RetryPeriod ago, and the machine wakes for the next.
//  4. Acquisition only while live, and quiet after a loss or a close, as checkStartStop and checkSession
//     check them on these walks.
//  5. Liveness: a shard the member wants while live, which nothing outside the machine has kept from it
//     for modelOwnBound, is owned. A lost machine is done within modelLossBound of the loss, and a drained
//     one is closed, or done after a loss, within modelDrainBound of the later of the Drain and the
//     latest answer dropped other than a KeepAlive's.
func (c *modelChecks) check(d *modelDriver, s propStep) error {
	const rule = "AT-MCH-48"
	st, now := s.st, s.st.Now
	tm := d.timing
	live := st.Session == core.SessionStateLive
	ended := sessionEnded(st)
	closing := st.Session == core.SessionStateClosing
	for _, a := range s.got {
		switch v := a.(type) {
		case core.KeepAlive:
			c.keepAlive = modelStamp{at: now, ok: true}
		case core.ClaimSlot:
			c.claim = modelStamp{at: now, ok: true}
		case core.CloseSession:
			c.closeSession = modelStamp{at: now, ok: true}
		case core.ReleaseSlot:
			c.slotRelease[v.Slot] = modelStamp{at: now, ok: true}
		case core.Release:
			c.release[d.index(propShard{v.Space, v.Shard})] = modelStamp{at: now, term: v.Term, ok: true}
		case core.Confirm:
			c.confirm[d.index(propShard{v.Space, v.Shard})] = modelStamp{at: now, term: v.Term, ok: true}
		case core.StopRun:
			c.stop[d.index(propShard{v.Space, v.Shard})] = modelStamp{at: now, term: v.Term, ok: true}
		}
	}
	if live && s.prior.Session != core.SessionStateLive {
		c.liveSince = now
	}
	due := func(at time.Duration, what string) error {
		if !s.armed || s.wake > at {
			return fmt.Errorf("%s: NextWake (%v, %v), want a wake by %v for %s", rule, s.wake, s.armed, at, what)
		}
		return nil
	}
	repeat := func(last modelStamp, what string) error {
		if !last.ok || now-last.at >= tm.RetryPeriod {
			return fmt.Errorf("%s: at %v the last %s went at %v (sent %v), not within RetryPeriod", rule, now, what, last.at, last.ok)
		}
		return due(last.at+tm.RetryPeriod, what)
	}

	// 1. The store.
	if live && !d.store.alive {
		return fmt.Errorf("%s: the session is live at %v, but the store has ended it", rule, now)
	}
	if st.Deadline != 0 && st.Deadline+tm.LeaseDuration-tm.RenewDeadline > d.store.expiresAt {
		return fmt.Errorf("%s: the deadline %v is too close to the store's end of the session %v", rule, st.Deadline, d.store.expiresAt)
	}
	if h := st.Slot; h >= 0 && live && !d.w.slots.releaseSent && (h >= modelSlotMax || d.store.slots[h] != d.holder) {
		return fmt.Errorf("%s: the machine holds slot %d, which the store gives %q", rule, h, d.slotHolder(h))
	}

	// 2 and 3. Wakes, deadlines and repeats.
	if live {
		if err := due(st.Deadline, "the deadline"); err != nil {
			return err
		}
	}
	if !ended {
		if err := repeat(c.keepAlive, "KeepAlive"); err != nil {
			return err
		}
	}
	if closing {
		if err := repeat(c.closeSession, "CloseSession"); err != nil {
			return err
		}
	}
	if st.Slot < 0 && d.w.slots.applied.Slots > 0 && live && !st.Draining {
		if err := repeat(c.claim, "ClaimSlot"); err != nil {
			return err
		}
	}
	if !ended {
		for slot := range modelSlotMax {
			if d.w.slots.givingBack[slot] {
				if err := repeat(c.slotRelease[slot], "ReleaseSlot{"+strconv.Itoa(slot)+"} of a give-back"); err != nil {
					return err
				}
			}
		}
		if h := st.Slot; h >= 0 && d.w.slots.releaseSent && shardsSettled(st) {
			if err := repeat(c.slotRelease[h], "ReleaseSlot of the held slot"); err != nil {
				return err
			}
		}
	}
	for i, k := range d.shards {
		sh, _ := shardOf(st, k)
		rec := d.store.records[i]
		name := fmt.Sprintf("shard %s/%d", k.space, k.shard)
		switch {
		case sh.State == core.ShardStateOwned, sh.State == core.ShardStateDegraded && live:
			if rec.holder != d.holder || rec.term < sh.Term {
				return fmt.Errorf("%s: %s is %v in term %d, but the store's record is %+v", rule, name, sh.State, sh.Term, rec)
			}
		}
		switch {
		case sh.State == core.ShardStateReleasing:
			if c.release[i].term != sh.Term {
				return fmt.Errorf("%s: %s releases term %d, but its last Release named %d", rule, name, sh.Term, c.release[i].term)
			}
			if err := repeat(c.release[i], "the Release of "+name); err != nil {
				return err
			}
		case sh.State == core.ShardStateDegraded && live:
			if c.confirm[i].term != sh.Term || c.confirm[i].at < c.liveSince {
				return fmt.Errorf("%s: %s is degraded in term %d with the session live since %v, but its last Confirm is %+v",
					rule, name, sh.Term, c.liveSince, c.confirm[i])
			}
			if err := repeat(c.confirm[i], "the Confirm of "+name); err != nil {
				return err
			}
		case toldToStop(sh.State):
			stuckAt := c.stop[i].at + tm.RenewDeadline
			if c.stop[i].term != sh.Term || sh.Stuck != (now >= stuckAt) {
				return fmt.Errorf("%s: %s is %+v at %v with its StopRun %+v", rule, name, sh, now, c.stop[i])
			}
			if !sh.Stuck {
				if err := due(stuckAt, "the stuck mark of "+name); err != nil {
					return err
				}
			}
			if sh.Stuck {
				d.reach.stuck++
			}
		case sh.State == core.ShardStateIdle && sh.Wanted && live && !st.Draining:
			if sh.RetryAt <= now {
				return fmt.Errorf("%s: %s is idle and wanted at %v with RetryAt %v, and no Acquire went", rule, name, now, sh.RetryAt)
			}
			if err := due(sh.RetryAt, "the RetryAt of "+name); err != nil {
				return err
			}
		}

		// 5. Liveness of a wanted shard.
		if !live || !sh.Wanted || st.Draining || rec.holder == modelOther {
			c.since[i] = now
		} else if now-c.since[i] > modelOwnBound {
			if sh.State != core.ShardStateOwned {
				return fmt.Errorf("%s: %s is wanted and %+v at %v, with nothing in its way since %v", rule, name, sh, now, c.since[i])
			}
			d.reach.ownedAfterWait++
		}
	}

	// 5. Liveness of a lost or drained machine.
	if st.Session == core.SessionStateLost && !c.lost {
		c.lost, c.lostAt = true, now
		d.reach.losses++
	}
	if st.Draining && !c.drained {
		c.drained, c.drainAt = true, now
		d.reach.drains++
	}
	if c.lost && now > c.lostAt+modelLossBound && !st.Done {
		return fmt.Errorf("%s: the session was lost at %v, and at %v the machine is not done", rule, c.lostAt, now)
	}
	if c.drained && !st.Done && now > max(c.drainAt, c.droppedAt)+modelDrainBound {
		return fmt.Errorf("%s: the member drains from %v, the latest answer dropped at %v, and at %v the session is %v",
			rule, c.drainAt, c.droppedAt, now, st.Session)
	}
	if st.Done && !s.prior.Done {
		switch {
		case st.Session == core.SessionStateClosed:
			d.reach.closed++
		case c.drained:
			d.reach.drainedDoneAfterLoss++
		}
	}
	return nil
}

// slotHolder returns the store's holder of slot s, or "" for a slot it does not have.
func (d *modelDriver) slotHolder(s int) string {
	if s < 0 || s >= modelSlotMax {
		return ""
	}
	return d.store.slots[s]
}

// shardsSettled reports that every shard in st is idle or has a stuck run, which lets the held slot go.
func shardsSettled(st core.Status) bool {
	for _, sp := range st.Spaces {
		for _, sh := range sp.Shards {
			if sh.State != core.ShardStateIdle && (!toldToStop(sh.State) || !sh.Stuck) {
				return false
			}
		}
	}
	return true
}

// modelReach counts how often the model walks reached the paths the checks are about.
type modelReach struct {
	actions                                                       map[string]int
	stops                                                         map[core.StopReason]int
	losses, closed, drains, drainedDoneAfterLoss, stuck, outages  int
	expiries, closedGone, droppedApplied, acquireFailed, h2Grants int
	releaseMissed, confirmReads, claimsUnanswered, heldBack       int
	ownedAfterWait, leftoverStarts                                int
	slots                                                         slotCounts
}

// newModelReach returns empty counts.
func newModelReach() *modelReach {
	return &modelReach{actions: map[string]int{}, stops: map[core.StopReason]int{}}
}

// count counts the actions of a step of a machine of holder.
func (r *modelReach) count(s propStep, holder string) {
	for _, a := range s.got {
		r.actions[fmt.Sprintf("%T", a)]++
		switch v := a.(type) {
		case core.StopRun:
			r.stops[v.Reason]++
		case core.StartRun:
			if hc, ok := s.in.(core.HolderChanged); ok && hc.Holder == holder {
				r.leftoverStarts++
			}
		}
	}
}

// runModelSeed runs the model walk of one seed for steps steps and returns the first rule it broke, with
// the step and the input, or nil. It adds what the walk reached to reach.
func runModelSeed(seed uint64, steps int, reach *modelReach) error {
	d := newModelDriver(seed, reach)
	if err := d.run(steps); err != nil {
		return fmt.Errorf("seed %d step %d: Step(%#v): %w", seed, d.steps, d.lastIn, err)
	}
	reach.slots.add(d.w.slots.counts)
	return nil
}

// TestATMCH48ModelDriver runs the model driver for seeds 1 to 1000, 500 steps each, against the model
// store, and checks after every step the rules of AT-MCH-45 to AT-MCH-47, as the chaos walks do, and those
// of AT-MCH-48 (see modelChecks.check). A failure names the seed, the step and the input, and the seed
// replays it exactly.
func TestATMCH48ModelDriver(t *testing.T) {
	const seeds, steps = 1000, 500
	reach := newModelReach()
	for seed := uint64(1); seed <= seeds; seed++ {
		if err := runModelSeed(seed, steps, reach); err != nil {
			t.Fatal(err)
		}
	}

	// The walks are only worth something if they reach the paths the rules are about.
	for _, c := range []struct {
		name  string
		count int
	}{
		{"StartRun", reach.actions["core.StartRun"]},
		{"StartRun from a record a dropped answer granted", reach.leftoverStarts},
		{"StopRun moved", reach.stops[core.StopMoved]},
		{"StopRun draining", reach.stops[core.StopDraining]},
		{"StopRun expired", reach.stops[core.StopExpired]},
		{"StopRun lost", reach.stops[core.StopLost]},
		{"Release", reach.actions["core.Release"]},
		{"Release that found no record", reach.releaseMissed},
		{"Degrade", reach.actions["core.Degrade"]},
		{"Confirm", reach.actions["core.Confirm"]},
		{"Restore", reach.actions["core.Restore"]},
		{"read held back by O5", reach.heldBack},
		{"ClaimSlot", reach.actions["core.ClaimSlot"]},
		{"claim with no answer", reach.claimsUnanswered},
		{"ReleaseSlot", reach.actions["core.ReleaseSlot"]},
		{"CloseSession", reach.actions["core.CloseSession"]},
		{"AcquireFailed from h2", reach.acquireFailed},
		{"dropped answer whose call applied", reach.droppedApplied},
		{"outage", reach.outages},
		{"stuck run", reach.stuck},
		{"session the store ended", reach.expiries},
		{"lost session", reach.losses},
		{"Drain", reach.drains},
		{"closed session", reach.closed},
		{"shard owned after waiting modelOwnBound", reach.ownedAfterWait},
		{"slot given back", reach.slots.giveBacks},
		{"held slot released", reach.slots.slotReleases},
		{"held slot retired", reach.slots.retirements},
		{"claim answer taken", reach.slots.claimsTaken},
	} {
		if c.count == 0 {
			t.Errorf("no model walk reached a %s", c.name)
		}
	}
	t.Logf("reach: %+v", *reach)
}
