// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The labels of a member's mono timers, and the stream its runs draw from.
const (
	labelWake    = "tenure/driver/wake"
	labelTimeout = "tenure/driver/timeout"
	labelRunStop = "tenure/driver/run_stop"
	streamRun    = "tenure/run/"
)

// The src attribute of a HolderChanged or a ViewChanged: where the driver read the state.
// records_version.go adds srcHoldings, and observer.go srcReply and srcObserve.
const (
	srcPush     = "push"
	srcSnapshot = "snapshot"
	srcConfirm  = "confirm"
)

// drivers holds the drivers of the members, and what outlives an incarnation of a member: the
// count of its requests, so that a reply to a request of an earlier incarnation never answers a
// request of the present one.
type drivers struct {
	w       *World
	members []*memberDrive // by the member's place in w.members

	// onStep, when set, is called after every step of every driver, once the step's records
	// have been emitted and its actions carried out, with the input as stepped and the
	// actions. The checks of 1.8-10 feed the checker from it.
	onStep func(d *driver, in core.Input, acts []core.Action)
	// onRetire, when set, is called when a new incarnation of a member takes over from the
	// driver of the one before, before that driver stops: its holder is gone from then on.
	onRetire func(d *driver)
}

// memberDrive is what a member keeps across its incarnations.
type memberDrive struct {
	lastID uint64     // the ID of the member's latest request
	cur    *driver    // the driver of the present incarnation, nil before the first
	runs   *rand.Rand // tenure/run/<member>
}

// newDrivers returns the drivers of w, with none started.
func newDrivers(w *World) *drivers {
	return &drivers{w: w}
}

// of returns what m keeps across its incarnations.
func (ds *drivers) of(m *Member) *memberDrive {
	if ds.members == nil {
		ds.members = make([]*memberDrive, len(ds.w.members))
	}
	i := int(m.Node.ID()) - int(ds.w.fence.ID()) - 1
	if ds.members[i] == nil {
		ds.members[i] = &memberDrive{runs: ds.w.sim.Rand(streamRun + m.Name)}
	}
	return ds.members[i]
}

// start is the startMember hook: it starts the driver of m's new incarnation, which takes
// over m's node from the driver of the incarnation before, and steps its first input.
func (ds *drivers) start(m *Member) {
	md := ds.of(m)
	if md.cur != nil {
		if ds.onRetire != nil {
			ds.onRetire(md.cur)
		}
		md.cur.stop()
	}
	d := newDriver(ds, md, m)
	md.cur = d
	d.begin()
}

// current returns the driver of m's present incarnation, nil before the first.
func (ds *drivers) current(m *Member) *driver {
	return ds.of(m).cur
}

// driver drives the machine of one incarnation of an active member, as the runtime's loop and
// executor drive theirs (SIM-300 to SIM-330, MCH O1 to O8), with requests to the store over the
// simulated network in place of store calls.
//
// Stepping (SIM-300). Every input carries the member's monotonic reading in the event that
// makes it. One mono timer is kept at the machine's NextWake. Before any input at a reading at
// or after it, the driver steps a Tick at that reading (O2), and the first input of an
// incarnation is a Tick. Each step emits tenure.input, then one tenure.action per action, in
// order, each carried out right after its record.
//
// Actions (SIM-310). KeepAlive, Acquire, Release, Confirm, ClaimSlot,
// ReleaseSlot and CloseSession become requests on the lanes; StartRun starts a run against the
// fence and StopRun stops it, which steps RunStopped once the run has stopped (run.go);
// ExtendDeadline moves the session deadline the runs check; Degrade, Restore and Wake are only
// recorded. Under the Observer rule, Acquire and ClaimSlot
// are takeovers of several requests on one lane, and views are the driver's own (observer.go).
//
// Replies and timeouts (SIM-320). A request times out RetryPeriod of the member's clock after
// it went out, with tenure.timeout; replyInputs and timeoutInputs say what is stepped. A reply
// after its timeout is dropped with tenure.reply_late.
//
// Pushes (SIM-330). A view newer than the last one stepped is stepped as ViewChanged with the
// member's wall reading and each member's Slot per RUN-220, unless it was read before the
// latest SlotClaimed or SlotReleased (O8). A record is stepped as HolderChanged through the
// version and confirmation filter of records_version.go (O4, O5).
//
// Resync (SIM-340). After a SessionRenewed that follows failed renewals, the driver reads the
// holdings of every space, in name order, through the same filter.
type driver struct {
	ds *drivers
	w  *World
	m  *Member
	md *memberDrive

	holder  string
	timing  core.Timing
	machine *core.Machine
	lanes   *lanes

	wake     *monoTimer
	inFlight map[uint64]*call // by request ID; looked up only
	late     map[uint64]bool  // the requests that timed out with no reply yet; looked up only
	stepping bool             // a step is being carried out
	stopped  bool             // a later incarnation took over, or the member left
	lost     bool             // the machine has lost its session (recover.go)

	// viewStepped reports that a view has been stepped, and viewVersion is the Version of the
	// latest. slotFloor is the version of the store's view after the latest SlotClaimed or
	// SlotReleased stepped: a view of a lower version was read before it (O8).
	viewStepped bool
	viewVersion uint64
	slotFloor   uint64

	// records holds, by shard lane key, the filter state of each shard (records_version.go);
	// looked up only.
	records map[laneKey]*shardRecord

	// spaceNames holds the names of the spaces, sorted, and resync the state of the holdings
	// resync (SIM-340).
	spaceNames []string
	resync     resync

	// runs holds the runs of the incarnation (run.go) from their StartRun to their
	// RunStopped, in the order they started; fenceCalls the run of each fence request in
	// flight, by request ID, looked up only. deadline is the session deadline Check reads: the
	// latest of StartRun and ExtendDeadline.
	runs       []*run
	fenceCalls map[uint64]*run
	deadline   time.Duration

	// obs is the state of the Observer rule (observer.go): the observations, the observer
	// view and its timer. nil under the Server rule.
	obs *observer
}

// newDriver returns the driver of m's present incarnation, with its machine.
func newDriver(ds *drivers, md *memberDrive, m *Member) *driver {
	sc := ds.w.sc
	d := &driver{
		ds:       ds,
		w:        ds.w,
		m:        m,
		md:       md,
		holder:   m.holder,
		timing:   sc.Timing,
		inFlight: make(map[uint64]*call),
		late:     make(map[uint64]bool),
		records:  make(map[laneKey]*shardRecord),

		spaceNames: sortedSpaceNames(sc.Spaces),
	}
	if sc.Takeover == Observer {
		d.obs = &observer{}
	}
	d.machine = core.NewMachine(core.MachineConfig{Holder: m.holder, Member: m.Name, Spaces: sc.Spaces, Timing: machineTiming(sc)})
	d.lanes = newLanes(d.send)
	return d
}

// machineTiming returns the timing of the machines of sc's members: the scenario's. With the
// planted bug LateDeadline (mutants_on.go) the machine's RenewDeadline is twice the
// scenario's, so each renewal moves its session deadline RenewDeadline further than the
// scenario allows, and its LeaseDuration (and its SlotGrace, when it would fall below it)
// grows by that RenewDeadline, which keeps the timing valid. The store and the driver keep the
// scenario's timing.
func machineTiming(sc Scenario) core.Timing {
	t := sc.Timing
	if sc.Mutation.plants(LateDeadline) {
		t.LeaseDuration += t.RenewDeadline
		t.RenewDeadline *= 2
		t.SlotGrace = max(t.SlotGrace, t.LeaseDuration)
	}
	return t
}

// begin installs the driver's handler on the member's node and steps the first input, a Tick.
func (d *driver) begin() {
	d.w.fw.Net.Handle(d.m.Node, d.handle)
	d.step(core.Tick{}, "", "")
}

// stop ends the driver: it cancels its timers and from then on ignores what reaches it. The
// handler stays on the node until the next incarnation installs its own; a member that exits
// removes it (recover.go).
func (d *driver) stop() {
	d.stopped = true
	if d.wake != nil {
		d.wake.cancel()
	}
	for _, l := range d.lanes.all {
		if l.cur != nil && l.cur.timer != nil {
			l.cur.timer.cancel()
		}
	}
	d.stopRuns()
	if d.obs != nil && d.obs.timer != nil {
		d.obs.timer.cancel()
	}
}

// status returns the machine's Status.
func (d *driver) status() core.Status { return d.machine.Status() }

// step steps in, which came with a message whose text is text, at the member's present reading,
// after a Tick at that reading when the machine's wake is at or before it (O2). src is the src
// attribute of a HolderChanged or a ViewChanged, "" for other inputs; extra are attributes of
// tenure.input after it.
func (d *driver) step(in core.Input, text, src string, extra ...kernel.Attr) {
	if d.stopped {
		return
	}
	now := d.m.Mono()
	d.tickUntil(now)
	d.stepOne(d.at(in, now), text, src, extra...)
}

// tickUntil steps a Tick at now while the machine's wake is at or before now. The machine arms
// every timer after its clock, so one Tick is enough.
func (d *driver) tickUntil(now time.Duration) {
	for {
		w, ok := d.machine.NextWake()
		if !ok || w > now {
			return
		}
		d.stepOne(core.Tick{Now: now}, "", "")
	}
}

// onWake runs when the wake timer fires.
func (d *driver) onWake() {
	if d.stopped {
		return
	}
	d.tickUntil(d.m.Mono())
	d.rearm()
}

// stepOne steps in, whose Now is set, and carries the step out: tenure.input, then, for each
// action in order, its tenure.action and what it asks for. Then it sets the wake timer for the
// machine's NextWake.
func (d *driver) stepOne(in core.Input, text, src string, extra ...kernel.Attr) {
	if d.stepping {
		panic("world: driver: a step inside a step")
	}
	d.stepping = true
	acts := d.machine.Step(in)
	typ, fields := inputFields(in)
	if src != "" {
		fields = append(fields, recAttr("src", src))
	}
	fields = append(fields, extra...)
	d.w.emitInput(d.m, text, typ, fields...)
	for _, a := range acts {
		d.execute(a)
	}
	d.stepping = false
	d.rearm()
	if d.ds.onStep != nil {
		d.ds.onStep(d, in, acts)
	}
	d.afterStep()
}

// rearm keeps the wake timer at the machine's NextWake.
func (d *driver) rearm() {
	w, ok := d.machine.NextWake()
	if d.wake != nil && d.wake.pending() {
		if ok && d.wake.target() == w {
			return
		}
		d.wake.cancel()
	}
	d.wake = nil
	if ok && !d.stopped {
		d.wake = d.w.setTimer(d.m.Node, w, labelWake, d.onWake)
	}
}

// at returns in with Now set to now; a ViewChanged also gets the member's wall reading at now.
func (d *driver) at(in core.Input, now time.Duration) core.Input {
	switch v := in.(type) {
	case core.Tick:
		v.Now = now
		return v
	case core.ViewChanged:
		v.Now, v.Wall = now, kernel.Epoch.Add(now+d.m.wall)
		return v
	case core.SessionRenewed:
		v.Now = now
		return v
	case core.SessionFailed:
		v.Now = now
		return v
	case core.SessionClosed:
		v.Now = now
		return v
	case core.Acquired:
		v.Now = now
		return v
	case core.AcquireFailed:
		v.Now = now
		return v
	case core.Released:
		v.Now = now
		return v
	case core.RunStopped:
		v.Now = now
		return v
	case core.HolderChanged:
		v.Now = now
		return v
	case core.SlotClaimed:
		v.Now = now
		return v
	case core.SlotReleased:
		v.Now = now
		return v
	case core.Drain:
		v.Now = now
		return v
	}
	panic(fmt.Sprintf("world: driver: unknown input %T", in))
}

// execute emits the tenure.action of a and carries a out (SIM-310's table, Server rule). The
// record of an action that becomes a request has the request's text; a request the lanes drop
// (O6, O7) has an empty text.
func (d *driver) execute(a core.Action) {
	typ, fields := actionFields(a)
	c := d.callFor(a)
	if _, ok := a.(core.Confirm); ok {
		d.noteConfirm(c)
	}
	if c == nil {
		d.w.emitAction(d.m, "", typ, fields...)
		switch a := a.(type) {
		case core.StartRun:
			d.startRun(a)
		case core.StopRun:
			d.stopRun(a)
		case core.ExtendDeadline:
			d.extendDeadline(a)
		case core.Degrade, core.Restore, core.Wake:
		case core.ClaimSlot:
			// Under the Observer rule, a ClaimSlot with no slot to claim sends nothing.
		default:
			panic(fmt.Sprintf("world: driver: unknown action %T", a))
		}
		return
	}
	if !d.lanes.admits(c) {
		d.w.emitAction(d.m, "", typ, fields...)
		return
	}
	d.md.lastID++
	c.req = withID(c.req, reqHead{ID: d.md.lastID, Holder: d.holder})
	d.w.emitAction(d.m, c.req.Describe(), typ, fields...)
	d.lanes.submit(c)
}

// callFor returns the call that carries out a, with its request still without its head, or nil
// for an action that is no request. Under the Observer rule observerCall makes the calls of
// Acquire and ClaimSlot, which take several requests.
func (d *driver) callFor(a core.Action) *call {
	if d.obs != nil {
		if c, ok := d.observerCall(a); ok {
			return c
		}
	}
	c := &call{act: a, onReply: d.answer, onTimeout: d.expire}
	switch a := a.(type) {
	case core.KeepAlive:
		c.key, c.req = laneKey{kind: laneSession}, keepAliveReq{Timing: d.timing}
	case core.CloseSession:
		c.key, c.req = laneKey{kind: laneSession}, closeSessionReq{}
	case core.Acquire:
		c.key, c.req = shardLane(a.Space, a.Shard), acquireReq{Space: a.Space, Shard: a.Shard}
	case core.Release:
		c.key, c.req = shardLane(a.Space, a.Shard), releaseReq{Space: a.Space, Shard: a.Shard, Term: a.Term}
	case core.Confirm:
		c.key, c.req = shardLane(a.Space, a.Shard), confirmReq{Space: a.Space, Shard: a.Shard, Term: a.Term}
	case core.ClaimSlot:
		c.key, c.req = laneKey{kind: laneSlot}, claimSlotReq{}
	case core.ReleaseSlot:
		c.key, c.req = laneKey{kind: laneSlot}, releaseSlotReq{Slot: a.Slot}
	default:
		return nil
	}
	return c
}

// shardLane returns the key of a shard's lane.
func shardLane(space string, shard int) laneKey {
	return laneKey{kind: laneShard, space: space, shard: shard}
}

// withID returns req with the head h. It panics for a request the driver does not send.
func withID(req request, h reqHead) request {
	switch r := req.(type) {
	case keepAliveReq:
		r.reqHead = h
		return r
	case closeSessionReq:
		r.reqHead = h
		return r
	case acquireReq:
		r.reqHead = h
		return r
	case releaseReq:
		r.reqHead = h
		return r
	case confirmReq:
		r.reqHead = h
		return r
	case claimSlotReq:
		r.reqHead = h
		return r
	case releaseSlotReq:
		r.reqHead = h
		return r
	case holdingsReq:
		r.reqHead = h
		return r
	case setSlotsReq:
		r.reqHead = h
		return r
	case getShardReq:
		r.reqHead = h
		return r
	case getSlotReq:
		r.reqHead = h
		return r
	case getSessionReq:
		r.reqHead = h
		return r
	case casReq:
		r.reqHead = h
		return r
	case casSlotReq:
		r.reqHead = h
		return r
	}
	panic(fmt.Sprintf("world: driver: cannot send %T", req))
}

// send puts c on the network to the store, at the member's present reading, and sets the
// timer of its timeout, RetryPeriod later on the member's clock (SIM-320). The lanes call it.
func (d *driver) send(c *call) {
	c.sentAt = d.m.Mono()
	d.inFlight[c.req.head().ID] = c
	c.timer = d.w.setTimer(d.m.Node, c.sentAt+d.timing.RetryPeriod, labelTimeout, func() { d.timeout(c) })
	d.w.fw.Net.Send(d.m.Node, d.w.store.ID(), c.req)
}

// timeout runs when c has had no reply for RetryPeriod: it emits tenure.timeout, the lane goes
// on, and the timeout is stepped. A reply that comes later is late.
func (d *driver) timeout(c *call) {
	if d.stopped {
		return
	}
	id := c.req.head().ID
	delete(d.inFlight, id)
	d.late[id] = true
	act := ""
	if c.act != nil {
		act, _ = actionFields(c.act)
	}
	d.w.emitTimeout(d.m, c.req.Describe(), id, act)
	d.lanes.finish(c)
	if c.onTimeout != nil {
		c.onTimeout(c)
	}
}

// handle takes one message delivered to the member's node: a reply of the fence, which goes to
// the run that sent the request (run.go), a reply of the store, or a push of the store.
func (d *driver) handle(from kernel.NodeID, payload any) {
	if d.stopped {
		return
	}
	switch p := payload.(type) {
	case raisedReply:
		d.runReply(p)
	case writtenReply:
		d.runReply(p)
	case reply:
		d.reply(p)
	case holderPush:
		d.record(p.Record, p.Describe(), srcPush)
	case viewPush:
		if d.obs != nil {
			d.observeSessions(p.Sessions)
		}
		d.view(p.View, p.Describe(), srcPush)
	case snapshotPush:
		text := p.Describe()
		if d.obs != nil {
			d.observeSessions(p.Sessions)
		}
		for _, rec := range p.Records {
			d.record(rec, text, srcSnapshot)
		}
		d.view(p.View, text, srcSnapshot)
	default:
		d.w.sim.Fail(fmt.Errorf("world: driver %s: unexpected message %T from node %d", d.m.Name, payload, from))
	}
}

// reply takes the reply rep: it frees the lane of the request it answers and steps what it
// means. A call of several requests (the Observer rule's takeovers) may instead send its next
// request, and keeps its lane and its timeout. A reply to a request that timed out is dropped
// with tenure.reply_late, and a reply to a request of an earlier incarnation is dropped.
func (d *driver) reply(rep reply) {
	id := rep.requestID()
	c := d.inFlight[id]
	if c == nil {
		if d.late[id] {
			delete(d.late, id)
			d.w.emitReplyLate(d.m, rep.Describe(), id, replyName(rep))
		}
		return
	}
	delete(d.inFlight, id)
	if c.next != nil && c.next(c, rep) {
		return
	}
	c.timer.cancel()
	// A Tick due now is stepped before the answer (O2). Step it while c still holds its lane:
	// the machine has not seen the answer yet and may repeat c's action in that Tick, as a
	// member that resumes after a pause past c's RetryPeriod does, and the repeat must be
	// dropped as equal to c, which is still in flight (O6).
	d.tickUntil(d.m.Mono())
	d.lanes.finish(c)
	if c.onReply != nil {
		c.onReply(c, rep)
	}
}

// replyName returns the name of a reply, the start of its text: "renewed", "granted" and so
// on.
func replyName(rep reply) string {
	name, _, _ := strings.Cut(rep.Describe(), "#")
	return name
}

// answer steps what the reply rep to the machine action of c means (SIM-320). A slot reply also
// raises the view floor of O8 first, and a granted reply the shard's delivered version. A
// Confirm's answer goes through the record filter (O4, O5) as a reading with src confirm. A
// renewal that ends a run of failed renewals starts a resync (SIM-340). A reply the table has
// no row for fails the run.
func (d *driver) answer(c *call, rep reply) {
	switch r := rep.(type) {
	case slotClaimedReply:
		d.slotFloor = max(d.slotFloor, r.View)
	case slotReleasedReply:
		d.slotFloor = max(d.slotFloor, r.View)
	case grantedReply:
		d.noteGrant(c, r)
	}
	ins, ok := replyInputs(c.act, rep, c.sentAt)
	if !ok {
		d.w.sim.Fail(fmt.Errorf("world: driver %s: reply %q to %q", d.m.Name, rep.Describe(), c.req.Describe()))
		return
	}
	_, renewal := c.act.(core.KeepAlive)
	failing := renewal && d.status().LastFailure == core.CauseUnreachable
	for _, in := range ins {
		if _, ok := in.(core.HolderChanged); ok {
			a, _ := c.act.(core.Confirm)
			r, _ := rep.(confirmedReply)
			d.observe(confirmReading(c, a, r))
			continue
		}
		d.step(in, rep.Describe(), "")
	}
	if failing && !d.stopped && d.status().LastFailure == core.CauseNone {
		d.startResync()
	}
	if renewal && d.obs != nil {
		// A renewal may make the session live again, which thaws the observer view.
		d.showObserved("", srcObserve)
	}
}

// expire steps what the timeout of c means (SIM-320).
func (d *driver) expire(c *call) {
	for _, in := range timeoutInputs(c.act) {
		d.step(in, "", "")
	}
}

// replyInputs returns the inputs a reply to the action act means, with Now 0, and false for a
// reply that does not answer such an action. sentAt is the reading at which the request went
// out. The table (SIM-320, as the runtime's RUN-150):
//
//   - KeepAlive: renewed → SessionRenewed{SentAt: sentAt}; lost → SessionFailed{CauseLost}.
//   - CloseSession: closed → SessionClosed; lost → SessionClosed too, the driver duty of
//     CloseSession's doc, since the machine ignores SessionFailed while closing.
//   - Acquire: granted → Acquired{Term}; held → AcquireFailed{Holder, RetryAfter}; lost →
//     Acquired{Term: 0} then SessionFailed{CauseLost}.
//   - Release: released → Released{Term}, whether or not it cleared the record.
//   - Confirm: confirmed → HolderChanged with the record's holder and term.
//   - ClaimSlot: slotClaimed → SlotClaimed; noSlot → nothing.
//   - ReleaseSlot: slotReleased → SlotReleased, whether or not it cleared the record.
//
// A lost reply to any other request steps SessionFailed{CauseLost}.
func replyInputs(act core.Action, rep reply, sentAt time.Duration) ([]core.Input, bool) {
	if _, ok := rep.(lostReply); ok {
		switch a := act.(type) {
		case core.KeepAlive, core.Release, core.Confirm, core.ClaimSlot, core.ReleaseSlot:
			return []core.Input{core.SessionFailed{Cause: core.CauseLost}}, true
		case core.CloseSession:
			return []core.Input{core.SessionClosed{}}, true
		case core.Acquire:
			return []core.Input{
				core.Acquired{Space: a.Space, Shard: a.Shard},
				core.SessionFailed{Cause: core.CauseLost},
			}, true
		}
		return nil, false
	}
	switch a := act.(type) {
	case core.KeepAlive:
		if _, ok := rep.(renewedReply); ok {
			return []core.Input{core.SessionRenewed{SentAt: sentAt}}, true
		}
	case core.CloseSession:
		if _, ok := rep.(closedReply); ok {
			return []core.Input{core.SessionClosed{}}, true
		}
	case core.Acquire:
		switch r := rep.(type) {
		case grantedReply:
			return []core.Input{core.Acquired{Space: a.Space, Shard: a.Shard, Term: r.Record.Term}}, true
		case heldReply:
			return []core.Input{core.AcquireFailed{
				Space: a.Space, Shard: a.Shard, Holder: r.Record.Holder, RetryAfter: r.RetryAfter,
			}}, true
		}
	case core.Release:
		if _, ok := rep.(releasedReply); ok {
			return []core.Input{core.Released{Space: a.Space, Shard: a.Shard, Term: a.Term}}, true
		}
	case core.Confirm:
		if r, ok := rep.(confirmedReply); ok {
			return []core.Input{core.HolderChanged{
				Space: a.Space, Shard: a.Shard, Holder: r.Record.Holder, Term: r.Record.Term,
			}}, true
		}
	case core.ClaimSlot:
		switch r := rep.(type) {
		case slotClaimedReply:
			return []core.Input{core.SlotClaimed{Slot: r.Slot}}, true
		case noSlotReply:
			return nil, true
		}
	case core.ReleaseSlot:
		if _, ok := rep.(slotReleasedReply); ok {
			return []core.Input{core.SlotReleased{Slot: a.Slot}}, true
		}
	}
	return nil, false
}

// timeoutInputs returns the inputs the timeout of a request for act means, with Now 0 (SIM-320,
// O3): a KeepAlive's steps SessionFailed{CauseUnreachable} and an Acquire's Acquired{Term: 0};
// the machine repeats every other request itself, so its timeout steps nothing.
func timeoutInputs(act core.Action) []core.Input {
	switch a := act.(type) {
	case core.KeepAlive:
		return []core.Input{core.SessionFailed{Cause: core.CauseUnreachable}}
	case core.Acquire:
		return []core.Input{core.Acquired{Space: a.Space, Shard: a.Shard}}
	}
	return nil
}

// record steps the shard record rec of a push or a snapshot, which came with a message whose
// text is text, as a HolderChanged if the record filter admits it (records_version.go: O4, O5,
// and a record naming this holder while an Acquire of the shard is in flight is left to the
// Acquire's reply). If that reply comes late, a later push, snapshot or resync has the record,
// which is then stepped (MCH-550).
func (d *driver) record(rec tenure.Holding, text, src string) {
	d.observe(recReading{rec: rec, src: src, text: text})
}

// acquiring reports whether an Acquire is in flight on the lane of key.
func (d *driver) acquiring(key laneKey) bool {
	l := d.lanes.shards[key]
	if l == nil || l.cur == nil {
		return false
	}
	_, ok := l.cur.act.(core.Acquire)
	return ok
}

// view steps the store's view v, which came with a message whose text is text, as a
// ViewChanged when it is newer than the last view stepped, and was not read before the latest
// SlotClaimed or SlotReleased (O8). Each member's Slot is set per RUN-220. Under the Observer
// rule v becomes the base of the observer view instead (observer.go).
func (d *driver) view(v core.View, text, src string) {
	if d.obs != nil {
		d.observerBase(v, text, src)
		return
	}
	if d.viewStepped && v.Version <= d.viewVersion {
		return
	}
	if v.Version < d.slotFloor {
		return
	}
	d.viewStepped, d.viewVersion = true, v.Version
	d.step(core.ViewChanged{View: withSlots(v)}, text, src)
}

// withSlots returns v with each member's Slot as the runtime merges slot data into a view
// (RUN-220): the lowest slot whose holder has the member's ID, else -1. The result shares no
// slice with v.
func withSlots(v core.View) core.View {
	out := cloneView(v)
	lowest := make(map[string]int) // by member ID: 1 + its lowest slot, so that 0 means none
	for s, h := range out.SlotHolders {
		if h == "" {
			continue
		}
		if id := sessionMember(h); lowest[id] == 0 {
			lowest[id] = s + 1
		}
	}
	for i := range out.Members {
		out.Members[i].Slot = lowest[out.Members[i].ID] - 1
	}
	return out
}

// inputFields returns the type of the input and its fields as attributes of tenure.input, in
// the order the type declares them: times as *_ns, a view as version, members (the member IDs
// joined by commas) and slots (as tenure.view prints them), a wall time as nanoseconds since the
// simulation's epoch.
func inputFields(in core.Input) (string, []kernel.Attr) {
	switch v := in.(type) {
	case core.Tick:
		return "Tick", []kernel.Attr{recNow(v.Now)}
	case core.ViewChanged:
		fields := append([]kernel.Attr{recNow(v.Now)}, viewFields(v.View)...)
		return "ViewChanged", append(fields, recAttr("wall_ns", recNanos(v.Wall.Sub(kernel.Epoch))))
	case core.SessionRenewed:
		return "SessionRenewed", []kernel.Attr{recNow(v.Now), recAttr("sent_at_ns", recNanos(v.SentAt))}
	case core.SessionFailed:
		return "SessionFailed", []kernel.Attr{recNow(v.Now), recAttr("cause", v.Cause.String())}
	case core.SessionClosed:
		return "SessionClosed", []kernel.Attr{recNow(v.Now)}
	case core.Acquired:
		return "Acquired", append([]kernel.Attr{recNow(v.Now)}, shardFields(v.Space, v.Shard, v.Term)...)
	case core.AcquireFailed:
		return "AcquireFailed", []kernel.Attr{
			recNow(v.Now), recAttr("space", v.Space), recAttr("shard", strconv.Itoa(v.Shard)),
			recAttr("holder", v.Holder), recAttr("retry_after_ns", recNanos(v.RetryAfter)),
		}
	case core.Released:
		return "Released", append([]kernel.Attr{recNow(v.Now)}, shardFields(v.Space, v.Shard, v.Term)...)
	case core.RunStopped:
		return "RunStopped", append([]kernel.Attr{recNow(v.Now)}, shardFields(v.Space, v.Shard, v.Term)...)
	case core.HolderChanged:
		return "HolderChanged", []kernel.Attr{
			recNow(v.Now), recAttr("space", v.Space), recAttr("shard", strconv.Itoa(v.Shard)),
			recAttr("holder", v.Holder), recAttr("term", recTerm(v.Term)),
		}
	case core.SlotClaimed:
		return "SlotClaimed", []kernel.Attr{recNow(v.Now), recAttr("slot", strconv.Itoa(v.Slot))}
	case core.SlotReleased:
		return "SlotReleased", []kernel.Attr{recNow(v.Now), recAttr("slot", strconv.Itoa(v.Slot))}
	case core.Drain:
		return "Drain", []kernel.Attr{recNow(v.Now)}
	}
	panic(fmt.Sprintf("world: driver: unknown input %T", in))
}

// actionFields returns the type of the action and its fields as attributes of tenure.action, in
// the order the type declares them.
func actionFields(a core.Action) (string, []kernel.Attr) {
	switch v := a.(type) {
	case core.KeepAlive:
		return "KeepAlive", nil
	case core.ExtendDeadline:
		return "ExtendDeadline", []kernel.Attr{recAttr("deadline_ns", recNanos(v.Deadline))}
	case core.Acquire:
		return "Acquire", []kernel.Attr{recAttr("space", v.Space), recAttr("shard", strconv.Itoa(v.Shard))}
	case core.Release:
		return "Release", shardFields(v.Space, v.Shard, v.Term)
	case core.StartRun:
		return "StartRun", append(shardFields(v.Space, v.Shard, v.Term), recAttr("deadline_ns", recNanos(v.Deadline)))
	case core.StopRun:
		return "StopRun", append(shardFields(v.Space, v.Shard, v.Term), recAttr("reason", v.Reason.String()))
	case core.Confirm:
		return "Confirm", shardFields(v.Space, v.Shard, v.Term)
	case core.Degrade:
		return "Degrade", shardFields(v.Space, v.Shard, v.Term)
	case core.Restore:
		return "Restore", shardFields(v.Space, v.Shard, v.Term)
	case core.ClaimSlot:
		return "ClaimSlot", nil
	case core.ReleaseSlot:
		return "ReleaseSlot", []kernel.Attr{recAttr("slot", strconv.Itoa(v.Slot))}
	case core.CloseSession:
		return "CloseSession", nil
	case core.Wake:
		return "Wake", []kernel.Attr{recAttr("at_ns", recNanos(v.At))}
	}
	panic(fmt.Sprintf("world: driver: unknown action %T", a))
}

// recNow returns the now_ns attribute of an input.
func recNow(now time.Duration) kernel.Attr { return recAttr("now_ns", recNanos(now)) }

// shardFields returns the space, shard and term attributes.
func shardFields(space string, shard int, t core.Term) []kernel.Attr {
	return []kernel.Attr{recAttr("space", space), recAttr("shard", strconv.Itoa(shard)), recAttr("term", recTerm(t))}
}

// viewFields returns the version, members and slots attributes of a view, as tenure.view has
// them.
func viewFields(v core.View) []kernel.Attr {
	ids := make([]string, len(v.Members))
	for i, m := range v.Members {
		ids[i] = m.ID
	}
	slots := strconv.Itoa(v.Slots)
	if v.Slots > 0 {
		slots += ":" + strings.Join(v.SlotHolders, ",")
	}
	return []kernel.Attr{
		recAttr("version", recUint(v.Version)),
		recAttr("members", strings.Join(ids, ",")),
		recAttr("slots", slots),
	}
}
