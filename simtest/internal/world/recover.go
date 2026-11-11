// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// The causes of tenure.boot and tenure.exit that this file adds.
const (
	bootCauseRecover = "recover"
	exitCauseDrain   = "drain"
)

// labelDrain is the label of the post that steps Drain on a member's node.
const labelDrain = "tenure/drain"

// finalRecovered is the name of the final check of session recovery (SIM-680).
const finalRecovered = invariantPrefix + "recovered"

// Session loss, drain and exit (SIM-145, SIM-350, SIM-380), as the runtime's loop has them
// (RUN-180, RUN-190):
//
//   - Loss. The step that loses the session abandons the lanes (O7): no request of the lost
//     holder goes out from then on, and a reply to one in flight is dropped. In its place the
//     driver steps Acquired with no term for every shard that waits for an Acquire's answer,
//     at once, in space and shard order, as the runtime's lose does.
//   - Recovery. When the machine of a member that was not told to drain is done after the
//     loss, every shard idle and every run returned (d.runs empty), the member starts a new
//     incarnation in that same event: a new holder, tenure.boot cause=recover, a new driver.
//     The old holder is reported Down by the drivers' onRetire.
//   - Drain. Drain sets the member's draining flag and posts tenure/drain on its node, which
//     steps Drain. When the machine is done, its session closed (or lost during the drain),
//     the member exits: its driver stops, the network handler is removed, its mono timers are
//     dropped, its phase becomes Exited, tenure.exit is emitted and its holder is reported
//     Down. A member told to drain while its node is down exits at the node's next boot, with
//     no new incarnation. An exited member never boots again.

// afterStep runs at the end of every step of d: it handles the loss of the session, and what
// follows once the machine is done.
func (d *driver) afterStep() {
	if d.stopped {
		return
	}
	st := d.status()
	if !d.lost && st.Session == core.SessionStateLost {
		d.lost = true
		d.lose(st)
		if d.stopped {
			return
		}
		st = d.status()
	}
	if st.Done {
		d.settle(st)
	}
}

// lose runs after the step that lost the session: it abandons the lanes, ends a resync, and
// steps Acquired with no term for every shard whose Acquire waits for its answer, which will
// never come now.
func (d *driver) lose(st core.Status) {
	d.lanes.abandon()
	clear(d.inFlight)
	d.resync = resync{} // its holdings request was dropped with the rest, and none follows
	var owed []core.Acquired
	for _, sp := range st.Spaces {
		for i, sh := range sp.Shards {
			if sh.State == core.ShardStateAcquiring {
				owed = append(owed, core.Acquired{Space: sp.Name, Shard: i})
			}
		}
	}
	for _, a := range owed {
		d.step(a, "", "")
	}
}

// settle runs after a step that leaves the machine done: a member told to drain, or whose
// session closed, exits; a member whose session was lost recovers once every run has returned.
func (d *driver) settle(st core.Status) {
	m := d.m
	switch {
	case m.draining || st.Session == core.SessionStateClosed:
		d.w.exit(m)
	case st.Session == core.SessionStateLost && len(d.runs) == 0:
		d.w.recoverMember(m)
	}
}

// recoverMember starts the new incarnation of m, whose machine is done after a lost session.
func (w *World) recoverMember(m *Member) {
	m.recoveries++
	w.incarnate(m, bootCauseRecover)
	w.sessions.recovered(m)
}

// Recoveries returns how many incarnations of the member started because the session of the
// one before was lost.
func (m *Member) Recoveries() int { return m.recoveries }

// Draining reports whether the member was told to drain.
func (m *Member) Draining() bool { return m.draining }

// Drain tells the member m to drain: it hands its shards over, closes its session and exits.
// The machine of m's present incarnation steps Drain in an event of m's node at the present
// time. A member told to drain while its node is down exits when the node boots again. Drain
// does nothing for a member that is not active, or that was told to drain before.
func (w *World) Drain(m *Member) {
	if m.phase != Active || m.draining {
		return
	}
	m.draining = true
	m.Node.Post(labelDrain, func() {
		if d := w.drivers.current(m); d != nil && !d.stopped && m.phase == Active {
			d.step(core.Drain{}, "", "")
		}
	})
}

// exit makes m leave for good: its driver stops, its node handles no message, its mono timers
// are dropped, its phase becomes Exited, tenure.exit is emitted and its holder reported Down.
func (w *World) exit(m *Member) {
	if d := w.drivers.current(m); d != nil {
		d.stop()
	}
	w.fw.Net.Handle(m.Node, nil)
	w.clocks.drop(m.Node)
	m.phase = Exited
	w.emitExit(m, exitCauseDrain)
	if w.checker != nil {
		w.checker.Down(m.holder)
	}
	w.sessions.exited(m)
}

// sessionEndTime returns the latest time at which the store can end, for EndAfter, a session
// that opened within two network delays of the start, whatever the store's drift:
// ⌈(EndAfter + 2·δ)·10⁶ / (10⁶ − ρ)⌉ ns. A scenario with EndAfter raises its recovery time to
// it, so that the first round of session ends happens before the recovery time.
func sessionEndTime(sc Scenario) kernel.Time {
	num := new(big.Int).SetInt64(int64(sc.EndAfter + 2*sc.Delta))
	num.Mul(num, big.NewInt(million))
	den := big.NewInt(million - int64(sc.MaxDrift()))
	if den.Sign() <= 0 {
		return kernel.Time(1<<63 - 1)
	}
	q, r := new(big.Int).QuoRem(num, den, new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() {
		return kernel.Time(1<<63 - 1)
	}
	return kernel.Time(q.Int64())
}

// endOwed is one end of a session of a member that the member owes a recovery for.
type endOwed struct {
	holder    string      // the holder whose session ended
	at        kernel.Time // when the store ended it
	recovers  int         // the recoveries of the member after it, before the next end
	newHolder string      // the holder of the first of them
	renewed   bool        // the store opened or renewed the session of newHolder
}

// sessionWatch keeps, from what the store and the members do, the session ends that the
// members owe a recovery for, and the recoveries (SIM-680).
type sessionWatch struct {
	w    *World
	ends [][]endOwed // by the member's place in w.members
	// owed counts the session ends that were owed a recovery when they happened, those
	// excused later included: a run with EndAfter in which none happened judged nothing.
	owed int
}

// installRecovered keeps the session watch and, when the scenario ends sessions by EndAfter,
// registers the final check tenure/recovered: every session end of a member that was not told
// to drain is followed by exactly one recovery of the member before the end of the run, whose
// new holder's session the store opened or renewed. An end of a session that a newer
// incarnation of the member replaced, of an incarnation that is not the member's present
// one, or of a member whose node is down, is owed nothing, and so is an end after which the
// node crashed or the member was told to drain before it recovered. A member whose node is
// paused owes the recovery: it learns of the end once it resumes. The check never passes for
// want of anything to judge: a run in which no end was owed fails it.
func (w *World) installRecovered() {
	w.sessions = &sessionWatch{w: w, ends: make([][]endOwed, len(w.members))}
	w.sim.OnCrash(func(n *kernel.Node) {
		if m := w.memberOf(n); m != nil {
			w.sessions.excuse(m)
		}
	})
	if w.sc.EndAfter <= 0 {
		return
	}
	w.fw.Final(finalRecovered, func() error {
		if w.finalsOff {
			return nil
		}
		return w.sessions.err()
	})
}

// index returns the place of m in w.members.
func (sw *sessionWatch) index(m *Member) int {
	return int(m.Node.ID()) - int(sw.w.fence.ID()) - 1
}

// member returns the member whose holder holder is, by the name before its slash, or nil.
func (sw *sessionWatch) member(holder string) *Member {
	name := sessionMember(holder)
	for _, m := range sw.w.members {
		if m.Name == name {
			return m
		}
	}
	return nil
}

// session takes one session effect of the store. A nil watch, of a world that Build did not
// make, takes nothing, and so for recovered and excuse.
func (sw *sessionWatch) session(e sessionEffect) {
	if sw == nil {
		return
	}
	switch e.Op {
	case sessionOpen, sessionRenew:
		for i := range sw.ends {
			for j := range sw.ends[i] {
				if sw.ends[i][j].newHolder == e.Holder {
					sw.ends[i][j].renewed = true
				}
			}
		}
	case sessionEnd:
		if e.Cause == causeReplaced {
			return
		}
		m := sw.member(e.Holder)
		if m == nil || m.holder != e.Holder || m.draining || m.phase != Active || m.Node.State() == kernel.NodeDown {
			return
		}
		sw.owed++
		i := sw.index(m)
		sw.ends[i] = append(sw.ends[i], endOwed{holder: e.Holder, at: sw.w.sim.Now()})
	}
}

// recovered notes a recovery of m: it counts for m's latest session end.
func (sw *sessionWatch) recovered(m *Member) {
	if sw == nil {
		return
	}
	ends := sw.ends[sw.index(m)]
	if len(ends) == 0 {
		return
	}
	last := &ends[len(ends)-1]
	last.recovers++
	if last.recovers == 1 {
		last.newHolder = m.holder
	}
}

// excuse drops m's latest session end when no recovery followed it yet: the node crashed, or
// the member exited, and owes it none.
func (sw *sessionWatch) excuse(m *Member) {
	if sw == nil {
		return
	}
	i := sw.index(m)
	if n := len(sw.ends[i]); n > 0 && sw.ends[i][n-1].recovers == 0 {
		sw.ends[i] = sw.ends[i][:n-1]
	}
}

// exited notes that m exited.
func (sw *sessionWatch) exited(m *Member) { sw.excuse(m) }

// err returns the first session end, by member in node order and then by time, that was not
// recovered from exactly once, or whose recovery's session the store never opened:
// "recovered: m1: session m1/<hex> ended at 41.207000000s, recovered 0 times", or
// "recovered: m1: session m1/<hex> ended at 41.207000000s, recovered as m1/<hex>, which never
// renewed". With no end owed at all it returns "recovered: no session of a member ended".
func (sw *sessionWatch) err() error {
	if sw.owed == 0 {
		return errors.New("recovered: no session of a member ended")
	}
	for i, ends := range sw.ends {
		name := sw.w.members[i].Name
		for _, e := range ends {
			if e.recovers != 1 {
				return fmt.Errorf("recovered: %s: session %s ended at %s, recovered %d times", name, e.holder, e.at, e.recovers)
			}
			if !e.renewed {
				return fmt.Errorf("recovered: %s: session %s ended at %s, recovered as %s, which never renewed",
					name, e.holder, e.at, e.newHolder)
			}
		}
	}
	return nil
}
