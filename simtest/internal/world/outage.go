// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
)

// The store outage of a scenario (SIM-650). An Outage cuts the store off from every other node
// once: a fault script isolates the store at Outage.At and heals the network at
// Outage.At + Outage.For. The heal raises the recovery time R (recoveryStart), so tenure/g5
// judges the shards B after it. While the store is cut off no renewal is answered: an
// unfenced shard stops at its session deadline (MCH-450), and a fenced one degrades and keeps
// running, since its fence rejects stale writes, until the first sign that it is lost (D15).
//
// The final tenure/outage, after the world's own finals, holds the outage to that. The first
// violation seen during the run is kept and reported; the end of the run adds one rule.
//
// Fenced shards:
//
//   - No StopRun of a fenced shard comes between the isolation and the heal unless its holder
//     was shown that it lost the shard first: a stepped record naming another holder or
//     another term for the shard, or a lost session.
//     `outage: ledger/3: m2/<hex> stopped its run in term 5 at 47.000000000s, during the
//     outage, with reason moved and no sign of loss`
//   - A fenced run never stops at the session deadline: `outage: ledger/3: m2/<hex> stopped its
//     fenced run in term 5 at 50.000000000s with reason expired`.
//   - A run that degraded either restores or is lost after the heal: a Restore makes it owned
//     again, or a StopRun ends it, and at the end of the run it is not still degraded,
//     `outage: ledger/3: m2/<hex> is still degraded in term 5 at the end`. The StopRun of a
//     lost run may say moved rather than lost: when the store ended the session during the
//     outage, the first view after the heal no longer has the member, and the machine stops
//     every run it no longer wants (MCH-500) before the lost renewal comes back.
//
// Unfenced shards:
//
//   - A run whose holder's session deadline passes during the outage stops with reason
//     expired by then: in the first step of its holder at a reading at or after the deadline,
//     which the wake timer brings at once. A run still going after that step, or still going
//     at the heal more than the timer's microsecond past the deadline, is reported as
//     `outage: book/3: m2/<hex> still runs in term 5 at 50.000001000s, past its session
//     deadline 50.000000000s` (readings of the holder's clock), and one stopped there for
//     another reason as `outage: book/3: m2/<hex> stopped its run in term 5 at its session
//     deadline 50.000000000s with reason moved, want expired`.
//   - Every unfenced shard is owned again, by the checker's ownership, at the heal plus the
//     bound: `outage: book/3 has no owner at 75.045000000s (heal 54.000000000s + bound
//     21.045000000s)`.

// The labels of the outage's global events.
const (
	labelOutageHeal  = "tenure/outage/heal"
	labelOutageBound = "tenure/outage/bound"
	finalOutage      = invariantPrefix + "outage"
)

// timerSlack is how late a mono timer may fire after its reading (SIM-130).
const timerSlack = time.Microsecond

// outageRun is what the outage final keeps of one run, from its StartRun.
type outageRun struct {
	holder   string
	space    string
	shard    int
	term     core.Term
	fenced   bool
	stopping bool // its StopRun was stepped
	degraded bool // it is degraded now
}

// outageHolder is what the outage final keeps of one holder.
type outageHolder struct {
	holder   string
	deadline time.Duration // the latest session deadline of StartRun and ExtendDeadline
	runs     []*outageRun  // the runs that have not stopped, in the order they started
	// loss holds the shards, as book/3, whose records the holder was shown naming another
	// holder or another term than its run's; lost reports a lost session. Looked up only.
	loss map[string]bool
	lost bool
}

// outageWatch decides tenure/outage over the steps of every driver.
type outageWatch struct {
	at, heal kernel.Time
	fenced   map[string]bool // by space name; looked up only
	byHolder map[string]*outageHolder
	holders  []*outageHolder // in the order of their first step
	all      []*outageRun    // every run, in the order they started
	err      error           // the first violation
}

// newOutageWatch returns the watch of an outage from at to heal over spaces.
func newOutageWatch(at, heal kernel.Time, spaces []core.SpaceSpec) *outageWatch {
	o := &outageWatch{at: at, heal: heal, fenced: map[string]bool{}, byHolder: map[string]*outageHolder{}}
	for _, sp := range spaces {
		o.fenced[sp.Name] = sp.Fenced
	}
	return o
}

// fail keeps the first violation.
func (o *outageWatch) fail(format string, args ...any) {
	if o.err == nil {
		o.err = fmt.Errorf("outage: "+format, args...)
	}
}

// holder returns the state of holder, made at its first use.
func (o *outageWatch) holder(holder string) *outageHolder {
	h := o.byHolder[holder]
	if h == nil {
		h = &outageHolder{holder: holder, loss: map[string]bool{}}
		o.byHolder[holder] = h
		o.holders = append(o.holders, h)
	}
	return h
}

// during reports whether now is in the outage, from the isolation to the heal.
func (o *outageWatch) during(now kernel.Time) bool { return now >= o.at && now <= o.heal }

// runOf returns the run of h on the shard that has not stopped, nil when there is none.
func (h *outageHolder) runOf(space string, shard int) *outageRun {
	for _, r := range h.runs {
		if r.space == space && r.shard == shard {
			return r
		}
	}
	return nil
}

// step takes one step of holder's machine at the global time now: the input as stepped, with
// its reading, and the actions.
func (o *outageWatch) step(holder string, now kernel.Time, in core.Input, acts []core.Action) {
	h := o.holder(holder)
	switch in := in.(type) {
	case core.HolderChanged:
		if r := h.runOf(in.Space, in.Shard); in.Holder != holder || r != nil && in.Term != r.term {
			h.loss[msgShard(in.Space, in.Shard)] = true
		}
	case core.SessionFailed:
		if in.Cause == core.CauseLost {
			h.lost = true
		}
	}
	reading := inputNow(in)
	// The unfenced runs whose session deadline this step is at or past, during the outage:
	// the step must stop each with reason expired.
	var due []*outageRun
	deadline := h.deadline
	if o.during(now) && deadline > 0 && reading >= deadline {
		for _, r := range h.runs {
			if !r.fenced {
				due = append(due, r)
			}
		}
	}
	for _, a := range acts {
		switch a := a.(type) {
		case core.StartRun:
			r := &outageRun{holder: holder, space: a.Space, shard: a.Shard, term: a.Term, fenced: o.fenced[a.Space]}
			h.runs = append(h.runs, r)
			o.all = append(o.all, r)
			h.deadline = max(h.deadline, a.Deadline)
		case core.ExtendDeadline:
			h.deadline = max(h.deadline, a.Deadline)
		case core.Degrade:
			if r := h.runOf(a.Space, a.Shard); r != nil && r.term == a.Term {
				r.degraded = true
			}
		case core.Restore:
			if r := h.runOf(a.Space, a.Shard); r != nil && r.term == a.Term {
				r.degraded = false
			}
		case core.StopRun:
			o.stopRun(h, now, deadline, due, a)
		}
	}
	for _, r := range due {
		if !r.stopping {
			o.fail("%s: %s still runs in term %d at %s, past its session deadline %s",
				msgShard(r.space, r.shard), holder, r.term, kernel.Time(reading), kernel.Time(deadline))
		}
	}
}

// stopRun takes the StopRun a of h at the global time now; deadline is h's session deadline
// before the step, and due the unfenced runs the step must stop as expired.
func (o *outageWatch) stopRun(h *outageHolder, now kernel.Time, deadline time.Duration, due []*outageRun, a core.StopRun) {
	r := h.runOf(a.Space, a.Shard)
	if r == nil || r.term != a.Term {
		return
	}
	r.stopping = true
	h.runs = slices.DeleteFunc(h.runs, func(x *outageRun) bool { return x == r })
	key := msgShard(a.Space, a.Shard)
	switch {
	case r.fenced && o.during(now) && !h.loss[key] && !h.lost:
		o.fail("%s: %s stopped its run in term %d at %s, during the outage, with reason %s and no sign of loss",
			key, h.holder, r.term, now, a.Reason)
	case r.fenced && a.Reason == core.StopExpired:
		o.fail("%s: %s stopped its fenced run in term %d at %s with reason expired", key, h.holder, r.term, now)
	case slices.Contains(due, r) && a.Reason != core.StopExpired:
		o.fail("%s: %s stopped its run in term %d at its session deadline %s with reason %s, want expired",
			key, h.holder, r.term, kernel.Time(deadline), a.Reason)
	}
	r.degraded = false
}

// atHeal checks, at the heal, that no unfenced run of a holder whose clock reads more than
// the timer's slack past its session deadline still goes. mono returns the reading of each
// holder's clock, and current reports whether a holder is the present incarnation of a member
// that is up: the others' runs stopped with them.
func (o *outageWatch) atHeal(mono func(holder string) time.Duration, current func(holder string) bool) {
	for _, h := range o.holders {
		if h.deadline == 0 || !current(h.holder) {
			continue
		}
		now := mono(h.holder)
		if now < h.deadline+timerSlack {
			continue
		}
		for _, r := range h.runs {
			if !r.fenced {
				o.fail("%s: %s still runs in term %d at %s, past its session deadline %s",
					msgShard(r.space, r.shard), h.holder, r.term, kernel.Time(now), kernel.Time(h.deadline))
				return
			}
		}
	}
}

// atBound checks, at the heal plus the bound b, that every shard of the unfenced spaces has an
// owner, in space name and shard order.
func (o *outageWatch) atBound(now kernel.Time, b time.Duration, spaces []core.SpaceSpec, owners func(space string, shard int) []string) {
	sorted := slices.Clone(spaces)
	slices.SortFunc(sorted, func(a, b core.SpaceSpec) int { return cmp.Compare(a.Name, b.Name) })
	for _, sp := range sorted {
		if sp.Fenced {
			continue
		}
		for shard := range sp.Shards {
			if len(owners(sp.Name, shard)) == 0 {
				o.fail("%s has no owner at %s (heal %s + bound %s)", msgShard(sp.Name, shard), now, o.heal, kernel.Time(b))
				return
			}
		}
	}
}

// final returns the first violation of the run, else the first run, in the order they started,
// that is still degraded at the end.
func (o *outageWatch) final() error {
	if o.err != nil {
		return o.err
	}
	for _, r := range o.all {
		if r.degraded && !r.stopping {
			return fmt.Errorf("outage: %s: %s is still degraded in term %d at the end", msgShard(r.space, r.shard), r.holder, r.term)
		}
	}
	return nil
}

// outageEvents returns the fault script of an outage: the store isolated at at, the network
// healed at heal. A heal ends every isolation and partition there is.
func outageEvents(at, heal kernel.Time) []fault.Event {
	return []fault.Event{
		{At: at, Kind: fault.KindIsolate, Node: storeName},
		{At: heal, Kind: fault.KindHeal},
	}
}

// installOutage plans the scenario's outage, if it has one, and registers tenure/outage, which
// runs after the world's other finals. It must run after installChecks: it watches every step
// after the checker has.
func (w *World) installOutage() {
	oc := w.sc.Outage
	if oc == nil {
		return
	}
	at, heal := kernel.Time(oc.At), kernel.Time(oc.At+oc.For)
	o := newOutageWatch(at, heal, w.sc.Spaces)
	w.outage = o
	w.fw.Plan(fault.Script(outageEvents(at, heal)...))
	prev := w.drivers.onStep
	w.drivers.onStep = func(d *driver, in core.Input, acts []core.Action) {
		if prev != nil {
			prev(d, in, acts)
		}
		o.step(d.holder, w.sim.Now(), in, acts)
	}
	w.sim.At(heal, labelOutageHeal, func() {
		o.atHeal(w.holderMono, w.currentHolder)
	})
	if b := heal.Add(w.bound); b <= w.end {
		w.sim.At(b, labelOutageBound, func() {
			o.atBound(b, w.bound, w.sc.Spaces, w.checker.Owners)
		})
	}
	w.fw.Final(finalOutage, func() error {
		if w.finalsOff {
			return nil
		}
		return o.final()
	})
}

// currentHolder reports whether holder is the holder of the present incarnation of an active
// member whose node is up, with a driver that runs.
func (w *World) currentHolder(holder string) bool {
	for _, m := range w.members {
		if m.holder != holder {
			continue
		}
		d := w.drivers.current(m)
		return m.phase == Active && m.Node.State() == kernel.NodeUp && d != nil && !d.stopped && d.holder == holder
	}
	return false
}

// outageHeal returns the time of the heal of sc's outage, 0 when it has none.
func outageHeal(sc Scenario) kernel.Time {
	if sc.Outage == nil {
		return 0
	}
	return kernel.Time(sc.Outage.At + sc.Outage.For)
}
