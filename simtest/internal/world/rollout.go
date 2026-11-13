// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// bootCauseActivate is the cause of tenure.boot for the first incarnation of a replacement,
// which starts when the replacement is activated.
const bootCauseActivate = "activate"

// The labels of a rollout's events: the global ones at the times of the plan, and the post on
// the replacement's node that activates it. The drain posts recover.go's tenure/drain.
const (
	labelActivate        = "tenure/activate"
	labelRolloutActivate = "tenure/rollout/activate"
	labelRolloutDrain    = "tenure/rollout/drain"
)

// finalRollout is the name of the final check of a rollout (SIM-640).
const finalRollout = invariantPrefix + "rollout"

// A rolling deploy (SIM-150, architecture §8.2, D16): the replacement mi-r of each member mi
// waits on standby from the start; at Start + (i−1)·Gap a global event posts tenure/activate
// on its node, which makes it active and starts its first incarnation, tenure.boot
// cause=activate. Its machine asks for a slot, and gets none while every slot is held. Surge
// later mi is told to drain (World.Drain): it hands its shards over, releases its slot, closes
// its session and exits. The replacement then claims the slot that mi released and takes the
// same shards, so that each shard moves once. R is at least the last drain, Start + (N−1)·Gap +
// Surge (see recoveryStart).

// lastDrain returns the time at which the last member of a scenario with a Rollout is told to
// drain, Start + (Members−1)·Gap + Surge, or the largest time when that does not fit.
func lastDrain(sc Scenario) kernel.Time {
	ro := sc.Rollout
	rest := time.Duration(math.MaxInt64) - ro.Start - ro.Surge // Start and Surge are not negative
	if rest < 0 {
		return kernel.Time(math.MaxInt64)
	}
	if n := time.Duration(sc.Members - 1); n > 0 && ro.Gap > rest/n {
		return kernel.Time(math.MaxInt64)
	}
	return kernel.Time(ro.Start + time.Duration(sc.Members-1)*ro.Gap + ro.Surge)
}

// rolloutWatch keeps what the final tenure/rollout judges, from the steps of the drivers.
type rolloutWatch struct {
	w *World
	// reps holds what each replacement did, by its Index − 1.
	reps []*replacementWatch
	// drainAt holds when each replaced member was told to drain, by its Index − 1; a member
	// not told yet has a negative time.
	drainAt []kernel.Time
	// drainSlot holds the slot that each replaced member's machine held when its drain began,
	// by its Index − 1; −1 when it held none, was down, or was not told yet.
	drainSlot []int
	// late is the first start of a shard on a replaced member after its drain began, "" when
	// there was none.
	late string
}

// replacementWatch is what one replacement did: the first of its incarnations whose machine
// held a slot, the slot, how many times each incarnation started each shard, and the most
// starts of each shard on each incarnation without a stop between them. A machine may start a
// shard before the reply to its claim is stepped, when a view showed it the slot first, so the
// starts of every incarnation are counted from its first step.
type replacementWatch struct {
	m      *Member
	holder string                        // the first incarnation that held a slot, "" before
	slot   int                           // the slot it held
	starts map[string]map[spaceShard]int // by holder; looked up only
	// streak counts the starts of each shard by holder since its run last stopped, and most
	// is the largest streak each reached. A run that stops and starts again is a new streak.
	streak map[string]map[spaceShard]int
	most   map[string]map[spaceShard]int
	spaces map[string]core.SpaceSpec // looked up only
}

// spaceShard names one shard of one space.
type spaceShard struct {
	space string
	shard int
}

// planRollout plans the rollout of a scenario with a Rollout and installs the final
// tenure/rollout, which runs after the finals of installFinals and installRecovered. A scenario
// without a Rollout plans nothing. Each run of the world function has its own state, kept in
// the World.
//
// tenure/rollout (SIM-640) judges each replacement, in order, by the first of its incarnations
// whose machine held a slot: when that incarnation is still the replacement's present one at
// the end, its node up, each shard that its machine assigns to that slot was started on it
// exactly once, and no other shard more than once; when it is not, as after a crash, no shard
// was started on it more than once. No later incarnation of the replacement started a shard
// more than once either. In a run that nothing but the rollout disturbs, that incarnation also
// took the slot its replaced member held when its drain began (D16), still runs at the end, and
// has some shard assigned to its slot. Under faults a shard may leave an incarnation and come
// back, so there "more than once" counts the starts between two stops of the shard's run, and
// an assigned shard was started at least once (see replacementWatch.err). Then no shard was
// started on a replaced member after its drain began. The messages are "rollout: replacement 1: m1-r never held a slot",
// "rollout: replacement 1: book/3 started 2 times on m1-r" (or 0 times),
// "rollout: replacement 1: m1-r took slot 3, not m1's slot 0",
// "rollout: replacement 1: m1-r took slot 0, but m1 held no slot when its drain began",
// "rollout: replacement 1: m1-r/<hex>, which took slot 0, does not run at the end",
// "rollout: replacement 1: no shard is assigned to slot-0 on m1-r", and
// "rollout: m1: book/3 started at 25.010000000s, after its drain began at 25.000000000s".
func (w *World) planRollout() {
	ro := w.sc.Rollout
	if ro == nil {
		return
	}
	n := w.sc.Members
	rw := &rolloutWatch{w: w, reps: make([]*replacementWatch, n), drainAt: make([]kernel.Time, n), drainSlot: make([]int, n)}
	spaces := make(map[string]core.SpaceSpec, len(w.sc.Spaces))
	for _, sp := range w.sc.Spaces {
		spaces[sp.Name] = sp
	}
	for i := range n {
		old, rep := w.members[i], w.members[n+i]
		rw.reps[i] = &replacementWatch{
			m: rep, slot: -1, spaces: spaces,
			starts: map[string]map[spaceShard]int{},
			streak: map[string]map[spaceShard]int{},
			most:   map[string]map[spaceShard]int{},
		}
		rw.drainAt[i], rw.drainSlot[i] = -1, -1
		at := ro.Start + time.Duration(i)*ro.Gap
		w.sim.At(kernel.Time(at), labelRolloutActivate, func() { w.activate(rep) })
		w.sim.At(kernel.Time(at+ro.Surge), labelRolloutDrain, func() {
			rw.drainAt[i] = w.sim.Now()
			if d := w.drivers.current(old); d != nil && !d.stopped && old.Node.State() == kernel.NodeUp {
				rw.drainSlot[i] = d.status().Slot
			}
			w.Drain(old)
		})
	}
	w.rollout = rw

	prev := w.drivers.onStep
	w.drivers.onStep = func(d *driver, in core.Input, acts []core.Action) {
		if prev != nil {
			prev(d, in, acts)
		}
		rw.step(d, acts)
	}
	w.fw.Final(finalRollout, func() error {
		if w.finalsOff {
			return nil
		}
		return rw.err()
	})
}

// activate makes the replacement m active, in a global event at its time. On a node that is up
// it posts tenure/activate, which makes m active and starts its first incarnation, cause
// "activate". A node that is down, which only a fault that picks among every server can make
// it, makes m active at once, and its next boot starts the incarnation, cause "boot".
func (w *World) activate(m *Member) {
	if m.phase != Standby {
		return
	}
	if m.Node.State() == kernel.NodeDown {
		m.phase = Active
		return
	}
	m.Node.Post(labelActivate, func() {
		if m.phase != Standby {
			return
		}
		m.phase = Active
		w.incarnate(m, bootCauseActivate)
	})
}

// step takes one step of the driver d, whose actions are acts: the first slot a replacement's
// machine holds and the runs each of its incarnations starts, and the runs a replaced member
// starts after its drain began.
func (rw *rolloutWatch) step(d *driver, acts []core.Action) {
	i := d.m.Index - 1
	if d.m.Replacement {
		r := rw.reps[i]
		if r.holder == "" {
			if slot := d.status().Slot; slot >= 0 {
				r.holder, r.slot = d.holder, slot
			}
		}
		for _, a := range acts {
			switch a := a.(type) {
			case core.StartRun:
				r.count(d.holder, spaceShard{a.Space, a.Shard})
			case core.StopRun:
				r.stop(d.holder, spaceShard{a.Space, a.Shard})
			}
		}
		return
	}
	if rw.drainAt[i] < 0 || rw.late != "" {
		return
	}
	for _, a := range acts {
		if s, ok := a.(core.StartRun); ok {
			rw.late = fmt.Sprintf("rollout: %s: %s/%d started at %s, after its drain began at %s",
				d.m.Name, s.Space, s.Shard, rw.w.sim.Now(), rw.drainAt[i])
			return
		}
	}
}

// err decides tenure/rollout at the end of the run (see planRollout).
func (rw *rolloutWatch) err() error {
	for i, r := range rw.reps {
		if err := r.err(rw.w, i+1, rw.drainSlot[i]); err != nil {
			return err
		}
	}
	if rw.late != "" {
		return fmt.Errorf("%s", rw.late)
	}
	return nil
}

// err judges the replacement r, the replacement number index, at the end of the run;
// drainSlot is the slot its replaced member held when its drain began, −1 for none.
//
// In a run that nothing but the rollout disturbs, the replacement must also have taken the
// replaced member's slot, its first slot-holding incarnation must still run at the end, and
// some shard must be assigned to that slot: otherwise the count of starts could pass on a
// replacement that took another slot, or on nothing. A fault can make a replacement take
// another member's expired slot, or end its first incarnation, so those three are not judged
// under faults. Every incarnation of the replacement, the first or a later one, starts each
// shard at most once.
//
// Under faults a shard may also leave a replacement and come back to the same incarnation:
// a member that crashes while it drains never releases its slot, the slot expires, and
// Balanced's caps move shards between the slots that are left, then back when a replacement
// claims the slot (ASG-140). So under faults each incarnation starts each shard at most once
// between two stops of its run, and a shard assigned to the slot at the end was started on it
// at least once.
func (r *replacementWatch) err(w *World, index, drainSlot int) error {
	m := r.m
	if r.holder == "" {
		return fmt.Errorf("rollout: replacement %d: %s never held a slot", index, m.Name)
	}
	quiet := !w.sc.disturbed()
	old := w.members[index-1].Name
	if quiet && drainSlot < 0 {
		return fmt.Errorf("rollout: replacement %d: %s took slot %d, but %s held no slot when its drain began", index, m.Name, r.slot, old)
	}
	if quiet && r.slot != drainSlot {
		return fmt.Errorf("rollout: replacement %d: %s took slot %d, not %s's slot %d", index, m.Name, r.slot, old, drainSlot)
	}
	d := w.drivers.current(m)
	alive := m.phase == Active && m.Node.State() == kernel.NodeUp && m.holder == r.holder &&
		d != nil && !d.stopped && d.holder == r.holder
	if quiet && !alive {
		return fmt.Errorf("rollout: replacement %d: %s, which took slot %d, does not run at the end", index, r.holder, r.slot)
	}
	key := "slot-" + strconv.Itoa(r.slot)
	assigned := 0
	for _, name := range sortedSpaceNames(w.sc.Spaces) {
		var assign []string
		if alive {
			assign = d.machine.Assignment(name)
		}
		for shard := range r.spaces[name].Shards {
			got := r.starts[r.holder][spaceShard{name, shard}]
			mine := shard < len(assign) && assign[shard] == key
			if mine {
				assigned++
			}
			if mine && (got == 0 || quiet && got != 1) { // more than once is judged below
				return fmt.Errorf("rollout: replacement %d: %s/%d started %d times on %s", index, name, shard, got, m.Name)
			}
		}
	}
	if quiet && assigned == 0 {
		return fmt.Errorf("rollout: replacement %d: no shard is assigned to %s on %s", index, key, m.Name)
	}
	repeats := r.starts
	if !quiet {
		repeats = r.most
	}
	for _, holder := range slices.Sorted(maps.Keys(repeats)) {
		for _, name := range sortedSpaceNames(w.sc.Spaces) {
			for shard := range r.spaces[name].Shards {
				if got := repeats[holder][spaceShard{name, shard}]; got > 1 {
					return fmt.Errorf("rollout: replacement %d: %s/%d started %d times on %s", index, name, shard, got, m.Name)
				}
			}
		}
	}
	return nil
}

// count counts a start of s by holder, in its starts and in the streak since its run of s last
// stopped.
func (r *replacementWatch) count(holder string, s spaceShard) {
	for _, m := range []map[string]map[spaceShard]int{r.starts, r.streak, r.most} {
		if m[holder] == nil {
			m[holder] = map[spaceShard]int{}
		}
	}
	r.starts[holder][s]++
	r.streak[holder][s]++
	r.most[holder][s] = max(r.most[holder][s], r.streak[holder][s])
}

// stop ends the streak of starts of s by holder: its run of s stopped.
func (r *replacementWatch) stop(holder string, s spaceShard) {
	if r.streak[holder] != nil {
		r.streak[holder][s] = 0
	}
}

// PlantStart counts one more start of a shard on the present incarnation of the replacement m,
// as if its machine had started the shard's run again, and starts nothing. It is for the tests
// that show what the rollout's final check reports; a scenario never calls it. It does nothing
// in a world without a rollout, for a member that is not a replacement, and before m's machine
// held a slot.
func (w *World) PlantStart(m *Member, space string, shard int) {
	if w.rollout == nil || !m.Replacement {
		return
	}
	r := w.rollout.reps[m.Index-1]
	if r.holder == "" || r.holder != m.holder {
		return
	}
	r.count(r.holder, spaceShard{space, shard})
}
