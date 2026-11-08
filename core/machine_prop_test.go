// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
	mc "github.com/hmdsefi/tenure/internal/machinecases"
)

// This file holds the property checkers that every walk of the machine runs after every step, and the
// chaos driver of AT-MCH-49 (MCH §10.7), which feeds the machine inputs that break the driver contract.
//
// A walk keeps a propWalk: the machine, the driver's own record of what it was told and what it
// reported, and two replay machines. propWalk.step steps one input under recover and runs every checker:
//
//   - checkOrder: MCH-120's output order (AT-MCH-47).
//   - checkStartStop: one StartRun per term and one StopRun per run (AT-MCH-46), and the rules of sections
//     5 to 8 for StartRun, StopRun, Degrade, Restore, Confirm and Acquire, with Status against the record.
//   - checkNoReleaseWhileRunning: no Release of a shard while its run is going, no CloseSession and no
//     Done while any run is going (AT-MCH-45, MCH-580, MCH-720).
//   - checkSession: the clock, the session's states, draining, closing and loss (sections 3, 7 and 11).
//   - checkSlots: the slot and vacancy rules (sections 12 and 13), against a model of the slots.
//   - checkReplay: two replays stepped in lockstep, one with the spaces reversed, give equal results
//     (AT-MCH-47, MCH-160).
//
// The checkers return the first rule a step breaks and leave reporting to the caller, which knows the
// seed, the step and the input.

// propShard names one shard of a walk's config.
type propShard struct {
	space string
	shard int
}

// propRun names one run: a shard and the term it was started in.
type propRun struct {
	propShard
	term core.Term
}

// propStep is one step of a walk as the checkers see it: the machine, the input, Status before and after
// the step, the result and the wake after it.
type propStep struct {
	m     *core.Machine
	in    core.Input
	prior core.Status
	st    core.Status
	got   []core.Action
	wake  time.Duration
	armed bool
}

// propWalk is one walk of one machine with the state every checker keeps.
type propWalk struct {
	m       *core.Machine
	runs    *runLedger
	session *sessionLedger
	slots   *slotModel
	replay  *replay
}

// newPropWalk returns a walk of a new machine for cfg.
func newPropWalk(cfg core.MachineConfig) *propWalk {
	m := core.NewMachine(cfg)
	runs := newRunLedger(cfg.Holder, core.MachineTiming(m).LeaseDuration)
	runs.timing = core.MachineTiming(m)
	return &propWalk{
		m:       m,
		runs:    runs,
		session: &sessionLedger{timing: core.MachineTiming(m)},
		slots:   newSlotModel(cfg, core.MachineTiming(m)),
		replay:  newReplay(cfg),
	}
}

// step steps in on the walk's machine under recover, then runs every checker on the step. It returns the
// step and the first rule the step broke, or nil. A Step that panics breaks a rule too.
func (w *propWalk) step(in core.Input) (propStep, error) {
	w.runs.report(in)
	s := propStep{m: w.m, in: in, prior: w.m.Status()}
	got, panicked := stepRecovered(w.m, in)
	if panicked != nil {
		return s, fmt.Errorf("Step panicked: %v", panicked)
	}
	s.got, s.st = got, w.m.Status()
	s.wake, s.armed = w.m.NextWake()
	if v := wakeViolations(s.got, s.st.Now, s.st.Done, s.wake, s.armed); len(v) > 0 {
		return s, fmt.Errorf("wake (MCH-810): %s, in %v", strings.Join(v, "; "), s.got)
	}
	if err := checkOrder(s); err != nil {
		return s, err
	}
	if err := checkStartStop(w.runs, s); err != nil {
		return s, err
	}
	if err := checkNoReleaseWhileRunning(w.runs, s); err != nil {
		return s, err
	}
	if err := checkSession(w.session, s); err != nil {
		return s, err
	}
	if err := checkSlots(w.slots, s); err != nil {
		return s, err
	}
	return s, checkReplay(w.replay, s)
}

// stepRecovered steps in on m and returns the result, or the value Step panicked with.
func stepRecovered(m *core.Machine, in core.Input) (got []core.Action, panicked any) {
	defer func() {
		if r := recover(); r != nil {
			got, panicked = nil, r
		}
	}()
	return m.Step(in), nil
}

// runLedger is the driver's record of the runs: what it has been told to start and stop and what it has
// reported. The checkers compare the machine against it, never against the machine's own Status.
type runLedger struct {
	holder string
	lease  time.Duration

	// running holds each shard's run that has not ended. A run ends only by a RunStopped of its term
	// after its StopRun.
	running map[propShard]core.Term

	// started, stopSent and degraded say which runs have had a StartRun, a StopRun, and a Degrade with
	// no Restore since.
	started, stopSent, degraded map[propRun]bool

	// pending says a Confirm of the shard's run has gone out with the session live at the end of every
	// step since, and no Restore has answered it.
	pending map[propShard]bool

	// timing is the machine's Timing, for the checks of when things go out: Stuck, the Confirm and
	// Release repeats and RetryAt. A ledger with no timing skips them.
	timing core.Timing

	// stopAt holds the reading of each run's StopRun, and confirmAt and releaseAt the reading of each
	// shard's last Confirm and Release.
	stopAt               map[propRun]time.Duration
	confirmAt, releaseAt map[propShard]time.Duration
}

// newRunLedger returns an empty record for a machine of the session holder, with LeaseDuration lease.
// It has no timing until its caller sets it.
func newRunLedger(holder string, lease time.Duration) *runLedger {
	return &runLedger{
		holder:    holder,
		lease:     lease,
		running:   map[propShard]core.Term{},
		started:   map[propRun]bool{},
		stopSent:  map[propRun]bool{},
		degraded:  map[propRun]bool{},
		pending:   map[propShard]bool{},
		stopAt:    map[propRun]time.Duration{},
		confirmAt: map[propShard]time.Duration{},
		releaseAt: map[propShard]time.Duration{},
	}
}

// report applies an input before it is stepped: a RunStopped of a run that has had its StopRun ends the
// run. Any other RunStopped is a report the driver had no grounds for, and ends nothing.
func (l *runLedger) report(in core.Input) {
	v, ok := in.(core.RunStopped)
	if !ok {
		return
	}
	k := propShard{v.Space, v.Shard}
	if t, ok := l.running[k]; ok && t == v.Term && l.stopSent[propRun{k, t}] {
		delete(l.running, k)
	}
}

// runs returns the runs that are going, by space and shard.
func (l *runLedger) runs() []propRun {
	list := make([]propRun, 0, len(l.running))
	for k, t := range l.running {
		list = append(list, propRun{k, t})
	}
	slices.SortFunc(list, func(a, b propRun) int {
		if c := strings.Compare(a.space, b.space); c != 0 {
			return c
		}
		return a.shard - b.shard
	})
	return list
}

// shardOf returns the Status of shard k, and whether the machine has it.
func shardOf(st core.Status, k propShard) (core.ShardStatus, bool) {
	for _, sp := range st.Spaces {
		if sp.Name == k.space && k.shard >= 0 && k.shard < len(sp.Shards) {
			return sp.Shards[k.shard], true
		}
	}
	return core.ShardStatus{}, false
}

// spaceFenced reports whether the named space is fenced.
func spaceFenced(st core.Status, space string) bool {
	for _, sp := range st.Spaces {
		if sp.Name == space {
			return sp.Fenced
		}
	}
	return false
}

// toldToStop reports that a shard in state s has a run with its StopRun.
func toldToStop(s core.ShardState) bool {
	return s == core.ShardStateStopping || s == core.ShardStateExpired || s == core.ShardStateLost
}

// checkStartStop checks the run actions of a step against the record, and records them (AT-MCH-46,
// sections 5 to 8). A StartRun goes once per term, for a shard with no run, while the session is live and
// the member does not drain, for a wanted shard and with the session deadline. A StopRun goes once per
// run, for a run that is going, with a reason that fits: expired only in an unfenced space with the
// session late, draining only while draining, moved never then. A Degrade goes only in a fenced space,
// with the session late, for a run going and not degraded. A Confirm goes only while live, for a degraded
// run, and a Restore only answers one: a record naming this holder in the run's term, with the session
// live at every step since the Confirm. An Acquire goes only while live and not draining, for a wanted
// shard with no run, and not before its RetryAt. Afterwards every shard's Status must match the record:
// a run with no StopRun has no stop reason and is not stuck, and one told to stop is stuck from
// RenewDeadline after its StopRun on. Only an idle shard of a member that does not drain waits to
// acquire, for at most LeaseDuration (MCH-430). With the machine's timing in the record, a Confirm and a
// Release repeat no sooner than RetryPeriod after the last, and checkShardAnswer checks what the step's
// input did to each shard.
func checkStartStop(l *runLedger, s propStep) error {
	const rule = "one StartRun per term, one StopRun per run (AT-MCH-46)"
	st := s.st
	live := st.Session == core.SessionStateLive
	timed := l.timing.RetryPeriod > 0
	if !live {
		clear(l.pending)
	}
	for _, a := range s.got {
		switch v := a.(type) {
		case core.StartRun:
			k := propShard{v.Space, v.Shard}
			r := propRun{k, v.Term}
			sh, ok := shardOf(st, k)
			switch {
			case !ok || v.Term == 0:
				return fmt.Errorf("%s: %+v names no shard of the machine or no term", rule, v)
			case l.started[r]:
				return fmt.Errorf("%s: a second %+v", rule, v)
			case l.running[k] != 0:
				return fmt.Errorf("%s: %+v while term %d runs", rule, v, l.running[k])
			case !live || st.Draining || !sh.Wanted || v.Deadline != st.Deadline:
				return fmt.Errorf("%s: %+v with the session %v, deadline %v, draining %v, shard %+v",
					rule, v, st.Session, st.Deadline, st.Draining, sh)
			}
			l.started[r] = true
			l.running[k] = v.Term
		case core.StopRun:
			k := propShard{v.Space, v.Shard}
			r := propRun{k, v.Term}
			if !l.started[r] || l.stopSent[r] || l.running[k] != v.Term {
				return fmt.Errorf("%s: %+v of a run that is not going or has its StopRun, with term %d going",
					rule, v, l.running[k])
			}
			fenced := spaceFenced(st, v.Space)
			switch {
			case v.Reason == core.StopLost:
			case v.Reason == core.StopExpired && !fenced && st.Session == core.SessionStateLate:
			case v.Reason == core.StopDraining && st.Draining:
			case v.Reason == core.StopMoved && !st.Draining:
			default:
				return fmt.Errorf("%s: %+v with the session %v, draining %v, fenced %v", rule, v, st.Session, st.Draining, fenced)
			}
			l.stopSent[r] = true
			l.stopAt[r] = st.Now
			delete(l.pending, k)
		case core.Degrade:
			k := propShard{v.Space, v.Shard}
			r := propRun{k, v.Term}
			if !spaceFenced(st, v.Space) || l.running[k] != v.Term || l.stopSent[r] || l.degraded[r] ||
				st.Session != core.SessionStateLate {
				return fmt.Errorf("%s: %+v with term %d going, the session %v, stopped %v, degraded %v",
					rule, v, l.running[k], st.Session, l.stopSent[r], l.degraded[r])
			}
			l.degraded[r] = true
		case core.Restore:
			k := propShard{v.Space, v.Shard}
			r := propRun{k, v.Term}
			rec, isRecord := s.in.(core.HolderChanged)
			answered := isRecord && rec.Space == v.Space && rec.Shard == v.Shard && rec.Holder == l.holder && rec.Term == v.Term
			if !answered || !live || !l.pending[k] || !l.degraded[r] || l.stopSent[r] || l.running[k] != v.Term {
				return fmt.Errorf("%s: %+v for %#v with the session %v, Confirm pending %v, degraded %v, stopped %v",
					rule, v, s.in, st.Session, l.pending[k], l.degraded[r], l.stopSent[r])
			}
			l.degraded[r] = false
			l.pending[k] = false
		case core.Confirm:
			k := propShard{v.Space, v.Shard}
			r := propRun{k, v.Term}
			if !live || !l.degraded[r] || l.stopSent[r] || l.running[k] != v.Term {
				return fmt.Errorf("%s: %+v with the session %v, term %d going, degraded %v, stopped %v",
					rule, v, st.Session, l.running[k], l.degraded[r], l.stopSent[r])
			}
			if timed && l.pending[k] && st.Now < l.confirmAt[k]+l.timing.RetryPeriod {
				// A Confirm repeats every RetryPeriod while the session stays live (MCH-480).
				return fmt.Errorf("%s: %+v at %v, the last at %v", rule, v, st.Now, l.confirmAt[k])
			}
			l.pending[k] = true
			l.confirmAt[k] = st.Now
		case core.Acquire:
			k := propShard{v.Space, v.Shard}
			sh, ok := shardOf(st, k)
			if !ok || !live || st.Draining || sh.State != core.ShardStateAcquiring || !sh.Wanted || l.running[k] != 0 {
				return fmt.Errorf("%s: %+v with the session %v, draining %v, shard %+v, term %d going",
					rule, v, st.Session, st.Draining, sh, l.running[k])
			}
			// An Acquire waits for RetryAt, unless a record with no holder has cleared it in this step.
			rec, isRecord := s.in.(core.HolderChanged)
			cleared := isRecord && rec.Space == v.Space && rec.Shard == v.Shard && rec.Holder == ""
			if b, ok := shardOf(s.prior, k); ok && b.State == core.ShardStateIdle && b.RetryAt > st.Now && !cleared {
				return fmt.Errorf("%s: %+v at %v, before RetryAt %v", rule, v, st.Now, b.RetryAt)
			}
		case core.Release:
			// A Release repeats every RetryPeriod while the shard releases the same term (D6).
			k := propShard{v.Space, v.Shard}
			b, ok := shardOf(s.prior, k)
			last, sent := l.releaseAt[k]
			if timed && ok && sent && b.State == core.ShardStateReleasing && b.Term == v.Term &&
				st.Now < last+l.timing.RetryPeriod {
				return fmt.Errorf("%s: %+v at %v, the last at %v", rule, v, st.Now, last)
			}
			l.releaseAt[k] = st.Now
		}
	}
	for _, sp := range st.Spaces {
		for shard, sh := range sp.Shards {
			k := propShard{sp.Name, shard}
			r := propRun{k, sh.Term}
			var ok bool
			switch sh.State {
			case core.ShardStateOwned, core.ShardStateDegraded:
				// A run with no StopRun has no stop reason and is not stuck.
				ok = l.started[r] && !l.stopSent[r] && l.running[k] == sh.Term &&
					l.degraded[r] == (sh.State == core.ShardStateDegraded) && sh.Reason == core.StopNone && !sh.Stuck
			case core.ShardStateStopping, core.ShardStateExpired, core.ShardStateLost:
				// A run told to stop is stuck from RenewDeadline after its StopRun on (D7).
				ok = l.stopSent[r] && l.running[k] == sh.Term &&
					(!timed || sh.Stuck == (st.Now >= l.stopAt[r]+l.timing.RenewDeadline))
			default:
				ok = l.running[k] == 0 && !sh.Stuck
			}
			if sh.RetryAt != 0 && (sh.State != core.ShardStateIdle || st.Draining || sh.RetryAt > st.Now+l.lease) {
				// RetryAt is the next Acquire of an idle shard, at most LeaseDuration away (MCH-430).
				ok = false
			}
			if !ok {
				return fmt.Errorf("%s: shard %s/%d is %+v, with term %d going, started %v, stopped %v, degraded %v",
					rule, sp.Name, shard, sh, l.running[k], l.started[r], l.stopSent[r], l.degraded[r])
			}
			if b, ok := shardOf(s.prior, k); ok && timed {
				if err := checkShardAnswer(l, s, k, b, sh); err != nil {
					return fmt.Errorf("%s: %w", rule, err)
				}
			}
		}
	}
	return nil
}

// checkShardAnswer checks what the step's input did to shard k, which was b before the step and is sh
// after it (sections 5 and 6). An answer to an Acquire that grants nothing waits RetryPeriod, and one that
// names another holder waits its RetryAfter, but at least RetryPeriod and at most LeaseDuration (MCH-430).
// A record with no holder clears an idle shard's RetryAt, so that it acquires at once. A releasing shard
// stops releasing only on the Released of its term, a record of another holder or none that is not
// older, or a lost session.
func checkShardAnswer(l *runLedger, s propStep, k propShard, b, sh core.ShardStatus) error {
	st, t := s.st, l.timing
	lost := st.Session == core.SessionStateLost
	var term core.Term
	var holder string
	named, record, released := false, false, false
	switch v := s.in.(type) {
	case core.Acquired:
		named, term = v.Space == k.space && v.Shard == k.shard, v.Term
		if named && b.State == core.ShardStateAcquiring && v.Term == 0 && !lost && !st.Draining &&
			sh.RetryAt != st.Now+t.RetryPeriod {
			return fmt.Errorf("%#v left shard %s/%d %+v, not waiting RetryPeriod", v, k.space, k.shard, sh)
		}
	case core.AcquireFailed:
		wait := min(max(v.RetryAfter, t.RetryPeriod), t.LeaseDuration)
		if v.Space == k.space && v.Shard == k.shard && b.State == core.ShardStateAcquiring && !lost && !st.Draining &&
			sh.RetryAt != st.Now+wait {
			return fmt.Errorf("%#v left shard %s/%d %+v, not waiting %v", v, k.space, k.shard, sh, wait)
		}
	case core.Released:
		named, term, released = v.Space == k.space && v.Shard == k.shard, v.Term, true
	case core.HolderChanged:
		named, term, holder, record = v.Space == k.space && v.Shard == k.shard, v.Term, v.Holder, true
		if named && b.State == core.ShardStateIdle && v.Holder == "" && sh.RetryAt != 0 {
			return fmt.Errorf("%#v left shard %s/%d %+v, still waiting", v, k.space, k.shard, sh)
		}
	}
	if b.State != core.ShardStateReleasing || sh.State == core.ShardStateReleasing || lost {
		return nil
	}
	switch {
	case named && released && term == b.Term:
	case named && record && holder != l.holder && (term == 0 || term >= b.Term):
	default:
		return fmt.Errorf("%#v ended the release of shard %s/%d %+v, now %+v", s.in, k.space, k.shard, b, sh)
	}
	return nil
}

// checkNoReleaseWhileRunning checks the safety promise (AT-MCH-45, MCH-580): no Release of a shard goes
// out while the shard has a run that has not reported its end after its StopRun, whatever term the
// Release names. Closing the session makes every record takeable, so no CloseSession goes out while any
// run is going, a stuck one included (MCH-720), and a machine that is done has no run going either. Run
// it after checkStartStop, so that the record holds the step's own StartRuns.
func checkNoReleaseWhileRunning(l *runLedger, s propStep) error {
	const rule = "no release while running (AT-MCH-45)"
	for _, a := range s.got {
		switch v := a.(type) {
		case core.Release:
			if t := l.running[propShard{v.Space, v.Shard}]; t != 0 {
				return fmt.Errorf("%s: %+v while term %d runs, before its RunStopped", rule, v, t)
			}
		case core.CloseSession:
			if len(l.running) != 0 {
				return fmt.Errorf("%s: CloseSession while %+v run", rule, l.runs())
			}
		}
	}
	if s.st.Done && len(l.running) != 0 {
		return fmt.Errorf("%s: done while %+v run", rule, l.runs())
	}
	return nil
}

// actionGroup returns the output group of a (MCH-120), with the Wake in group 7, and whether a is one of
// the machine's actions.
func actionGroup(a core.Action) (int, bool) {
	switch a.(type) {
	case core.ExtendDeadline, core.KeepAlive:
		return 0, true
	case core.StopRun, core.Degrade, core.Restore:
		return 1, true
	case core.StartRun:
		return 2, true
	case core.Release, core.Confirm, core.Acquire:
		return 3, true
	case core.ReleaseSlot, core.ClaimSlot:
		return 4, true
	case core.CloseSession:
		return 5, true
	case core.Wake:
		return 7, true
	}
	return 0, false
}

// actionShard returns the shard a names, and whether it names one.
func actionShard(a core.Action) (propShard, bool) {
	switch v := a.(type) {
	case core.StopRun:
		return propShard{v.Space, v.Shard}, true
	case core.Degrade:
		return propShard{v.Space, v.Shard}, true
	case core.Restore:
		return propShard{v.Space, v.Shard}, true
	case core.StartRun:
		return propShard{v.Space, v.Shard}, true
	case core.Release:
		return propShard{v.Space, v.Shard}, true
	case core.Confirm:
		return propShard{v.Space, v.Shard}, true
	case core.Acquire:
		return propShard{v.Space, v.Shard}, true
	}
	return propShard{}, false
}

// checkOrder checks MCH-120's output order (AT-MCH-47): a nil result rather than an empty one, only the
// machine's actions, the groups in order, and inside a group the space name order, then shard order.
// ExtendDeadline comes before KeepAlive, and each at most once. In the slot group the ReleaseSlot of the
// held slot comes first, then those of the slots given back, by slot, then at most one ClaimSlot. A step
// has at most one CloseSession, and its Wake is last.
func checkOrder(s propStep) error {
	const rule = "MCH-120 order (AT-MCH-47)"
	if s.got != nil && len(s.got) == 0 {
		return fmt.Errorf("%s: an empty result that is not nil", rule)
	}
	group := -1
	var last propShard
	var named, extends, keepAlives, claims, closes, giveBacks int
	lastGiveBack := -1
	for i, a := range s.got {
		g, ok := actionGroup(a)
		if !ok {
			return fmt.Errorf("%s: %#v is not an action of the machine, in %v", rule, a, s.got)
		}
		if g < group {
			return fmt.Errorf("%s: %#v after an action of a later group, in %v", rule, a, s.got)
		}
		if g > group {
			group, named = g, 0
		}
		if k, ok := actionShard(a); ok {
			if named > 0 && (k.space < last.space || k.space == last.space && k.shard < last.shard) {
				return fmt.Errorf("%s: %#v after an action of shard %s/%d, in %v", rule, a, last.space, last.shard, s.got)
			}
			last = k
			named++
		}
		switch v := a.(type) {
		case core.ExtendDeadline:
			if extends > 0 || keepAlives > 0 {
				return fmt.Errorf("%s: ExtendDeadline after another or after KeepAlive, in %v", rule, s.got)
			}
			extends++
		case core.KeepAlive:
			if keepAlives > 0 {
				return fmt.Errorf("%s: two KeepAlives, in %v", rule, s.got)
			}
			keepAlives++
		case core.ReleaseSlot:
			switch {
			case claims > 0:
				return fmt.Errorf("%s: %+v after the ClaimSlot, in %v", rule, v, s.got)
			case v.Slot == s.st.Slot && v.Slot >= 0:
				if giveBacks > 0 {
					return fmt.Errorf("%s: the held slot's %+v after a give-back, in %v", rule, v, s.got)
				}
			case v.Slot <= lastGiveBack || v.Slot < 0:
				return fmt.Errorf("%s: %+v not in slot order, in %v", rule, v, s.got)
			default:
				lastGiveBack = v.Slot
				giveBacks++
			}
		case core.ClaimSlot:
			if claims > 0 {
				return fmt.Errorf("%s: two ClaimSlots, in %v", rule, s.got)
			}
			claims++
		case core.CloseSession:
			if closes > 0 {
				return fmt.Errorf("%s: two CloseSessions, in %v", rule, s.got)
			}
			closes++
		case core.Wake:
			if i != len(s.got)-1 {
				return fmt.Errorf("%s: a Wake that is not last, in %v", rule, s.got)
			}
		}
	}
	return nil
}

// replay holds two machines that replay a walk's inputs: one from the walk's config and one with its
// spaces in reverse order.
type replay struct {
	names          []string
	same, reversed *core.Machine
}

// newReplay returns the replays of a walk of a machine for cfg.
func newReplay(cfg core.MachineConfig) *replay {
	names := make([]string, len(cfg.Spaces))
	for i, s := range cfg.Spaces {
		names[i] = s.Name
	}
	rev := cfg
	rev.Spaces = slices.Clone(cfg.Spaces)
	slices.Reverse(rev.Spaces)
	return &replay{names: names, same: core.NewMachine(cfg), reversed: core.NewMachine(rev)}
}

// checkReplay steps both replays with the step's input, in lockstep with the walk's machine, and checks
// that each gives what the walk's machine gave (AT-MCH-47, MCH-160): the result, Status, NextWake and
// every space's assignment. The first replay shows that the machine depends on nothing but its config
// and inputs, the second that the order of the spaces in the config does not matter. A replay that
// panics differs too.
func checkReplay(r *replay, s propStep) error {
	const rule = "equal replays with Spaces reversed (AT-MCH-47)"
	for _, rm := range [...]struct {
		name string
		m    *core.Machine
	}{{"the replay", r.same}, {"the replay with the spaces reversed", r.reversed}} {
		got, panicked := stepRecovered(rm.m, s.in)
		if panicked != nil {
			return fmt.Errorf("%s: %s panicked: %v", rule, rm.name, panicked)
		}
		if !sameResult(got, s.got) {
			return fmt.Errorf("%s: %s returned %v, not %v", rule, rm.name, got, s.got)
		}
		if st := rm.m.Status(); !sameStatus(st, s.st) {
			return fmt.Errorf("%s: %s has Status %+v, not %+v", rule, rm.name, st, s.st)
		}
		if at, armed := rm.m.NextWake(); at != s.wake || armed != s.armed {
			return fmt.Errorf("%s: %s has NextWake (%v, %v), not (%v, %v)", rule, rm.name, at, armed, s.wake, s.armed)
		}
		for _, name := range r.names {
			if got, want := rm.m.Assignment(name), s.m.Assignment(name); !slices.Equal(got, want) {
				return fmt.Errorf("%s: %s has Assignment(%q) %q, not %q", rule, rm.name, name, got, want)
			}
		}
	}
	return nil
}

// sameResult reports whether two results are equal, nil and empty apart, as reflect.DeepEqual says but
// faster: every action is a comparable struct.
func sameResult(a, b []core.Action) bool {
	return (a == nil) == (b == nil) && slices.Equal(a, b)
}

// sameStatus reports whether two Status values are equal, as reflect.DeepEqual says but faster.
func sameStatus(a, b core.Status) bool {
	x, y := a, b
	x.Spaces, y.Spaces = nil, nil
	return reflect.DeepEqual(x, y) && slices.EqualFunc(a.Spaces, b.Spaces, func(p, q core.SpaceStatus) bool {
		return p.Name == q.Name && p.Fenced == q.Fenced && slices.Equal(p.Shards, q.Shards)
	})
}

// sessionLedger is what checkSession keeps across steps: the readings of the last KeepAlive and the last
// CloseSession.
type sessionLedger struct {
	timing                   core.Timing
	keepAliveAt, closeAt     time.Duration
	keepAliveSent, closeSent bool
}

// sessionEnded reports that the session in st can no longer renew.
func sessionEnded(st core.Status) bool {
	switch st.Session {
	case core.SessionStateLost, core.SessionStateClosing, core.SessionStateClosed:
		return true
	}
	return false
}

// checkSession checks the clock and the session (sections 3, 7 and 11). A closed machine returns nil and
// its Status, clock included, never changes (MCH-140). Otherwise the clock moves to the input's reading,
// never back and never past maxReading. A session goes lost and stays lost, closing goes only to closed,
// and closed comes only from closing. Live means the deadline is ahead, late that it is not, and the
// deadline never moves earlier: only an accepted renewal moves it, to its SentAt plus RenewDeadline. Done
// means closed, or lost with every shard idle, and a done machine returns nil. KeepAlive goes every
// RetryPeriod at most, never once the session has ended or in the step that starts closing.
//
// Draining is for good, starts only with a Drain and never after a loss, and nothing is wanted, started or
// acquired then. A closing or closed session has every shard idle, drains and holds no slot. From the step
// that starts closing a step returns only CloseSession and Wake, so the renewal step that starts it returns
// no ExtendDeadline, and while closing a step changes nothing but the clock and the session.
// CloseSession goes only from such a state, and again RetryPeriod after the last at the soonest. After a loss the machine holds no slot and no shard is owned,
// degraded or releasing or waits to acquire, and it returns only the StopRuns of the step that accepted the
// loss, then Wakes for the runs still to mark stuck.
func checkSession(l *sessionLedger, s propStep) error {
	const rule = "session rules (MCH-140, MCH-700 to MCH-730, MCH-860)"
	const (
		lost    = core.SessionStateLost
		closing = core.SessionStateClosing
		closed  = core.SessionStateClosed
	)
	prior, st := s.prior, s.st
	if prior.Session == closed {
		if s.got != nil || s.armed || !sameStatus(st, prior) {
			return fmt.Errorf("%s: a closed machine returned %v and moved to %+v", rule, s.got, st)
		}
		return nil
	}
	if want := min(max(prior.Now, inputNowOf(s.in)), maxReading); st.Now != want {
		return fmt.Errorf("%s: the clock is %v after %v, not %v", rule, st.Now, prior.Now, want)
	}
	if prior.Done && s.got != nil {
		return fmt.Errorf("%s: a done machine returned %v", rule, s.got)
	}
	idle := true
	for _, sp := range st.Spaces {
		for _, sh := range sp.Shards {
			idle = idle && sh.State == core.ShardStateIdle
		}
	}
	wasLost, isLost := prior.Session == lost, st.Session == lost
	wasClosing, isClosing, isClosed := prior.Session == closing, st.Session == closing, st.Session == closed
	switch {
	case st.Done != (isLost && idle || isClosed):
		return fmt.Errorf("%s: Done %v with the session %v and every shard idle %v", rule, st.Done, st.Session, idle)
	case wasLost && !isLost, wasClosing && !isClosing && !isClosed, isClosed && !wasClosing:
		return fmt.Errorf("%s: the session went from %v to %v", rule, prior.Session, st.Session)
	case st.Session == core.SessionStateLive && st.Now >= st.Deadline,
		st.Session == core.SessionStateLate && st.Now < st.Deadline && st.Now < maxReading:
		return fmt.Errorf("%s: the session is %v at %v with the deadline %v", rule, st.Session, st.Now, st.Deadline)
	case st.Deadline < prior.Deadline:
		return fmt.Errorf("%s: the deadline moved from %v to %v", rule, prior.Deadline, st.Deadline)
	case prior.Draining && !st.Draining:
		return fmt.Errorf("%s: draining ended", rule)
	case (isClosing || isClosed) && (!idle || !st.Draining || st.Slot != -1):
		return fmt.Errorf("%s: the session is %v with Status %+v", rule, st.Session, st)
	case isLost && st.Slot != -1:
		return fmt.Errorf("%s: slot %d after the loss", rule, st.Slot)
	}
	if wasClosing {
		// Only a Tick or a SessionClosed takes effect while closing, and neither changes anything but the
		// clock and the session.
		x := st
		x.Now, x.Session, x.Done = prior.Now, prior.Session, prior.Done
		if !sameStatus(x, prior) {
			return fmt.Errorf("%s: %#v while closing moved Status from %+v to %+v", rule, s.in, prior, st)
		}
	}
	if isLost && s.armed {
		// After a loss only the stuck timer of a run told to stop is left.
		waiting := false
		for _, sp := range st.Spaces {
			for _, sh := range sp.Shards {
				waiting = waiting || toldToStop(sh.State) && !sh.Stuck
			}
		}
		if !waiting {
			return fmt.Errorf("%s: a timer at %v after the loss with no run to mark stuck", rule, s.wake)
		}
	}
	if !prior.Draining && st.Draining {
		if _, ok := s.in.(core.Drain); !ok || wasLost {
			return fmt.Errorf("%s: draining started on %#v with the session %v", rule, s.in, prior.Session)
		}
	}
	for _, sp := range st.Spaces {
		for shard, sh := range sp.Shards {
			after := sh.RetryAt != 0 || sh.State == core.ShardStateOwned || sh.State == core.ShardStateDegraded ||
				sh.State == core.ShardStateReleasing
			if st.Draining && sh.Wanted || isLost && after {
				return fmt.Errorf("%s: shard %s/%d is %+v with the session %v, draining %v", rule, sp.Name, shard, sh, st.Session, st.Draining)
			}
		}
	}
	for _, a := range s.got {
		switch v := a.(type) {
		case core.ExtendDeadline:
			r, ok := s.in.(core.SessionRenewed)
			if !ok || v.Deadline != st.Deadline || v.Deadline <= prior.Deadline || r.SentAt > st.Now ||
				r.SentAt+l.timing.RenewDeadline != v.Deadline {
				return fmt.Errorf("%s: %+v for %#v, with the deadline %v before", rule, v, s.in, prior.Deadline)
			}
		case core.KeepAlive:
			if sessionEnded(st) || l.keepAliveSent && st.Now < l.keepAliveAt+l.timing.RetryPeriod {
				return fmt.Errorf("%s: KeepAlive at %v with the session %v, the last at %v", rule, st.Now, st.Session, l.keepAliveAt)
			}
			l.keepAliveAt, l.keepAliveSent = st.Now, true
		case core.CloseSession:
			if !isClosing || l.closeSent && st.Now < l.closeAt+l.timing.RetryPeriod {
				return fmt.Errorf("%s: CloseSession at %v with the session %v, the last at %v", rule, st.Now, st.Session, l.closeAt)
			}
			l.closeAt, l.closeSent = st.Now, true
		case core.StartRun, core.Acquire:
			if st.Draining {
				return fmt.Errorf("%s: %+v while draining", rule, v)
			}
		}
		switch v := a.(type) {
		case core.Wake:
		case core.CloseSession:
			if isLost {
				return fmt.Errorf("%s: %+v after the loss", rule, v)
			}
		case core.StopRun:
			if wasClosing || isClosing || isLost && (wasLost || v.Reason != core.StopLost) {
				return fmt.Errorf("%s: %+v with the session %v before and %v after", rule, v, prior.Session, st.Session)
			}
		default:
			if wasClosing || isClosing || isLost {
				return fmt.Errorf("%s: %#v with the session %v before and %v after", rule, a, prior.Session, st.Session)
			}
		}
	}
	return nil
}

// slotModel is a walk's model of the slots (sections 12 and 13): the applied view, the vacancy streaks and
// the timer of a held slot the views show vacant, what has been asked of the held slot and which slots are
// being given back. checkSlots updates it in the machine's order from each step's input and result, and
// counts in counts how often the walk reached each path.
type slotModel struct {
	holder, member string
	spaces         []core.SpaceSpec
	vac            vacancyModel

	// applied is the view the machine last applied, at the reading appliedAt with the wall time
	// appliedWall, and viewApplied says one has been.
	applied     core.View
	appliedAt   time.Duration
	appliedWall time.Time
	viewApplied bool

	// releaseSent says a ReleaseSlot of the held slot has gone out since the machine took it, and retired
	// that a view has retired it since.
	releaseSent, retired bool

	// givingBack holds the other slots a ReleaseSlot has gone out for that no SlotReleased has answered.
	givingBack map[int]bool

	// retry is RetryPeriod. claimAt, releaseAt and givingBackAt hold the readings of the last ClaimSlot, of
	// the last ReleaseSlot of the held slot and of each slot given back, and claimSent says a ClaimSlot has
	// gone out since the member last held a slot.
	retry              time.Duration
	claimAt, releaseAt time.Duration
	claimSent          bool
	givingBackAt       map[int]time.Duration

	counts slotCounts
}

// slotCounts counts how often a walk reached each path of the slot rules.
type slotCounts struct {
	claimsSent, claimsTaken, adoptions, giveBacks, heldReleases, releasesOverStuck, slotReleases int
	slotLosses, slotLostRuns, retirements, slotStarts, drainedSlots                              int
	marksSet, marksEndedByView, marksEndedByClaim, streaksOnLeaving, markMoves, markAcquires     int
	vacantLosses, vacantLostRuns, vacantLossesAfterWait, vacantStops                             int
}

// add adds the counts of o to c.
func (c *slotCounts) add(o slotCounts) {
	c.claimsSent += o.claimsSent
	c.claimsTaken += o.claimsTaken
	c.adoptions += o.adoptions
	c.giveBacks += o.giveBacks
	c.heldReleases += o.heldReleases
	c.releasesOverStuck += o.releasesOverStuck
	c.slotReleases += o.slotReleases
	c.slotLosses += o.slotLosses
	c.slotLostRuns += o.slotLostRuns
	c.retirements += o.retirements
	c.slotStarts += o.slotStarts
	c.drainedSlots += o.drainedSlots
	c.marksSet += o.marksSet
	c.marksEndedByView += o.marksEndedByView
	c.marksEndedByClaim += o.marksEndedByClaim
	c.streaksOnLeaving += o.streaksOnLeaving
	c.markMoves += o.markMoves
	c.markAcquires += o.markAcquires
	c.vacantLosses += o.vacantLosses
	c.vacantLostRuns += o.vacantLostRuns
	c.vacantLossesAfterWait += o.vacantLossesAfterWait
	c.vacantStops += o.vacantStops
}

// newSlotModel returns the slot model of a new machine for cfg, which keeps timing.
func newSlotModel(cfg core.MachineConfig, timing core.Timing) *slotModel {
	return &slotModel{
		holder:       cfg.Holder,
		member:       cfg.Member,
		spaces:       slices.Clone(cfg.Spaces),
		vac:          vacancyModel{holder: cfg.Holder, grace: timing.SlotGrace, lease: timing.LeaseDuration},
		givingBack:   map[int]bool{},
		retry:        timing.RetryPeriod,
		givingBackAt: map[int]time.Duration{},
	}
}

// viewApplies reports whether the machine applies the view of in, a ViewChanged, after Status prior: the
// session is not closing or closed, the view is valid and not older than the applied one.
func viewApplies(prior core.Status, in core.ViewChanged) bool {
	return prior.Session != core.SessionStateClosing && prior.Session != core.SessionStateClosed &&
		in.View.Version >= prior.ViewVersion && in.View.Validate() == nil
}

// checkSlots checks the slot and vacancy rules (sections 12 and 13). The machine holds at most one slot,
// and takes it only from a claim answer, for a slot the applied view counts, or as the lowest slot a view
// lists for its holder that it is not giving back, never while draining. It gives the held slot up only on
// the answer to a ReleaseSlot of it, a view naming another holder, views that have shown it vacant for
// LeaseDuration while the session is live, or a lost session, and a slot lost to another holder or to
// vacancy stops every owned or degraded run as lost. Every assignment is the one the view gives with the
// model's expiry marks, and the machine wakes by every vacancy timer of the model.
//
// A StartRun goes only for a shard of the held slot, one the view counts and the machine has not retired,
// or without slots for a member holding none. A ReleaseSlot of the held slot goes only when the member
// drains or the slot is retired, and only while every shard is idle or has a stuck run (MCH-720). A
// ClaimSlot goes only from a live standby that is not draining, under a view with slots. No slot action
// goes out after a loss, while closing, or in the step that starts closing, and CloseSession needs the
// held slot -1: given up on the answer to an earlier ReleaseSlot, or lost in that step. A ClaimSlot, the
// held slot's ReleaseSlot and each give-back repeat no sooner than RetryPeriod after the last, but a
// member that has given up its slot may claim at once.
func checkSlots(sm *slotModel, s propStep) error {
	const rule = "slot rules (MCH-600 to MCH-680, MCH-310)"
	const lost, closing = core.SessionStateLost, core.SessionStateClosing
	in, prior, st, got := s.in, s.prior, s.st, s.got
	view, isView := in.(core.ViewChanged)
	isView = isView && viewApplies(prior, view)
	if isView {
		sm.applied, sm.appliedAt, sm.appliedWall, sm.viewApplied = view.View, st.Now, view.Wall.Round(0), true
	}
	h, was := st.Slot, prior.Slot
	vac := &sm.vac
	c := &sm.counts

	// The vacancy model, in the machine's order: the view's streaks before its slot rules, then the held
	// slot's changes in phase A, the timer of a held slot shown vacant, and phase B's marks and loss. After
	// the session has ended nothing changes. hA is the held slot after phase A: the slot a loss in phase B
	// gives up, if there is one. liveA is the step's one evaluation of live, in phase A.
	endedAtB := prior.Session == lost || prior.Session == closing || prior.Session == core.SessionStateClosed || st.Session == lost
	liveA := !endedAtB && st.Now < st.Deadline && st.Now < maxReading
	vacantLoss := false
	if !endedAtB {
		shownBefore := vac.shown
		if isView {
			c.marksEndedByView += vac.applyView(sm.applied, was, st.Now)
		}
		hA := h
		r, isRelease := in.(core.SlotReleased)
		claim, isClaim := in.(core.SlotClaimed)
		otherHolder := isView && was >= 0 && was < sm.applied.Slots && sm.applied.SlotHolders[was] != "" &&
			sm.applied.SlotHolders[was] != sm.holder
		releasedHeld := isRelease && r.Slot == was && sm.releaseSent
		countedClaim := !sm.viewApplied || claim.Slot < sm.applied.Slots
		switch {
		case h != -1:
		case was >= 0 && !releasedHeld && !otherHolder:
			hA = was
		case was == -1 && isClaim && claim.Slot >= 0 && !prior.Draining && !sm.givingBack[claim.Slot] && countedClaim:
			hA = claim.Slot
		}
		if hA != was && was >= 0 && vac.start(was, st.Now, sm.applied) {
			c.streaksOnLeaving++
		}
		if hA != was && hA >= 0 {
			vac.heldAt = st.Now
			if vac.end(hA) {
				c.marksEndedByClaim++
			}
		}
		vac.display(hA, sm.applied, sm.appliedAt)
		if shownBefore && !vac.shown && isView && hA >= 0 && hA < sm.applied.Slots && sm.applied.SlotHolders[hA] == sm.holder {
			c.vacantStops++
		}
		set := vac.mark(st.Now)
		c.marksSet += set
		if vac.shown && liveA && st.Now >= vac.since+vac.lease {
			vacantLoss = true
			if prior.Session != core.SessionStateLive && prior.Now >= vac.since+vac.lease {
				c.vacantLossesAfterWait++
			}
			vac.shown = false
			if vac.start(hA, st.Now, sm.applied) {
				c.streaksOnLeaving++
			}
		}
		if (h == -1 && hA >= 0) != vacantLoss {
			return fmt.Errorf("%s: slot %d after phase A, %d after the step, model loss %v", rule, hA, h, vacantLoss)
		}
		if set > 0 {
			for _, a := range got {
				switch v := a.(type) {
				case core.StopRun:
					if v.Reason == core.StopMoved {
						c.markMoves++
					}
				case core.Acquire:
					c.markAcquires++
				}
			}
		}
	}
	at := sm.appliedWall.Add(st.Now - sm.appliedAt)
	for _, sp := range sm.spaces {
		if got, want := s.m.Assignment(sp.Name), vac.assignment(sp.Name, sp.Shards, sm.applied, sm.viewApplied, at); !slices.Equal(got, want) {
			return fmt.Errorf("%s: Assignment(%q) = %q, want %q with marks %+v", rule, sp.Name, got, want, vac.streaks)
		}
	}
	if !sessionEnded(st) {
		for _, due := range vac.timers(st.Session == core.SessionStateLive) {
			if !s.armed || s.wake > due {
				return fmt.Errorf("%s: NextWake = (%v, %v), want a wake by %v", rule, s.wake, s.armed, due)
			}
		}
	}

	if r, ok := in.(core.SlotReleased); ok && prior.Session != closing && prior.Session != core.SessionStateClosed {
		delete(sm.givingBack, r.Slot)
		delete(sm.givingBackAt, r.Slot)
	}
	sentBefore := sm.releaseSent
	lostToOther := isView && was >= 0 && was < sm.applied.Slots && sm.applied.SlotHolders[was] != "" &&
		sm.applied.SlotHolders[was] != sm.holder
	if was >= 0 || h >= 0 {
		// A member that gives up its slot may claim again at once.
		sm.claimSent = false
	}
	closes, slotActions := false, false
	for _, a := range got {
		switch a.(type) {
		case core.CloseSession:
			closes = true
		case core.ClaimSlot, core.ReleaseSlot:
			slotActions = true
		}
	}
	if slotActions && (st.Session == lost || prior.Session == closing || closes) {
		return fmt.Errorf("%s: %v with the session %v", rule, got, prior.Session)
	}
	if h != was {
		if err := sm.checkSlotChange(s, isView, vacantLoss); err != nil {
			return fmt.Errorf("%s: %w", rule, err)
		}
		sm.releaseSent, sm.retired = false, false
	}
	if h >= 0 && isView && sm.applied.Slots <= h {
		if !sm.retired {
			c.retirements++
		}
		sm.retired = true
	}
	for _, a := range got {
		switch v := a.(type) {
		case core.ReleaseSlot:
			if v.Slot != h {
				// A give-back repeats every RetryPeriod until its answer (MCH-680).
				if at, ok := sm.givingBackAt[v.Slot]; ok && sm.givingBack[v.Slot] && st.Now < at+sm.retry {
					return fmt.Errorf("%s: %+v at %v, the last at %v", rule, v, st.Now, at)
				}
				c.giveBacks++
				sm.givingBack[v.Slot] = true
				sm.givingBackAt[v.Slot] = st.Now
				continue
			}
			if sentBefore && st.Now < sm.releaseAt+sm.retry {
				// The held slot's ReleaseSlot repeats every RetryPeriod until its answer (MCH-660).
				return fmt.Errorf("%s: %+v at %v, the last at %v", rule, v, st.Now, sm.releaseAt)
			}
			if was != h || !st.Draining && !sm.retired {
				return fmt.Errorf("%s: %+v of a slot taken %d, draining %v, retired %v", rule, v, was, st.Draining, sm.retired)
			}
			over := false
			for _, sp := range st.Spaces {
				for shard, sh := range sp.Shards {
					if sh.State != core.ShardStateIdle && (!toldToStop(sh.State) || !sh.Stuck) {
						return fmt.Errorf("%s: %+v with shard %s/%d %+v", rule, v, sp.Name, shard, sh)
					}
					over = over || sh.Stuck
				}
			}
			c.heldReleases++
			if over {
				c.releasesOverStuck++
			}
			sm.releaseSent, sm.releaseAt = true, st.Now
		case core.ClaimSlot:
			if h != -1 || sm.applied.Slots == 0 || st.Session != core.SessionStateLive || st.Draining {
				return fmt.Errorf("%s: %+v with Status %+v", rule, v, st)
			}
			if sm.claimSent && st.Now < sm.claimAt+sm.retry {
				// A standby claims every RetryPeriod (MCH-610).
				return fmt.Errorf("%s: %+v at %v, the last at %v", rule, v, st.Now, sm.claimAt)
			}
			sm.claimSent, sm.claimAt = true, st.Now
			c.claimsSent++
		case core.StartRun:
			var key string
			if a := s.m.Assignment(v.Space); v.Shard >= 0 && v.Shard < len(a) {
				key = a[v.Shard]
			}
			switch {
			case h >= 0 && (key != "slot-"+strconv.Itoa(h) || sm.applied.Slots <= h || sm.retired || sm.releaseSent):
				return fmt.Errorf("%s: %+v with slot %d, key %q", rule, v, h, key)
			case h < 0 && (sm.applied.Slots != 0 || key != sm.member):
				return fmt.Errorf("%s: %+v with no slot, key %q", rule, v, key)
			case h >= 0:
				c.slotStarts++
			}
		case core.CloseSession:
			// The slot held at the start of the step was released earlier or lost in this step: closing frees
			// the record of a lost one.
			if h != -1 || was != -1 && !sentBefore && !vacantLoss && !lostToOther {
				return fmt.Errorf("%s: %+v while holding slot %d", rule, v, was)
			}
		}
	}
	if closes || st.Session == lost {
		clear(sm.givingBack)
		clear(sm.givingBackAt)
	}
	return nil
}

// checkSlotChange checks a step that changed the held slot: how the machine gave up the slot it held and
// how it took the one it holds. isView says the step applied a view, and vacantLoss that the model lost
// the held slot to views showing it vacant.
func (sm *slotModel) checkSlotChange(s propStep, isView, vacantLoss bool) error {
	in, prior, st, got := s.in, s.prior, s.st, s.got
	h, was := st.Slot, prior.Slot
	c := &sm.counts
	stoppedAsLost := func() (int, error) {
		n := 0
		for _, sp := range prior.Spaces {
			for shard, b := range sp.Shards {
				if b.State != core.ShardStateOwned && b.State != core.ShardStateDegraded {
					continue
				}
				if !slices.Contains(got, core.Action(core.StopRun{Space: sp.Name, Shard: shard, Term: b.Term, Reason: core.StopLost})) {
					return 0, fmt.Errorf("slot %d lost with shard %s/%d %+v, returning %v", was, sp.Name, shard, b, got)
				}
				n++
			}
		}
		return n, nil
	}
	if was >= 0 {
		r, isRelease := in.(core.SlotReleased)
		view, _ := in.(core.ViewChanged)
		switch {
		case st.Session == core.SessionStateLost:
		case isRelease && r.Slot == was && sm.releaseSent:
			c.slotReleases++
			if prior.Draining {
				c.drainedSlots++
			}
		case isView && was < view.View.Slots && view.View.SlotHolders[was] != "" && view.View.SlotHolders[was] != sm.holder:
			c.slotLosses++
			n, err := stoppedAsLost()
			if err != nil {
				return err
			}
			c.slotLostRuns += n
		case vacantLoss:
			// The views showed the slot vacant for LeaseDuration: its runs stop as lost, as when another
			// holder takes it, and checkNoReleaseWhileRunning sees that each record is released only after
			// the run's RunStopped.
			c.vacantLosses++
			n, err := stoppedAsLost()
			if err != nil {
				return err
			}
			c.vacantLostRuns += n
		default:
			return fmt.Errorf("slot %d became %d", was, h)
		}
	}
	if h < 0 {
		return nil
	}
	claim, isClaim := in.(core.SlotClaimed)
	lowest := -1
	if view, ok := in.(core.ViewChanged); ok && isView {
		for slot, holder := range view.View.SlotHolders {
			if holder == sm.holder && !sm.givingBack[slot] && slot != was {
				lowest = slot
				break
			}
		}
	}
	switch {
	case st.Draining || sm.givingBack[h]:
		return fmt.Errorf("took slot %d while draining or giving it back", h)
	case isClaim && claim.Slot == h && was == -1 && (!sm.viewApplied || sm.applied.Slots > h):
		c.claimsTaken++
	case isView && h == lowest:
		c.adoptions++
	default:
		return fmt.Errorf("took slot %d", h)
	}
	return nil
}

// vacancyModel is a test's model of the slots' vacancy streaks and of the timer of a held slot the views
// show vacant. Its caller updates it in the machine's order from each step's input and result.
type vacancyModel struct {
	holder       string
	grace, lease time.Duration

	// streaks holds each slot's streak, by slot of the latest view applied while the session had not
	// ended.
	streaks []vacancyStreak

	// heldAt is the reading at which the member took the held slot.
	heldAt time.Duration

	// shown says the views show the held slot shownFor vacant while the member holds it, since the reading
	// since.
	shown    bool
	shownFor int
	since    time.Duration
}

// vacancyStreak is one slot's streak in a vacancyModel.
type vacancyStreak struct {
	on, expired bool
	since       time.Duration
}

// applyView applies a view at the reading now to the streaks while the member holds held, and returns
// how many marks it ended by showing their slot held.
func (v *vacancyModel) applyView(view core.View, held int, now time.Duration) int {
	if len(v.streaks) > view.Slots {
		v.streaks = v.streaks[:view.Slots]
	}
	for len(v.streaks) < view.Slots {
		v.streaks = append(v.streaks, vacancyStreak{})
	}
	ended := 0
	for s, holder := range view.SlotHolders {
		switch {
		case holder != "" || s == held:
			if v.end(s) && holder != "" {
				ended++
			}
		default:
			v.start(s, now, view)
		}
	}
	return ended
}

// start starts a streak of slot s at the reading at if view shows it vacant and it has none, and reports
// whether it did.
func (v *vacancyModel) start(s int, at time.Duration, view core.View) bool {
	if s < 0 || s >= len(v.streaks) || s >= view.Slots || view.SlotHolders[s] != "" || v.streaks[s].on {
		return false
	}
	v.streaks[s] = vacancyStreak{on: true, since: at}
	return true
}

// end ends the streak of slot s and reports whether it had a mark.
func (v *vacancyModel) end(s int) bool {
	if s < 0 || s >= len(v.streaks) {
		return false
	}
	marked := v.streaks[s].expired
	v.streaks[s] = vacancyStreak{}
	return marked
}

// display sets the timer of the held slot h after phase A, with view the latest applied view, applied at
// the reading appliedAt.
func (v *vacancyModel) display(h int, view core.View, appliedAt time.Duration) {
	switch {
	case h < 0 || h >= view.Slots || view.SlotHolders[h] != "":
		v.shown = false
	case !v.shown || v.shownFor != h:
		v.shown, v.shownFor, v.since = true, h, max(appliedAt, v.heldAt)
	}
}

// mark marks every streak that has lasted SlotGrace at the reading now, and returns how many it marked.
func (v *vacancyModel) mark(now time.Duration) int {
	n := 0
	for s := range v.streaks {
		if st := &v.streaks[s]; st.on && !st.expired && now >= st.since+v.grace {
			st.expired = true
			n++
		}
	}
	return n
}

// timers returns the readings by which the machine must wake: each streak's expiry and, while the
// session is live, the loss of a held slot shown vacant.
func (v *vacancyModel) timers(live bool) []time.Duration {
	var due []time.Duration
	for _, st := range v.streaks {
		if st.on && !st.expired {
			due = append(due, st.since+v.grace)
		}
	}
	if v.shown && live {
		due = append(due, v.since+v.lease)
	}
	return due
}

// assignment returns the assignment of the named space of shards shards that view gives with the model's
// marks at the wall time at, or none before a view is applied. The machine places at the time of its last
// recompute, so the walks' views give no member a JoinAt or LeaveAt, and then the time does not matter.
func (v *vacancyModel) assignment(space string, shards int, view core.View, applied bool, at time.Time) []string {
	assignment := make([]string, shards)
	if !applied {
		return assignment
	}
	var expired []bool
	if view.Slots > 0 {
		expired = make([]bool, view.Slots)
		for s := range min(view.Slots, len(v.streaks)) {
			expired[s] = v.streaks[s].expired
		}
	}
	keys, _ := core.PlacementKeys(view, at, expired)
	for shard, k := range core.Balanced(space, shards, keys) {
		if k >= 0 {
			assignment[shard] = keys[k]
		}
	}
	return assignment
}

// chaosConfig is the config of the chaos walks, the model driver's (MCH §10.7): one fenced space of two
// shards and one unfenced space of three, with a SlotGrace equal to LeaseDuration. The chaos driver also
// names zz, a space the machine does not have.
func chaosConfig() core.MachineConfig {
	return core.MachineConfig{
		Holder: "h1",
		Member: "m1",
		Spaces: []core.SpaceSpec{{Name: "a", Shards: 2, Fenced: true}, {Name: "b", Shards: 3}},
		Timing: core.Timing{
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
			SlotGrace:     15 * time.Second,
		},
	}
}

// The small sets the chaos driver draws fields from, so that stale, duplicate and impossible inputs
// collide often: shards out of range on both spaces, slots out of range of every view, waits a driver
// should never ask for, and causes without a name.
var (
	chaosSpaces      = []string{"a", "b", "zz"}
	chaosShards      = []int{-1, 0, 1, 2, 3}
	chaosHolders     = []string{"h1", "h2", ""}
	chaosSlots       = []int{-1, 0, 1, 2, 3}
	chaosRetryAfters = []time.Duration{math.MinInt64, -time.Second, 0, time.Second, 2 * time.Second, 20 * time.Second, math.MaxInt64}
	chaosCauses      = []core.Cause{core.CauseNone, core.CauseUnreachable, core.CauseUnreachable, core.Cause(7)}
	chaosMembers     = [][]core.Member{
		{{ID: "m1", Slot: -1}},
		{{ID: "m1", Slot: -1}},
		{{ID: "m1", Slot: -1}, {ID: "m2", Slot: -1}},
		{{ID: "m1", Slot: -1}, {ID: "m3", Slot: -1}},
		{{ID: "m2", Slot: -1}},
		{{ID: "m2", Slot: -1}, {ID: "m3", Slot: -1}},
		{{ID: "m1", Slot: -1, Draining: true}, {ID: "m2", Slot: -1}},
		{{ID: "m1", Slot: -1}, {ID: "m2", Slot: -1, Draining: true}},
		nil,
	}
)

// chaosDriver draws the inputs of a driver that breaks its contract (MCH §10.7): inputs of every type, at
// readings that jump, stall or go back, with fields from small sets, and answers to the machine's own
// actions given late, twice, or for the wrong term. Many draws read the machine's Status, so that reports
// often match the shard they name. It is the same for the same seed on every platform.
type chaosDriver struct {
	rng *rand.Rand

	// slotMode says which views the walk sends: 0 none with slots, 1 nearly all with slots, 2 either.
	slotMode int

	now     time.Duration
	version uint64
	term    core.Term
	holders []string

	// sent holds the latest actions the machine returned, each with its reading, to answer.
	sent []sentAction
}

// sentAction is an action the machine returned at the reading at.
type sentAction struct {
	at time.Duration
	a  core.Action
}

// newChaosDriver returns the chaos driver of a seed.
func newChaosDriver(seed uint64) *chaosDriver {
	// A seeded generator, so that a seed replays its walk exactly; nothing here needs to be unpredictable.
	return &chaosDriver{rng: rand.New(rand.NewPCG(seed, 0)), slotMode: int(seed % 3)} //nolint:gosec // G404: a test's seeded walk.
}

// pick returns one element of list.
func pick[T any](d *chaosDriver, list []T) T {
	return list[d.rng.IntN(len(list))]
}

// addSat returns a + b, or the largest Duration where that overflows. b is not negative.
func addSat(a, b time.Duration) time.Duration {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// reading returns the reading of the next input: mostly a little after the last, sometimes much later,
// sometimes behind it, negative, or near the largest Duration.
func (d *chaosDriver) reading() time.Duration {
	switch r := d.rng.IntN(1000); {
	case r < 420:
		d.now = addSat(d.now, pick(d, []time.Duration{0, time.Millisecond, 50 * time.Millisecond, 300 * time.Millisecond}))
	case r < 760:
		d.now = addSat(d.now, pick(d, []time.Duration{time.Second, 2 * time.Second}))
	case r < 930:
		d.now = addSat(d.now, pick(d, []time.Duration{5 * time.Second, 11 * time.Second, 16 * time.Second}))
	case r < 975:
		return d.now - pick(d, []time.Duration{time.Second, 10 * time.Second, 30 * time.Second})
	case r < 996:
		return pick(d, []time.Duration{0, -time.Second, math.MinInt64})
	case r < 998:
		d.now = addSat(d.now, time.Hour)
	case d.rng.IntN(10) == 0:
		d.now = pick(d, []time.Duration{maxReading - time.Second, maxReading, math.MaxInt64})
	}
	return d.now
}

// target returns a space and shard: mostly one of the machine's, sometimes one it does not have.
func (d *chaosDriver) target() propShard {
	if d.rng.IntN(8) == 0 {
		return propShard{pick(d, chaosSpaces), pick(d, chaosShards)}
	}
	if d.rng.IntN(2) == 0 {
		return propShard{"a", d.rng.IntN(2)}
	}
	return propShard{"b", d.rng.IntN(3)}
}

// termFor returns a term for a report about shard k: often the shard's own, sometimes a new one, none,
// or any term near those in use.
func (d *chaosDriver) termFor(st core.Status, k propShard) core.Term {
	switch r := d.rng.IntN(10); {
	case r < 4:
		sh, _ := shardOf(st, k)
		return sh.Term
	case r < 7:
		d.term++
		return d.term
	case r < 8:
		return 0
	default:
		return core.Term(d.rng.Uint64N(uint64(d.term) + 2))
	}
}

// view returns a view: usually newer than the last, sometimes the same version or older, now and then not
// valid. Its slots, if any, are held by this member, another or no one, and half the time they are those
// of the last view, as a group that has not changed shows them, so that a vacancy lasts.
func (d *chaosDriver) view() core.View {
	switch r := d.rng.IntN(20); {
	case r < 14:
		d.version++
	case r < 17:
	case r < 19:
		if d.version > 0 {
			d.version--
		}
	default:
		d.version = 0
	}
	if d.rng.IntN(32) == 0 {
		if d.rng.IntN(2) == 0 {
			return core.View{Version: d.version, Slots: 2, SlotHolders: []string{"h1"}}
		}
		return core.View{Version: d.version, Members: []core.Member{{ID: "m2", Slot: -1}, {ID: "m1", Slot: -1}}}
	}
	v := core.View{Version: d.version, Members: slices.Clone(pick(d, chaosMembers))}
	slotted := d.slotMode == 1 && d.rng.IntN(10) != 0 || d.slotMode == 2 && d.rng.IntN(2) == 0
	if !slotted {
		return v
	}
	if d.holders == nil || d.rng.IntN(2) == 0 {
		d.holders = make([]string, d.rng.IntN(4))
		for i := range d.holders {
			d.holders[i] = pick(d, chaosHolders)
		}
	}
	v.Slots, v.SlotHolders = len(d.holders), slices.Clone(d.holders)
	return v
}

// answer returns an answer to one of the latest actions, which may have been answered already, and
// whether there was one to answer.
func (d *chaosDriver) answer(now time.Duration) (core.Input, bool) {
	if len(d.sent) == 0 {
		return nil, false
	}
	s := pick(d, d.sent)
	switch v := s.a.(type) {
	case core.KeepAlive:
		if d.rng.IntN(6) == 0 {
			return core.SessionFailed{Now: now, Cause: core.CauseUnreachable}, true
		}
		return core.SessionRenewed{Now: now, SentAt: s.at}, true
	case core.Acquire:
		switch r := d.rng.IntN(10); {
		case r < 7:
			d.term++
			return core.Acquired{Now: now, Space: v.Space, Shard: v.Shard, Term: d.term}, true
		case r < 8:
			return core.Acquired{Now: now, Space: v.Space, Shard: v.Shard}, true
		default:
			return core.AcquireFailed{Now: now, Space: v.Space, Shard: v.Shard, Holder: "h2", RetryAfter: time.Second}, true
		}
	case core.StopRun:
		return core.RunStopped{Now: now, Space: v.Space, Shard: v.Shard, Term: v.Term}, true
	case core.StartRun:
		// A run that ends on its own, before any StopRun: a report the machine must not believe.
		return core.RunStopped{Now: now, Space: v.Space, Shard: v.Shard, Term: v.Term}, true
	case core.Release:
		if d.rng.IntN(5) == 0 {
			return core.HolderChanged{Now: now, Space: v.Space, Shard: v.Shard, Holder: pick(d, []string{"h2", ""})}, true
		}
		return core.Released{Now: now, Space: v.Space, Shard: v.Shard, Term: v.Term}, true
	case core.Confirm:
		return core.HolderChanged{Now: now, Space: v.Space, Shard: v.Shard, Holder: "h1", Term: v.Term}, true
	case core.Degrade:
		return core.HolderChanged{Now: now, Space: v.Space, Shard: v.Shard, Holder: "h1", Term: v.Term}, true
	case core.Restore:
		return core.HolderChanged{Now: now, Space: v.Space, Shard: v.Shard, Holder: "h2", Term: v.Term + 1}, true
	case core.ClaimSlot:
		return core.SlotClaimed{Now: now, Slot: d.rng.IntN(4)}, true
	case core.ReleaseSlot:
		return core.SlotReleased{Now: now, Slot: v.Slot}, true
	case core.CloseSession:
		return core.SessionClosed{Now: now}, true
	}
	return nil, false
}

// next returns the next input for m, whose Status is st.
func (d *chaosDriver) next(m *core.Machine, st core.Status) core.Input {
	now := d.reading()
	switch w := d.rng.IntN(1000); {
	case w < 100:
		if at, armed := m.NextWake(); armed && d.rng.IntN(2) == 0 {
			d.now = max(d.now, at)
			return core.Tick{Now: max(now, at)}
		}
		return core.Tick{Now: now}
	case w < 250:
		sentAt := now
		if d.rng.IntN(8) == 0 {
			sentAt = pick(d, []time.Duration{0, -20 * time.Second, math.MinInt64})
		} else {
			switch off := pick(d, []time.Duration{0, 0, 0, -time.Second, -5 * time.Second, -12 * time.Second, time.Second}); {
			case off > 0:
				sentAt = addSat(now, off)
			case now >= math.MinInt64-off:
				sentAt = now + off
			}
		}
		return core.SessionRenewed{Now: now, SentAt: sentAt}
	case w < 280:
		if d.rng.IntN(24) == 0 {
			return core.SessionFailed{Now: now, Cause: core.CauseLost}
		}
		return core.SessionFailed{Now: now, Cause: pick(d, chaosCauses)}
	case w < 360:
		return core.ViewChanged{Now: now, View: d.view(), Wall: mc.Wall(now)}
	case w < 440:
		k := d.target()
		return core.Acquired{Now: now, Space: k.space, Shard: k.shard, Term: d.termFor(st, k)}
	case w < 480:
		k := d.target()
		return core.AcquireFailed{Now: now, Space: k.space, Shard: k.shard, Holder: pick(d, chaosHolders), RetryAfter: pick(d, chaosRetryAfters)}
	case w < 530:
		k := d.target()
		return core.Released{Now: now, Space: k.space, Shard: k.shard, Term: d.termFor(st, k)}
	case w < 600:
		k := d.target()
		return core.RunStopped{Now: now, Space: k.space, Shard: k.shard, Term: d.termFor(st, k)}
	case w < 680:
		k := d.target()
		return core.HolderChanged{Now: now, Space: k.space, Shard: k.shard, Holder: pick(d, chaosHolders), Term: d.termFor(st, k)}
	case w < 710:
		return core.SlotClaimed{Now: now, Slot: pick(d, chaosSlots)}
	case w < 740:
		if st.Slot >= 0 && d.rng.IntN(2) == 0 {
			return core.SlotReleased{Now: now, Slot: st.Slot}
		}
		return core.SlotReleased{Now: now, Slot: pick(d, chaosSlots)}
	case w < 741:
		return core.Drain{Now: now}
	case w < 760:
		return core.SessionClosed{Now: now}
	case w < 765:
		// A value that satisfies Input but is not one of the machine's inputs.
		return pick(d, []core.Input{&core.Tick{Now: now}, &core.Drain{Now: now}, &core.SessionRenewed{Now: now, SentAt: now}})
	}
	if in, ok := d.answer(now); ok {
		return in
	}
	return core.Tick{Now: now}
}

// observe keeps the actions of a result the machine returned at the reading now, to answer later.
func (d *chaosDriver) observe(got []core.Action, now time.Duration) {
	for _, a := range got {
		switch a.(type) {
		case core.Wake, core.ExtendDeadline:
			continue
		}
		d.sent = append(d.sent, sentAction{at: now, a: a})
	}
	if n := len(d.sent) - 32; n > 0 {
		d.sent = slices.Delete(d.sent, 0, n)
	}
}

// chaosReach counts how often the chaos walks reached the paths the checkers are about.
type chaosReach struct {
	actions                        map[string]int
	stops                          map[core.StopReason]int
	losses, closed, ceiling, stuck int
	slots                          slotCounts
}

// runChaosSeed runs the chaos walk of one seed for steps steps and returns the first rule it broke, with
// the step and the input, or nil. It adds what the walk reached to reach.
func runChaosSeed(seed uint64, steps int, reach *chaosReach) error {
	w := newPropWalk(chaosConfig())
	d := newChaosDriver(seed)
	st := w.m.Status()
	for n := 1; n <= steps; n++ {
		in := d.next(w.m, st)
		s, err := w.step(in)
		if err != nil {
			return fmt.Errorf("seed %d step %d: Step(%#v): %w", seed, n, in, err)
		}
		d.observe(s.got, s.st.Now)
		st = s.st
		for _, a := range s.got {
			reach.actions[fmt.Sprintf("%T", a)]++
			if v, ok := a.(core.StopRun); ok {
				reach.stops[v.Reason]++
			}
		}
		if s.st.Session == core.SessionStateLost && s.prior.Session != core.SessionStateLost {
			reach.losses++
		}
		if s.st.Session == core.SessionStateClosed && s.prior.Session != core.SessionStateClosed {
			reach.closed++
		}
		if s.st.Now == maxReading && s.prior.Now != maxReading {
			reach.ceiling++
		}
		for _, sp := range s.st.Spaces {
			for _, sh := range sp.Shards {
				if sh.Stuck {
					reach.stuck++
				}
			}
		}
	}
	reach.slots.add(w.slots.counts)
	return nil
}

// TestATMCH49Chaos runs the chaos driver for seeds 1 to 3000, 400 steps each, and checks after every step
// that no Step panics and that AT-MCH-45, AT-MCH-46 and AT-MCH-47 hold, with the session, slot and vacancy
// rules of sections 3 to 13 (MCH §10.7). A failure names the seed, the step and the input, and the seed
// replays it exactly.
func TestATMCH49Chaos(t *testing.T) {
	const seeds, steps = 3000, 400
	reach := chaosReach{actions: map[string]int{}, stops: map[core.StopReason]int{}}

	// The seeds that once broke a rule, each with the rule, run first so that a regression names it.
	for _, tc := range []struct {
		seed uint64
		rule string
	}{
		// Step 345 is a renewal that makes the session live again, which gives up a draining member's slot
		// the views had shown vacant too long, and with every shard idle the session starts closing in that
		// step. The step returned the renewal's ExtendDeadline beside CloseSession.
		{154, "the step that starts closing returns only CloseSession and Wake"},
		// Past the seeds below. Step 61 is a RunStopped of an owned run, sent before any StopRun, in the
		// step whose reading loses the held slot the views had shown vacant too long. The slot loss
		// stopped the run first, so the RunStopped ended it and its record was released.
		{6769, "a RunStopped in the step that tells its run to stop does not end the run"},
		// Past the seeds below, and a fault of the slot checker, not of the machine. Step 252 answers the
		// last Acquire of a draining member whose held slot the views had shown vacant too long: the slot
		// is lost, every shard is idle, and the session starts closing in that step. Closing frees the
		// lost slot's record, so no ReleaseSlot of it is owed.
		{43312, "a slot lost in the step that starts closing needs no ReleaseSlot"},
	} {
		t.Run("seed "+strconv.FormatUint(tc.seed, 10), func(t *testing.T) {
			if err := runChaosSeed(tc.seed, steps, &chaosReach{actions: map[string]int{}, stops: map[core.StopReason]int{}}); err != nil {
				t.Fatalf("%s: %v", tc.rule, err)
			}
		})
	}

	for seed := uint64(1); seed <= seeds; seed++ {
		if err := runChaosSeed(seed, steps, &reach); err != nil {
			t.Fatal(err)
		}
	}

	// The walks are only worth something if they reach the paths the rules are about.
	for _, c := range []struct {
		name  string
		count int
	}{
		{"StartRun", reach.actions["core.StartRun"]},
		{"StopRun moved", reach.stops[core.StopMoved]},
		{"StopRun draining", reach.stops[core.StopDraining]},
		{"StopRun expired", reach.stops[core.StopExpired]},
		{"StopRun lost", reach.stops[core.StopLost]},
		{"Release", reach.actions["core.Release"]},
		{"Acquire", reach.actions["core.Acquire"]},
		{"Degrade", reach.actions["core.Degrade"]},
		{"Confirm", reach.actions["core.Confirm"]},
		{"Restore", reach.actions["core.Restore"]},
		{"ExtendDeadline", reach.actions["core.ExtendDeadline"]},
		{"ClaimSlot", reach.actions["core.ClaimSlot"]},
		{"ReleaseSlot", reach.actions["core.ReleaseSlot"]},
		{"CloseSession", reach.actions["core.CloseSession"]},
		{"stuck run", reach.stuck},
		{"lost session", reach.losses},
		{"closed session", reach.closed},
		{"clock at its ceiling", reach.ceiling},
		{"claim answer taken", reach.slots.claimsTaken},
		{"slot adopted from a view", reach.slots.adoptions},
		{"slot given back", reach.slots.giveBacks},
		{"ReleaseSlot of the held slot", reach.slots.heldReleases},
		{"ReleaseSlot of the held slot over a stuck run", reach.slots.releasesOverStuck},
		{"held slot released", reach.slots.slotReleases},
		{"held slot lost to another holder", reach.slots.slotLosses},
		{"held slot retired", reach.slots.retirements},
		{"slot marked expired", reach.slots.marksSet},
		{"held slot lost to views showing it vacant", reach.slots.vacantLosses},
		{"run stopped as lost with a slot shown vacant", reach.slots.vacantLostRuns},
	} {
		if c.count == 0 {
			t.Errorf("no chaos walk reached a %s", c.name)
		}
	}
}

// TestPropCheckers checks that each checker finds the rule it is about broken, on steps made up for it,
// and that the rule it names is that one rather than another.
func TestPropCheckers(t *testing.T) {
	cfg := chaosConfig()
	m := core.NewMachine(cfg)
	timing := core.MachineTiming(m)
	st := m.Status()
	live := st
	live.Session, live.Deadline = core.SessionStateLive, 10*time.Second
	// at returns s at the reading now, and with shard b/0 set to sh.
	at := func(s core.Status, now time.Duration, sh core.ShardStatus) core.Status {
		s.Now = now
		s.Spaces = slices.Clone(s.Spaces)
		s.Spaces[1].Shards = slices.Clone(s.Spaces[1].Shards)
		s.Spaces[1].Shards[0] = sh
		return s
	}
	// running returns a record with term 7 going on b/0, which keeps timing if timed.
	b0 := propShard{"b", 0}
	running := func(timed bool) *runLedger {
		l := newRunLedger("h1", timing.LeaseDuration)
		l.running[b0] = 7
		l.started[propRun{b0, 7}] = true
		if timed {
			l.timing = timing
		}
		return l
	}
	idle := func() *runLedger {
		l := newRunLedger("h1", timing.LeaseDuration)
		l.timing = timing
		return l
	}
	ownedB0 := core.ShardStatus{State: core.ShardStateOwned, Term: 7, Holder: "h1", Wanted: true}
	owned := at(live, 0, ownedB0)
	releasing := at(live, 0, core.ShardStatus{State: core.ShardStateReleasing, Term: 7, Holder: "h1"})

	tests := []struct {
		name, want string
		check      func() error
	}{
		{"Release while running", "while term 7 runs, before its RunStopped", func() error {
			return checkNoReleaseWhileRunning(running(false), propStep{m: m, st: owned, got: []core.Action{core.Release{Space: "b", Term: 3}}})
		}},
		{"CloseSession while running", "CloseSession while", func() error {
			return checkNoReleaseWhileRunning(running(false), propStep{m: m, st: owned, got: []core.Action{core.CloseSession{}}})
		}},
		{"done while running", "done while", func() error {
			done := owned
			done.Done = true
			return checkNoReleaseWhileRunning(running(false), propStep{m: m, st: done})
		}},
		{"a second StartRun", "a second", func() error {
			return checkStartStop(running(false), propStep{m: m, st: owned, got: []core.Action{core.StartRun{Space: "b", Term: 7, Deadline: 10 * time.Second}}})
		}},
		{"StopRun of a run not going", "of a run that is not going", func() error {
			return checkStartStop(idle(), propStep{m: m, st: live, got: []core.Action{core.StopRun{Space: "b", Term: 7, Reason: core.StopMoved}}})
		}},
		{"two StopRuns of one run", "of a run that is not going or has its StopRun", func() error {
			l := running(false)
			stop := core.StopRun{Space: "b", Term: 7, Reason: core.StopMoved}
			stopping := at(live, 0, core.ShardStatus{State: core.ShardStateStopping, Term: 7, Reason: core.StopMoved, Holder: "h1"})
			if err := checkStartStop(l, propStep{m: m, st: stopping, got: []core.Action{stop}}); err != nil {
				return nil
			}
			return checkStartStop(l, propStep{m: m, st: stopping, got: []core.Action{stop}})
		}},
		{"Restore of a degraded run with no Confirm", "Confirm pending false, degraded true", func() error {
			l := running(false)
			l.degraded[propRun{b0, 7}] = true
			return checkStartStop(l, propStep{m: m, st: owned, in: core.HolderChanged{Space: "b", Holder: "h1", Term: 7},
				got: []core.Action{core.Restore{Space: "b", Term: 7}}})
		}},
		{"a stuck owned run", "Stuck:true", func() error {
			stuck := ownedB0
			stuck.Stuck = true
			return checkStartStop(running(false), propStep{m: m, st: at(live, 0, stuck)})
		}},
		{"a run stuck before RenewDeadline", "Stuck:true", func() error {
			l := running(true)
			l.stopSent[propRun{b0, 7}] = true
			stopping := core.ShardStatus{State: core.ShardStateStopping, Term: 7, Reason: core.StopMoved, Holder: "h1", Stuck: true}
			return checkStartStop(l, propStep{m: m, st: at(live, time.Second, stopping)})
		}},
		{"Acquire before RetryAt", "before RetryAt", func() error {
			waiting := at(live, 0, core.ShardStatus{Wanted: true, RetryAt: 5 * time.Second})
			acquiring := at(live, time.Second, core.ShardStatus{State: core.ShardStateAcquiring, Since: time.Second, Wanted: true})
			return checkStartStop(idle(), propStep{m: m, prior: waiting, st: acquiring, got: []core.Action{core.Acquire{Space: "b"}}})
		}},
		{"Confirm before RetryPeriod", "the last at 0s", func() error {
			l := running(true)
			l.degraded[propRun{b0, 7}] = true
			l.pending[b0] = true
			degraded := core.ShardStatus{State: core.ShardStateDegraded, Term: 7, Holder: "h1", Wanted: true}
			return checkStartStop(l, propStep{m: m, st: at(live, time.Second, degraded), got: []core.Action{core.Confirm{Space: "b", Term: 7}}})
		}},
		{"Release repeat before RetryPeriod", "the last at 0s", func() error {
			l := idle()
			l.releaseAt[b0] = 0
			return checkStartStop(l, propStep{m: m, prior: releasing, st: at(releasing, time.Second, releasing.Spaces[1].Shards[0]),
				got: []core.Action{core.Release{Space: "b", Term: 7}}})
		}},
		{"no wait after an Acquired with no grant", "not waiting RetryPeriod", func() error {
			acquiring := at(live, 0, core.ShardStatus{State: core.ShardStateAcquiring, Wanted: true})
			waiting := at(live, time.Second, core.ShardStatus{Since: time.Second, Wanted: true, RetryAt: time.Second})
			return checkStartStop(idle(), propStep{m: m, prior: acquiring, st: waiting, in: core.Acquired{Now: time.Second, Space: "b"}})
		}},
		{"a release ended by another term's Released", "ended the release", func() error {
			return checkStartStop(idle(), propStep{m: m, prior: releasing, st: at(live, time.Second, core.ShardStatus{Since: time.Second, Holder: "h1"}),
				in: core.Released{Now: time.Second, Space: "b", Term: 6}})
		}},
		{"group order", "after an action of a later group", func() error {
			return checkOrder(propStep{st: st, got: []core.Action{core.Acquire{Space: "a"}, core.StartRun{Space: "a"}}})
		}},
		{"space order", "after an action of shard b/0", func() error {
			return checkOrder(propStep{st: st, got: []core.Action{core.Acquire{Space: "b"}, core.Acquire{Space: "a"}}})
		}},
		{"shard order", "after an action of shard a/1", func() error {
			return checkOrder(propStep{st: st, got: []core.Action{core.Acquire{Space: "a", Shard: 1}, core.Acquire{Space: "a"}}})
		}},
		{"give-backs out of order", "not in slot order", func() error {
			return checkOrder(propStep{st: st, got: []core.Action{core.ReleaseSlot{Slot: 2}, core.ReleaseSlot{Slot: 1}}})
		}},
		{"ClaimSlot before ReleaseSlot", "after the ClaimSlot", func() error {
			return checkOrder(propStep{st: st, got: []core.Action{core.ClaimSlot{}, core.ReleaseSlot{Slot: 1}}})
		}},
		{"empty result", "an empty result", func() error { return checkOrder(propStep{st: st, got: []core.Action{}}) }},
		{"a replay that differs", "the replay returned", func() error {
			r := newReplay(cfg)
			return checkReplay(r, propStep{m: m, in: core.Tick{}, st: core.NewMachine(cfg).Status()})
		}},
		{"a closed machine that moves", "a closed machine returned", func() error {
			closed := st
			closed.Session = core.SessionStateClosed
			return checkSession(&sessionLedger{timing: timing}, propStep{m: m, prior: closed, st: st})
		}},
		{"KeepAlive too soon", "KeepAlive at", func() error {
			l := &sessionLedger{timing: timing, keepAliveSent: true, keepAliveAt: 0}
			return checkSession(l, propStep{m: m, prior: live, st: live, in: core.Tick{}, got: []core.Action{core.KeepAlive{}}})
		}},
		{"ClaimSlot while holding a slot", "{} with Status", func() error {
			holding := live
			holding.Slot = 0
			return checkSlots(newSlotModel(cfg, timing), propStep{m: m, prior: holding, st: holding, in: core.Tick{}, got: []core.Action{core.ClaimSlot{}}})
		}},
		{"ClaimSlot before RetryPeriod", "the last at 0s", func() error {
			sm := newSlotModel(cfg, timing)
			sm.applied = core.View{Slots: 1, SlotHolders: []string{""}}
			sm.claimSent = true
			now := live
			now.Now = time.Second
			return checkSlots(sm, propStep{m: m, prior: live, st: now, in: core.Tick{Now: time.Second}, got: []core.Action{core.ClaimSlot{}}})
		}},
		{"a slot taken from no answer or view", "took slot 1", func() error {
			holding := live
			holding.Slot = 1
			return checkSlots(newSlotModel(cfg, timing), propStep{m: m, prior: live, st: holding, in: core.Tick{}})
		}},
		{"no wake by a streak's expiry", "want a wake by 15s", func() error {
			sm := newSlotModel(cfg, timing)
			sm.vac.streaks = []vacancyStreak{{on: true}}
			return checkSlots(sm, propStep{m: m, prior: live, st: live, in: core.Tick{}})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			switch err := tc.check(); {
			case err == nil:
				t.Error("the checker found nothing")
			case !strings.Contains(err.Error(), tc.want):
				t.Errorf("the checker found %v, want %q", err, tc.want)
			}
		})
	}
}
