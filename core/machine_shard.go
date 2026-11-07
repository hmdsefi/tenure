// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"slices"
	"time"
)

// shardBook is a shard's bookkeeping that Status does not report.
type shardBook struct {
	// highestStarted is the highest term the shard has returned a StartRun for, so a term never
	// starts twice and an older grant never starts at all.
	highestStarted Term

	// stopAt is the reading of the run's StopRun. It means something only while the run has that
	// StopRun and has not reported its end.
	stopAt time.Duration

	// releaseAt is the reading of the next Release repeat. It means something only while the
	// shard is releasing.
	releaseAt time.Duration

	// knownGone says the record no longer names this run while the shard is stopping or expired, so
	// the run's RunStopped has nothing of the run's to release. A lost shard needs no flag: its state
	// says the same.
	knownGone bool

	// grantAfterRun is the term of a record that names this member above the term of a run still
	// going, or 0 for none. It holds the largest such term, until a record that is not older names
	// another holder or none. The run's RunStopped hands it to grant unless the session is lost, and
	// nothing else acts on it, so nothing of that term goes out while the run is going.
	grantAfterRun Term

	// confirmPending says a Confirm has gone out since the session was last not live, and no answer
	// has restored the run. Nothing else lets an answer restore it. It means something only while
	// the shard is degraded.
	confirmPending bool

	// confirmAt is the reading of the next Confirm, and 0 sends it in the first live step. It means
	// something only while the shard is degraded.
	confirmAt time.Duration
}

// noShard is the target of an input that names no shard the machine has.
var noShard = shardRef{space: -1, shard: -1}

// shardRef names one shard by its index in cfg.Spaces and its number.
type shardRef struct {
	space, shard int
}

// inputShard returns the shard that in names, or noShard for an input that names no shard.
func (m *Machine) inputShard(in Input) shardRef {
	switch v := in.(type) {
	case Acquired:
		return m.findShard(v.Space, v.Shard)
	case AcquireFailed:
		return m.findShard(v.Space, v.Shard)
	case RunStopped:
		return m.findShard(v.Space, v.Shard)
	case Released:
		return m.findShard(v.Space, v.Shard)
	case HolderChanged:
		return m.findShard(v.Space, v.Shard)
	}
	return noShard
}

// findShard returns the named shard, or noShard when the machine has no such shard.
func (m *Machine) findShard(space string, shard int) shardRef {
	i, ok := m.spaceIndex(space)
	if !ok || shard < 0 || shard >= m.cfg.Spaces[i].Shards {
		return noShard
	}
	return shardRef{space: i, shard: shard}
}

// spaceIndex returns the index in cfg.Spaces of the named space, and whether it has one.
func (m *Machine) spaceIndex(space string) (int, bool) {
	return slices.BinarySearchFunc(m.cfg.Spaces, space, func(s SpaceSpec, name string) int {
		return cmp.Compare(s.Name, name)
	})
}

// forgetConfirms is the part of phase A that a session not live applies to every shard (MCH-110): no
// Confirm is pending any more, and the next goes out in the first live step. A record read before the
// session stopped being live says nothing about who holds the shard now, so its answer must not count.
// This stops such a record only until that next Confirm goes out: see restoreIfConfirmed.
func (m *Machine) forgetConfirms() {
	for i := range m.shards {
		for j := range m.shards[i] {
			book := &m.shards[i][j]
			book.confirmPending = false
			book.confirmAt = 0
		}
	}
}

// stepShards is phase D. It visits the spaces by name and each space's shards in order, so that
// the actions of a group come out in that order. After a lost session nothing here returns anything
// but the StopRuns of the step that accepted the loss: no shard can start, acquire or release, and only
// a run still going has anything left to do.
func (m *Machine) stepShards(in Input) {
	target := m.inputShard(in)
	for i := range m.status.Spaces {
		shards := m.status.Spaces[i].Shards
		for shard := range shards {
			ref := shardRef{space: i, shard: shard}
			if m.sessionLost() {
				m.applySessionLoss(ref)
			}
			if ref == target {
				m.applyShardInput(ref, in)
			}
			m.expireAtDeadline(ref)
			m.stopIfUnwanted(ref)
			m.repeatConfirm(ref)
			m.acquireIfDue(ref)
			m.repeatRelease(ref)
			m.markStuck(ref)
		}
	}
}

// applySessionLoss is the part of D1 that a lost session applies to every shard, starting in the step
// that accepted the loss (MCH-250, MCH-470 item 1). The records are takeable once the session is gone,
// so nothing is released: a run still going gets its one StopRun, a Release in flight is forgotten, and
// a shard waiting to ask again no longer will. An acquiring shard stays until its answer, because the
// driver owes it one. No shard can come back to a state this changes, so later steps change nothing.
func (m *Machine) applySessionLoss(ref shardRef) {
	sh := m.shardStatus(ref)
	switch sh.State {
	case ShardStateOwned, ShardStateDegraded:
		m.loseRun(ref, sh.Holder)
	case ShardStateReleasing:
		m.setShardState(sh, ShardStateIdle)
		sh.Term = 0
		sh.Reason = StopNone
	case ShardStateIdle:
		sh.RetryAt = 0
	case ShardStateAcquiring, ShardStateStopping, ShardStateExpired, ShardStateLost:
		// An acquiring shard waits for its answer, and a run told to stop has its StopRun already.
	}
}

// applyShardInput is D1: it applies the input that names the shard.
func (m *Machine) applyShardInput(ref shardRef, in Input) {
	switch v := in.(type) {
	case Acquired:
		m.applyAcquired(ref, v.Term)
	case AcquireFailed:
		m.applyAcquireFailed(ref, v.Holder, v.RetryAfter)
	case RunStopped:
		m.applyRunStopped(ref, v.Term)
	case Released:
		m.applyReleased(ref, v.Term)
	case HolderChanged:
		m.applyHolderChanged(ref, v.Holder, v.Term)
	}
}

// applyAcquired applies the answer to an Acquire: a grant of term t, or no grant when t is 0. Only an
// acquiring shard takes it, so an answer the shard has stopped waiting for does nothing. After a lost
// session any answer ends the wait and nothing more: a granted record is takeable (MCH-430 item 3).
func (m *Machine) applyAcquired(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateAcquiring {
		return
	}
	if m.sessionLost() {
		m.setShardState(sh, ShardStateIdle)
		return
	}
	if t == 0 {
		m.waitToAcquire(sh, m.cfg.Timing.RetryPeriod)
		return
	}
	m.grant(ref, t)
}

// applyAcquireFailed applies the answer that the record names another holder. Only an acquiring shard
// takes it. The driver's RetryAfter may be any value: the wait stays between RetryPeriod and
// LeaseDuration, each of them a Timing field, so RetryAt fits in a Duration and is after the clock.
// After a lost session there is no next Acquire, so the answer only ends the wait.
func (m *Machine) applyAcquireFailed(ref shardRef, holder string, retryAfter time.Duration) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateAcquiring {
		return
	}
	sh.Holder = holder
	if m.sessionLost() {
		m.setShardState(sh, ShardStateIdle)
		return
	}
	m.waitToAcquire(sh, min(max(retryAfter, m.cfg.Timing.RetryPeriod), m.cfg.Timing.LeaseDuration))
}

// waitToAcquire moves an acquiring shard to idle, with its next Acquire wait away. A draining member
// acquires nothing again, so its shard has no next Acquire to wait for.
func (m *Machine) waitToAcquire(sh *ShardStatus, wait time.Duration) {
	m.setShardState(sh, ShardStateIdle)
	if !m.status.Draining {
		sh.RetryAt = m.status.Now + wait
	}
}

// grant applies a record that names this member in term t, whether an Acquired or a HolderChanged
// said so. The shard must have no run: it is idle or acquiring, or applyRunStopped has just ended its
// run. A grant the machine can run starts it, and any other is released without a run. That covers a
// term at or below the highest started: it never starts twice, and since the shard has no run,
// whatever run it had has returned.
func (m *Machine) grant(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	book := &m.shards[ref.space][ref.shard]
	if !m.live || !sh.Wanted || m.status.Draining || t <= book.highestStarted {
		m.setShardState(sh, ShardStateReleasing)
		m.releaseWithoutRun(ref, t)
		return
	}
	m.setShardState(sh, ShardStateOwned)
	sh.Term = t
	sh.Holder = m.cfg.Holder
	sh.RetryAt = 0
	book.highestStarted = t
	m.emit(groupRunStart, StartRun{
		Space:    m.cfg.Spaces[ref.space].Name,
		Shard:    ref.shard,
		Term:     t,
		Deadline: m.status.Deadline,
	})
}

// releaseWithoutRun makes t the term a releasing shard is releasing and returns its Release. It is
// the other way to a Release, beside applyRunStopped, and it needs no RunStopped because no run is
// going: its callers are grant, whose shard has no run, and applyHolderChanged for a shard already
// releasing, whose run, if it had one, has returned.
func (m *Machine) releaseWithoutRun(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	sh.Term = t
	sh.Reason = StopNone
	sh.Holder = m.cfg.Holder
	sh.RetryAt = 0
	m.shards[ref.space][ref.shard].releaseAt = m.status.Now + m.cfg.Timing.RetryPeriod
	m.emit(groupRecord, Release{Space: m.cfg.Spaces[ref.space].Name, Shard: ref.shard, Term: t})
}

// applyHolderChanged applies what the shard's record says now: that it names holder, or no one when
// holder is "", in term t. A record older than the shard's term says nothing about it. Term 0 is not
// older, because it comes with no term to compare: a record with no holder may have none.
//
// A larger term that names this member is not this run's, so a shard with a run treats it as a record
// the run has lost. It never takes that term while the run is going: its Term stays the run's, so the
// run's RunStopped still matches, and no Release of either term goes out. That RunStopped grants the
// term instead, so the record does not stay in this member's name.
//
// After a lost session a record has no effect at all: whatever it says, this member will not act on it.
func (m *Machine) applyHolderChanged(ref shardRef, holder string, t Term) {
	sh := m.shardStatus(ref)
	book := &m.shards[ref.space][ref.shard]
	if m.sessionLost() || (t != 0 && t < max(sh.Term, book.grantAfterRun)) {
		return
	}
	mine := holder == m.cfg.Holder
	notThisRun := !mine || t > sh.Term
	switch sh.State {
	case ShardStateIdle, ShardStateAcquiring:
		switch {
		case mine && t != 0:
			m.grant(ref, t)
		case !mine:
			sh.Holder = holder
			if holder == "" {
				sh.RetryAt = 0
			}
		}
	case ShardStateOwned:
		if notThisRun {
			book.noteGrantAfterRun(mine, t)
			m.loseRun(ref, holder)
		}
	case ShardStateDegraded:
		switch {
		case notThisRun:
			book.noteGrantAfterRun(mine, t)
			m.loseRun(ref, holder)
		case t == sh.Term:
			m.restoreIfConfirmed(ref)
		}
	case ShardStateStopping, ShardStateExpired:
		if notThisRun {
			book.noteGrantAfterRun(mine, t)
			sh.Holder = holder
			book.knownGone = true
		}
	case ShardStateLost:
		// The run has its StopRun and nothing here brings it back. All a record can still change is
		// what the run's RunStopped grants.
		if notThisRun && (mine || book.grantAfterRun != 0) {
			book.noteGrantAfterRun(mine, t)
			sh.Holder = holder
		}
	case ShardStateReleasing:
		switch {
		case !mine:
			m.setShardState(sh, ShardStateIdle)
			sh.Term = 0
			sh.Reason = StopNone
			sh.Holder = holder
		case t > sh.Term:
			m.releaseWithoutRun(ref, t)
		}
	}
}

// restoreIfConfirmed applies a record that names this member in the degraded run's own term (MCH-490).
// Only the answer to a Confirm returned while the session is live, with the session still live, shows
// that no one took the shard meanwhile. The machine holds up its side of that: such a record changes
// nothing until a Confirm is out, or once the session has stopped being live since. A Confirm carries no
// ID, so after that the machine cannot tell its answer from an older reading of the record, and keeping
// those out is the driver's side, as Confirm's doc says (MCH O5). Phase A forgets every Confirm when the
// session is not live, so a pending Confirm already implies live here. The check stays because a Restore
// that should not happen brings back a shard someone else may hold.
func (m *Machine) restoreIfConfirmed(ref shardRef) {
	book := &m.shards[ref.space][ref.shard]
	if !book.confirmPending || !m.live {
		return
	}
	book.confirmPending = false
	sh := m.shardStatus(ref)
	m.setShardState(sh, ShardStateOwned)
	m.emit(groupRunChange, Restore{Space: m.cfg.Spaces[ref.space].Name, Shard: ref.shard, Term: sh.Term})
}

// noteGrantAfterRun keeps what a record that is not older and does not name the shard's run says about
// the run's RunStopped: a term of this member's is granted then, and any other holder, or none, ends
// the wait for one that came before it.
func (b *shardBook) noteGrantAfterRun(mine bool, t Term) {
	b.grantAfterRun = 0
	if mine {
		b.grantAfterRun = t
	}
}

// loseRun tells an owned or degraded shard's run to stop because the record no longer names it, or the
// session it was held under is gone. The shard's Term stays the run's until its RunStopped.
func (m *Machine) loseRun(ref shardRef, holder string) {
	m.shardStatus(ref).Holder = holder
	m.stopRun(ref, ShardStateLost, StopLost)
}

// stopRun returns the StopRun of the shard's run and moves the shard to state, which must be one that
// runToldToStop reports, and starts the count toward Stuck.
func (m *Machine) stopRun(ref shardRef, state ShardState, reason StopReason) {
	sh := m.shardStatus(ref)
	m.setShardState(sh, state)
	sh.Reason = reason
	m.shards[ref.space][ref.shard].stopAt = m.status.Now
	m.emit(groupRunChange, StopRun{
		Space:  m.cfg.Spaces[ref.space].Name,
		Shard:  ref.shard,
		Term:   sh.Term,
		Reason: reason,
	})
}

// applyRunStopped applies the report that the run of term t has ended, which is what makes the term
// safe to release. Only a shard that told its run to stop can take the report, and only for that
// run's term: an owned shard's run was never told to stop, and any other term is not this run's. When
// the record no longer names the run, or the session it was held under is gone, there is nothing of the
// run's to release, so the shard goes to idle. A term the record gave this member while the run was going
// now goes to grant, unless the session is lost: then the record is takeable, and nothing is released.
//
// A Release of a shard with a run comes only after this: the shard enters ShardStateReleasing from a
// run only here, sendRelease runs only here and for a shard in that state, and grant's Release for a
// term that waited for this RunStopped goes out here too. The one Release that comes without a
// RunStopped is releaseWithoutRun's, which only a shard with no run reaches.
func (m *Machine) applyRunStopped(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	if !runToldToStop(sh) || t != sh.Term {
		return
	}
	sh.Stuck = false
	book := &m.shards[ref.space][ref.shard]
	if sh.State == ShardStateLost || book.knownGone || m.sessionLost() {
		next := book.grantAfterRun
		book.knownGone, book.grantAfterRun = false, 0
		m.setShardState(sh, ShardStateIdle)
		sh.Term = 0
		sh.Reason = StopNone
		if next != 0 && !m.sessionLost() {
			m.grant(ref, next)
		}
		return
	}
	m.setShardState(sh, ShardStateReleasing)
	m.sendRelease(ref)
}

// runToldToStop reports that the shard's run has its StopRun and has not yet reported its end. A degraded
// run keeps going and has no StopRun, so it is not one of these.
func runToldToStop(sh *ShardStatus) bool {
	return sh.State == ShardStateStopping || sh.State == ShardStateExpired || sh.State == ShardStateLost
}

// applyReleased applies the answer to a Release of term t. Only a shard releasing that term takes
// it. A wanted shard can then acquire again in this step, for a new term.
func (m *Machine) applyReleased(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateReleasing || t != sh.Term {
		return
	}
	m.setShardState(sh, ShardStateIdle)
	sh.Term = 0
	sh.Reason = StopNone
}

// sendRelease returns a Release of the shard's term and sets the reading of the next one. The shard
// is releasing, so it has no run: see applyRunStopped and releaseWithoutRun.
func (m *Machine) sendRelease(ref shardRef) {
	m.shards[ref.space][ref.shard].releaseAt = m.status.Now + m.cfg.Timing.RetryPeriod
	m.emit(groupRecord, Release{
		Space: m.cfg.Spaces[ref.space].Name,
		Shard: ref.shard,
		Term:  m.shardStatus(ref).Term,
	})
}

// expireAtDeadline is D2 (MCH-450, MCH-460): an owned shard stops at the session deadline itself, before
// any other holder may take over. A renewal that comes later does not undo it: the shard stops, releases
// and acquires again for a new term, as a moved shard does. A fenced space has a fence point downstream
// of its runs, so its run keeps going instead, degraded, and only an answered Confirm makes it owned
// again.
func (m *Machine) expireAtDeadline(ref shardRef) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateOwned || m.status.Session != SessionStateLate {
		return
	}
	if m.cfg.Spaces[ref.space].Fenced {
		m.setShardState(sh, ShardStateDegraded)
		m.emit(groupRunChange, Degrade{Space: m.cfg.Spaces[ref.space].Name, Shard: ref.shard, Term: sh.Term})
		return
	}
	m.stopRun(ref, ShardStateExpired, StopExpired)
}

// stopIfUnwanted is D3 (MCH-500): a run the assignment no longer gives this member is told to stop,
// whether it is owned or degraded and whether or not the session is live, and the shard goes on stopping
// even if the assignment gives it back: it must stop, release and acquire again for a new term.
func (m *Machine) stopIfUnwanted(ref shardRef) {
	sh := m.shardStatus(ref)
	if (sh.State != ShardStateOwned && sh.State != ShardStateDegraded) || sh.Wanted {
		return
	}
	reason := StopMoved
	if m.status.Draining {
		reason = StopDraining
	}
	m.stopRun(ref, ShardStateStopping, reason)
}

// repeatConfirm is D4 (MCH-480): a degraded shard asks for its record to be read back while the session
// is live, every RetryPeriod, counted from the last one. The clock is below maxReading while live, so
// the next reading fits in a Duration.
func (m *Machine) repeatConfirm(ref shardRef) {
	sh := m.shardStatus(ref)
	book := &m.shards[ref.space][ref.shard]
	if !m.live || sh.State != ShardStateDegraded || m.status.Now < book.confirmAt {
		return
	}
	book.confirmPending = true
	book.confirmAt = m.status.Now + m.cfg.Timing.RetryPeriod
	m.emit(groupRecord, Confirm{Space: m.cfg.Spaces[ref.space].Name, Shard: ref.shard, Term: sh.Term})
}

// repeatRelease is D6: an unanswered Release goes again every RetryPeriod, counted from the last one,
// whether or not the session is live.
func (m *Machine) repeatRelease(ref shardRef) {
	if m.shardStatus(ref).State == ShardStateReleasing && m.status.Now >= m.shards[ref.space][ref.shard].releaseAt {
		m.sendRelease(ref)
	}
}

// markStuck is D7: a run that has outlived RenewDeadline since its StopRun is stuck. That changes
// nothing about the shard's release, which still waits for the run's RunStopped.
func (m *Machine) markStuck(ref shardRef) {
	sh := m.shardStatus(ref)
	if runToldToStop(sh) && m.status.Now >= m.stuckAt(ref) {
		sh.Stuck = true
	}
}

// stuckAt returns the reading at which a run told to stop counts as stuck.
func (m *Machine) stuckAt(ref shardRef) time.Duration {
	return m.shards[ref.space][ref.shard].stopAt + m.cfg.Timing.RenewDeadline
}

// acquireIfDue is D5: an idle, wanted shard asks for its record while the session is live, once
// RetryAt has passed. No timer runs while it is acquiring: the driver answers every Acquire once.
func (m *Machine) acquireIfDue(ref shardRef) {
	sh := m.shardStatus(ref)
	if !m.mayAcquire(sh) || sh.RetryAt > m.status.Now {
		return
	}
	m.setShardState(sh, ShardStateAcquiring)
	sh.RetryAt = 0
	m.emit(groupRecord, Acquire{Space: m.cfg.Spaces[ref.space].Name, Shard: ref.shard})
}

// mayAcquire reports that RetryAt is all that stands between the shard and an Acquire.
func (m *Machine) mayAcquire(sh *ShardStatus) bool {
	return m.live && sh.State == ShardStateIdle && sh.Wanted
}

// shardStatus returns the shard's entry in Status.
func (m *Machine) shardStatus(ref shardRef) *ShardStatus {
	return &m.status.Spaces[ref.space].Shards[ref.shard]
}

// setShardState moves the shard to state at the machine's reading.
func (m *Machine) setShardState(sh *ShardStatus, state ShardState) {
	sh.State = state
	sh.Since = m.status.Now
}

// armShardTimers arms each shard's timer: the RetryAt of a shard waiting for it, the next Release of
// a releasing shard, the next Confirm of a degraded shard while the session is live, and the reading at
// which a run told to stop counts as stuck. Each is after the clock: the RetryAt condition says so,
// repeatRelease and repeatConfirm have just returned what was due, and markStuck has just marked a run
// that was due. The step that brings the session back to live returns a degraded shard's Confirm, so
// the shard needs no timer while the session is not live. Any step that makes a shard able to acquire runs
// this again, so a shard that cannot acquire needs no timer. After a lost session no shard can acquire
// or is releasing, so only the stuck timer is left, and a done machine, whose shards are all idle, arms
// none.
func (m *Machine) armShardTimers(w *wakeTimer) {
	for i := range m.status.Spaces {
		shards := m.status.Spaces[i].Shards
		for j := range shards {
			ref := shardRef{space: i, shard: j}
			switch sh := &shards[j]; {
			case m.mayAcquire(sh) && sh.RetryAt > m.status.Now:
				w.arm(sh.RetryAt)
			case sh.State == ShardStateReleasing:
				w.arm(m.shards[i][j].releaseAt)
			case sh.State == ShardStateDegraded && m.live:
				w.arm(m.shards[i][j].confirmAt)
			case runToldToStop(sh) && !sh.Stuck:
				w.arm(m.stuckAt(ref))
			}
		}
	}
}
