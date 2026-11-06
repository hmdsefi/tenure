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

	// stopAt is the reading of the run's StopRun. It means something only while the shard is
	// stopping.
	stopAt time.Duration

	// releaseAt is the reading of the next Release repeat. It means something only while the
	// shard is releasing.
	releaseAt time.Duration

	// knownGone says the record no longer names this run while the shard is stopping, so the run's
	// RunStopped has nothing to release. A lost shard needs no flag: its state says the same.
	knownGone bool
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

// stepShards is phase D. It visits the spaces by name and each space's shards in order, so that
// the actions of a group come out in that order. A done machine returns nothing, so its shards stay
// as they are until the machine handles a lost session.
func (m *Machine) stepShards(in Input) {
	if m.done() {
		return
	}
	target := m.inputShard(in)
	for i := range m.status.Spaces {
		shards := m.status.Spaces[i].Shards
		for shard := range shards {
			ref := shardRef{space: i, shard: shard}
			if ref == target {
				m.applyShardInput(ref, in)
			}
			m.stopIfUnwanted(ref)
			m.acquireIfDue(ref)
			m.repeatRelease(ref)
			m.markStuck(ref)
		}
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
// acquiring shard takes it, so an answer the shard has stopped waiting for does nothing.
func (m *Machine) applyAcquired(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateAcquiring {
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
func (m *Machine) applyAcquireFailed(ref shardRef, holder string, retryAfter time.Duration) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateAcquiring {
		return
	}
	sh.Holder = holder
	m.waitToAcquire(sh, min(max(retryAfter, m.cfg.Timing.RetryPeriod), m.cfg.Timing.LeaseDuration))
}

// waitToAcquire moves an acquiring shard to idle, with its next Acquire wait away.
func (m *Machine) waitToAcquire(sh *ShardStatus, wait time.Duration) {
	m.setShardState(sh, ShardStateIdle)
	sh.RetryAt = m.status.Now + wait
}

// grant applies a record that names this member in term t, whether an Acquired or a HolderChanged
// said so. The shard must be idle or acquiring, and so has no run. A grant the machine can run starts
// it, and any other is released without a run. That covers a term at or below the highest started:
// it never starts twice, and since the shard has no run, whatever run it had has returned.
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
// run's RunStopped still matches, and no Release of either term goes out.
func (m *Machine) applyHolderChanged(ref shardRef, holder string, t Term) {
	sh := m.shardStatus(ref)
	if t != 0 && t < sh.Term {
		return
	}
	mine := holder == m.cfg.Holder
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
		if !mine || t > sh.Term {
			m.loseRun(ref, holder)
		}
	case ShardStateStopping:
		if !mine || t > sh.Term {
			sh.Holder = holder
			m.shards[ref.space][ref.shard].knownGone = true
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
	case ShardStateLost:
		// The run has its StopRun and its RunStopped leads to idle, whatever the record says.
	case ShardStateDegraded, ShardStateExpired:
		// No shard reaches these states yet. The tasks that add them add their rows here.
	}
}

// loseRun tells an owned shard's run to stop because the record no longer names it. The shard's Term
// stays the run's until its RunStopped.
func (m *Machine) loseRun(ref shardRef, holder string) {
	sh := m.shardStatus(ref)
	m.setShardState(sh, ShardStateLost)
	sh.Reason = StopLost
	sh.Holder = holder
	m.shards[ref.space][ref.shard].stopAt = m.status.Now
	m.emit(groupRunChange, StopRun{
		Space:  m.cfg.Spaces[ref.space].Name,
		Shard:  ref.shard,
		Term:   sh.Term,
		Reason: StopLost,
	})
}

// applyRunStopped applies the report that the run of term t has ended, which is what makes the term
// safe to release. Only a shard that told its run to stop can take the report, and only for that
// run's term: an owned shard's run was never told to stop, and any other term is not this run's. When
// the record no longer names the run there is nothing to release, so the shard goes to idle.
//
// This is the only way from a run into ShardStateReleasing, and sendRelease runs only here and for a
// shard in that state, so no Release comes before the RunStopped of the shard's run. The one Release
// that comes without a RunStopped is releaseWithoutRun's, which only a shard with no run reaches.
func (m *Machine) applyRunStopped(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	if !runToldToStop(sh) || t != sh.Term {
		return
	}
	sh.Stuck = false
	book := &m.shards[ref.space][ref.shard]
	if sh.State == ShardStateLost || book.knownGone {
		book.knownGone = false
		m.setShardState(sh, ShardStateIdle)
		sh.Term = 0
		sh.Reason = StopNone
		return
	}
	m.setShardState(sh, ShardStateReleasing)
	m.sendRelease(ref)
}

// runToldToStop reports that the shard's run has its StopRun and has not yet reported its end.
func runToldToStop(sh *ShardStatus) bool {
	return sh.State == ShardStateStopping || sh.State == ShardStateLost
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

// stopIfUnwanted is D3: a run the assignment no longer gives this member is told to stop, and the
// shard goes on stopping even if the assignment gives it back: it must stop, release and acquire
// again for a new term.
func (m *Machine) stopIfUnwanted(ref shardRef) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateOwned || sh.Wanted {
		return
	}
	m.setShardState(sh, ShardStateStopping)
	sh.Reason = StopMoved
	if m.status.Draining {
		sh.Reason = StopDraining
	}
	m.shards[ref.space][ref.shard].stopAt = m.status.Now
	m.emit(groupRunChange, StopRun{
		Space:  m.cfg.Spaces[ref.space].Name,
		Shard:  ref.shard,
		Term:   sh.Term,
		Reason: sh.Reason,
	})
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
// a releasing shard, and the reading at which a run told to stop counts as stuck. Each is after
// the clock: the RetryAt condition says so, repeatRelease has just returned a Release that was due,
// and markStuck has just marked a run that was due. Any step that makes a shard able to acquire runs
// this again, so a shard that cannot acquire needs no timer. A done machine arms none.
func (m *Machine) armShardTimers(w *wakeTimer) {
	if m.done() {
		return
	}
	for i := range m.status.Spaces {
		shards := m.status.Spaces[i].Shards
		for j := range shards {
			ref := shardRef{space: i, shard: j}
			switch sh := &shards[j]; {
			case m.mayAcquire(sh) && sh.RetryAt > m.status.Now:
				w.arm(sh.RetryAt)
			case sh.State == ShardStateReleasing:
				w.arm(m.shards[i][j].releaseAt)
			case runToldToStop(sh) && !sh.Stuck:
				w.arm(m.stuckAt(ref))
			}
		}
	}
}
