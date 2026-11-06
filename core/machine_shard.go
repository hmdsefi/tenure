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
}

// noShard is the target of an input that names no shard the machine has.
var noShard = shardRef{space: -1, shard: -1}

// shardRef names one shard by its index in cfg.Spaces and its number.
type shardRef struct {
	space, shard int
}

// inputShard returns the shard that in names. Only Acquired, RunStopped and Released have an effect
// so far, so the other shard inputs name none.
func (m *Machine) inputShard(in Input) shardRef {
	switch v := in.(type) {
	case Acquired:
		return m.findShard(v.Space, v.Shard)
	case RunStopped:
		return m.findShard(v.Space, v.Shard)
	case Released:
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

// applyShardInput is D1: it applies the input that names the shard. Only Acquired, RunStopped and
// Released have an effect so far.
func (m *Machine) applyShardInput(ref shardRef, in Input) {
	switch v := in.(type) {
	case Acquired:
		m.applyAcquired(ref, v.Term)
	case RunStopped:
		m.applyRunStopped(ref, v.Term)
	case Released:
		m.applyReleased(ref, v.Term)
	}
}

// applyAcquired applies a grant of term t to the shard. A shard that is not acquiring ignores it,
// and so does a shard told there is no grant (term 0), which goes on acquiring like one told
// AcquireFailed until the machine handles those answers.
func (m *Machine) applyAcquired(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateAcquiring || t == 0 {
		return
	}
	book := &m.shards[ref.space][ref.shard]
	if !m.live || !sh.Wanted || m.status.Draining || t <= book.highestStarted {
		m.dropGrant(sh)
		return
	}
	m.setShardState(sh, ShardStateOwned)
	sh.Term = t
	sh.Holder = m.cfg.Holder
	book.highestStarted = t
	m.emit(groupRunStart, StartRun{
		Space:    m.cfg.Spaces[ref.space].Name,
		Shard:    ref.shard,
		Term:     t,
		Deadline: m.status.Deadline,
	})
}

// dropGrant handles a grant that cannot start a run. The machine cannot release a grant yet, so
// the record stays with this member until its lease ends: the shard goes back to idle with term 0
// and no run, its Holder says who the record names, and it may try again after RetryPeriod. The
// wait keeps a term that can never start from asking the store again within the same step. No
// StartRun comes out of this.
func (m *Machine) dropGrant(sh *ShardStatus) {
	m.setShardState(sh, ShardStateIdle)
	sh.Holder = m.cfg.Holder
	sh.RetryAt = m.status.Now + m.cfg.Timing.RetryPeriod
}

// applyRunStopped applies the report that the run of term t has ended, which is what makes the term
// safe to release. Only a shard that told its run to stop can take the report, and only for that
// run's term: an owned shard's run was never told to stop, and any other term is not this run's.
//
// This is the only way into ShardStateReleasing, and sendRelease runs only here and for a shard in
// that state, so no Release of a run's term comes before that run's RunStopped.
func (m *Machine) applyRunStopped(ref shardRef, t Term) {
	sh := m.shardStatus(ref)
	if sh.State != ShardStateStopping || t != sh.Term {
		return
	}
	m.setShardState(sh, ShardStateReleasing)
	sh.Stuck = false
	m.sendRelease(ref)
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
// is releasing, so its run is over: see applyRunStopped.
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
	if sh.State == ShardStateStopping && m.status.Now >= m.stuckAt(ref) {
		sh.Stuck = true
	}
}

// stuckAt returns the reading at which the run of a stopping shard counts as stuck.
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
// a releasing shard, and the reading at which a stopping shard's run counts as stuck. Each is after
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
			case sh.State == ShardStateStopping && !sh.Stuck:
				w.arm(m.stuckAt(ref))
			}
		}
	}
}
