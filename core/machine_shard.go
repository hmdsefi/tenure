// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"slices"
)

// shardBook is a shard's bookkeeping that Status does not report.
type shardBook struct {
	// highestStarted is the highest term the shard has returned a StartRun for, so a term never
	// starts twice and an older grant never starts at all.
	highestStarted Term
}

// noShard is the target of an input that names no shard the machine has.
var noShard = shardRef{space: -1, shard: -1}

// shardRef names one shard by its index in cfg.Spaces and its number.
type shardRef struct {
	space, shard int
}

// inputShard returns the shard that in names. Only Acquired has an effect so far, so the other
// shard inputs name none.
func (m *Machine) inputShard(in Input) shardRef {
	if v, ok := in.(Acquired); ok {
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
// the actions of a group come out in that order.
func (m *Machine) stepShards(in Input) {
	target := m.inputShard(in)
	for i := range m.status.Spaces {
		shards := m.status.Spaces[i].Shards
		for shard := range shards {
			ref := shardRef{space: i, shard: shard}
			if ref == target {
				m.applyShardInput(ref, in)
			}
			m.acquireIfDue(ref)
		}
	}
}

// applyShardInput is D1: it applies the input that names the shard. Only Acquired has an effect so
// far.
func (m *Machine) applyShardInput(ref shardRef, in Input) {
	if v, ok := in.(Acquired); ok {
		m.applyAcquired(ref, v.Term)
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

// armShardTimers arms the RetryAt of each shard that is waiting for it. Each is after the clock:
// the condition says so. Any step that makes such a shard able to acquire runs this again, so
// a shard that cannot acquire needs no timer.
func (m *Machine) armShardTimers(w *wakeTimer) {
	for i := range m.status.Spaces {
		shards := m.status.Spaces[i].Shards
		for j := range shards {
			if sh := &shards[j]; m.mayAcquire(sh) && sh.RetryAt > m.status.Now {
				w.arm(sh.RetryAt)
			}
		}
	}
}
