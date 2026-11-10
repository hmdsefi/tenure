// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package check

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// owner is a run that owns its shard: its holder started it in its term and has neither
// stopped it nor gone down since.
type owner struct {
	holder   string
	term     core.Term
	degraded bool // the run is degraded: its holder's session is not live
}

// gap is an ownerless interval of a shard that ended: no run owned the shard from from, which
// is included, to to, which is not.
type gap struct {
	from, to kernel.Time
}

// compareOwners orders the owners of a shard by term, then by holder.
func compareOwners(a, b owner) int {
	return cmp.Or(cmp.Compare(a.term, b.term), strings.Compare(a.holder, b.holder))
}

// compareShards orders shards by space name, then by shard number.
func compareShards(a, b shardKey) int {
	return cmp.Or(strings.Compare(a.space, b.space), cmp.Compare(a.shard, b.shard))
}

// find returns the position of holder's run in term among the owners of the shard, or the
// position at which it would be inserted and false.
func (s *shardState) find(holder string, term core.Term) (int, bool) {
	return slices.BinarySearchFunc(s.owners, owner{holder: holder, term: term}, compareOwners)
}

// own makes holder's run in term an owner of the shard. A run that already owns the shard
// stays as it is, degraded or not: a second start of it is g1's to report. A holder that
// already owns the shard in another term owns it twice from now on, once per run, and each
// run is degraded, restored and stopped by its own term.
func (c *Checker) own(s *shardState, holder string, term core.Term) {
	i, found := s.find(holder, term)
	if found {
		return
	}
	switch len(s.owners) {
	case 0:
		// The ownerless interval ends here. An empty one is not kept: it contains no time.
		if now := c.cfg.Now(); now > s.since {
			s.gaps = append(s.gaps, gap{from: s.since, to: now})
		}
	case 1:
		c.shared++
	}
	s.owners = slices.Insert(s.owners, i, owner{holder: holder, term: term})
}

// disown updates the Checker after before owners of the shard became len(s.owners): the shard
// may have stopped being shared, and an ownerless interval starts when the last owner left.
func (c *Checker) disown(s *shardState, before int) {
	if before >= 2 && len(s.owners) < 2 {
		c.shared--
	}
	if before > 0 && len(s.owners) == 0 {
		s.since = c.cfg.Now()
	}
}

// owning returns the shard's state and the position of holder's run in term among its owners,
// or nil when that run does not own the shard. It creates no state: an observation of a run
// that owns nothing leaves the Checker as it was.
func (c *Checker) owning(holder, space string, shard int, term core.Term) (*shardState, int) {
	s := c.shards[shardKey{space, shard}]
	if s == nil {
		return nil, 0
	}
	i, found := s.find(holder, term)
	if !found {
		return nil, 0
	}
	return s, i
}

// Renewed reports holder's session deadline, a reading of holder's own monotonic clock: holder
// may own its shards of unfenced spaces while its clock reads less. The last deadline reported
// replaces the one before, whether it is later or not.
func (c *Checker) Renewed(holder string, deadline time.Duration) {
	c.deadlines[holder] = deadline
}

// Degrade reports that holder's run of the shard in term is degraded: holder's session is not
// live, and the run goes on because its space is fenced. A degraded run still owns its shard.
// Degrade changes nothing when the run does not own the shard (it was never started, it was
// stopped, or holder went down) or is degraded already.
func (c *Checker) Degrade(holder, space string, shard int, term core.Term) {
	if s, i := c.owning(holder, space, shard, term); s != nil {
		s.owners[i].degraded = true
	}
}

// Restore reports that holder's run of the shard in term is no longer degraded. It changes
// nothing when the run does not own the shard or is not degraded.
func (c *Checker) Restore(holder, space string, shard int, term core.Term) {
	if s, i := c.owning(holder, space, shard, term); s != nil {
		s.owners[i].degraded = false
	}
}

// StopRun reports that holder's machine stopped its run of the shard in term: from now on the
// run does not own the shard, however long it takes to wind down. StopRun changes nothing, and
// fails no check, when the run does not own the shard: it was never started, it was stopped
// before, holder went down, or holder's run of the shard is in another term.
func (c *Checker) StopRun(holder, space string, shard int, term core.Term) {
	s, i := c.owning(holder, space, shard, term)
	if s == nil {
		return
	}
	before := len(s.owners)
	s.owners = slices.Delete(s.owners, i, i+1)
	c.disown(s, before)
}

// Down reports that holder's node crashed or exited: from now on none of holder's runs owns a
// shard. The deadline and the last reading of holder stay, for a node that comes back with the
// same holder string. Down changes nothing for a holder that owns no shard.
func (c *Checker) Down(holder string) {
	// The order of the shards does not matter: each one changes on its own, every ownerless
	// interval that starts here starts at the same time, and nothing is reported.
	for _, s := range c.shards {
		before := len(s.owners)
		s.owners = slices.DeleteFunc(s.owners, func(o owner) bool { return o.holder == holder })
		c.disown(s, before)
	}
}

// Liveness decides the check g5: every shard of Config.Spaces has an owner at the time r + b,
// where r is the time the faults stopped and b the bound on how long a shard may then stay
// without an owner. It returns nil when that holds, and otherwise the violation of the first
// shard, by space name and then shard number, that had no owner at r + b. Unlike Err it keeps
// nothing: every call decides again, for its own r and b.
//
// A shard has no owner from time 0 to the first StartRun for it, and from each StopRun or Down
// that leaves it without an owning run to the next StartRun. The intervals are half-open: the
// shard is owned at the instant of that StartRun, and ownerless at the instant of that StopRun
// or Down. An interval still open at the call ends one nanosecond after Now(), so it contains
// Now() and nothing later: a call made before r + b passes for such a shard. A shard no
// observation named has been ownerless since time 0. The shards of a space that Config.Spaces
// does not list, and shard numbers outside a listed space, are not checked.
func (c *Checker) Liveness(r kernel.Time, b time.Duration) error {
	at := r.Add(b)
	now := c.cfg.Now()
	for _, sp := range c.spaces {
		for shard := range sp.Shards {
			key := shardKey{sp.Name, shard}
			if g, ok := c.ownerless(key, at, now); ok {
				return violation(g5, "%v has no owner from %v to %v, which contains %v (recovery %v + bound %v)",
					key, g.from, g.to, at, r, kernel.Time(b))
			}
		}
	}
	return nil
}

// ownerless returns the ownerless interval of the shard that contains at, when there is one.
// now is the time of the call, which ends an interval that is still open.
func (c *Checker) ownerless(key shardKey, at, now kernel.Time) (gap, bool) {
	s := c.shards[key]
	if s == nil {
		s = &shardState{} // no observation: ownerless since time 0
	}
	// The intervals are disjoint, so at most one contains at.
	for _, g := range s.gaps {
		if g.from <= at && at < g.to {
			return g, true
		}
	}
	if len(s.owners) == 0 && s.since <= at && at <= now {
		return gap{from: s.since, to: now.Add(1)}, true
	}
	return gap{}, false
}

// checkG2 decides g2 over the owners of this moment and records the first violation. Err
// documents the rule.
func (c *Checker) checkG2() {
	if c.shared == 0 {
		return
	}
	keys := make([]shardKey, 0, c.shared)
	for key, s := range c.shards {
		if len(s.owners) >= 2 {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, compareShards) // the map's order must not choose the violation reported
	for _, key := range keys {
		owners := c.shards[key].owners
		newest := owners[len(owners)-1]
		for _, o := range owners[:len(owners)-1] {
			if o.holder == newest.holder || (o.degraded && c.fenced[key.space]) {
				continue
			}
			deadline, renewed := c.deadlines[o.holder]
			var state string
			switch {
			case o.degraded:
				state = "is degraded in an unfenced space"
			case !renewed || c.mono(o.holder) >= deadline:
				state = "owns it past its deadline " + kernel.Time(deadline).String()
			default:
				state = "owns it inside its deadline " + kernel.Time(deadline).String()
			}
			c.fail(g2, "%v: %s (term %d) %s while %s (term %d) owns it",
				key, o.holder, o.term, state, newest.holder, newest.term)
			return
		}
	}
}

// mono returns the reading of holder's monotonic clock now.
func (c *Checker) mono(holder string) time.Duration {
	if c.cfg.Mono == nil {
		return time.Duration(c.cfg.Now())
	}
	return c.cfg.Mono(holder)
}
