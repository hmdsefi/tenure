// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package check holds the invariant checker of the simulations.
//
// A simulation reports what it observes to a Checker: the store's grants, what each member
// learned and started, the writes a fenced downstream accepted and the clock readings each
// member stepped. The Checker decides each observation as it arrives and keeps the first
// violation of each check:
//
//   - g1: the terms a store grants for a shard only grow, a term has one grantee, and only that
//     grantee acquires the term and starts its run, once;
//   - g3: the writes a fenced downstream accepts for a shard form a single-owner history: their
//     terms never go back, and every write of a term comes from the term's grantee;
//   - g4: a store grants a shard that another holder has only after that holder's session
//     ended, or after the deciding clock saw the session unrenewed for its whole lease, and a
//     run starts only in a term the store granted to its holder;
//   - monotonic: the readings a member steps never go back.
//
// The package is pure. A Checker starts no goroutine, takes no lock and reads no clock but the
// functions of its Config, and no message depends on the iteration order of a map, so equal
// observations in equal order give equal results. A Checker is not safe for concurrent use; a
// caller with several goroutines guards it with a mutex.
package check

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// The names Err takes.
const (
	g1        = "g1"
	g2        = "g2"
	g3        = "g3"
	g4        = "g4"
	monotonic = "monotonic"
)

// Config configures a Checker.
type Config struct {
	// Now returns the global time of the observation being reported. It is required: New
	// rejects a nil Now, although no check of this version reads it yet.
	Now func() kernel.Time
}

// Grant is one grant of a shard, as the store decided it.
type Grant struct {
	Space string
	Shard int
	// Term is the term the store granted.
	Term core.Term
	// Holder is the grantee.
	Holder string
	// Prev is the holder the shard's record named before the grant, or "" when it named none.
	Prev string
	// Ended reports whether Prev's session had ended (closed, replaced or lost) when the grant
	// was decided.
	Ended bool
	// Unseen is how long the deciding clock had seen Prev's session without a renewal. The
	// deciding clock is the store's where the store judges sessions, and the acquirer's where
	// acquirers do.
	Unseen time.Duration
	// Lease is the lease duration of Prev's session.
	Lease time.Duration
}

// Write is one write that a fenced downstream accepted.
type Write struct {
	Space string
	Shard int
	// Term is the term the write carried.
	Term core.Term
	// Holder is the writer.
	Holder string
}

// Checker decides the checks over the observations reported to it. The zero value is not
// usable; New returns one.
type Checker struct {
	cfg      Config
	shards   map[shardKey]*shardState
	readings map[string]time.Duration // each holder's last reading
	errs     map[string]error         // the first violation of each check, by name
}

// shardKey names a shard.
type shardKey struct {
	space string
	shard int
}

// String prints the shard as <space>/<shard>, for example book/3.
func (k shardKey) String() string {
	return k.space + "/" + strconv.Itoa(k.shard)
}

// shardState is what the Checker has observed of one shard.
type shardState struct {
	terms      map[core.Term]*termState // the terms of the shard's observations
	maxGranted core.Term                // the largest term granted; 0 before a grant

	written   bool      // some write was accepted
	lastTerm  core.Term // the term of the last accepted write; 0 before a write
	lastWrite string    // the first holder that wrote in lastTerm, when written
}

// termState is what the Checker has observed of one term of a shard.
type termState struct {
	granted  bool   // the store granted the term
	grantee  string // the first holder the store granted the term to, when granted
	acquired bool   // the grantee learned of its grant
	started  bool   // a run was started in the term
	starter  string // the first holder that started a run in the term, when started
}

// New returns a Checker with no observations. It panics if cfg.Now is nil.
func New(cfg Config) *Checker {
	if cfg.Now == nil {
		panic("check: Config.Now is nil")
	}
	return &Checker{
		cfg:      cfg,
		shards:   make(map[shardKey]*shardState),
		readings: make(map[string]time.Duration),
		errs:     make(map[string]error),
	}
}

// Grant reports that the store granted a shard.
//
// It fails g1 when the term is below a term granted earlier for the shard, and otherwise when
// the term was granted before, to any holder. It fails g4 when the grant takes the shard from
// another holder (Prev is neither "" nor Holder) whose session had not ended and that the
// deciding clock had seen unrenewed for less than its lease.
func (c *Checker) Grant(g Grant) {
	key := shardKey{g.Space, g.Shard}
	s := c.shard(key)
	t := s.term(g.Term)

	switch {
	case g.Term < s.maxGranted:
		c.fail(g1, "%v: term %d granted to %s after term %d", key, g.Term, g.Holder, s.maxGranted)
	case t.granted:
		c.fail(g1, "%v: term %d granted to %s, already granted to %s", key, g.Term, g.Holder, t.grantee)
	}
	if g.Prev != "" && g.Prev != g.Holder && !g.Ended && g.Unseen < g.Lease {
		c.fail(g4, "%v: term %d: %s took over from %s after %v without renewal, lease %v",
			key, g.Term, g.Holder, g.Prev, kernel.Time(g.Unseen), kernel.Time(g.Lease))
	}

	// A term keeps its first grantee, so a second grant of it cannot hide the first.
	if !t.granted {
		t.granted, t.grantee = true, g.Holder
	}
	s.maxGranted = max(s.maxGranted, g.Term)
}

// Acquired reports that holder learned that the store granted it term of the shard. Reporting
// the same acquisition again changes nothing.
//
// It fails g1 when the store never granted the term, or granted it to another holder.
func (c *Checker) Acquired(holder, space string, shard int, term core.Term) {
	key := shardKey{space, shard}
	t := c.shard(key).term(term)

	switch {
	case !t.granted:
		c.fail(g1, "%v: %s acquired term %d, which the store never granted", key, holder, term)
	case t.grantee != holder:
		c.fail(g1, "%v: %s acquired term %d, granted to %s", key, holder, term, t.grantee)
	default:
		t.acquired = true
	}
}

// StartRun reports that holder started the run of the shard in term.
//
// It fails g4 when the store did not grant the term to holder. It fails g1 when a run was
// already started in the term, by any holder, and otherwise when holder did not acquire the
// term first: a second start is reported as such even when its holder never acquired the
// term, so the message names both holders.
func (c *Checker) StartRun(holder, space string, shard int, term core.Term) {
	key := shardKey{space, shard}
	t := c.shard(key).term(term)

	own := t.granted && t.grantee == holder
	if !own {
		c.fail(g4, "%v: %s started a run in term %d without a grant", key, holder, term)
	}
	switch {
	case t.started:
		c.fail(g1, "%v: term %d started twice, by %s and %s", key, term, t.starter, holder)
	case !own || !t.acquired:
		c.fail(g1, "%v: %s started a run in term %d without acquiring it", key, holder, term)
	}

	if !t.started {
		t.started, t.starter = true, holder
	}
}

// Accepted reports that a fenced downstream accepted a write.
//
// It fails g3 for the first of these that holds: the term is below the term of the last
// accepted write of the shard; another holder wrote in the term; the store granted the term
// to another holder, or never granted it.
func (c *Checker) Accepted(w Write) {
	key := shardKey{w.Space, w.Shard}
	s := c.shard(key)

	if w.Term < s.lastTerm {
		c.fail(g3, "%v: write in term %d by %s accepted after term %d", key, w.Term, w.Holder, s.lastTerm)
		return
	}
	first := !s.written || w.Term > s.lastTerm // the first write of its term
	switch t := s.term(w.Term); {
	case !first && s.lastWrite != w.Holder:
		c.fail(g3, "%v: term %d written by %s and %s", key, w.Term, s.lastWrite, w.Holder)
	case !t.granted:
		c.fail(g3, "%v: %s wrote in term %d, which the store never granted", key, w.Holder, w.Term)
	case t.grantee != w.Holder:
		c.fail(g3, "%v: %s wrote in term %d, granted to %s", key, w.Holder, w.Term, t.grantee)
	}
	if first {
		s.written, s.lastTerm, s.lastWrite = true, w.Term, w.Holder
	}
}

// Reading reports a reading of holder's monotonic clock that its driver stepped.
//
// It fails monotonic when now is below holder's last reading. An equal reading passes.
func (c *Checker) Reading(holder string, now time.Duration) {
	last, ok := c.readings[holder]
	if ok && now < last {
		c.fail(monotonic, "%s read %v after %v", holder, kernel.Time(now), kernel.Time(last))
		return
	}
	c.readings[holder] = now
}

// Err returns the first violation of the check called name, or nil when the check has held so
// far. A check keeps its first violation: later observations never replace or clear it. The
// names are "g1", "g2", "g3", "g4" and "monotonic"; Err panics for another name.
//
// The check g2 needs the session deadlines and the runs that stopped, which this version of
// the Checker is not told, so Err("g2") is always nil.
func (c *Checker) Err(name string) error {
	switch name {
	case g1, g3, g4, monotonic:
		return c.errs[name]
	case g2:
		return nil
	default:
		panic(fmt.Sprintf("check: unknown check %q", name))
	}
}

// fail records a violation of the check called name unless the check already has one. The
// message is the name, a colon and the formatted text.
func (c *Checker) fail(name, format string, args ...any) {
	if c.errs[name] != nil {
		return
	}
	c.errs[name] = errors.New(name + ": " + fmt.Sprintf(format, args...))
}

// shard returns the state of the shard, which it creates on the first observation.
func (c *Checker) shard(key shardKey) *shardState {
	s := c.shards[key]
	if s == nil {
		s = &shardState{terms: make(map[core.Term]*termState)}
		c.shards[key] = s
	}
	return s
}

// term returns the state of a term of the shard, which it creates on the first observation.
func (s *shardState) term(term core.Term) *termState {
	t := s.terms[term]
	if t == nil {
		t = &termState{}
		s.terms[term] = t
	}
	return t
}
