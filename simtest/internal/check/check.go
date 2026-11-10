// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package check holds the invariant checker of the simulations.
//
// A simulation reports what it observes to a Checker: the store's grants, what each member
// learned and started, the writes a fenced downstream accepted and the clock readings each
// member stepped, the session deadlines each member reported, and the runs that degraded,
// stopped or went down with their member. The Checker decides four checks when an observation
// arrives, and keeps the first violation of each:
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
// It decides two checks when it is asked, from the runs it was told of. A run owns its shard
// from its start to its stop, or to the moment its holder goes down:
//
//   - g2 (Err): no two holders own one shard at once, unless the space is fenced and every
//     owner but the one with the newest term is degraded. Its first violation is kept too;
//   - g5 (Liveness): every shard has an owner at a given time after the faults stopped.
//     Nothing is kept: each call decides for the times it is given.
//
// The package is pure. A Checker starts no goroutine, takes no lock and reads no clock but the
// functions of its Config, and no message depends on the iteration order of a map, so equal
// observations in equal order give equal results. A Checker is not safe for concurrent use; a
// caller with several goroutines guards it with a mutex.
package check

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
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

// g5 is the name of the check Liveness decides. Err does not take it.
const g5 = "g5"

// Config configures a Checker.
type Config struct {
	// Now returns the global time of the observation being reported. It is required: New
	// rejects a nil Now. It dates the moments a shard gains its first owner and loses its
	// last, for Liveness.
	Now func() kernel.Time
	// Mono returns the reading of holder's monotonic clock now, which the check g2 compares
	// with the deadline holder reported. A nil Mono stands for time.Duration(Now()), the
	// reading of a clock that never drifts.
	Mono func(holder string) time.Duration
	// Spaces lists the spaces of the run. Liveness covers exactly their shards, 0 to Shards-1
	// of each, and the check g2 excuses a degraded owner only in a space listed as Fenced: a
	// space that is not listed is unfenced. New rejects two spaces with one name.
	Spaces []core.SpaceSpec
	// AssertG2 reports whether the check g2 is decided. While it is false, Err("g2") is nil.
	AssertG2 bool
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
	cfg       Config
	spaces    []core.SpaceSpec         // a copy of cfg.Spaces, by name
	fenced    map[string]bool          // whether each space of cfg.Spaces is fenced, by name
	shards    map[shardKey]*shardState // the shards with an observation
	contested []shardKey               // the shards two or more runs own, by space name then shard
	readings  map[string]time.Duration // each holder's last reading
	deadlines map[string]time.Duration // each holder's last reported session deadline
	errs      map[string]error         // the first violation of each check, by name
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

	owners []owner     // the runs that own the shard now, by term then holder
	since  kernel.Time // when the last owner left, 0 before the first owner; read while owners is empty
	gaps   []gap       // the ownerless intervals that ended, oldest first
}

// termState is what the Checker has observed of one term of a shard.
type termState struct {
	granted  bool   // the store granted the term
	grantee  string // the first holder the store granted the term to, when granted
	acquired bool   // the grantee learned of its grant
	started  bool   // a run was started in the term
	starter  string // the first holder that started a run in the term, when started
}

// New returns a Checker with no observations. It panics if cfg.Now is nil or cfg.Spaces names
// a space twice. It copies cfg.Spaces, so a later change of the caller's slice changes nothing.
func New(cfg Config) *Checker {
	if cfg.Now == nil {
		panic("check: Config.Now is nil")
	}
	spaces := slices.Clone(cfg.Spaces)
	slices.SortStableFunc(spaces, func(a, b core.SpaceSpec) int { return strings.Compare(a.Name, b.Name) })
	fenced := make(map[string]bool, len(spaces))
	for i, sp := range spaces {
		if i > 0 && spaces[i-1].Name == sp.Name {
			panic(fmt.Sprintf("check: Config.Spaces names %q twice", sp.Name))
		}
		fenced[sp.Name] = sp.Fenced
	}
	cfg.Spaces = spaces
	return &Checker{
		cfg:       cfg,
		spaces:    spaces,
		fenced:    fenced,
		shards:    make(map[shardKey]*shardState),
		readings:  make(map[string]time.Duration),
		deadlines: make(map[string]time.Duration),
		errs:      make(map[string]error),
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

// StartRun reports that holder started the run of the shard in term. From now until its
// StopRun or holder's Down the run owns the shard, whether or not g1 and g4 hold for it.
//
// It fails g4 when the store did not grant the term to holder. It fails g1 when a run was
// already started in the term, by any holder, and otherwise when holder did not acquire the
// term first: a second start is reported as such even when its holder never acquired the
// term, so the message names both holders.
func (c *Checker) StartRun(holder, space string, shard int, term core.Term) {
	key := shardKey{space, shard}
	s := c.shard(key)
	t := s.term(term)

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
	c.own(s, key, holder, term)
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
// The checks g1, g3, g4 and monotonic are decided when an observation arrives. The check g2 is
// decided here, over the runs that own a shard at the moment of the call, so a caller that
// wants every moment checked calls Err("g2") after every event; a violation that came and went
// between two calls is not seen. While Config.AssertG2 is false, Err("g2") decides nothing and
// returns nil.
//
// A shard is contested while runs of two or more holders own it. Its newest owner is the run
// with the largest term and, among equal terms, the larger holder string. Every other owner
// must be excused: it is degraded and its space is fenced, or it is another run of the newest
// owner's holder. The violation is the first owner that is not excused, scanning spaces by
// name, shards ascending and owners by term then holder. Its message says, in this order of
// preference, that the owner is degraded in an unfenced space, that it owns the shard past its
// deadline (its clock reads at least the deadline it last reported, or it reported none, which
// prints as the deadline 0), or that it owns the shard inside its deadline.
func (c *Checker) Err(name string) error {
	switch name {
	case g1, g3, g4, monotonic:
		return c.errs[name]
	case g2:
		if !c.cfg.AssertG2 {
			return nil
		}
		if c.errs[g2] == nil {
			c.checkG2()
		}
		return c.errs[g2]
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
	c.errs[name] = violation(name, format, args...)
}

// violation returns the error of a check: the check's name, a colon and the formatted text.
func violation(name, format string, args ...any) error {
	return errors.New(name + ": " + fmt.Sprintf(format, args...))
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
