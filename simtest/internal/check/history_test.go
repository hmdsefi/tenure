// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package check_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// allChecks are the names Err takes.
var allChecks = []string{"g1", "g2", "g3", "g4", "monotonic"}

// historyHolders are the holders of the generated histories.
var historyHolders = []string{hA, hB, hC, "m4/0d"}

// historyLeases are the lease durations of the generated grants.
var historyLeases = []time.Duration{1500 * time.Millisecond, 15 * time.Second}

// termModel is one granted term of a generated history.
type termModel struct {
	term     core.Term
	grantee  string
	acquired bool
	started  bool
}

// shardModel is what a generated history has done to one shard.
type shardModel struct {
	space  string
	shard  int
	terms  []*termModel // the granted terms, ascending
	holder string       // the holder the record names; "" before a grant and after a release
	wrote  *termModel   // the term of the last accepted write; nil before a write
}

func (s *shardModel) String() string { return fmt.Sprintf("%s/%d", s.space, s.shard) }

// top returns the largest granted term, or 0 before a grant.
func (s *shardModel) top() core.Term {
	if len(s.terms) == 0 {
		return 0
	}
	return s.terms[len(s.terms)-1].term
}

// find returns the model of a granted term, or nil.
func (s *shardModel) find(term core.Term) *termModel {
	for _, tm := range s.terms {
		if tm.term == term {
			return tm
		}
	}
	return nil
}

// ungranted returns the terms from, from+1, … up to three above the largest granted term that
// the shard's store never granted. It is never empty for a from at or below that end.
func (s *shardModel) ungranted(from core.Term) []core.Term {
	var terms []core.Term
	for term := from; term <= s.top()+3; term++ {
		if s.find(term) == nil {
			terms = append(terms, term)
		}
	}
	return terms
}

// history generates the observations of a legal history from a seed, reports each one to a
// Checker and keeps its own model of them, from which it derives the message a violation must
// carry. It is a second, simpler statement of the checks: it knows which observations are
// legal, not how the Checker decides.
type history struct {
	t        *testing.T
	rng      *rand.Rand
	c        *check.Checker
	shards   []*shardModel
	readings map[string]time.Duration // each holder's last reading; looked up, never ranged over
	log      []string                 // the observations reported, for a failure's message
}

func newHistory(t *testing.T, seed1, seed2 uint64) *history {
	t.Helper()
	return &history{
		t:   t,
		rng: rand.New(rand.NewPCG(seed1, seed2)), //nolint:gosec // G404: a test's seeded walk.
		c:   newChecker(),
		shards: []*shardModel{
			{space: "book", shard: 0},
			{space: "book", shard: 3},
			{space: "ledger", shard: 3},
			{space: "ledger", shard: 12},
		},
		readings: make(map[string]time.Duration),
		log:      []string{fmt.Sprintf("the history of seed (%d, %d)", seed1, seed2)},
	}
}

// The five methods below report one observation and log it.

func (h *history) grant(s *shardModel, g check.Grant) {
	g.Space, g.Shard = s.space, s.shard
	h.log = append(h.log, fmt.Sprintf("Grant(%+v)", g))
	h.c.Grant(g)
}

func (h *history) acquired(s *shardModel, holder string, term core.Term) {
	h.log = append(h.log, fmt.Sprintf("Acquired(%s, %v, %d)", holder, s, term))
	h.c.Acquired(holder, s.space, s.shard, term)
}

func (h *history) startRun(s *shardModel, holder string, term core.Term) {
	h.log = append(h.log, fmt.Sprintf("StartRun(%s, %v, %d)", holder, s, term))
	h.c.StartRun(holder, s.space, s.shard, term)
}

func (h *history) accepted(s *shardModel, holder string, term core.Term) {
	h.log = append(h.log, fmt.Sprintf("Accepted(%s, %v, %d)", holder, s, term))
	h.c.Accepted(check.Write{Space: s.space, Shard: s.shard, Term: term, Holder: holder})
}

func (h *history) reading(holder string, now time.Duration) {
	h.log = append(h.log, fmt.Sprintf("Reading(%s, %d)", holder, now))
	h.c.Reading(holder, now)
}

// holder draws a holder.
func (h *history) holder() string {
	return historyHolders[h.rng.IntN(len(historyHolders))]
}

// other draws a holder that is not holder.
func (h *history) other(holder string) string {
	for {
		if o := h.holder(); o != holder {
			return o
		}
	}
}

// pick draws one of the elements of xs that ok accepts. It reports false, without a draw, when
// ok accepts none.
func pick[T any](rng *rand.Rand, xs []T, ok func(T) bool) (T, bool) {
	var fit []T
	for _, x := range xs {
		if ok(x) {
			fit = append(fit, x)
		}
	}
	if len(fit) == 0 {
		var zero T
		return zero, false
	}
	return fit[rng.IntN(len(fit))], true
}

// legalStep reports one observation that breaks no check, or changes the model without an
// observation. A step that finds nothing to do in the shard it drew does nothing.
func (h *history) legalStep() {
	s := h.shards[h.rng.IntN(len(h.shards))]
	switch k := h.rng.IntN(12); {
	case k < 2 || len(s.terms) == 0:
		h.legalGrant(s)
	case k == 2:
		// The holder releases the shard: the record names nobody, and the next grant takes
		// the shard from nobody.
		s.holder = ""
	case k < 6:
		// A grantee learns of its grant: at once, late (after newer terms), or once more.
		tm := s.terms[h.rng.IntN(len(s.terms))]
		tm.acquired = true
		h.acquired(s, tm.grantee, tm.term)
	case k < 8:
		// A run starts once, in a term its holder acquired, however old the term is.
		if tm, ok := pick(h.rng, s.terms, func(tm *termModel) bool { return tm.acquired && !tm.started }); ok {
			tm.started = true
			h.startRun(s, tm.grantee, tm.term)
		}
	case k < 11:
		// A run writes in its own term until a write of a newer term is accepted.
		if tm, ok := pick(h.rng, s.terms, func(tm *termModel) bool {
			return tm.started && (s.wrote == nil || tm.term >= s.wrote.term)
		}); ok {
			s.wrote = tm
			h.accepted(s, tm.grantee, tm.term)
		}
	default:
		// A holder's clock stays or moves forward. Its first reading may be below zero.
		holder := h.holder()
		now, ok := h.readings[holder]
		if ok {
			now += time.Duration(h.rng.IntN(3)) * time.Millisecond
		} else {
			now = time.Duration(h.rng.Int64N(int64(time.Second))) - 500*time.Millisecond
		}
		h.readings[holder] = now
		h.reading(holder, now)
	}
}

// legalGrant grants the shard's next term: a term above every earlier one, and, when it takes
// the shard from another holder, with that holder's session ended or unrenewed for its lease.
func (h *history) legalGrant(s *shardModel) {
	g := check.Grant{
		Term:   s.top() + 1 + core.Term(h.rng.Uint64N(3)),
		Holder: h.holder(),
		Prev:   s.holder,
		Lease:  historyLeases[h.rng.IntN(len(historyLeases))],
	}
	switch h.rng.IntN(3) {
	case 0:
		g.Ended, g.Unseen = true, time.Duration(h.rng.Int64N(int64(2*g.Lease)))
	case 1:
		g.Unseen = g.Lease + time.Duration(h.rng.Int64N(int64(g.Lease)))
	default:
		if g.Prev == "" || g.Prev == g.Holder {
			// Nobody loses the shard, so the grant needs no evidence.
			g.Unseen = time.Duration(h.rng.Int64N(int64(g.Lease)))
		} else {
			g.Unseen = g.Lease
		}
	}
	s.terms = append(s.terms, &termModel{term: g.Term, grantee: g.Holder})
	s.holder = g.Holder
	h.grant(s, g)
}

// violation is one way to break a check: do reports the breaking observation and returns the
// exact message of every check it breaks. It reports false, with nothing reported to the
// Checker, when the history has nothing to break yet.
type violation struct {
	name string
	do   func(h *history) (map[string]string, bool)
}

func hasTerms(s *shardModel) bool { return len(s.terms) > 0 }

// violations has one entry for each message of the checks.
var violations = []violation{
	{"a grant below an earlier term", func(h *history) (map[string]string, bool) {
		s, ok := pick(h.rng, h.shards, hasTerms)
		if !ok {
			return nil, false
		}
		// Any term below the largest, granted before or not.
		term, holder := core.Term(h.rng.Uint64N(uint64(s.top()))), h.holder()
		h.grant(s, check.Grant{Term: term, Holder: holder, Prev: s.holder, Ended: true})
		return map[string]string{
			"g1": fmt.Sprintf("g1: %v: term %d granted to %s after term %d", s, term, holder, s.top()),
		}, true
	}},
	{"a grant of the largest term again", func(h *history) (map[string]string, bool) {
		s, ok := pick(h.rng, h.shards, hasTerms)
		if !ok {
			return nil, false
		}
		tm, holder := s.terms[len(s.terms)-1], h.holder()
		h.grant(s, check.Grant{Term: tm.term, Holder: holder, Prev: s.holder, Ended: true})
		return map[string]string{
			"g1": fmt.Sprintf("g1: %v: term %d granted to %s, already granted to %s", s, tm.term, holder, tm.grantee),
		}, true
	}},
	{"an acquisition of a term never granted", func(h *history) (map[string]string, bool) {
		s := h.shards[h.rng.IntN(len(h.shards))]
		terms := s.ungranted(0)
		term, holder := terms[h.rng.IntN(len(terms))], h.holder()
		h.acquired(s, holder, term)
		return map[string]string{
			"g1": fmt.Sprintf("g1: %v: %s acquired term %d, which the store never granted", s, holder, term),
		}, true
	}},
	{"an acquisition of another holder's term", func(h *history) (map[string]string, bool) {
		s, ok := pick(h.rng, h.shards, hasTerms)
		if !ok {
			return nil, false
		}
		tm := s.terms[h.rng.IntN(len(s.terms))]
		holder := h.other(tm.grantee)
		h.acquired(s, holder, tm.term)
		return map[string]string{
			"g1": fmt.Sprintf("g1: %v: %s acquired term %d, granted to %s", s, holder, tm.term, tm.grantee),
		}, true
	}},
	{"a second start of a term", func(h *history) (map[string]string, bool) {
		started := func(tm *termModel) bool { return tm.started }
		s, ok := pick(h.rng, h.shards, func(s *shardModel) bool { return slices.ContainsFunc(s.terms, started) })
		if !ok {
			return nil, false
		}
		tm, _ := pick(h.rng, s.terms, started)
		holder := h.holder()
		h.startRun(s, holder, tm.term)
		want := map[string]string{
			"g1": fmt.Sprintf("g1: %v: term %d started twice, by %s and %s", s, tm.term, tm.grantee, holder),
		}
		if holder != tm.grantee {
			want["g4"] = fmt.Sprintf("g4: %v: %s started a run in term %d without a grant", s, holder, tm.term)
		}
		return want, true
	}},
	{"a start without the acquisition", func(h *history) (map[string]string, bool) {
		pending := func(tm *termModel) bool { return !tm.acquired }
		s, ok := pick(h.rng, h.shards, func(s *shardModel) bool { return slices.ContainsFunc(s.terms, pending) })
		if !ok {
			return nil, false
		}
		tm, _ := pick(h.rng, s.terms, pending)
		h.startRun(s, tm.grantee, tm.term)
		return map[string]string{
			"g1": fmt.Sprintf("g1: %v: %s started a run in term %d without acquiring it", s, tm.grantee, tm.term),
		}, true
	}},
	{"a start in a term never granted", func(h *history) (map[string]string, bool) {
		s := h.shards[h.rng.IntN(len(h.shards))]
		terms := s.ungranted(0)
		term, holder := terms[h.rng.IntN(len(terms))], h.holder()
		h.startRun(s, holder, term)
		return map[string]string{
			"g1": fmt.Sprintf("g1: %v: %s started a run in term %d without acquiring it", s, holder, term),
			"g4": fmt.Sprintf("g4: %v: %s started a run in term %d without a grant", s, holder, term),
		}, true
	}},
	{"a first start in another holder's term", func(h *history) (map[string]string, bool) {
		// The grantee may have acquired the term; the starter never did.
		idle := func(tm *termModel) bool { return !tm.started }
		s, ok := pick(h.rng, h.shards, func(s *shardModel) bool { return slices.ContainsFunc(s.terms, idle) })
		if !ok {
			return nil, false
		}
		tm, _ := pick(h.rng, s.terms, idle)
		holder := h.other(tm.grantee)
		h.startRun(s, holder, tm.term)
		return map[string]string{
			"g1": fmt.Sprintf("g1: %v: %s started a run in term %d without acquiring it", s, holder, tm.term),
			"g4": fmt.Sprintf("g4: %v: %s started a run in term %d without a grant", s, holder, tm.term),
		}, true
	}},
	{"a takeover without evidence", func(h *history) (map[string]string, bool) {
		s, ok := pick(h.rng, h.shards, func(s *shardModel) bool { return s.holder != "" })
		if !ok {
			return nil, false
		}
		g := check.Grant{
			Term:   s.top() + 1 + core.Term(h.rng.Uint64N(3)),
			Holder: h.other(s.holder),
			Prev:   s.holder,
			Lease:  historyLeases[h.rng.IntN(len(historyLeases))],
		}
		g.Unseen = time.Duration(h.rng.Int64N(int64(g.Lease)))
		h.grant(s, g)
		return map[string]string{
			"g4": fmt.Sprintf("g4: %v: term %d: %s took over from %s after %v without renewal, lease %v",
				s, g.Term, g.Holder, g.Prev, kernel.Time(g.Unseen), kernel.Time(g.Lease)),
		}, true
	}},
	{"a write below the last written term", func(h *history) (map[string]string, bool) {
		s, ok := pick(h.rng, h.shards, func(s *shardModel) bool { return s.wrote != nil && s.wrote.term > 0 })
		if !ok {
			return nil, false
		}
		// Any term below the last written one, granted or not, by its grantee or not.
		term, holder := core.Term(h.rng.Uint64N(uint64(s.wrote.term))), h.holder()
		if tm := s.find(term); tm != nil && h.rng.IntN(2) == 0 {
			holder = tm.grantee
		}
		h.accepted(s, holder, term)
		return map[string]string{
			"g3": fmt.Sprintf("g3: %v: write in term %d by %s accepted after term %d", s, term, holder, s.wrote.term),
		}, true
	}},
	{"a second writer in the last written term", func(h *history) (map[string]string, bool) {
		s, ok := pick(h.rng, h.shards, func(s *shardModel) bool { return s.wrote != nil })
		if !ok {
			return nil, false
		}
		holder := h.other(s.wrote.grantee)
		h.accepted(s, holder, s.wrote.term)
		return map[string]string{
			"g3": fmt.Sprintf("g3: %v: term %d written by %s and %s", s, s.wrote.term, s.wrote.grantee, holder),
		}, true
	}},
	{"a first write in another holder's term", func(h *history) (map[string]string, bool) {
		unwritten := func(s *shardModel) func(tm *termModel) bool {
			return func(tm *termModel) bool { return s.wrote == nil || tm.term > s.wrote.term }
		}
		s, ok := pick(h.rng, h.shards, func(s *shardModel) bool { return slices.ContainsFunc(s.terms, unwritten(s)) })
		if !ok {
			return nil, false
		}
		tm, _ := pick(h.rng, s.terms, unwritten(s))
		holder := h.other(tm.grantee)
		h.accepted(s, holder, tm.term)
		return map[string]string{
			"g3": fmt.Sprintf("g3: %v: %s wrote in term %d, granted to %s", s, holder, tm.term, tm.grantee),
		}, true
	}},
	{"a write in a term never granted", func(h *history) (map[string]string, bool) {
		s := h.shards[h.rng.IntN(len(h.shards))]
		from := core.Term(0)
		if s.wrote != nil {
			from = s.wrote.term + 1
		}
		terms := s.ungranted(from)
		term, holder := terms[h.rng.IntN(len(terms))], h.holder()
		h.accepted(s, holder, term)
		return map[string]string{
			"g3": fmt.Sprintf("g3: %v: %s wrote in term %d, which the store never granted", s, holder, term),
		}, true
	}},
	{"a reading below the last", func(h *history) (map[string]string, bool) {
		holder, ok := pick(h.rng, historyHolders, func(holder string) bool {
			_, ok := h.readings[holder]
			return ok
		})
		if !ok {
			return nil, false
		}
		last := h.readings[holder]
		now := last - 1 - time.Duration(h.rng.IntN(1000))
		h.reading(holder, now)
		return map[string]string{
			"monotonic": fmt.Sprintf("monotonic: %s read %v after %v", holder, kernel.Time(now), kernel.Time(last)),
		}, true
	}},
}

// messages returns the message of every check, "" for one that holds.
func (h *history) messages() map[string]string {
	got := make(map[string]string, len(allChecks))
	for _, name := range allChecks {
		got[name] = ""
		if err := h.c.Err(name); err != nil {
			got[name] = err.Error()
		}
	}
	return got
}

// mustReport fails the test unless every check reports exactly the message want has for it,
// and nothing when want does not name it.
func (h *history) mustReport(want map[string]string) {
	h.t.Helper()
	got := h.messages()
	for _, name := range allChecks {
		if got[name] != want[name] {
			h.t.Fatalf("Err(%q) = %q, want %q, after:\n  %s", name, got[name], want[name], strings.Join(h.log, "\n  "))
		}
	}
}

// TestGeneratedHistories reports generated legal histories, which break no check at any
// observation, then one observation that breaks a check, which must report the exact message
// the history's own model derives and leave the other checks alone. Whatever is reported after
// that, legal or not, the broken checks keep their first message.
func TestGeneratedHistories(t *testing.T) {
	const (
		seeds    = 60   // histories for each violation
		maxSteps = 5000 // a history that still has nothing to break by then fails the test
	)
	var kind uint64
	for _, v := range violations {
		kind++
		t.Run(v.name, func(t *testing.T) {
			for seed := range uint64(seeds) {
				h := newHistory(t, kind, seed)
				steps := 10 + h.rng.IntN(150)
				var want map[string]string
				for n := 0; ; n++ {
					if n >= steps {
						// A history that has nothing to break yet grows by one step and tries again.
						if w, ok := v.do(h); ok {
							want = w
							break
						}
					}
					if n == maxSteps {
						t.Fatalf("seed %d: nothing to break after %d steps", seed, n)
					}
					h.legalStep()
					h.mustReport(nil)
				}
				h.mustReport(want)

				for range 40 {
					if h.rng.IntN(2) == 0 {
						h.legalStep()
					} else {
						violations[h.rng.IntN(len(violations))].do(h)
					}
				}
				got := h.messages()
				for _, name := range allChecks {
					if msg, broken := want[name]; broken && got[name] != msg {
						t.Fatalf("seed %d: Err(%q) = %q after later observations, want the first message %q",
							seed, name, got[name], msg)
					}
				}
			}
		})
	}
}
