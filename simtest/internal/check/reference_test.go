// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package check_test

import (
	"cmp"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// The kinds of the generated observations.
const (
	opAt = iota
	opMono
	opStart
	opRenewed
	opDegrade
	opRestore
	opStop
	opDown
	opKinds // how many kinds there are
)

// op is one generated change of a sim: a clock that moves, or an observation.
type op struct {
	kind   int
	holder string
	space  string
	shard  int
	term   core.Term
	d      time.Duration // the reading of opMono, the deadline of opRenewed
	t      kernel.Time   // the time of opAt
}

// move returns the change of a sim that o stands for.
func (o op) move() move {
	switch o.kind {
	case opAt:
		return at(o.t)
	case opMono:
		return mono(o.holder, o.d)
	case opStart:
		return func(s *sim) { s.c.StartRun(o.holder, o.space, o.shard, o.term) }
	case opRenewed:
		return renewed(o.holder, o.d)
	case opDegrade:
		return degrade(o.holder, o.space, o.shard, o.term)
	case opRestore:
		return restore(o.holder, o.space, o.shard, o.term)
	case opStop:
		return stopRun(o.holder, o.space, o.shard, o.term)
	default:
		return down(o.holder)
	}
}

// refShard names a shard of the reference.
type refShard struct {
	space string
	shard int
}

// refRun is a run that owns its shard in the reference.
type refRun struct {
	holder   string
	term     core.Term
	degraded bool
}

// refChange is a moment at which the number of runs that own a shard changed, and the number
// from then on.
type refChange struct {
	at   kernel.Time
	runs int
}

// reference is a second statement of the checks g2 and g5. It keeps the runs of each shard in
// the order they started and the whole log of each shard's changes, and decides both checks
// from scratch each time it is asked, with none of the Checker's bookkeeping.
type reference struct {
	spaces    []core.SpaceSpec
	now       kernel.Time
	monos     map[string]time.Duration
	deadlines map[string]time.Duration
	runs      map[refShard][]refRun
	changes   map[refShard][]refChange
}

func newReference(spaces []core.SpaceSpec) *reference {
	return &reference{
		spaces:    spaces,
		monos:     make(map[string]time.Duration),
		deadlines: make(map[string]time.Duration),
		runs:      make(map[refShard][]refRun),
		changes:   make(map[refShard][]refChange),
	}
}

// apply makes the change o stands for.
func (r *reference) apply(o op) {
	key := refShard{o.space, o.shard}
	same := func(run refRun) bool { return run.holder == o.holder && run.term == o.term }
	switch o.kind {
	case opAt:
		r.now = o.t
	case opMono:
		r.monos[o.holder] = o.d
	case opRenewed:
		r.deadlines[o.holder] = o.d
	case opStart:
		if !slices.ContainsFunc(r.runs[key], same) {
			r.runs[key] = append(r.runs[key], refRun{holder: o.holder, term: o.term})
			r.changed(key)
		}
	case opDegrade, opRestore:
		if i := slices.IndexFunc(r.runs[key], same); i >= 0 {
			r.runs[key][i].degraded = o.kind == opDegrade
		}
	case opStop:
		if i := slices.IndexFunc(r.runs[key], same); i >= 0 {
			r.runs[key] = slices.Delete(r.runs[key], i, i+1)
			r.changed(key)
		}
	case opDown:
		delete(r.deadlines, o.holder)
		for key, runs := range r.runs {
			kept := slices.DeleteFunc(runs, func(run refRun) bool { return run.holder == o.holder })
			if len(kept) != len(runs) {
				r.runs[key] = kept
				r.changed(key)
			}
		}
	}
}

// changed logs the number of runs that own the shard from now on.
func (r *reference) changed(key refShard) {
	r.changes[key] = append(r.changes[key], refChange{at: r.now, runs: len(r.runs[key])})
}

// fenced reports whether the shard's space is listed as fenced.
func (r *reference) fenced(space string) bool {
	return slices.ContainsFunc(r.spaces, func(sp core.SpaceSpec) bool { return sp.Name == space && sp.Fenced })
}

// g2 returns the message of the violation of g2 at this moment, or "".
func (r *reference) g2() string {
	var keys []refShard
	for key := range r.runs {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b refShard) int {
		return cmp.Or(strings.Compare(a.space, b.space), cmp.Compare(a.shard, b.shard))
	})
	for _, key := range keys {
		runs := slices.Clone(r.runs[key])
		if len(runs) == 0 {
			continue
		}
		slices.SortFunc(runs, func(a, b refRun) int {
			return cmp.Or(cmp.Compare(a.term, b.term), strings.Compare(a.holder, b.holder))
		})
		newest := runs[len(runs)-1]
		for _, o := range runs {
			if o.holder == newest.holder || (o.degraded && r.fenced(key.space)) {
				continue
			}
			reading, set := r.monos[o.holder]
			if !set {
				reading = time.Duration(r.now)
			}
			deadline, renewed := r.deadlines[o.holder]
			state := "owns it inside its deadline " + kernel.Time(deadline).String()
			if !renewed || reading >= deadline {
				state = "owns it past its deadline " + kernel.Time(deadline).String()
			}
			if o.degraded {
				state = "is degraded in an unfenced space"
			}
			return fmt.Sprintf("g2: %s/%d: %s (term %d) %s while %s (term %d) owns it",
				key.space, key.shard, o.holder, o.term, state, newest.holder, newest.term)
		}
	}
	return ""
}

// g5 returns the message of the violation of g5 for the recovery time rec and the bound, or "".
func (r *reference) g5(rec kernel.Time, bound time.Duration) string {
	point := rec.Add(bound)
	spaces := slices.Clone(r.spaces)
	slices.SortFunc(spaces, func(a, b core.SpaceSpec) int { return strings.Compare(a.Name, b.Name) })
	for _, sp := range spaces {
		for shard := range sp.Shards {
			// Walk the log: an ownerless interval runs from time 0, or from the change that
			// left no run, to the next change, or to 1 ns after now.
			from, ownerless := kernel.Time(0), true
			var found bool
			var to kernel.Time
			for _, ch := range r.changes[refShard{sp.Name, shard}] {
				switch {
				case ownerless && ch.runs > 0:
					ownerless = false
					if from <= point && point < ch.at {
						found, to = true, ch.at
					}
				case !ownerless && ch.runs == 0:
					ownerless, from = true, ch.at
				}
				if found {
					break
				}
			}
			if !found && ownerless && from <= point && point <= r.now {
				found, to = true, r.now+1
			}
			if found {
				return fmt.Sprintf("g5: %s/%d has no owner from %v to %v, which contains %v (recovery %v + bound %v)",
					sp.Name, shard, from, to, point, rec, kernel.Time(bound))
			}
		}
	}
	return ""
}

// The shards, holders and terms of the generated histories: few, so that runs meet.
var (
	refSpaces  = []core.SpaceSpec{{Name: "ledger", Shards: 2, Fenced: true}, {Name: "book", Shards: 3}}
	refShards  = []refShard{{"book", 0}, {"book", 1}, {"book", 2}, {"ledger", 0}, {"ledger", 1}, {"cache", 0}}
	refHolders = []string{hA, hB, hC}
)

// generate returns a history of n changes. safe histories keep to the fenced space and degrade
// a run before another holder starts one, so that many of their moments pass g2.
func generate(rng *rand.Rand, n int, safe bool) []op {
	var (
		ops    []op
		now    kernel.Time
		ref    = newReference(refSpaces) // follows the history, to aim observations at runs that exist
		holder = func() string { return refHolders[rng.IntN(len(refHolders))] }
	)
	add := func(o op) {
		ops = append(ops, o)
		ref.apply(o)
	}
	for len(ops) < n {
		key := refShards[rng.IntN(len(refShards))]
		if safe {
			key = refShard{"ledger", rng.IntN(2)}
		}
		o := op{kind: rng.IntN(opKinds), holder: holder(), space: key.space, shard: key.shard, term: 1 + core.Term(rng.Uint64N(5))}
		// Most observations of a run are of one that owns its shard.
		if runs := ref.runs[key]; len(runs) > 0 && rng.IntN(4) > 0 {
			run := runs[rng.IntN(len(runs))]
			o.holder, o.term = run.holder, run.term
		}
		switch o.kind {
		case opAt:
			now += kernel.Time(rng.Int64N(int64(2*time.Second))) * kernel.Time(rng.IntN(2)) // half of them stay
			o.t = now
		case opMono, opRenewed:
			o.d = time.Duration(rng.Int64N(int64(30 * time.Second)))
		case opStart:
			o.holder = holder()
			if safe {
				// Every run of another holder degrades first, and the new run is the newest.
				for _, run := range slices.Clone(ref.runs[key]) {
					o.term = max(o.term, run.term+1)
					if run.holder != o.holder && rng.IntN(20) > 0 {
						add(op{kind: opDegrade, holder: run.holder, space: key.space, shard: key.shard, term: run.term})
					}
				}
			}
		case opRestore:
			if safe && rng.IntN(10) > 0 {
				continue
			}
		case opDown:
			if rng.IntN(4) > 0 {
				continue
			}
		}
		add(o)
	}
	return ops
}

// TestOwnersAgainstReference compares g2 and g5 with the reference over generated histories:
// g2 at every moment of a history, each on a Checker that is asked once; the violation a
// Checker keeps when it is asked at some moments only; and g5 for recovery times and bounds
// around every change.
func TestOwnersAgainstReference(t *testing.T) {
	const histories, length = 400, 40
	var failing, passing int
	for seed := range uint64(histories) {
		rng := rand.New(rand.NewPCG(seed, 0x85)) //nolint:gosec // G404: a test's seeded walk.
		ops := generate(rng, length, seed%2 == 1)

		// g2 at every moment: a fresh Checker replays the history up to it and is asked once.
		for n := 1; n <= len(ops); n++ {
			s, ref := newSim(check.Config{Spaces: refSpaces, AssertG2: true}), newReference(refSpaces)
			for _, o := range ops[:n] {
				o.move()(s)
				ref.apply(o)
			}
			want := ref.g2()
			if got := message(s.c.Err("g2")); got != want {
				t.Fatalf("seed %d, after %d changes: Err(\"g2\") = %q, want %q\n%v", seed, n, got, want, ops[:n])
			}
			if want == "" {
				passing++
			} else {
				failing++
			}
		}

		// One Checker asked at some moments keeps the first violation it was asked about, and
		// one that does not assert g2 never has one. g5 is decided again at every call.
		s, ref := newSim(check.Config{Spaces: refSpaces, AssertG2: true}), newReference(refSpaces)
		quiet := newSim(check.Config{Spaces: refSpaces})
		kept := ""
		for n, o := range ops {
			o.move()(s)
			o.move()(quiet)
			ref.apply(o)
			if rng.IntN(3) == 0 {
				if kept == "" {
					kept = ref.g2()
				}
				if got := message(s.c.Err("g2")); got != kept {
					t.Fatalf("seed %d, asked after %d changes: Err(\"g2\") = %q, want %q\n%v", seed, n+1, got, kept, ops[:n+1])
				}
			}
			if err := quiet.c.Err("g2"); err != nil {
				t.Fatalf("seed %d: Err(\"g2\") = %v without AssertG2", seed, err)
			}
			for _, point := range []kernel.Time{0, ref.now - 1, ref.now, ref.now + 1, kernel.Time(rng.Int64N(int64(ref.now) + 2))} {
				rec := kernel.Time(rng.Int64N(int64(max(point, 0)) + 1))
				bound := point.Sub(rec)
				want := ref.g5(rec, bound)
				if got := message(s.c.Liveness(rec, bound)); got != want {
					t.Fatalf("seed %d, after %d changes: Liveness(%v, %v) = %q, want %q\n%v", seed, n+1, rec, bound, got, want, ops[:n+1])
				}
			}
		}
	}
	// The histories must show both outcomes often, or the comparison says little.
	if failing < 1000 || passing < 1000 {
		t.Errorf("%d moments fail g2 and %d pass it, want at least 1000 of each", failing, passing)
	}
}

// TestLivenessOutOfRange checks Liveness for times no run reaches: a sum beyond the largest
// time is that time, which no call has reached, and no interval contains a negative time.
func TestLivenessOutOfRange(t *testing.T) {
	s := newSim(check.Config{Spaces: []core.SpaceSpec{{Name: "book", Shards: 1}}})
	at(90 * sec)(s)
	tests := []struct {
		r    kernel.Time
		b    time.Duration
		want string
	}{
		{40 * sec, math.MaxInt64, ""},
		{math.MaxInt64, math.MaxInt64, ""},
		{-1, 0, ""},
		{10 * sec, -11 * time.Second, ""},
		{math.MinInt64, math.MinInt64, ""},
		{10 * sec, -10 * time.Second, "g5: book/0 has no owner from 0.000000000s to 90.000000001s, which contains 0.000000000s (recovery 10.000000000s + bound -10.000000000s)"},
	}
	for _, tt := range tests {
		if got := message(s.c.Liveness(tt.r, tt.b)); got != tt.want {
			t.Errorf("Liveness(%d, %d) = %q, want %q", tt.r, tt.b, got, tt.want)
		}
	}
}
