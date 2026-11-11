// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package check_test

import (
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// sec is one second of global time.
const sec = kernel.Time(time.Second)

// spaces are the spaces of the tests' Checkers: one unfenced and one fenced.
var spaces = []core.SpaceSpec{
	{Name: "book", Shards: 16},
	{Name: "ledger", Shards: 4, Fenced: true},
}

// sim is a Checker and the clocks it reads, which a test sets.
type sim struct {
	c     *check.Checker
	now   kernel.Time              // the global time
	monos map[string]time.Duration // the holders' monotonic readings that differ from now
}

// newSim returns a sim at time 0 whose Checker has cfg's Spaces and AssertG2.
func newSim(cfg check.Config) *sim {
	s := &sim{monos: make(map[string]time.Duration)}
	cfg.Now = func() kernel.Time { return s.now }
	cfg.Mono = func(holder string) time.Duration {
		if d, ok := s.monos[holder]; ok {
			return d
		}
		return time.Duration(s.now)
	}
	s.c = check.New(cfg)
	return s
}

// move is one change of a sim: of its clocks, or an observation reported to its Checker.
type move func(s *sim)

// at moves the global time to t.
func at(t kernel.Time) move {
	return func(s *sim) { s.now = t }
}

// mono sets the reading of holder's monotonic clock.
func mono(holder string, d time.Duration) move {
	return func(s *sim) { s.monos[holder] = d }
}

// own reports the grant of a term of a shard, its acquisition and the start of its run.
func own(holder, space string, shard int, term core.Term) move {
	return func(s *sim) {
		s.c.Grant(check.Grant{Space: space, Shard: shard, Term: term, Holder: holder})
		s.c.Acquired(holder, space, shard, term)
		s.c.StartRun(holder, space, shard, term)
	}
}

func renewed(holder string, deadline time.Duration) move {
	return func(s *sim) { s.c.Renewed(holder, deadline) }
}

func degrade(holder, space string, shard int, term core.Term) move {
	return func(s *sim) { s.c.Degrade(holder, space, shard, term) }
}

func restore(holder, space string, shard int, term core.Term) move {
	return func(s *sim) { s.c.Restore(holder, space, shard, term) }
}

func stopRun(holder, space string, shard int, term core.Term) move {
	return func(s *sim) { s.c.StopRun(holder, space, shard, term) }
}

func down(holder string) move {
	return func(s *sim) { s.c.Down(holder) }
}

// message returns err's text, or "" for nil.
func message(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestATSIM24G2 checks g2: of the owners of a contested shard, every one but the newest must
// be a degraded owner in a fenced space, and each kind of owner that is not has its message.
func TestATSIM24G2(t *testing.T) {
	// Holder a owns book/3 in term 4 with the deadline 10 s on its clock, and b takes it in
	// term 5 while a's run goes on.
	contested := func(reading time.Duration) []move {
		return []move{
			own(hA, "book", 3, 4), renewed(hA, 10*time.Second), mono(hA, reading),
			own(hB, "book", 3, 5), renewed(hB, 30*time.Second),
		}
	}
	const (
		inside = "g2: book/3: " + hA + " (term 4) owns it inside its deadline 10.000000000s while " + hB + " (term 5) owns it"
		past   = "g2: book/3: " + hA + " (term 4) owns it past its deadline 10.000000000s while " + hB + " (term 5) owns it"
	)
	tests := []struct {
		name  string
		moves []move
		want  string
	}{
		{
			name:  "item 1: the older owner is inside its deadline",
			moves: contested(10*time.Second - 1),
			want:  inside,
		},
		{
			name:  "item 2: the older owner's clock reads its deadline",
			moves: contested(10 * time.Second),
			want:  past,
		},
		{
			name:  "item 2: the older owner is past its deadline",
			moves: contested(12 * time.Second),
			want:  past,
		},
		{
			name:  "item 2: the older owner reported no deadline",
			moves: []move{own(hA, "book", 3, 4), own(hB, "book", 3, 5), renewed(hB, 30*time.Second)},
			want:  "g2: book/3: " + hA + " (term 4) owns it past its deadline 0.000000000s while " + hB + " (term 5) owns it",
		},
		{
			name: "item 3: the older owner is degraded in an unfenced space",
			// Its reading is inside its deadline: item 3 comes before items 2 and 1.
			moves: append(contested(9*time.Second), degrade(hA, "book", 3, 4)),
			want:  "g2: book/3: " + hA + " (term 4) is degraded in an unfenced space while " + hB + " (term 5) owns it",
		},
		{
			name:  "item 3: a space that is not listed is unfenced",
			moves: []move{own(hA, "cache", 0, 4), degrade(hA, "cache", 0, 4), own(hB, "cache", 0, 5)},
			want:  "g2: cache/0: " + hA + " (term 4) is degraded in an unfenced space while " + hB + " (term 5) owns it",
		},
		{
			name: "a degraded older owner in a fenced space",
			moves: []move{
				own(hA, "ledger", 1, 4), renewed(hA, 10*time.Second), mono(hA, 9*time.Second),
				degrade(hA, "ledger", 1, 4), own(hB, "ledger", 1, 5),
			},
		},
		{
			name: "two degraded older owners in a fenced space",
			moves: []move{
				own(hA, "ledger", 1, 4), degrade(hA, "ledger", 1, 4),
				own(hB, "ledger", 1, 5), degrade(hB, "ledger", 1, 5),
				own(hC, "ledger", 1, 6),
			},
		},
		{
			name: "an older owner in a fenced space that is not degraded",
			moves: []move{
				own(hA, "ledger", 1, 4), renewed(hA, 10*time.Second), mono(hA, 9*time.Second),
				own(hB, "ledger", 1, 5),
			},
			want: "g2: ledger/1: " + hA + " (term 4) owns it inside its deadline 10.000000000s while " + hB + " (term 5) owns it",
		},
		{
			name: "an older owner in a fenced space that was restored",
			moves: []move{
				own(hA, "ledger", 1, 4), degrade(hA, "ledger", 1, 4), restore(hA, "ledger", 1, 4),
				own(hB, "ledger", 1, 5),
			},
			want: "g2: ledger/1: " + hA + " (term 4) owns it past its deadline 0.000000000s while " + hB + " (term 5) owns it",
		},
		{
			name: "a degraded newest owner excuses nobody",
			moves: []move{
				own(hA, "ledger", 1, 4), renewed(hA, 10*time.Second), mono(hA, 9*time.Second),
				own(hB, "ledger", 1, 5), degrade(hB, "ledger", 1, 5),
			},
			want: "g2: ledger/1: " + hA + " (term 4) owns it inside its deadline 10.000000000s while " + hB + " (term 5) owns it",
		},
		{
			name:  "one owner",
			moves: []move{own(hA, "book", 3, 4), degrade(hA, "book", 3, 4)},
		},
		{
			name:  "the older owner stopped before the newer one started",
			moves: []move{own(hA, "book", 3, 4), stopRun(hA, "book", 3, 4), own(hB, "book", 3, 5)},
		},
		{
			name:  "the older owner went down before the newer one started",
			moves: []move{own(hA, "book", 3, 4), own(hA, "book", 4, 9), down(hA), own(hB, "book", 3, 5), own(hB, "book", 4, 10)},
		},
		{
			name:  "owners of different shards",
			moves: []move{own(hA, "book", 3, 4), own(hB, "book", 4, 5), own(hC, "ledger", 3, 5)},
		},
		{
			name: "a holder that owns a shard in two terms does not contest it",
			// Each run is stopped by its own term: stopping term 4 leaves the run of term 5.
			moves: []move{own(hA, "book", 3, 4), own(hA, "book", 3, 5), stopRun(hA, "book", 3, 4)},
		},
		{
			name: "both runs of a holder are judged against a newer holder",
			moves: []move{
				own(hA, "book", 3, 4), own(hA, "book", 3, 5), renewed(hA, 10*time.Second), mono(hA, 9*time.Second),
				own(hB, "book", 3, 6),
				// The run of term 4 stops, and the run of term 5 still owns the shard.
				stopRun(hA, "book", 3, 4),
			},
			want: "g2: book/3: " + hA + " (term 5) owns it inside its deadline 10.000000000s while " + hB + " (term 6) owns it",
		},
		{
			name: "an older run of the newest owner's holder is not judged",
			moves: []move{
				own(hA, "book", 3, 4), own(hB, "book", 3, 5), renewed(hB, 10*time.Second), mono(hB, 11*time.Second),
				own(hA, "book", 3, 6),
			},
			want: "g2: book/3: " + hB + " (term 5) owns it past its deadline 10.000000000s while " + hA + " (term 6) owns it",
		},
		{
			name: "the newest owner has the largest term, whatever the order of the starts",
			moves: []move{
				own(hB, "book", 3, 5), own(hA, "book", 3, 4), renewed(hA, 10*time.Second), mono(hA, 9*time.Second),
			},
			want: inside,
		},
		{
			name: "equal terms: the larger holder string is the newest owner",
			moves: []move{
				func(s *sim) {
					s.c.StartRun(hB, "book", 3, 5)
					s.c.StartRun(hA, "book", 3, 5)
				},
			},
			want: "g2: book/3: " + hA + " (term 5) owns it past its deadline 0.000000000s while " + hB + " (term 5) owns it",
		},
		{
			name: "owners are scanned by term",
			// Both older owners are violations; the one with the lower term is reported.
			moves: []move{
				own(hC, "book", 3, 5), renewed(hC, 10*time.Second), mono(hC, 9*time.Second),
				own(hA, "book", 3, 6),
				own(hB, "book", 3, 4), renewed(hB, 20*time.Second), mono(hB, 21*time.Second),
			},
			want: "g2: book/3: " + hB + " (term 4) owns it past its deadline 20.000000000s while " + hA + " (term 6) owns it",
		},
		{
			name: "an excused owner does not hide the next one",
			moves: []move{
				own(hA, "ledger", 1, 4), degrade(hA, "ledger", 1, 4),
				own(hB, "ledger", 1, 5), renewed(hB, 10*time.Second), mono(hB, 9*time.Second),
				own(hC, "ledger", 1, 6),
			},
			want: "g2: ledger/1: " + hB + " (term 5) owns it inside its deadline 10.000000000s while " + hC + " (term 6) owns it",
		},
		{
			name: "shards are scanned by number, not by text",
			moves: []move{
				own(hA, "book", 10, 4), own(hB, "book", 10, 5),
				own(hA, "book", 9, 7), own(hB, "book", 9, 8),
			},
			want: "g2: book/9: " + hA + " (term 7) owns it past its deadline 0.000000000s while " + hB + " (term 8) owns it",
		},
		{
			name: "spaces are scanned by name, listed or not",
			moves: []move{
				own(hA, "ledger", 0, 4), own(hB, "ledger", 0, 5),
				own(hA, "book", 9, 7), own(hB, "book", 9, 8),
				own(hA, "atlas", 12, 2), own(hB, "atlas", 12, 3),
			},
			want: "g2: atlas/12: " + hA + " (term 2) owns it past its deadline 0.000000000s while " + hB + " (term 3) owns it",
		},
		{
			name: "the last deadline reported is the one compared",
			moves: []move{
				own(hA, "book", 3, 4), renewed(hA, 20*time.Second), renewed(hA, 10*time.Second), mono(hA, 15*time.Second),
				own(hB, "book", 3, 5),
			},
			want: past,
		},
		{
			name: "Down forgets the deadline",
			// The holder comes back under the same string and has reported no deadline since.
			moves: []move{
				renewed(hA, 10*time.Second), mono(hA, 9*time.Second), down(hA),
				own(hA, "book", 3, 4), own(hB, "book", 3, 5),
			},
			want: "g2: book/3: " + hA + " (term 4) owns it past its deadline 0.000000000s while " + hB + " (term 5) owns it",
		},
		{
			name: "a deadline reported after Down is the one compared",
			moves: []move{
				renewed(hA, 20*time.Second), mono(hA, 9*time.Second), down(hA), renewed(hA, 10*time.Second),
				own(hA, "book", 3, 4), own(hB, "book", 3, 5),
			},
			want: inside,
		},
		{
			name:  "Down of another holder keeps the deadline",
			moves: append(contested(9*time.Second), down(hC)),
			want:  inside,
		},
		{
			name: "observations of a run that does not own its shard change nothing",
			moves: append([]move{
				// Before the start, for another term, another shard and another holder.
				degrade(hA, "book", 3, 4), stopRun(hA, "book", 3, 4), restore(hA, "book", 3, 4),
			}, append(contested(9*time.Second),
				degrade(hA, "book", 3, 5), degrade(hA, "book", 4, 4), degrade(hC, "book", 3, 4),
				stopRun(hA, "book", 3, 5), stopRun(hA, "book", 4, 4), stopRun(hC, "book", 3, 4),
				stopRun(hA, "ledger", 3, 4), down(hC),
			)...),
			want: inside,
		},
		{
			name: "a second start of a run that owns its shard changes nothing",
			moves: append(contested(9*time.Second),
				degrade(hA, "book", 3, 4),
				func(s *sim) { s.c.StartRun(hA, "book", 3, 4) },
			),
			want: "g2: book/3: " + hA + " (term 4) is degraded in an unfenced space while " + hB + " (term 5) owns it",
		},
		{
			name: "a run started again after its stop owns the shard again",
			moves: append(contested(9*time.Second),
				stopRun(hA, "book", 3, 4),
				func(s *sim) { s.c.StartRun(hA, "book", 3, 4) },
			),
			want: inside,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, assert := range []bool{true, false} {
				s := newSim(check.Config{Spaces: spaces, AssertG2: assert})
				for _, m := range tt.moves {
					m(s)
				}
				want := tt.want
				if !assert {
					want = "" // a Checker that does not assert g2 passes every case
				}
				if got := message(s.c.Err("g2")); got != want {
					t.Errorf("AssertG2 %v: Err(\"g2\") = %q, want %q", assert, got, want)
				}
			}
		})
	}
}

// TestG2IsDecidedWhenAsked checks that g2 is decided over the owners of the moment Err is
// called, and that its first violation is kept from then on.
func TestG2IsDecidedWhenAsked(t *testing.T) {
	s := newSim(check.Config{Spaces: spaces, AssertG2: true})
	// Two owners that nobody asks about, until one stops.
	own(hA, "book", 3, 4)(s)
	own(hB, "book", 3, 5)(s)
	stopRun(hA, "book", 3, 4)(s)
	if err := s.c.Err("g2"); err != nil {
		t.Fatalf(`Err("g2") = %v, want nil: no shard has two owners now`, err)
	}

	own(hC, "book", 3, 6)(s)
	first := s.c.Err("g2")
	const want = "g2: book/3: " + hB + " (term 5) owns it past its deadline 0.000000000s while " + hC + " (term 6) owns it"
	if message(first) != want {
		t.Fatalf(`Err("g2") = %q, want %q`, message(first), want)
	}

	// The contest ends and another one starts on an earlier shard: the first violation stays.
	down(hB)(s)
	own(hA, "book", 0, 1)(s)
	own(hB, "book", 0, 2)(s)
	if again := s.c.Err("g2"); again != first {
		t.Errorf(`Err("g2") = %v after the contest ended, want the first error, %v`, again, first)
	}
}

// TestG2MonoDefault checks that a Checker without Mono reads a holder's clock as the global
// time.
func TestG2MonoDefault(t *testing.T) {
	tests := []struct {
		now  kernel.Time
		want string
	}{
		{10*sec - 1, "inside"},
		{10 * sec, "past"},
	}
	for _, tt := range tests {
		now := tt.now
		c := check.New(check.Config{Now: func() kernel.Time { return now }, AssertG2: true})
		c.StartRun(hA, "book", 3, 4)
		c.Renewed(hA, 10*time.Second)
		c.StartRun(hB, "book", 3, 5)
		want := "g2: book/3: " + hA + " (term 4) owns it " + tt.want + " its deadline 10.000000000s while " + hB + " (term 5) owns it"
		if got := message(c.Err("g2")); got != want {
			t.Errorf("at %v: Err(\"g2\") = %q, want %q", now, got, want)
		}
	}
}

// TestATSIM28Liveness checks g5: a shard may be ownerless up to, and not at, the recovery time
// plus the bound, over half-open ownerless intervals.
func TestATSIM28Liveness(t *testing.T) {
	const (
		r   = 40 * sec
		b   = 23_383_333_334 * time.Nanosecond
		end = kernel.Time(63_383_333_334) // r + b
		// what every message says of r and b
		tail = ", which contains 63.383333334s (recovery 40.000000000s + bound 23.383333334s)"
	)
	one := []core.SpaceSpec{{Name: "book", Shards: 1}}
	// Holder a owns book/0 from 1 s to 50 s, and b takes it at the time next.
	handoff := func(next kernel.Time) []move {
		return []move{
			at(1 * sec), own(hA, "book", 0, 4),
			at(50 * sec), stopRun(hA, "book", 0, 4),
			at(next), own(hB, "book", 0, 5),
			at(90 * sec),
		}
	}
	tests := []struct {
		name   string
		spaces []core.SpaceSpec
		moves  []move
		want   string
	}{
		{
			name:  "an interval that ends 1 ns before the bound",
			moves: handoff(end - 1),
		},
		{
			name: "an interval that ends at the bound",
			// Half-open: the shard has its owner at the instant of the StartRun.
			moves: handoff(end),
		},
		{
			name:  "an interval that contains the bound",
			moves: handoff(end + 1),
			want:  "g5: book/0 has no owner from 50.000000000s to 63.383333335s" + tail,
		},
		{
			name:  "a long interval that contains the bound",
			moves: handoff(80 * sec),
			want:  "g5: book/0 has no owner from 50.000000000s to 80.000000000s" + tail,
		},
		{
			name: "an interval that starts at the bound",
			// Half-open: the shard has no owner at the instant of the StopRun.
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(end), stopRun(hA, "book", 0, 4),
				at(70 * sec), own(hB, "book", 0, 5),
			},
			want: "g5: book/0 has no owner from 63.383333334s to 70.000000000s" + tail,
		},
		{
			name: "an interval that starts 1 ns after the bound",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(end + 1), stopRun(hA, "book", 0, 4),
				at(70 * sec),
			},
		},
		{
			name: "an empty interval at the bound",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(end), stopRun(hA, "book", 0, 4), own(hB, "book", 0, 5),
				at(90 * sec),
			},
		},
		{
			name: "an interval still open at a call 1 ns before the bound",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(50 * sec), stopRun(hA, "book", 0, 4),
				at(end - 1),
			},
		},
		{
			name: "an interval still open at a call at the bound",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(50 * sec), stopRun(hA, "book", 0, 4),
				at(end),
			},
			want: "g5: book/0 has no owner from 50.000000000s to 63.383333335s" + tail,
		},
		{
			name: "an interval still open at a later call",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(50 * sec), stopRun(hA, "book", 0, 4),
				at(90 * sec),
			},
			want: "g5: book/0 has no owner from 50.000000000s to 90.000000001s" + tail,
		},
		{
			name:  "a shard never owned",
			moves: []move{at(90 * sec)},
			want:  "g5: book/0 has no owner from 0.000000000s to 90.000000001s" + tail,
		},
		{
			name:  "a shard never owned, at a call before the bound",
			moves: []move{at(end - 1)},
		},
		{
			name: "a shard granted and acquired, and never started",
			moves: []move{
				at(1 * sec),
				func(s *sim) {
					s.c.Grant(check.Grant{Space: "book", Shard: 0, Term: 4, Holder: hA})
					s.c.Acquired(hA, "book", 0, 4)
				},
				at(90 * sec),
			},
			want: "g5: book/0 has no owner from 0.000000000s to 90.000000001s" + tail,
		},
		{
			name:  "a shard first owned after the bound",
			moves: []move{at(70 * sec), own(hA, "book", 0, 4), at(90 * sec)},
			want:  "g5: book/0 has no owner from 0.000000000s to 70.000000000s" + tail,
		},
		{
			name: "two owners that overlap leave no gap",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(60 * sec), own(hB, "book", 0, 5),
				at(end), stopRun(hA, "book", 0, 4),
				at(90 * sec),
			},
		},
		{
			name: "two runs of one holder that overlap leave no gap",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(60 * sec), own(hA, "book", 0, 5),
				at(end), stopRun(hA, "book", 0, 4),
				at(90 * sec),
			},
		},
		{
			name: "a degraded owner is an owner",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(50 * sec), degrade(hA, "book", 0, 4),
				at(90 * sec),
			},
		},
		{
			name: "the owner went down",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(50 * sec), down(hA),
				at(90 * sec),
			},
			want: "g5: book/0 has no owner from 50.000000000s to 90.000000001s" + tail,
		},
		{
			name: "one of two owners went down",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(40 * sec), own(hB, "book", 0, 5),
				at(50 * sec), down(hA),
				at(90 * sec),
			},
		},
		{
			name: "observations of runs that do not own the shard open no interval",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4),
				at(50 * sec), stopRun(hA, "book", 0, 5), stopRun(hB, "book", 0, 4), down(hB),
				at(90 * sec),
			},
		},
		{
			name: "an interval before the bound and one after it",
			moves: []move{
				at(10 * sec), own(hA, "book", 0, 4),
				at(20 * sec), stopRun(hA, "book", 0, 4),
				at(30 * sec), own(hB, "book", 0, 5),
				at(70 * sec), stopRun(hB, "book", 0, 5),
				at(80 * sec), own(hA, "book", 0, 6),
				at(90 * sec),
			},
		},
		{
			name:   "the first shard by number, not by text",
			spaces: []core.SpaceSpec{{Name: "book", Shards: 12}},
			moves: func() []move {
				// Every shard is owned from 1 s but book/9 and book/10.
				moves := []move{at(1 * sec)}
				for shard := range 12 {
					if shard != 9 && shard != 10 {
						moves = append(moves, own(hA, "book", shard, 4))
					}
				}
				return append(moves, at(90*sec))
			}(),
			want: "g5: book/9 has no owner from 0.000000000s to 90.000000001s" + tail,
		},
		{
			name:   "the first space by name, whatever the order of the list",
			spaces: []core.SpaceSpec{{Name: "book", Shards: 1}, {Name: "atlas", Shards: 2}},
			moves:  []move{at(1 * sec), own(hA, "atlas", 0, 4), at(90 * sec)},
			want:   "g5: atlas/1 has no owner from 0.000000000s to 90.000000001s" + tail,
		},
		{
			name: "shards outside the listed spaces are not checked",
			moves: []move{
				at(1 * sec), own(hA, "book", 0, 4), own(hA, "book", 1, 4), own(hA, "cache", 0, 4),
				at(50 * sec), stopRun(hA, "book", 1, 4), stopRun(hA, "cache", 0, 4),
				at(90 * sec),
			},
		},
		{
			name:   "no spaces",
			spaces: []core.SpaceSpec{},
			moves:  []move{at(90 * sec)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := check.Config{Spaces: tt.spaces}
			if tt.spaces == nil {
				cfg.Spaces = one
			}
			s := newSim(cfg)
			for _, m := range tt.moves {
				m(s)
			}
			if got := message(s.c.Liveness(r, b)); got != tt.want {
				t.Errorf("Liveness(%v, %v) = %q, want %q", r, b, got, tt.want)
			}
		})
	}
}

// TestLivenessKeepsNothing checks that each call of Liveness decides again, for its own
// recovery time and bound, and that the Checker's own copy of the spaces is the one checked.
func TestLivenessKeepsNothing(t *testing.T) {
	listed := []core.SpaceSpec{{Name: "book", Shards: 1}}
	s := newSim(check.Config{Spaces: listed})
	listed[0].Shards = 9 // New copied the list
	for _, m := range []move{
		at(10 * sec), own(hA, "book", 0, 4),
		at(20 * sec), stopRun(hA, "book", 0, 4),
		at(30 * sec), own(hB, "book", 0, 5),
		at(90 * sec),
	} {
		m(s)
	}
	tests := []struct {
		r    kernel.Time
		b    time.Duration
		want string
	}{
		{15 * sec, 5 * time.Second, "g5: book/0 has no owner from 20.000000000s to 30.000000000s, which contains 20.000000000s (recovery 15.000000000s + bound 5.000000000s)"},
		{15 * sec, 15 * time.Second, ""},
		{0, 0, "g5: book/0 has no owner from 0.000000000s to 10.000000000s, which contains 0.000000000s (recovery 0.000000000s + bound 0.000000000s)"},
		{0, 10 * time.Second, ""},
		{0, 30*time.Second - 1, "g5: book/0 has no owner from 20.000000000s to 30.000000000s, which contains 29.999999999s (recovery 0.000000000s + bound 29.999999999s)"},
		{80 * sec, 20 * time.Second, ""}, // after the call: nothing is known of that time yet
	}
	for _, tt := range tests {
		if got := message(s.c.Liveness(tt.r, tt.b)); got != tt.want {
			t.Errorf("Liveness(%v, %v) = %q, want %q", tt.r, tt.b, got, tt.want)
		}
	}
	for _, name := range []string{"g1", "g2", "g3", "g4", "monotonic"} {
		if err := s.c.Err(name); err != nil {
			t.Errorf("Err(%q) = %v, want nil: Liveness records no violation", name, err)
		}
	}
}

// TestNewRejectsDuplicateSpaces checks that New rejects a Config that names a space twice.
func TestNewRejectsDuplicateSpaces(t *testing.T) {
	const want = `check: Config.Spaces names "book" twice`
	defer func() {
		if got := recover(); got != want {
			t.Errorf("New panicked with %v, want %q", got, want)
		}
	}()
	check.New(check.Config{
		Now:    func() kernel.Time { return 0 },
		Spaces: []core.SpaceSpec{{Name: "book", Shards: 2}, {Name: "atlas", Shards: 1}, {Name: "book", Shards: 3, Fenced: true}},
	})
}

// TestOwners checks that Owners lists the holders whose runs own a shard now, each once, by
// term then holder, and nil for a shard nobody owns or no observation named.
func TestOwners(t *testing.T) {
	s := newSim(check.Config{Spaces: spaces})
	for _, m := range []move{
		at(sec), own(hB, "book", 0, 4),
		own(hA, "book", 0, 5),
		own(hB, "book", 0, 6), // hB's second run: hB is listed once, by its first
		own(hA, "book", 1, 2),
		at(2 * sec), stopRun(hA, "book", 1, 2),
	} {
		m(s)
	}
	tests := []struct {
		shard int
		want  []string
	}{
		{0, []string{hB, hA}},
		{1, nil}, // owned, then stopped
		{2, nil}, // never named
	}
	for _, tt := range tests {
		if got := s.c.Owners("book", tt.shard); !slices.Equal(got, tt.want) {
			t.Errorf("Owners(book, %d) = %q, want %q", tt.shard, got, tt.want)
		}
	}
	down(hB)(s)
	if got, want := s.c.Owners("book", 0), []string{hA}; !slices.Equal(got, want) {
		t.Errorf("after Down(%s): Owners(book, 0) = %q, want %q", hB, got, want)
	}
}
