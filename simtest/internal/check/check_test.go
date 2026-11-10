// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package check_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// The holders of the tests: a member ID and an incarnation, as the simulations build them.
const (
	hA = "m1/0a"
	hB = "m2/0b"
	hC = "m3/0c"
)

// checks are the names of the checks this file tests, in the order the tests compare them.
var checks = []string{"g1", "g3", "g4", "monotonic"}

// step is one observation reported to a Checker.
type step func(c *check.Checker)

// The steps below report observations of shard book/3.

func grant(term core.Term, holder string) step {
	return func(c *check.Checker) {
		c.Grant(check.Grant{Space: "book", Shard: 3, Term: term, Holder: holder})
	}
}

func takeover(g check.Grant) step {
	g.Space, g.Shard = "book", 3
	return func(c *check.Checker) { c.Grant(g) }
}

func acquired(holder string, term core.Term) step {
	return func(c *check.Checker) { c.Acquired(holder, "book", 3, term) }
}

func startRun(holder string, term core.Term) step {
	return func(c *check.Checker) { c.StartRun(holder, "book", 3, term) }
}

func accepted(term core.Term, holder string) step {
	return func(c *check.Checker) {
		c.Accepted(check.Write{Space: "book", Shard: 3, Term: term, Holder: holder})
	}
}

func reading(holder string, now time.Duration) step {
	return func(c *check.Checker) { c.Reading(holder, now) }
}

// owns reports the grant of a term, its acquisition and the start of its run.
func owns(term core.Term, holder string) step {
	return func(c *check.Checker) {
		grant(term, holder)(c)
		acquired(holder, term)(c)
		startRun(holder, term)(c)
	}
}

// newChecker returns a Checker whose global time stays 0 and that does not assert g2: no check
// this file tests depends on the time.
func newChecker() *check.Checker {
	return check.New(check.Config{Now: func() kernel.Time { return 0 }})
}

// scenario is a list of observations and the message each check must report after them. A
// check that want does not name must hold.
type scenario struct {
	name  string
	steps []step
	want  map[string]string
}

// run reports each scenario's observations to a new Checker and compares every check's result
// with the exact message.
func run(t *testing.T, tests []scenario) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newChecker()
			for _, s := range tt.steps {
				s(c)
			}
			for name := range tt.want {
				if !slices.Contains(checks, name) {
					t.Fatalf("the test wants a message of the unknown check %q", name)
				}
			}
			for _, name := range checks {
				got := ""
				if err := c.Err(name); err != nil {
					got = err.Error()
				}
				if got != tt.want[name] {
					t.Errorf("Err(%q) = %q, want %q", name, got, tt.want[name])
				}
			}
			if err := c.Err("g2"); err != nil {
				t.Errorf(`Err("g2") = %v, want nil`, err)
			}
		})
	}
}

// TestATSIM23GrantTerms checks g1: terms only grow, a term has one grantee, and only the
// grantee acquires the term and starts its run, once.
func TestATSIM23GrantTerms(t *testing.T) {
	const noAcquire = "g1: book/3: " + hA + " started a run in term 5 without acquiring it"
	run(t, []scenario{
		{
			name: "growing terms with one owner each",
			steps: []step{
				owns(4, hA), owns(5, hB), owns(7, hA),
				// Another shard and another space have their own terms.
				func(c *check.Checker) {
					c.Grant(check.Grant{Space: "book", Shard: 4, Term: 5, Holder: hC})
					c.Acquired(hC, "book", 4, 5)
					c.StartRun(hC, "book", 4, 5)
					c.Grant(check.Grant{Space: "ledger", Shard: 3, Term: 5, Holder: hC})
					c.Acquired(hC, "ledger", 3, 5)
					c.StartRun(hC, "ledger", 3, 5)
				},
			},
		},
		{
			name:  "the same acquisition reported twice",
			steps: []step{grant(5, hA), acquired(hA, 5), acquired(hA, 5), startRun(hA, 5), acquired(hA, 5)},
		},
		{
			name:  "a term below an earlier one",
			steps: []step{grant(7, hA), grant(5, hB)},
			want:  map[string]string{"g1": "g1: book/3: term 5 granted to " + hB + " after term 7"},
		},
		{
			name:  "a term granted before, to another holder",
			steps: []step{grant(5, hA), grant(5, hB)},
			want:  map[string]string{"g1": "g1: book/3: term 5 granted to " + hB + ", already granted to " + hA},
		},
		{
			name:  "a term granted before, to the same holder",
			steps: []step{grant(5, hA), grant(5, hA)},
			want:  map[string]string{"g1": "g1: book/3: term 5 granted to " + hA + ", already granted to " + hA},
		},
		{
			// Both grant rows hold for the second grant of term 5: the lower term is reported.
			name:  "a term granted before and below a later one",
			steps: []step{grant(5, hA), grant(7, hB), grant(5, hC)},
			want:  map[string]string{"g1": "g1: book/3: term 5 granted to " + hC + " after term 7"},
		},
		{
			// A term keeps its first grantee: the second grantee's write and run have no grant.
			name:  "a term granted twice and used by the second grantee",
			steps: []step{grant(5, hA), grant(5, hB), accepted(5, hB), startRun(hB, 5)},
			want: map[string]string{
				"g1": "g1: book/3: term 5 granted to " + hB + ", already granted to " + hA,
				"g3": "g3: book/3: " + hB + " wrote in term 5, granted to " + hA,
				"g4": "g4: book/3: " + hB + " started a run in term 5 without a grant",
			},
		},
		{
			name:  "an acquisition of a term never granted",
			steps: []step{grant(4, hA), acquired(hB, 5)},
			want:  map[string]string{"g1": "g1: book/3: " + hB + " acquired term 5, which the store never granted"},
		},
		{
			name: "an acquisition of a term granted for another shard",
			steps: []step{
				func(c *check.Checker) { c.Grant(check.Grant{Space: "book", Shard: 4, Term: 5, Holder: hB}) },
				acquired(hB, 5),
			},
			want: map[string]string{"g1": "g1: book/3: " + hB + " acquired term 5, which the store never granted"},
		},
		{
			name:  "an acquisition of another holder's term",
			steps: []step{grant(5, hA), acquired(hB, 5)},
			want:  map[string]string{"g1": "g1: book/3: " + hB + " acquired term 5, granted to " + hA},
		},
		{
			name:  "a run started without the acquisition",
			steps: []step{grant(5, hA), startRun(hA, 5)},
			want:  map[string]string{"g1": noAcquire},
		},
		{
			// The acquisition of another term of the shard is not the acquisition of term 5.
			name:  "a run started with the acquisition of another term",
			steps: []step{grant(4, hA), acquired(hA, 4), grant(5, hA), startRun(hA, 5)},
			want:  map[string]string{"g1": noAcquire},
		},
		{
			name:  "a run started without the acquisition and without the grant",
			steps: []step{startRun(hA, 5)},
			want: map[string]string{
				"g1": noAcquire,
				"g4": "g4: book/3: " + hA + " started a run in term 5 without a grant",
			},
		},
		{
			name:  "a term started twice by its holder",
			steps: []step{owns(5, hA), startRun(hA, 5)},
			want:  map[string]string{"g1": "g1: book/3: term 5 started twice, by " + hA + " and " + hA},
		},
		{
			// The second holder neither acquired the term nor holds its grant. The second start
			// is the g1 violation reported, so the message names both holders.
			name:  "a term started twice by two holders",
			steps: []step{owns(5, hA), startRun(hB, 5)},
			want: map[string]string{
				"g1": "g1: book/3: term 5 started twice, by " + hA + " and " + hB,
				"g4": "g4: book/3: " + hB + " started a run in term 5 without a grant",
			},
		},
		{
			name:  "the first violation is kept",
			steps: []step{grant(7, hA), grant(5, hB), acquired(hC, 9), grant(7, hC), startRun(hA, 7)},
			want:  map[string]string{"g1": "g1: book/3: term 5 granted to " + hB + " after term 7"},
		},
	})
}

// TestATSIM25SingleOwnerHistory checks g3: the writes a fenced downstream accepts for a shard
// have terms that never go back, and every write of a term comes from the term's grantee.
func TestATSIM25SingleOwnerHistory(t *testing.T) {
	run(t, []scenario{
		{
			name: "one writer per term, terms growing",
			steps: []step{
				grant(4, hA), accepted(4, hA), accepted(4, hA),
				grant(5, hB), accepted(5, hB),
				grant(7, hA), accepted(7, hA), accepted(7, hA),
				// A lower term of another shard or space is no step back.
				func(c *check.Checker) {
					c.Grant(check.Grant{Space: "book", Shard: 4, Term: 2, Holder: hC})
					c.Accepted(check.Write{Space: "book", Shard: 4, Term: 2, Holder: hC})
					c.Grant(check.Grant{Space: "ledger", Shard: 3, Term: 2, Holder: hC})
					c.Accepted(check.Write{Space: "ledger", Shard: 3, Term: 2, Holder: hC})
				},
			},
		},
		{
			// The store granted term 5 before the old owner's last write arrived: the fence
			// accepts it until the new owner raises the fence.
			name:  "a write of the old term before the first write of the new one",
			steps: []step{grant(4, hA), accepted(4, hA), grant(5, hB), accepted(4, hA), accepted(5, hB)},
		},
		{
			name:  "a write in a term below the last accepted one",
			steps: []step{grant(4, hA), accepted(4, hA), grant(5, hB), accepted(5, hB), accepted(4, hA)},
			want:  map[string]string{"g3": "g3: book/3: write in term 4 by " + hA + " accepted after term 5"},
		},
		{
			// The write also comes from a holder without the grant, in a term never granted;
			// the step back is the violation reported.
			name:  "a write in a lower term that was never granted",
			steps: []step{grant(5, hB), accepted(5, hB), accepted(4, hA)},
			want:  map[string]string{"g3": "g3: book/3: write in term 4 by " + hA + " accepted after term 5"},
		},
		{
			// hA also lacks the grant of term 5; the second writer is the violation reported.
			name:  "a write in a term another holder wrote",
			steps: []step{grant(5, hB), accepted(5, hB), accepted(5, hA)},
			want:  map[string]string{"g3": "g3: book/3: term 5 written by " + hB + " and " + hA},
		},
		{
			name:  "a write in a term granted to another holder",
			steps: []step{grant(5, hB), accepted(5, hA)},
			want:  map[string]string{"g3": "g3: book/3: " + hA + " wrote in term 5, granted to " + hB},
		},
		{
			name:  "a write in a term never granted",
			steps: []step{grant(4, hA), accepted(5, hA)},
			want:  map[string]string{"g3": "g3: book/3: " + hA + " wrote in term 5, which the store never granted"},
		},
		{
			name: "a write in a term granted for another shard",
			steps: []step{
				func(c *check.Checker) { c.Grant(check.Grant{Space: "book", Shard: 4, Term: 5, Holder: hA}) },
				accepted(5, hA),
			},
			want: map[string]string{"g3": "g3: book/3: " + hA + " wrote in term 5, which the store never granted"},
		},
		{
			name: "the first violation is kept",
			steps: []step{
				grant(5, hB), accepted(5, hA), accepted(5, hB), accepted(4, hC), accepted(9, hC),
			},
			want: map[string]string{"g3": "g3: book/3: " + hA + " wrote in term 5, granted to " + hB},
		},
	})
}

// TestATSIM26TakeoverEvidence checks g4: a grant takes a shard from another holder only after
// that holder's session ended or went unrenewed for its lease, and a run starts only in a term
// granted to its holder.
func TestATSIM26TakeoverEvidence(t *testing.T) {
	const lease = 15 * time.Second
	early := check.Grant{Term: 5, Holder: hB, Prev: hA, Unseen: lease - time.Nanosecond, Lease: lease}
	with := func(g check.Grant, change func(g *check.Grant)) check.Grant {
		change(&g)
		return g
	}
	const tookOver = "g4: book/3: term 5: " + hB + " took over from " + hA +
		" after 14.999999999s without renewal, lease 15.000000000s"
	run(t, []scenario{
		{
			name:  "no previous holder",
			steps: []step{takeover(with(early, func(g *check.Grant) { g.Prev = "" }))},
		},
		{
			name:  "a new term for the holder the record named",
			steps: []step{grant(4, hB), takeover(with(early, func(g *check.Grant) { g.Prev = hB }))},
		},
		{
			name:  "the previous session had ended",
			steps: []step{grant(4, hA), takeover(with(early, func(g *check.Grant) { g.Ended = true }))},
		},
		{
			name:  "unrenewed for exactly the lease",
			steps: []step{grant(4, hA), takeover(with(early, func(g *check.Grant) { g.Unseen = lease }))},
		},
		{
			name:  "unrenewed for longer than the lease",
			steps: []step{grant(4, hA), takeover(with(early, func(g *check.Grant) { g.Unseen = lease + time.Nanosecond }))},
		},
		{
			name:  "a takeover one nanosecond before the lease ran out",
			steps: []step{grant(4, hA), takeover(early)},
			want:  map[string]string{"g4": tookOver},
		},
		{
			name: "a takeover of a session renewed just before",
			steps: []step{takeover(check.Grant{
				Term: 12, Holder: hC, Prev: hB, Unseen: 0, Lease: 1500 * time.Millisecond,
			})},
			want: map[string]string{
				"g4": "g4: book/3: term 12: " + hC + " took over from " + hB +
					" after 0.000000000s without renewal, lease 1.500000000s",
			},
		},
		{
			name:  "a run started with its grant",
			steps: []step{grant(5, hA), acquired(hA, 5), startRun(hA, 5)},
		},
		{
			name:  "a run started without a grant",
			steps: []step{grant(4, hA), acquired(hA, 4), startRun(hA, 5)},
			want: map[string]string{
				"g1": "g1: book/3: " + hA + " started a run in term 5 without acquiring it",
				"g4": "g4: book/3: " + hA + " started a run in term 5 without a grant",
			},
		},
		{
			name:  "a run started in another holder's term",
			steps: []step{grant(5, hB), acquired(hB, 5), startRun(hA, 5)},
			want: map[string]string{
				"g1": "g1: book/3: " + hA + " started a run in term 5 without acquiring it",
				"g4": "g4: book/3: " + hA + " started a run in term 5 without a grant",
			},
		},
		{
			name: "the first violation is kept",
			steps: []step{
				grant(4, hA), takeover(early), startRun(hC, 9),
				takeover(check.Grant{Term: 6, Holder: hC, Prev: hB, Lease: lease}),
			},
			want: map[string]string{
				"g1": "g1: book/3: " + hC + " started a run in term 9 without acquiring it",
				"g4": tookOver,
			},
		},
	})
}

// TestATSIM30Monotonic checks that the readings a holder steps never go back.
func TestATSIM30Monotonic(t *testing.T) {
	const (
		t1 = 41_207 * time.Millisecond
		t2 = 41_208 * time.Millisecond
	)
	run(t, []scenario{
		{
			name: "growing and equal readings",
			steps: []step{
				reading(hA, 0), reading(hA, 0), reading(hA, t1), reading(hA, t1), reading(hA, t2),
				// Another holder has its own clock.
				reading(hB, t1), reading(hB, t1+1),
			},
		},
		{
			name:  "readings below zero that grow",
			steps: []step{reading(hA, -t2), reading(hA, -t1), reading(hA, 0)},
		},
		{
			name:  "a reading below the last",
			steps: []step{reading(hA, t1), reading(hA, t2), reading(hA, t1)},
			want:  map[string]string{"monotonic": "monotonic: " + hA + " read 41.207000000s after 41.208000000s"},
		},
		{
			name:  "a reading one nanosecond below the last",
			steps: []step{reading(hB, t2), reading(hA, time.Second), reading(hA, time.Second-1)},
			want:  map[string]string{"monotonic": "monotonic: " + hA + " read 0.999999999s after 1.000000000s"},
		},
		{
			name:  "the first violation is kept",
			steps: []step{reading(hA, t2), reading(hA, t1), reading(hB, 5), reading(hB, 4), reading(hA, 0)},
			want:  map[string]string{"monotonic": "monotonic: " + hA + " read 41.207000000s after 41.208000000s"},
		},
	})
}

// TestErrKeepsTheFirstViolation checks that Err returns the same error for a check on every
// call, before and after later violations of it.
func TestErrKeepsTheFirstViolation(t *testing.T) {
	c := newChecker()
	grant(7, hA)(c)
	grant(5, hB)(c)
	first := c.Err("g1")
	if first == nil {
		t.Fatal(`Err("g1") = nil after a term went back`)
	}
	grant(5, hC)(c)
	acquired(hC, 9)(c)
	if again := c.Err("g1"); again != first {
		t.Errorf(`Err("g1") = %v after later violations, want the first error, %v`, again, first)
	}
}

// TestErrNames checks the names Err takes: the four checks decided as observations arrive, g2,
// which is nil for a Checker that does not assert it, and a panic for any other name.
func TestErrNames(t *testing.T) {
	c := newChecker()
	// One violation of every check decided as observations arrive: g2 stays nil.
	grant(7, hA)(c)
	grant(5, hB)(c)
	accepted(9, hC)(c)
	startRun(hC, 9)(c)
	reading(hA, 2)(c)
	reading(hA, 1)(c)
	for _, name := range checks {
		if c.Err(name) == nil {
			t.Errorf("Err(%q) = nil, want a violation", name)
		}
	}
	if err := c.Err("g2"); err != nil {
		t.Errorf(`Err("g2") = %v, want nil`, err)
	}

	for _, name := range []string{"", "g5", "G1", "tenure/g1", "liveness"} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			want := fmt.Sprintf("check: unknown check %q", name)
			defer func() {
				if got := recover(); got != want {
					t.Errorf("Err(%q) panicked with %v, want %q", name, got, want)
				}
			}()
			_ = c.Err(name)
		})
	}
}

// TestNewNeedsNow checks that New rejects a Config without Now.
func TestNewNeedsNow(t *testing.T) {
	const want = "check: Config.Now is nil"
	defer func() {
		if got := recover(); got != want {
			t.Errorf("New(Config{}) panicked with %v, want %q", got, want)
		}
	}()
	check.New(check.Config{})
}
