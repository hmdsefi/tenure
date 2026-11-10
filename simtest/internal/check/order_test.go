// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package check_test

import (
	"math"
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// TestObservationOrders reports observations in orders that a correct simulation never
// produces, and observations that break two checks: each check decides on its own, and an
// observation that fails one check is still recorded for the others.
func TestObservationOrders(t *testing.T) {
	const lease = 15 * time.Second
	early := check.Grant{Term: 5, Holder: hB, Prev: hA, Unseen: lease - time.Nanosecond, Lease: lease}
	const tookOver = "g4: book/3: term 5: " + hB + " took over from " + hA +
		" after 14.999999999s without renewal, lease 15.000000000s"
	const wentBack = "g1: book/3: term 5 granted to " + hB + " after term 7"
	run(t, []scenario{
		{
			// The grant that arrives late still counts: the run and the write that follow it
			// have their grant.
			name:  "an acquisition reported before its grant",
			steps: []step{acquired(hA, 5), grant(5, hA), acquired(hA, 5), startRun(hA, 5), accepted(5, hA)},
			want:  map[string]string{"g1": "g1: book/3: " + hA + " acquired term 5, which the store never granted"},
		},
		{
			name:  "a run started before its grant and its acquisition",
			steps: []step{startRun(hA, 5), grant(5, hA), acquired(hA, 5), startRun(hA, 5)},
			want: map[string]string{
				"g1": "g1: book/3: " + hA + " started a run in term 5 without acquiring it",
				"g4": "g4: book/3: " + hA + " started a run in term 5 without a grant",
			},
		},
		{
			name:  "a write accepted before its grant",
			steps: []step{accepted(5, hA), grant(5, hA), accepted(5, hA)},
			want:  map[string]string{"g3": "g3: book/3: " + hA + " wrote in term 5, which the store never granted"},
		},
		{
			name:  "one grant that breaks g1 and g4",
			steps: []step{grant(7, hA), takeover(early)},
			want:  map[string]string{"g1": wentBack, "g4": tookOver},
		},
		{
			name:  "one grant that grants a term again and breaks g4",
			steps: []step{grant(5, hA), takeover(early)},
			want: map[string]string{
				"g1": "g1: book/3: term 5 granted to " + hB + ", already granted to " + hA,
				"g4": tookOver,
			},
		},
		{
			name: "a takeover without evidence after a term went back",
			steps: []step{
				grant(7, hA), grant(5, hB),
				takeover(check.Grant{Term: 8, Holder: hC, Prev: hA, Lease: lease}),
			},
			want: map[string]string{
				"g1": wentBack,
				"g4": "g4: book/3: term 8: " + hC + " took over from " + hA +
					" after 0.000000000s without renewal, lease 15.000000000s",
			},
		},
		{
			// Term 5 is granted although it went back, and so is term 8 after it: their
			// grantees acquire, run and write without another violation.
			name: "the grants at and after a g1 violation are recorded",
			steps: []step{
				grant(7, hA), grant(5, hB), acquired(hB, 5), startRun(hB, 5), accepted(5, hB),
				owns(8, hC), accepted(8, hC),
			},
			want: map[string]string{"g1": wentBack},
		},
		{
			name: "the grants at and after a g4 violation are recorded",
			steps: []step{
				grant(4, hA), takeover(early), acquired(hB, 5), startRun(hB, 5), accepted(5, hB),
				owns(6, hC), accepted(6, hC),
			},
			want: map[string]string{"g4": tookOver},
		},
		{
			// A term that was never granted has no grantee, not the grantee "".
			name:  "a run started by the empty holder",
			steps: []step{startRun("", 5)},
			want: map[string]string{
				"g1": "g1: book/3:  started a run in term 5 without acquiring it",
				"g4": "g4: book/3:  started a run in term 5 without a grant",
			},
		},
		{
			name: "a violation in another space names its shard",
			steps: []step{func(c *check.Checker) {
				c.Grant(check.Grant{Space: "ledger", Shard: 12, Term: 7, Holder: hA})
				c.Grant(check.Grant{Space: "ledger", Shard: 12, Term: 5, Holder: hB})
				c.Accepted(check.Write{Space: "ledger", Shard: 0, Term: 2, Holder: hC})
				c.StartRun(hC, "ledger", 120, 2)
			}},
			want: map[string]string{
				"g1": "g1: ledger/12: term 5 granted to " + hB + " after term 7",
				"g3": "g3: ledger/0: " + hC + " wrote in term 2, which the store never granted",
				"g4": "g4: ledger/120: " + hC + " started a run in term 2 without a grant",
			},
		},
	})
}

// TestTermRange checks the two ends of the terms: term 0 is a term like any other, and the
// largest terms print as unsigned decimals.
func TestTermRange(t *testing.T) {
	const top = core.Term(math.MaxUint64)
	run(t, []scenario{
		{
			name: "term 0 granted, acquired, started and written",
			steps: []step{
				owns(0, hA), accepted(0, hA), accepted(0, hA),
				owns(1, hB), accepted(1, hB),
			},
		},
		{
			name:  "term 0 granted twice",
			steps: []step{grant(0, hA), grant(0, hB)},
			want:  map[string]string{"g1": "g1: book/3: term 0 granted to " + hB + ", already granted to " + hA},
		},
		{
			name:  "term 0 after term 1",
			steps: []step{grant(1, hA), grant(0, hB)},
			want:  map[string]string{"g1": "g1: book/3: term 0 granted to " + hB + " after term 1"},
		},
		{
			name:  "term 0 written by two holders",
			steps: []step{grant(0, hA), accepted(0, hA), accepted(0, hB)},
			want:  map[string]string{"g3": "g3: book/3: term 0 written by " + hA + " and " + hB},
		},
		{
			name:  "term 0 written without its grant",
			steps: []step{accepted(0, hA)},
			want:  map[string]string{"g3": "g3: book/3: " + hA + " wrote in term 0, which the store never granted"},
		},
		{
			name:  "a write in term 0 after term 1",
			steps: []step{grant(0, hA), grant(1, hB), accepted(1, hB), accepted(0, hA)},
			want:  map[string]string{"g3": "g3: book/3: write in term 0 by " + hA + " accepted after term 1"},
		},
		{
			name:  "the largest term",
			steps: []step{owns(top-1, hA), accepted(top-1, hA), owns(top, hB), accepted(top, hB)},
		},
		{
			name:  "a term below the largest term",
			steps: []step{grant(top, hA), grant(top-1, hB), accepted(top, hA), accepted(top-1, hB)},
			want: map[string]string{
				"g1": "g1: book/3: term 18446744073709551614 granted to " + hB + " after term 18446744073709551615",
				"g3": "g3: book/3: write in term 18446744073709551614 by " + hB + " accepted after term 18446744073709551615",
			},
		},
	})
}
