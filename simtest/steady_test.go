// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"fmt"
	"slices"
	"testing"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// TestSteady runs five members that share the 16 shards of one space for 60 s under each rule,
// with a stop delay of up to 1 s and no fault (SIM-600's first row, AT-SIM-31): every invariant
// holds after every event, every shard has an owner once the bound has passed, and at the end
// the members agree on the placement and each shard runs on its assignee. Every seed runs twice
// with equal trace hashes.
//
// It also checks AT-SIM-33: the trace hashes of the seeds of TestSteady/server are pairwise
// distinct, so the seeds do explore different runs. The steps were written for this task from
// the issue and the design note, since the original acceptance-test text is lost.
func TestSteady(t *testing.T) {
	r := rowNamed(t, "TestSteady")
	// The trace hash of each run, by rule and seed. A seed that runs twice, for the
	// determinism check, writes the same hash twice.
	hashes := map[world.Takeover]map[uint64]uint64{}
	for _, rule := range r.takeovers() {
		hashes[rule] = map[uint64]uint64{}
	}
	runRow(t, r, func(fw *faultline.World, w *world.World) {
		byRule := hashes[w.Scenario().Takeover]
		fw.Final("test/trace_hash", func() error {
			byRule[fw.Seed()] = fw.Sim.TraceHash()
			return nil
		})
	})
	if t.Failed() {
		return
	}
	if len(hashes[world.Server]) == 0 {
		t.Fatal("TestSteady/server: no trace hash was collected")
	}
	if err := distinctHashes(hashes[world.Server]); err != nil {
		t.Errorf("TestSteady/server: %v", err)
	}
}

// distinctHashes returns an error naming the first two seeds, in seed order, whose trace
// hashes are equal.
func distinctHashes(bySeed map[uint64]uint64) error {
	seeds := make([]uint64, 0, len(bySeed))
	for seed := range bySeed {
		seeds = append(seeds, seed)
	}
	slices.Sort(seeds)
	first := map[uint64]uint64{} // the first seed of each hash
	for _, seed := range seeds {
		h := bySeed[seed]
		if other, ok := first[h]; ok {
			return fmt.Errorf("seeds %#x and %#x have the same trace hash %#016x", other, seed, h)
		}
		first[h] = seed
	}
	return nil
}

// TestDistinctHashes checks that distinctHashes finds two seeds with one hash.
func TestDistinctHashes(t *testing.T) {
	if err := distinctHashes(map[uint64]uint64{1: 10, 2: 20, 3: 30}); err != nil {
		t.Errorf("distinct hashes: %v", err)
	}
	const want = "seeds 0x2 and 0x7 have the same trace hash 0x0000000000000014"
	if err := distinctHashes(map[uint64]uint64{7: 20, 1: 10, 2: 20, 3: 10}); err == nil || err.Error() != "seeds 0x1 and 0x3 have the same trace hash 0x000000000000000a" {
		t.Errorf("distinctHashes = %v, want the first pair by seed", err)
	}
	if err := distinctHashes(map[uint64]uint64{7: 20, 2: 20}); err == nil || err.Error() != want {
		t.Errorf("distinctHashes = %v, want %q", err, want)
	}
}

// The record kinds and the attributes TestSteadyRecords reads (SIM §8).
const (
	kindBoot   = "tenure.boot"
	kindInput  = "tenure.input"
	kindAction = "tenure.action"
	kindEvent  = "kernel.event"
)

// TestSteadyRecords checks AT-SIM-15 in TestSteady's world, under each rule, with every record
// kept: in the records of each member, the first input after each tenure.boot is a Tick; every
// tenure.action comes after a tenure.input of the same member in the same event, the input of
// its step; and every StartRun comes after an Acquired input of the same incarnation for the
// same space, shard and term. The steps were written for this task from the issue and the
// design note, since the original acceptance-test text is lost.
func TestSteadyRecords(t *testing.T) {
	r := rowNamed(t, "TestSteady")
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			opts := r.options()
			opts.Seeds = 2
			opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				fw.Final("test/records", func() error {
					return inputActionOrder(fw.Sim.Records(), w.Members())
				})
			}))
		})
	}
}

// inputActionOrder checks AT-SIM-15's order over a full trace, and that the trace has at least
// one StartRun for each shard of a steady world.
func inputActionOrder(recs []kernel.Record, members []*world.Member) error {
	starts := 0
	for _, m := range members {
		id := m.Node.ID()
		boots := 0
		first := false                // the next input of the member is the first of an incarnation
		inEvent := false              // the member stepped an input in the present event
		acquired := map[string]bool{} // the shards and terms the incarnation acquired
		for _, r := range recs {
			if r.Kind == kindEvent {
				inEvent = false
				continue
			}
			if r.Node != id {
				continue
			}
			switch r.Kind {
			case kindBoot:
				boots++
				first, inEvent = true, false
				acquired = map[string]bool{}
			case kindInput:
				typ := attr(r, "type")
				if first && typ != "Tick" {
					return fmt.Errorf("%s at %v: the first input of an incarnation is %s, want a Tick", m.Name, r.At, typ)
				}
				first, inEvent = false, true
				if typ == "Acquired" {
					acquired[shardTerm(r)] = true
				}
			case kindAction:
				if !inEvent {
					return fmt.Errorf("%s at %v: action %s comes before any input of its event", m.Name, r.At, attr(r, "type"))
				}
				if attr(r, "type") == "StartRun" {
					starts++
					if !acquired[shardTerm(r)] {
						return fmt.Errorf("%s at %v: StartRun %s with no Acquired before it", m.Name, r.At, shardTerm(r))
					}
				}
			}
		}
		if boots != 1 {
			return fmt.Errorf("%s booted %d times, want 1", m.Name, boots)
		}
	}
	if starts < 16 {
		return fmt.Errorf("%d StartRun actions, want at least one per shard (16)", starts)
	}
	return nil
}

// attr returns the value of the attribute key of r, or "" when r has none.
func attr(r kernel.Record, key string) string {
	for _, a := range r.Attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return ""
}

// shardTerm returns the space, shard and term attributes of r as book/3@5.
func shardTerm(r kernel.Record) string {
	return attr(r, "space") + "/" + attr(r, "shard") + "@" + attr(r, "term")
}
