// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
)

// TestChecksSteady runs a world of five members with a stop delay under each rule for 60 s,
// with every invariant Build registers running after every event: none fails. It also checks
// what the checks were fed: every shard's fence was raised and took writes, the checker holds
// no violation at the end, and every shard is owned then. Set FAULTLINE_SEEDS for more seeds.
func TestChecksSteady(t *testing.T) {
	forRules(t, func(t *testing.T, rule Takeover) {
		sc := Scenario{Takeover: rule, StopDelay: time.Second}
		runWorld(t, fullTrace(60*time.Second), sc, func(fw *faultline.World, w *World) {
			t := fw.T()
			fw.RunFor(60*time.Second - time.Nanosecond)
			if !w.AssertsG2() {
				t.Fatal("a steady world does not assert g2")
			}
			for _, name := range invariantChecks {
				if err := w.Checker().Err(name); err != nil {
					t.Errorf("Err(%q) = %v", name, err)
				}
			}
			if err := w.Checker().Liveness(fw.Sim.Now(), 0); err != nil {
				t.Errorf("at the end: %v", err)
			}
			raised, written := map[string]bool{}, map[string]bool{}
			for _, r := range records(fw.Sim, kindFence) {
				raised[attrValue(r, "space")+"/"+attrValue(r, "shard")] = true
			}
			for _, r := range records(fw.Sim, kindWrite) {
				if attrValue(r, "result") == "accepted" {
					written[attrValue(r, "space")+"/"+attrValue(r, "shard")] = true
				}
			}
			for _, sp := range w.Scenario().Spaces {
				for shard := range sp.Shards {
					key := sp.Name + "/" + strconv.Itoa(shard)
					if !raised[key] || !written[key] {
						t.Errorf("%s: raised %t, written %t; want both", key, raised[key], written[key])
					}
				}
			}
		})
	})
}

// TestHolderMono checks the checker's Mono: a holder reads the clock of its member's node,
// whatever incarnation it is and whether the node is up; a holder of no member reads the
// global time.
func TestHolderMono(t *testing.T) {
	sc := Scenario{Members: 2, Rho: 100_000}
	runWorld(t, fullTrace(30*time.Second), sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		m2 := w.Members()[1]
		var old string
		fw.Sim.At(kernel.Time(3*time.Second), "test/crash", func() {
			old = m2.Holder()
			m2.Node.Crash()
		})
		fw.Sim.At(kernel.Time(5*time.Second), "test/restart", m2.Node.Restart)
		fw.RunFor(4 * time.Second)
		if got, want := w.holderMono(old), m2.Mono(); got != want {
			t.Errorf("down holder %s reads %v, want its node's %v", old, got, want)
		}
		fw.RunFor(2 * time.Second)
		if old == m2.Holder() {
			t.Fatal("no new incarnation")
		}
		for _, h := range []string{old, m2.Holder()} {
			if got, want := w.holderMono(h), m2.Mono(); got != want {
				t.Errorf("holder %s reads %v, want its node's %v", h, got, want)
			}
		}
		if got, want := w.holderMono("x9/00"), time.Duration(fw.Sim.Now()); got != want {
			t.Errorf("a holder of no member reads %v, want the global time %v", got, want)
		}
	})
}
