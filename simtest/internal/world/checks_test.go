// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// TestChecksSteady runs a world of five members with a stop delay under each rule for 60 s,
// with every invariant Build registers running after every event: none fails. It also checks
// what the checks were fed: the hooks that feed the checker are set and saw grants, accepted
// writes and StartRun actions, every shard's fence was raised and took writes, the checker holds
// no violation at the end, and every shard is owned then. Set FAULTLINE_SEEDS for more seeds.
func TestChecksSteady(t *testing.T) {
	forRules(t, func(t *testing.T, rule Takeover) {
		sc := Scenario{Takeover: rule, StopDelay: time.Second}
		runWorld(t, fullTrace(60*time.Second), sc, func(fw *faultline.World, w *World) {
			t := fw.T()
			// The store's grants, the fence's accepted writes and the drivers' steps reach the
			// checker through these hooks; count what passes through them.
			if w.storeSrv.onGrant == nil || w.fenceSrv.onAccepted == nil || w.drivers.onStep == nil || w.drivers.onRetire == nil {
				t.Fatal("a hook of the checker is not set")
			}
			grants, accepted, starts := 0, 0, 0
			onGrant, onAccepted, onStep := w.storeSrv.onGrant, w.fenceSrv.onAccepted, w.drivers.onStep
			w.storeSrv.onGrant = func(g check.Grant) { grants++; onGrant(g) }
			w.fenceSrv.onAccepted = func(wr check.Write) { accepted++; onAccepted(wr) }
			w.drivers.onStep = func(d *driver, in core.Input, acts []core.Action) {
				for _, a := range acts {
					if _, ok := a.(core.StartRun); ok {
						starts++
					}
				}
				onStep(d, in, acts)
			}
			fw.RunFor(60*time.Second - time.Nanosecond)
			if !w.AssertsG2() {
				t.Fatal("a steady world does not assert g2")
			}
			if shards := w.Scenario().Spaces[0].Shards; grants < shards || accepted < shards || starts < shards {
				t.Errorf("the checker saw %d grants, %d accepted writes and %d StartRun; want at least %d each",
					grants, accepted, starts, shards)
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

// TestChecksPartition cuts m1 of three members off from the store from 10 s to 40 s, so its
// shards move to the others while it still runs, with every invariant on: none fails. In a
// fenced space m1's runs degrade and go on writing, and the fence, raised by the new owners,
// rejects those writes (a run that skipped the raise would fail tenure/g3 here); in an
// unfenced space m1 stops at its session deadline, before the others start (a deadline later
// than its renewals allow would fail tenure/g2 here).
func TestChecksPartition(t *testing.T) {
	for _, fenced := range []bool{true, false} {
		t.Run("fenced="+strconv.FormatBool(fenced), func(t *testing.T) {
			forRules(t, func(t *testing.T, rule Takeover) {
				sc := Scenario{
					Takeover: rule, Members: 3, StopDelay: time.Second,
					Spaces: []core.SpaceSpec{{Name: "book", Shards: 8, Fenced: fenced}},
				}
				runWorld(t, fullTrace(60*time.Second), sc, func(fw *faultline.World, w *World) {
					t := fw.T()
					m1, store := w.Members()[0], w.Store().ID()
					fw.Sim.At(kernel.Time(10*time.Second), "test/cut", func() {
						fw.Net.Cut(m1.Node.ID(), store)
						fw.Net.Cut(store, m1.Node.ID())
					})
					fw.Sim.At(kernel.Time(40*time.Second), "test/heal", func() {
						fw.Net.HealLink(m1.Node.ID(), store)
						fw.Net.HealLink(store, m1.Node.ID())
					})
					fw.RunFor(60*time.Second - time.Nanosecond)
					if !w.AssertsG2() {
						t.Fatal("the world does not assert g2")
					}
					degrades, rejected := 0, 0
					for _, r := range records(fw.Sim, kindAction) {
						if r.Node == m1.Node.ID() && isAction(r, "Degrade") {
							degrades++
						}
					}
					for _, r := range records(fw.Sim, kindWrite) {
						if attrValue(r, "result") == "rejected" && sessionMember(attrValue(r, "holder")) == m1.Name {
							rejected++
						}
					}
					if fenced != (degrades > 0) || fenced != (rejected > 0) {
						t.Errorf("m1 degraded %d runs and had %d writes rejected; want both > 0 iff the space is fenced",
							degrades, rejected)
					}
				})
			})
		})
	}
}

// plantEnv names the check whose violation TestInvariantsChild plants.
const plantEnv = "TENURE_SIM_PLANT"

// plants reports, for each check Build registers, observations that violate that check and no
// check registered before it, in a space of its own and by holders of no member.
var plants = map[string]func(c *check.Checker){
	"g1": func(c *check.Checker) {
		c.Grant(check.Grant{Space: "plant", Term: 5, Holder: "x1/01"})
		c.Grant(check.Grant{Space: "plant", Term: 5, Holder: "x2/02"})
	},
	"g2": func(c *check.Checker) {
		for i, h := range []string{"x1/01", "x2/02"} {
			term := core.Term(5 + i)
			c.Grant(check.Grant{Space: "plant", Term: term, Holder: h})
			c.Acquired(h, "plant", 0, term)
			c.StartRun(h, "plant", 0, term)
		}
	},
	"g3": func(c *check.Checker) {
		c.Accepted(check.Write{Space: "plant", Term: 5, Holder: "x1/01"})
	},
	"g4": func(c *check.Checker) {
		c.Grant(check.Grant{Space: "plant", Term: 5, Holder: "x1/01", Prev: "x2/02", Lease: time.Second})
	},
	"monotonic": func(c *check.Checker) {
		c.Reading("x1/01", 2*time.Second)
		c.Reading("x1/01", time.Second)
	},
}

// TestInvariantsRegistered checks that Build registers every check of the checker as the
// invariant tenure/<check>, which fails the run: for each check, a child process plants a
// violation of it at 1 s in a steady world, and its run fails naming exactly that invariant.
func TestInvariantsRegistered(t *testing.T) {
	if os.Getenv(plantEnv) != "" {
		t.Skip("in the child process")
	}
	if len(plants) != len(invariantChecks) {
		t.Fatalf("%d plants for the checks %v", len(plants), invariantChecks)
	}
	for _, name := range invariantChecks {
		t.Run(name, func(t *testing.T) {
			// The child's output is matched by substring, and none of faultline's variables of
			// this process reaches it, as in TestATSIM06ClockJump.
			cmd := exec.Command(os.Args[0], "-test.run=^TestInvariantsChild$", "-test.v") //nolint:gosec // the test binary itself
			cmd.Env = append(childEnviron(), plantEnv+"="+name, "FAULTLINE_ARTIFACTS=off")
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() == 0 {
				t.Fatalf("child exited with %v, want a failure; output:\n%s", err, out.String())
			}
			want := `invariant "` + invariantPrefix + name + `" violated at t=1.000000000s`
			if !strings.Contains(out.String(), want) {
				t.Errorf("child's output does not contain %q:\n%s", want, out.String())
			}
		})
	}
}

// TestInvariantsChild is the child process of TestInvariantsRegistered: it plants the violation
// that TENURE_SIM_PLANT names into the checker of a steady world at 1 s.
func TestInvariantsChild(t *testing.T) {
	plant := plants[os.Getenv(plantEnv)]
	if plant == nil {
		t.Skip("runs only as the child process of TestInvariantsRegistered")
	}
	faultline.Run(t, faultline.Options{Seeds: 1, Duration: 30 * time.Second}, func(fw *faultline.World) {
		w := Build(fw, Scenario{})
		fw.Sim.At(kernel.Time(time.Second), "test/plant", func() { plant(w.Checker()) })
		fw.RunFor(2*time.Second - time.Nanosecond)
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
