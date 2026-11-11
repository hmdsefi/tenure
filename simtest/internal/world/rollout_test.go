// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
)

// TestLastDrain checks the time of the last drain, which is the recovery time of a rollout
// that nothing else disturbs, and that a plan too long for the clock saturates.
func TestLastDrain(t *testing.T) {
	ro := &Rollout{Start: 20 * time.Second, Gap: 86 * time.Second, Surge: 5 * time.Second}
	sc := Scenario{Members: 5, Slots: 5, Rollout: ro}.withDefaults()
	if got, want := lastDrain(sc), at(20*time.Second+4*86*time.Second+5*time.Second); got != want {
		t.Errorf("lastDrain = %s, want %s", got, want)
	}
	if got, want := recoveryStart(sc, 0, at(time.Hour)), lastDrain(sc); got != want {
		t.Errorf("recoveryStart = %s, want the last drain %s", got, want)
	}
	one := Scenario{Members: 1, Slots: 1, Rollout: ro}.withDefaults()
	if got, want := lastDrain(one), at(25*time.Second); got != want {
		t.Errorf("lastDrain of one member = %s, want %s", got, want)
	}
	huge := Scenario{Members: 5, Slots: 5, Rollout: &Rollout{Start: 1, Gap: math.MaxInt64 / 2, Surge: 1}}.withDefaults()
	if got := lastDrain(huge); got != kernel.Time(math.MaxInt64) {
		t.Errorf("lastDrain of a huge Gap = %d, want the largest time", got)
	}
}

// twoRollout is a rollout of two members, each replaced well before a run of 300 s ends.
var twoRollout = &Rollout{Start: 20 * time.Second, Gap: 90 * time.Second, Surge: 5 * time.Second}

// TestRolloutMessages checks the messages of tenure/rollout that hold under faults, from
// planted observations at the start of a run with faults: the checks of a run that nothing but
// the rollout disturbs are left out there (TestRolloutQuiet).
func TestRolloutMessages(t *testing.T) {
	sc := Scenario{Members: 2, Slots: 2, Rollout: twoRollout, Faults: &fault.Random{}}
	runWorld(t, faultline.Options{Duration: 300 * time.Second}, sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		rw := w.rollout
		r1 := rw.reps[0]
		if err := rw.err(); err == nil || err.Error() != "rollout: replacement 1: m1-r never held a slot" {
			t.Errorf("before any slot: %v", err)
		}
		r1.holder, r1.slot = "m1-r/a", 0
		for _, r := range rw.reps[1:] {
			r.holder = "m2-r/a" // not the present holder: judged only for repeated starts
		}
		if err := rw.err(); err != nil {
			t.Errorf("no start: %v, want nil", err)
		}
		r1.count("m1-r/a", spaceShard{"book", 3})
		r1.count("m1-r/b", spaceShard{"book", 3}) // a later incarnation starts it again: not a repeat
		if err := rw.err(); err != nil {
			t.Errorf("one start per incarnation: %v, want nil", err)
		}
		r1.count("m1-r/a", spaceShard{"book", 3})
		if err := rw.err(); err == nil || err.Error() != "rollout: replacement 1: book/3 started 2 times on m1-r" {
			t.Errorf("two starts: %v", err)
		}
		r1.starts["m1-r/a"][spaceShard{"book", 3}] = 1
		r1.count("m1-r/b", spaceShard{"book", 4})
		r1.count("m1-r/b", spaceShard{"book", 4})
		if err := rw.err(); err == nil || err.Error() != "rollout: replacement 1: book/4 started 2 times on m1-r" {
			t.Errorf("two starts in a later incarnation: %v", err)
		}
		r1.starts["m1-r/b"][spaceShard{"book", 4}] = 1
		rw.late = "rollout: m1: book/3 started at 25.010000000s, after its drain began at 25.000000000s"
		if err := rw.err(); err == nil || err.Error() != rw.late {
			t.Errorf("a late start: %v", err)
		}
	})
}

// TestRolloutQuiet runs a rollout of two members that nothing else disturbs and judges its
// observations at the end: they pass, and each planted change gives its message. It also
// steps a planted start on a replaced member before and after its drain began.
func TestRolloutQuiet(t *testing.T) {
	sc := Scenario{Members: 2, Slots: 2, Rollout: twoRollout}
	runWorld(t, faultline.Options{Duration: 300 * time.Second}, sc, func(fw *faultline.World, w *World) {
		rw := w.rollout
		fw.Final("test/rollout_quiet", func() error {
			if err := rw.err(); err != nil {
				return fmt.Errorf("as run: %w", err)
			}
			r1 := rw.reps[0]
			if rw.drainSlot[0] < 0 || r1.slot != rw.drainSlot[0] {
				return fmt.Errorf("m1-r took slot %d, m1 held %d at its drain", r1.slot, rw.drainSlot[0])
			}
			k := r1.slot
			other := 1 - k
			for _, c := range []struct {
				name string
				set  func()
				want string
			}{
				{"another slot", func() { r1.slot = other },
					fmt.Sprintf("rollout: replacement 1: m1-r took slot %d, not m1's slot %d", other, k)},
				{"no slot at the drain", func() { rw.drainSlot[0] = -1 },
					fmt.Sprintf("rollout: replacement 1: m1-r took slot %d, but m1 held no slot when its drain began", k)},
				{"not running", func() { r1.holder = "m1-r/gone" },
					fmt.Sprintf("rollout: replacement 1: m1-r/gone, which took slot %d, does not run at the end", k)},
				{"an empty slot", func() { r1.slot, rw.drainSlot[0] = 7, 7 },
					"rollout: replacement 1: no shard is assigned to slot-7 on m1-r"},
			} {
				holder, slot, drainSlot := r1.holder, r1.slot, rw.drainSlot[0]
				c.set()
				if err := rw.err(); err == nil || err.Error() != c.want {
					return fmt.Errorf("%s: %v, want %q", c.name, err, c.want)
				}
				r1.holder, r1.slot, rw.drainSlot[0] = holder, slot, drainSlot
			}
			// A shard of the slot that was never started on it.
			shard := slices.Index(w.drivers.current(r1.m).machine.Assignment("book"), "slot-"+strconv.Itoa(k))
			if shard < 0 {
				return fmt.Errorf("no shard of book is assigned to slot-%d", k)
			}
			s := spaceShard{"book", shard}
			r1.starts[r1.holder][s] = 0
			want0 := fmt.Sprintf("rollout: replacement 1: book/%d started 0 times on m1-r", shard)
			if err := rw.err(); err == nil || err.Error() != want0 {
				return fmt.Errorf("no start: %v, want %q", err, want0)
			}
			r1.starts[r1.holder][s] = 1
			m1 := &driver{m: w.members[0]}
			start := []core.Action{core.StartRun{Space: "book", Shard: 3}}
			drainAt := rw.drainAt[0]
			rw.drainAt[0] = -1
			rw.step(m1, start)
			if rw.late != "" {
				return fmt.Errorf("a start before the drain: %q", rw.late)
			}
			rw.drainAt[0] = drainAt
			rw.step(m1, start)
			want := fmt.Sprintf("rollout: m1: book/3 started at %s, after its drain began at %s", fw.Sim.Now(), drainAt)
			if err := rw.err(); err == nil || err.Error() != want {
				return fmt.Errorf("a late start: %v, want %q", err, want)
			}
			return nil
		})
	})
}
