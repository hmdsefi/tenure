// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"math"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
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

// TestRolloutMessages checks the messages of tenure/rollout, from planted observations.
func TestRolloutMessages(t *testing.T) {
	sc := Scenario{Members: 2, Slots: 2, Rollout: &Rollout{Start: 20 * time.Second, Gap: 90 * time.Second, Surge: 5 * time.Second}}
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
		r1.count("m1-r/a", spaceShard{"book", 3})
		r1.count("m1-r/b", spaceShard{"book", 4}) // another incarnation's starts do not count
		r1.count("m1-r/b", spaceShard{"book", 4})
		if err := rw.err(); err == nil || err.Error() != "rollout: replacement 1: book/3 started 2 times on m1-r" {
			t.Errorf("two starts: %v", err)
		}
		r1.starts["m1-r/a"][spaceShard{"book", 3}] = 1
		rw.late = "rollout: m1: book/3 started at 25.010000000s, after its drain began at 25.000000000s"
		if err := rw.err(); err == nil || err.Error() != rw.late {
			t.Errorf("a late start: %v", err)
		}
	})
}
