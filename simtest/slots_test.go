// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// The times of the slot rows: when m1 lowers the slot count and to what, and when m5 crashes.
const (
	scaleDownAt = 40 * time.Second
	scaleDownTo = 3
	graceCrash  = 30 * time.Second
	graceRun    = 150 * time.Second
)

// The rows of TestSlotScaleDown and TestSlotGrace are in the table of scenario_test.go.
//
// TestSlotScaleDown lasts 125 s, not the 90 s that the design note gives: R is SetSlotsAt, 40 s,
// and with slots the bound B counts SlotGrace (60 s), 81.045 s under the server rule, so a run
// that ends before 121.045 s cannot show that every shard has an owner again, and world.SetupError
// refuses it ("recovery 40.000000000s + bound 81.045000000s is after the end 90.000000000s").
//
// TestSlotGrace has no random faults. Its crash is a scripted event, and R must be the crash
// time, 30 s; the scenario has no field for that, so its row names a fault plan of no rules
// whose quiet window is the last 120 s of the 150 s run, which makes R the end minus the quiet
// window: graceRun - graceCrash.

// TestSlotScaleDown lowers the slot count from 5 to 3 at 40 s, by a request of m1 (SIM-360),
// under each rule (SIM-600): the slots 3 and 4 are retired, the placement goes to the three
// that remain, and by the bound after 40 s every shard runs on its assignee and no shard was
// ownerless for longer than that (tenure/g5, tenure/converged). Every invariant holds after
// every event.
func TestSlotScaleDown(t *testing.T) {
	runRow(t, rowNamed(t, "TestSlotScaleDown"), nil)
}

// TestSlotGrace crashes m5 at 30 s and never restarts it (SIM-600). Its slot stays vacant,
// and after SlotGrace the slot drops out of the placement, so that its shards do not stay
// ownerless: by the bound, which counts SlotGrace, after the crash the four members that are
// left agree on the placement over the four slots that are held, and every shard runs on its
// assignee. The run ends with m5's node still down.
func TestSlotGrace(t *testing.T) {
	runRow(t, rowNamed(t, "TestSlotGrace"), func(fw *faultline.World, w *world.World) {
		fw.Plan(fault.Script(fault.Event{At: kernel.Time(graceCrash), Kind: fault.KindCrash, Node: "m5"}))
		m5 := w.Members()[4]
		fw.Final("test/m5_down", func() error {
			if m5.Node.State() == kernel.NodeUp {
				return errors.New("m5 is up at the end, but it never restarts")
			}
			return nil
		})
	})
}

// TestATSIM29ScaleDown is AT-SIM-29 on the row of TestSlotScaleDown: m1 sends setSlots(3) at
// 40 s, and at the end no shard runs on the member that held slot 3 before, all machines agree
// on an Assignment over slot-0 to slot-2, and every shard runs on its assignee. Its steps were
// written for this task from the issue and the design note, since the original text of the
// acceptance test is lost. The runs keep a full trace, to see the request at the store.
func TestATSIM29ScaleDown(t *testing.T) {
	r := rowNamed(t, "TestSlotScaleDown")
	opts := r.options()
	opts.Seeds = 5
	opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				var former string // the member that held slot 3 just before the change
				fw.Sim.At(kernel.Time(scaleDownAt-5*time.Second), "test/former_holder", func() {
					places, err := w.Placements()
					if err != nil {
						fw.Sim.Fail(err)
					}
					for _, p := range places {
						if p.Slot == scaleDownTo {
							former = p.Member
						}
					}
				})
				fw.Final("test/at_sim_29", func() error {
					return scaleDownErr(w, fw.Sim.Records(), former)
				})
			}))
		})
	}
}

// scaleDownErr decides AT-SIM-29 at the end of a run: the store set the slot count to 3 once m1
// asked for it, after 40 s and before 40 s plus a retry period; the machines that are up agree
// on an assignment over slot-0 to slot-2 that gives every shard an assignee, each of the three
// slots is some member's, every shard runs on the member of its assignee and on no other, and
// no shard runs on former, the member that held slot 3 before.
func scaleDownErr(w *world.World, recs []kernel.Record, former string) error {
	if former == "" {
		return errors.New("no member held slot 3 before the change")
	}
	if err := setRecordErr(recs, w.Scenario().Timing.RetryPeriod); err != nil {
		return err
	}
	places, err := w.Placements()
	if err != nil {
		return err
	}
	if len(places) == 0 {
		return errors.New("no member is up at the end")
	}
	keys := map[string]bool{"slot-0": true, "slot-1": true, "slot-2": true}
	want := places[0].Assignment("book")
	if len(want) == 0 {
		return fmt.Errorf("%s has no assignment for book", places[0].Member)
	}
	holderOf := map[string]string{} // slot key to the holder of the member that holds the slot
	for _, p := range places {
		if p.Slot >= 0 {
			holderOf[fmt.Sprintf("slot-%d", p.Slot)] = p.Holder
		}
		if got := p.Assignment("book"); !slices.Equal(got, want) {
			return fmt.Errorf("%s's assignment %q differs from %s's %q", p.Member, got, places[0].Member, want)
		}
	}
	for shard, key := range want {
		if !keys[key] {
			return fmt.Errorf("book/%d is assigned to %q, not to one of slot-0 to slot-2", shard, key)
		}
		holder, ok := holderOf[key]
		if !ok {
			return fmt.Errorf("book/%d is assigned to %s, which no member holds", shard, key)
		}
		if got := w.Checker().Owners("book", shard); !slices.Equal(got, []string{holder}) {
			return fmt.Errorf("book/%d is assigned to %s (%s), owned by %q", shard, key, holder, got)
		}
		for _, h := range w.Checker().Owners("book", shard) {
			if strings.HasPrefix(h, former+"/") {
				return fmt.Errorf("book/%d runs on %s, the member that held slot 3", shard, h)
			}
		}
	}
	for key := range keys {
		if _, ok := holderOf[key]; !ok {
			return fmt.Errorf("no member holds %s at the end", key)
		}
	}
	return nil
}

// setRecordErr checks that the records show the store setting the slot count to 3 once, at or
// after 40 s and within retry of it: m1's request goes out at 40 s and reaches the store a few
// milliseconds later. The record of a lost reply may repeat it, so a later one is not an error.
func setRecordErr(recs []kernel.Record, retry time.Duration) error {
	for _, r := range recs {
		if r.Kind != "tenure.slot" || attr(r, "op") != "set" {
			continue
		}
		if attr(r, "count") != fmt.Sprint(scaleDownTo) {
			return fmt.Errorf("the store set the slot count to %s at %s, want %d", attr(r, "count"), r.At, scaleDownTo)
		}
		if at := time.Duration(r.At); at < scaleDownAt || at >= scaleDownAt+retry {
			return fmt.Errorf("the store first set the slot count at %s, want [%s, %s)", r.At, kernel.Time(scaleDownAt), kernel.Time(scaleDownAt+retry))
		}
		return nil
	}
	return errors.New("the store never set the slot count")
}
