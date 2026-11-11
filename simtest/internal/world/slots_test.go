// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
)

// TestSetSlots checks the change of the slot count that SetSlotsAt plans (SIM-360), under each
// rule, in a world of 3 members and 5 slots whose slot count m1 sets to 2 at 10 s:
//
//   - plain: m1 sends setSlots once, at 10 s, and the store sets the count to 2 once, within a
//     retry period of it;
//   - store cut off: the store is isolated from 8 s to 15 s, so the request is lost; m1 sends it
//     again every retry period, and the store sets the count once, after the heal;
//   - m1 down: m1 crashes at 8 s and restarts at 15 s, so no request can go out at 10 s; the
//     request is sent by the new incarnation, and the store sets the count once, after the
//     restart.
func TestSetSlots(t *testing.T) {
	const (
		setAt   = 10 * time.Second
		healAt  = 15 * time.Second
		cutAt   = 8 * time.Second
		runFor  = 100 * time.Second
		retries = 2 * time.Second // the default RetryPeriod
	)
	tests := []struct {
		name   string
		script []fault.Event
		first  time.Duration // the earliest time the store may set the count
		sends  int           // the least number of requests m1 sends
	}{
		{"plain", nil, setAt, 1},
		{"store cut off", []fault.Event{
			{At: kernel.Time(cutAt), Kind: fault.KindIsolate, Node: "store"},
			{At: kernel.Time(healAt), Kind: fault.KindHeal},
		}, healAt, 2},
		{"m1 down", []fault.Event{
			{At: kernel.Time(cutAt), Kind: fault.KindCrash, Node: "m1"},
			{At: kernel.Time(healAt), Kind: fault.KindRestart, Node: "m1"},
		}, healAt, 1},
	}
	forRules(t, func(t *testing.T, rule Takeover) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				sc := Scenario{Takeover: rule, Members: 3, Slots: 5, SetSlotsAt: setAt, SetSlotsTo: 2}
				opts := fixedNet(runFor)
				runWorld(t, opts, sc, func(fw *faultline.World, w *World) {
					t := fw.T()
					if got := w.Recovery(); got != kernel.Time(setAt) {
						t.Fatalf("R = %v, want SetSlotsAt %v", got, kernel.Time(setAt))
					}
					if len(tt.script) > 0 {
						fw.Plan(fault.Script(tt.script...))
					}
					fw.RunFor(30 * time.Second)

					var sets []kernel.Record
					for _, r := range records(fw.Sim, kindSlot) {
						if attrValue(r, "op") == "set" {
							sets = append(sets, r)
						}
					}
					if len(sets) != 1 {
						t.Fatalf("the store set the slot count %d times, want once: %v", len(sets), sets)
					}
					at := time.Duration(sets[0].At)
					if at < tt.first || at >= tt.first+3*retries {
						t.Errorf("the store set the count at %v, want in [%v, %v)", at, tt.first, tt.first+3*retries)
					}
					if got := attrValue(sets[0], "count"); got != "2" {
						t.Errorf("the store set the count to %s, want 2", got)
					}
					if got := attrValue(sets[0], "holder"); !strings.HasPrefix(got, "m1/") {
						t.Errorf("the request came from %q, want m1", got)
					}
					var sent []kernel.Record
					for _, r := range fw.Sim.Records() {
						if r.Kind == "net.send" && attrValue(r, "from") == "m1" && strings.HasPrefix(attrValue(r, "payload"), "setSlots") {
							sent = append(sent, r)
						}
					}
					if len(sent) < tt.sends {
						t.Errorf("m1 sent setSlots %d times, want at least %d", len(sent), tt.sends)
					}
					if len(sent) > 0 && time.Duration(sent[0].At) < setAt {
						t.Errorf("m1 sent setSlots at %v, before SetSlotsAt %v", sent[0].At, kernel.Time(setAt))
					}
				})
			})
		}
	})
}
