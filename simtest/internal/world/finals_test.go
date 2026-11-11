// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// convergedRig is the input of convergedErr in TestConvergedMessages: the live members, each
// with an assignment per space, and the owners of each shard.
type convergedRig struct {
	spaces  []core.SpaceSpec
	slots   int
	live    []liveMember
	assign  map[string]map[string][]string // by member, then space
	owners  map[string][]string            // by "space/shard"; a shard not listed has its assignee's holder
	balance map[string][]string            // the balanced assignment of each space, the default of every member
}

// newConvergedRig returns a rig of three live members m1 to m3 with holders m1/01 to m3/03,
// without slots, one space book of 4 shards, every machine assigning book as core.Balanced does
// over m1 to m3, which is m2, m1, m3, m1, and every shard owned by its assignee alone.
func newConvergedRig() *convergedRig {
	r := &convergedRig{
		spaces:  []core.SpaceSpec{{Name: "book", Shards: 4}},
		assign:  map[string]map[string][]string{},
		owners:  map[string][]string{},
		balance: map[string][]string{"book": {"m2", "m1", "m3", "m1"}},
	}
	for _, name := range []string{"m1", "m2", "m3"} {
		r.live = append(r.live, liveMember{name: name, holder: name + "/0" + name[1:], slot: -1})
	}
	return r
}

// set gives member's machine the assignment keys of book.
func (r *convergedRig) set(member string, keys ...string) {
	if r.assign[member] == nil {
		r.assign[member] = map[string][]string{}
	}
	r.assign[member]["book"] = keys
}

// err runs convergedErr over the rig.
func (r *convergedRig) err() error {
	live := slices.Clone(r.live)
	for i := range live {
		name := live[i].name
		live[i].assign = func(space string) []string {
			if keys, ok := r.assign[name][space]; ok {
				return keys
			}
			return r.balance[space]
		}
	}
	owners := func(space string, shard int) []string {
		if o, ok := r.owners[space+"/"+strconv.Itoa(shard)]; ok {
			return o
		}
		key := keyAt(r.balance[space], shard)
		for _, m := range r.live {
			if m.name == key || (r.slots > 0 && "slot-"+strconv.Itoa(m.slot) == key) {
				return []string{m.holder}
			}
		}
		return nil
	}
	return convergedErr(r.spaces, r.slots, live, owners)
}

// TestConvergedMessages checks every message of tenure/converged, exactly, and that it passes
// when the members agree on the balanced placement over them and each shard runs on its
// assignee alone.
func TestConvergedMessages(t *testing.T) {
	tests := []struct {
		name  string
		setup func(r *convergedRig)
		want  string
	}{
		{"converged", func(*convergedRig) {}, ""},
		{"no live member", func(r *convergedRig) { r.live = nil }, ""},
		{"members disagree", func(r *convergedRig) {
			r.set("m1", "m1", "m2", "m3", "m4")
			r.set("m3", "m1", "m2", "m3", "m5")
		}, `converged: book: m2's assignment differs from m1's at shard 0: "m2" != "m1"`},
		{"the third disagrees", func(r *convergedRig) {
			r.set("m3", "m2", "m1", "m3", "m5")
		}, `converged: book: m3's assignment differs from m1's at shard 3: "m5" != "m1"`},
		{"a shorter assignment", func(r *convergedRig) {
			r.set("m2", "m2", "m1", "m3")
		}, `converged: book: m2's assignment differs from m1's at shard 3: "" != "m1"`},
		{"agreed, but not balanced over the live members", func(r *convergedRig) {
			for _, m := range []string{"m1", "m2", "m3"} {
				r.set(m, "m2", "m1", "m4", "m1")
			}
		}, `converged: book: m1's assignment differs from the live members' at shard 2: "m4" != "m3"`},
		{"agreed on a member that is gone", func(r *convergedRig) {
			r.live = r.live[:2] // m3 is down: balanced over m1, m2 is m2, m1, m2, m1
		}, `converged: book: m1's assignment differs from the live members' at shard 2: "m3" != "m2"`},
		{"owned by another", func(r *convergedRig) {
			r.owners["book/3"] = []string{"m3/03"}
		}, `converged: book/3 assigned to m1, owned by m3/03`},
		{"owned by an earlier holder of the assignee", func(r *convergedRig) {
			r.owners["book/1"] = []string{"m1/00"}
		}, `converged: book/1 assigned to m1, owned by m1/00`},
		{"owned by nobody", func(r *convergedRig) {
			r.owners["book/0"] = nil
		}, `converged: book/0 assigned to m2, owned by nobody`},
		{"owned by the assignee and another", func(r *convergedRig) {
			r.owners["book/2"] = []string{"m1/01", "m3/03"}
		}, `converged: book/2 assigned to m3, owned by m1/01, m3/03`},
		{"no assignee", func(r *convergedRig) {
			// Without a live member that is eligible, every key is "" and nothing is owed.
			for i := range r.live {
				r.live[i].draining = true
			}
			r.balance["book"] = []string{"", "", "", ""}
		}, ""},
		{"spaces by name", func(r *convergedRig) {
			r.spaces = append(r.spaces, core.SpaceSpec{Name: "atlas", Shards: 4})
			r.balance["atlas"] = []string{"m2", "m2", "m3", "m1"}
			r.owners["book/0"] = nil
			r.owners["atlas/1"] = nil
		}, `converged: atlas/1 assigned to m2, owned by nobody`},
		{"slots", func(r *convergedRig) {
			// Three slots: m2 holds 0, m1 holds 1, m3 none, and slot 2 is vacant.
			r.slots = 3
			r.live[0].slot, r.live[1].slot = 1, 0
			r.balance["book"] = []string{"slot-0", "slot-1", "slot-0", "slot-1"}
		}, ""},
		{"a slot's shard owned by nobody", func(r *convergedRig) {
			r.slots = 3
			r.live[0].slot, r.live[1].slot = 1, 0
			r.balance["book"] = []string{"slot-0", "slot-1", "slot-0", "slot-1"}
			r.owners["book/3"] = nil
		}, `converged: book/3 assigned to slot-1 (m1), owned by nobody`},
		{"a slot's shard owned by its old holder", func(r *convergedRig) {
			r.slots = 3
			r.live[0].slot, r.live[1].slot = 1, 0
			r.balance["book"] = []string{"slot-0", "slot-1", "slot-0", "slot-1"}
			r.owners["book/2"] = []string{"m1/01"}
		}, `converged: book/2 assigned to slot-0 (m2), owned by m1/01`},
		{"agreed on a vacant slot", func(r *convergedRig) {
			r.slots = 3
			r.live[0].slot, r.live[1].slot = 1, 0
			for _, m := range []string{"m1", "m2", "m3"} {
				r.set(m, "slot-0", "slot-1", "slot-2", "slot-1")
			}
			r.balance["book"] = []string{"slot-0", "slot-1", "slot-0", "slot-1"}
		}, `converged: book: m1's assignment differs from the live members' at shard 2: "slot-2" != "slot-0"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newConvergedRig()
			tt.setup(r)
			if got := message(r.err()); got != tt.want {
				t.Errorf("convergedErr = %q, want %q", got, tt.want)
			}
		})
	}
}

// message returns err's text, or "" for nil.
func message(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// finalEnv names the final check that TestFinalsChild makes fail.
const finalEnv = "TENURE_SIM_FINAL"

// TestFinalsRegistered checks that Build registers tenure/g5 and then tenure/converged as final
// checks, and leaves tenure/g5 out with NoLiveness: a child process takes every run off the
// checker's shards at 1 s of a steady world, which nothing starts again, and its run fails
// both finals, in that order, or only tenure/converged with NoLiveness.
func TestFinalsRegistered(t *testing.T) {
	if os.Getenv(finalEnv) != "" {
		t.Skip("in the child process")
	}
	tests := []struct {
		child string
		want  []string
		not   string
	}{
		{"g5", []string{
			`final check "tenure/g5" failed after the run at t=30.000000000s`,
			"g5: book/0 has no owner from 1.000000000s to 30.000000001s, which contains 21.045000000s (recovery 0.000000000s + bound 21.045000000s)",
			`also failed: "tenure/converged"`,
		}, ""},
		{"converged", []string{
			`final check "tenure/converged" failed after the run at t=30.000000000s`,
			"converged: book/0 assigned to m",
		}, "tenure/g5"},
	}
	for _, tt := range tests {
		t.Run(tt.child, func(t *testing.T) {
			// The child's output is matched by substring, and none of faultline's variables of
			// this process reaches it, as in TestInvariantsRegistered.
			cmd := exec.Command(os.Args[0], "-test.run=^TestFinalsChild$", "-test.v") //nolint:gosec // the test binary itself
			cmd.Env = append(childEnviron(), finalEnv+"="+tt.child, "FAULTLINE_ARTIFACTS=off")
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() == 0 {
				t.Fatalf("child exited with %v, want a failure; output:\n%s", err, out.String())
			}
			for _, want := range tt.want {
				if !strings.Contains(out.String(), want) {
					t.Errorf("child's output does not contain %q:\n%s", want, out.String())
				}
			}
			if tt.not != "" && strings.Contains(out.String(), tt.not) {
				t.Errorf("child's output names %q:\n%s", tt.not, out.String())
			}
		})
	}
}

// TestFinalsChild is the child process of TestFinalsRegistered: in a steady world of 30 s, it
// reports every member's holder Down to the checker at 1 s, so the checker sees no shard owned
// from then on. With TENURE_SIM_FINAL=converged the scenario has NoLiveness.
func TestFinalsChild(t *testing.T) {
	child := os.Getenv(finalEnv)
	if child == "" {
		t.Skip("runs only as the child process of TestFinalsRegistered")
	}
	sc := Scenario{NoLiveness: child == "converged"}
	faultline.Run(t, faultline.Options{Seeds: 1, Duration: 30 * time.Second}, func(fw *faultline.World) {
		w := Build(fw, sc)
		fw.Sim.At(kernel.Time(time.Second), "test/down", func() {
			for _, m := range w.Members() {
				w.Checker().Down(m.Holder())
			}
		})
	})
}

// TestConvergedSlots runs a steady world with slots under each rule, with the finals on: the
// members agree on the balanced placement over the slots they hold, and each shard runs on the
// member that holds its slot.
func TestConvergedSlots(t *testing.T) {
	forRules(t, func(t *testing.T, rule Takeover) {
		sc := Scenario{Takeover: rule, Slots: 5, StopDelay: time.Second}
		opts := faultline.Options{Seeds: 2, Duration: 90 * time.Second, CheckDeterminism: false}
		faultline.Run(t, opts, func(fw *faultline.World) {
			w := Build(fw, sc)
			fw.RunFor(90*time.Second - time.Nanosecond)
			live := w.liveMembers()
			if len(live) != 5 {
				fw.T().Fatalf("%d live members, want 5", len(live))
			}
			for _, m := range live {
				if m.slot < 0 {
					fw.T().Errorf("%s holds no slot", m.name)
				}
			}
		})
	})
}
