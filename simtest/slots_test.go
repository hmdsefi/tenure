// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
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
// TestSlotScaleDown lasts 125 s, not 90 s: R is SetSlotsAt, 40 s, and with slots the bound B counts SlotGrace (60 s), 81.045 s under the server rule, so a run
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
// every event. A run in which the count never changes would pass those finals, so a final of
// its own checks that the members place the shards over the three slots that are left, which
// they do only once the store has set the count to 3.
func TestSlotScaleDown(t *testing.T) {
	runRow(t, rowNamed(t, "TestSlotScaleDown"), func(fw *faultline.World, w *world.World) {
		fw.Final("test/scaled_down", func() error {
			_, err := placementErr(w)
			return err
		})
	})
}

// TestSlotGrace crashes m5 at 30 s and never restarts it (SIM-600). Its slot stays vacant,
// and after SlotGrace the slot drops out of the placement, so that its shards do not stay
// ownerless: by the bound, which counts SlotGrace, after the crash the four members that are
// left agree on the placement over the four slots that are held, and every shard runs on its
// assignee. The run ends with m5's node still down.
func TestSlotGrace(t *testing.T) {
	runRow(t, rowNamed(t, "TestSlotGrace"), func(fw *faultline.World, w *world.World) {
		if got := w.Recovery(); got != kernel.Time(graceCrash) {
			fw.T().Fatalf("R = %v, want the crash time %v", got, kernel.Time(graceCrash))
		}
		fw.Plan(fault.Script(fault.Event{At: kernel.Time(graceCrash), Kind: fault.KindCrash, Node: "m5"}))
		m5 := w.Members()[4]
		fw.Sim.At(kernel.Time(graceCrash-time.Second), "test/m5_slot", func() {
			places, err := w.Placements()
			if err != nil {
				fw.Sim.Fail(err)
				return
			}
			for _, p := range places {
				if p.Member == m5.Name && p.Slot < 0 {
					fw.Sim.Fail(errors.New("m5 holds no slot just before it crashes, so no slot becomes vacant"))
				}
			}
		})
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

// scaleDownErr decides AT-SIM-29 at the end of a run: m1 sent setSlots at 40 s (sentErr), the store set the slot count to 3 once m1
// asked for it, after 40 s and before 40 s plus a retry period (setRecordErr); the machines that
// are up place the shards as placementErr says; and no shard runs on former, the member that
// held slot 3 before.
func scaleDownErr(w *world.World, recs []kernel.Record, former string) error {
	if former == "" {
		return errors.New("no member held slot 3 before the change")
	}
	if err := sentErr(recs); err != nil {
		return err
	}
	if err := setRecordErr(recs, w.Scenario().Timing.RetryPeriod); err != nil {
		return err
	}
	places, err := placementErr(w)
	if err != nil {
		return err
	}
	for shard := range places[0].Assignment("book") {
		for _, h := range w.Checker().Owners("book", shard) {
			if strings.HasPrefix(h, former+"/") {
				return fmt.Errorf("book/%d runs on %s, the member that held slot 3", shard, h)
			}
		}
	}
	return nil
}

// sentErr checks that the first setSlots that goes out on the network is m1's, and that it goes
// out at 40 s exactly: m1 is up and has a driver then, so the post of the change is not late.
// It needs the records of a full trace.
func sentErr(recs []kernel.Record) error {
	for _, r := range recs {
		if r.Kind != "net.send" || !strings.HasPrefix(attr(r, "payload"), "setSlots") {
			continue
		}
		if from := attr(r, "from"); from != "m1" {
			return fmt.Errorf("the first setSlots was sent by %s, want m1", from)
		}
		if time.Duration(r.At) != scaleDownAt {
			return fmt.Errorf("m1 first sent setSlots at %s, want %s", r.At, kernel.Time(scaleDownAt))
		}
		return nil
	}
	return errors.New("no setSlots was sent")
}

// placementErr checks the placement at the end of a run that went from 5 slots to 3: the
// machines that are up agree on an assignment of book over slot-0 to slot-2 that gives every
// shard an assignee, each of the three slots is some member's, and every shard runs on the
// member of its assignee and on no other. It returns the placements it judged.
func placementErr(w *world.World) ([]world.Placement, error) {
	places, err := w.Placements()
	if err != nil {
		return nil, err
	}
	if len(places) == 0 {
		return nil, errors.New("no member is up at the end")
	}
	keys := []string{"slot-0", "slot-1", "slot-2"}
	want := places[0].Assignment("book")
	if len(want) == 0 {
		return nil, fmt.Errorf("%s has no assignment for book", places[0].Member)
	}
	holderOf := map[string]string{} // slot key to the holder of the member that holds the slot; looked up only
	for _, p := range places {
		if p.Slot >= 0 {
			holderOf[fmt.Sprintf("slot-%d", p.Slot)] = p.Holder
		}
		if got := p.Assignment("book"); !slices.Equal(got, want) {
			return nil, fmt.Errorf("%s's assignment %q differs from %s's %q", p.Member, got, places[0].Member, want)
		}
	}
	for shard, key := range want {
		if !slices.Contains(keys, key) {
			return nil, fmt.Errorf("book/%d is assigned to %q, not to one of slot-0 to slot-2", shard, key)
		}
		holder, ok := holderOf[key]
		if !ok {
			return nil, fmt.Errorf("book/%d is assigned to %s, which no member holds", shard, key)
		}
		if got := w.Checker().Owners("book", shard); !slices.Equal(got, []string{holder}) {
			return nil, fmt.Errorf("book/%d is assigned to %s (%s), owned by %q", shard, key, holder, got)
		}
	}
	for _, key := range keys {
		if _, ok := holderOf[key]; !ok {
			return nil, fmt.Errorf("no member holds %s at the end", key)
		}
	}
	return places, nil
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

// The rollout rows (SIM-600): Slots 5 and Rollout{Start: 20 s, Gap: B + 5 s, Surge: 5 s}, for
// a run of Start + 5·Gap + B + 10 s. B is world.ComputeBound of the scenario, which counts
// SlotGrace; it is larger under the Observer rule, whose takeover takes more messages, and both
// rules run one scenario, so the rows take the larger of the two. TestSlotRolloutFaults adds
// TestCrashRestart's crashes of the active members, one every 20 s on average.
var (
	rolloutBound = max(world.ComputeBound(world.Scenario{Slots: 5, Takeover: world.Server}),
		world.ComputeBound(world.Scenario{Slots: 5, Takeover: world.Observer}))
	rolloutPlan = &world.Rollout{Start: 20 * time.Second, Gap: rolloutBound + 5*time.Second, Surge: 5 * time.Second}
	rolloutRun  = rolloutPlan.Start + 5*rolloutPlan.Gap + rolloutBound + 10*time.Second
)

// rolloutFaults returns the fault plan of TestSlotRolloutFaults: TestCrashRestart's rule, with
// Every 20 s.
func rolloutFaults() *fault.Random {
	return &fault.Random{MaxDown: 2, Rules: []fault.Rule{
		{Kind: fault.KindCrash, Target: "members", Every: 20 * time.Second, MinFor: time.Second, MaxFor: 20 * time.Second},
	}}
}

// TestSlotRollout replaces each of the five members once, under each rule (SIM-600): m1-r is
// activated at 20 s, m1 drains 5 s later, and so on every Gap. Each replacement claims the slot
// of the member it replaces and takes its shards, each shard once (tenure/rollout); every shard
// has an owner again within B of the last drain (tenure/g5), and at the end the five
// replacements agree on the placement and run it (tenure/converged). Every invariant holds after
// every event.
func TestSlotRollout(t *testing.T) {
	runRow(t, rowNamed(t, "TestSlotRollout"), nil)
}

// TestSlotRolloutFaults is TestSlotRollout with members crashing during the rollout (SIM-600):
// the final checks hold for every seed, under each rule.
func TestSlotRolloutFaults(t *testing.T) {
	runRow(t, rowNamed(t, "TestSlotRolloutFaults"), nil)
}

// rolloutChildEnv makes TestRolloutChild run.
const rolloutChildEnv = "TENURE_SIM_ROLLOUT_CHILD"

// TestATSIM35RolloutMessage is AT-SIM-35: a child process runs one seed of TestSlotRollout's
// row under the Server rule, in which the world counts a second start of one of m1-r's shards
// (World.PlantStart) after m1-r took them over, and the run fails with tenure/rollout's
// message "rollout: replacement 1: book/<n> started 2 times on m1-r". Its steps were written for
// this task from the issue and the design note, since the original text of the acceptance test
// is lost.
func TestATSIM35RolloutMessage(t *testing.T) {
	if os.Getenv(rolloutChildEnv) != "" {
		t.Skip("in the child process")
	}
	// The child's output is matched by substring, and none of faultline's variables of this
	// process reaches it.
	cmd := exec.Command(os.Args[0], "-test.run=^TestRolloutChild$", "-test.v") //nolint:gosec // the test binary itself
	cmd.Env = append(slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "FAULTLINE_") }),
		rolloutChildEnv+"=1", "FAULTLINE_ARTIFACTS=off")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("child exited with %v, want a failure; output:\n%s", err, out.String())
	}
	want := regexp.MustCompile(`(?s)tenure/rollout.*rollout: replacement 1: book/\d+ started 2 times on m1-r`)
	if !want.MatchString(out.String()) {
		t.Errorf("child's output does not match %q:\n%s", want, out.String())
	}
}

// TestRolloutChild is the child process of TestATSIM35RolloutMessage: one seed of
// TestSlotRollout's row under the Server rule, with a second start of a shard of m1-r planted
// just before m2-r is activated, when m1-r has long held m1's slot and its shards.
func TestRolloutChild(t *testing.T) {
	if os.Getenv(rolloutChildEnv) == "" {
		t.Skip("runs only as the child process of TestATSIM35RolloutMessage")
	}
	r := rowNamed(t, "TestSlotRollout")
	opts := r.options()
	opts.Seeds = 1
	faultline.Run(t, opts, r.body(world.Server, func(fw *faultline.World, w *world.World) {
		at := kernel.Time(rolloutPlan.Start + rolloutPlan.Gap - time.Second)
		fw.Sim.At(at, "test/plant_start", func() {
			m1r := w.Members()[5]
			shard, err := shardOf(w, m1r.Name)
			if err != nil {
				fw.Sim.Fail(err)
				return
			}
			w.PlantStart(m1r, "book", shard)
		})
	}))
}

// shardOf returns the first shard of book that the machine of the member name assigns to the
// slot it holds.
func shardOf(w *world.World, name string) (int, error) {
	places, err := w.Placements()
	if err != nil {
		return 0, err
	}
	for _, p := range places {
		if p.Member != name {
			continue
		}
		key := fmt.Sprintf("slot-%d", p.Slot)
		for shard, k := range p.Assignment("book") {
			if k == key {
				return shard, nil
			}
		}
		return 0, fmt.Errorf("%s holds %s, which has no shard of book", name, key)
	}
	return 0, fmt.Errorf("%s is not active and up", name)
}

// TestSlotRolloutRecords checks the records of TestSlotRollout's runs, with a full trace, under
// each rule: AT-SIM-09, AT-SIM-19 and AT-SIM-41 (see rolloutRecordsErr). Their steps were
// written for this task from the issue and the design note, since the original texts of the
// acceptance tests are lost.
func TestSlotRolloutRecords(t *testing.T) {
	r := rowNamed(t, "TestSlotRollout")
	opts := r.options()
	opts.Seeds = 3
	opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				fw.Final("test/rollout_records", func() error {
					return rolloutRecordsErr(w, fw.Sim.Records())
				})
			}))
		})
	}
}

// The kinds of records that only the rollout tests read.
const (
	kindSlot = "tenure.slot"
	kindView = "tenure.view"
)

// rolloutRecordsErr checks the records of a rollout without faults, replacement by replacement,
// with mi the member that mi-r replaces, activated at A = Start + (i−1)·Gap and told to drain
// at D = A + Surge:
//
//   - AT-SIM-09 (activation, SIM-150): mi-r has no tenure record before A; at A it has exactly
//     one tenure.boot, cause activate, whose holder is its holder at the end; it boots no more.
//   - AT-SIM-41 (the handover, D16): mi's holder last claimed some slot k before D; at or after
//     D the store records mi releasing slot k, then mi exits with cause drain; the first slot
//     mi-r's holder claims is k, after that exit. Each shard of book is started once at or
//     after Start, on the replacement of the member that ran it before Start.
//   - AT-SIM-19 (the view, SIM-270): from D on, slot k of the store's views is held by mi's
//     holder, then by nobody, then by mi-r's holder, and by no one else; the last view lists
//     the five replacements and no replaced member.
func rolloutRecordsErr(w *world.World, recs []kernel.Record) error {
	ro := w.Scenario().Rollout
	n := w.Scenario().Members
	ms := w.Members()
	store := w.Store().ID()
	for i := range n {
		old, rep := ms[i], ms[n+i]
		activate := kernel.Time(ro.Start + time.Duration(i)*ro.Gap)
		drain := activate.Add(ro.Surge)
		if err := activationErr(recs, rep, activate); err != nil {
			return err
		}
		k, err := handoverErr(recs, store, old, rep, drain)
		if err != nil {
			return err
		}
		if err := viewSlotErr(recs, store, k, old, rep, drain); err != nil {
			return err
		}
	}
	if err := movesErr(recs, w); err != nil {
		return err
	}
	var last kernel.Record
	for _, r := range recs {
		if r.Node == store && r.Kind == kindView {
			last = r
		}
	}
	if got, want := attr(last, "members"), "m1-r,m2-r,m3-r,m4-r,m5-r"; got != want {
		return fmt.Errorf("the last view lists the members %q, want %q", got, want)
	}
	return nil
}

// activationErr checks AT-SIM-09 for the replacement rep, activated at at.
func activationErr(recs []kernel.Record, rep *world.Member, at kernel.Time) error {
	boots := 0
	for _, r := range recs {
		if r.Node != rep.Node.ID() || !strings.HasPrefix(r.Kind, "tenure.") {
			continue
		}
		if boots == 0 && r.Kind != kindBoot {
			return fmt.Errorf("%s: %s at %s before its activation", rep.Name, r.Kind, r.At)
		}
		if r.Kind != kindBoot {
			continue
		}
		boots++
		if r.At != at || attr(r, "cause") != "activate" || attr(r, "holder") != rep.Holder() {
			return fmt.Errorf("%s: tenure.boot at %s cause=%q holder=%q, want %s, activate and %s",
				rep.Name, r.At, attr(r, "cause"), attr(r, "holder"), at, rep.Holder())
		}
	}
	if boots != 1 {
		return fmt.Errorf("%s booted %d times, want once", rep.Name, boots)
	}
	return nil
}

// handoverErr checks AT-SIM-41's slot clauses for old and its replacement rep, with old told to
// drain at drain, and returns the slot handed over.
func handoverErr(recs []kernel.Record, store kernel.NodeID, old, rep *world.Member, drain kernel.Time) (int, error) {
	k, released, exited := "", kernel.Time(-1), false
	for _, r := range recs {
		claimed := r.Node == store && r.Kind == kindSlot && attr(r, "op") == "claim" && attr(r, "result") == "claimed"
		switch {
		case claimed && attr(r, "holder") == old.Holder() && r.At < drain:
			k = attr(r, "slot")
		case r.Node == store && r.Kind == kindSlot && attr(r, "op") == "release" && attr(r, "holder") == old.Holder() &&
			released < 0:
			if r.At < drain || k == "" || attr(r, "slot") != k {
				return 0, fmt.Errorf("%s released slot %s at %s, want slot %q at or after %s", old.Name, attr(r, "slot"), r.At, k, drain)
			}
			released = r.At
		case r.Node == old.Node.ID() && r.Kind == kindExit:
			if released < 0 || attr(r, "cause") != "drain" {
				return 0, fmt.Errorf("%s exited at %s with cause %q, want drain after it released its slot", old.Name, r.At, attr(r, "cause"))
			}
			exited = true
		case claimed && attr(r, "holder") == rep.Holder():
			if !exited || attr(r, "slot") != k {
				return 0, fmt.Errorf("%s claimed slot %s at %s, want slot %q after %s released it and exited", rep.Name, attr(r, "slot"), r.At, k, old.Name)
			}
			return strconv.Atoi(k)
		}
	}
	return 0, fmt.Errorf("%s never claimed %s's slot %q (released at %s, exited %v)", rep.Name, old.Name, k, released, exited)
}

// viewSlotErr checks AT-SIM-19 for slot k, handed over from old to rep, from drain on.
func viewSlotErr(recs []kernel.Record, store kernel.NodeID, k int, old, rep *world.Member, drain kernel.Time) error {
	var seen []string // the holders of slot k, without repeats
	for _, r := range recs {
		if r.Node != store || r.Kind != kindView || r.At < drain {
			continue
		}
		_, list, ok := strings.Cut(attr(r, "slots"), ":")
		holders := strings.Split(list, ",")
		if !ok || k >= len(holders) {
			return fmt.Errorf("view at %s: slots %q has no slot %d", r.At, attr(r, "slots"), k)
		}
		if h := holders[k]; len(seen) == 0 || seen[len(seen)-1] != h {
			seen = append(seen, h)
		}
	}
	if want := []string{old.Holder(), "", rep.Holder()}; !slices.Equal(seen, want) && !slices.Equal(seen, want[1:]) {
		return fmt.Errorf("slot %d in the views from %s: holders %q, want %s's, none, then %s's", k, drain, seen, old.Name, rep.Name)
	}
	return nil
}

// movesErr checks that each shard of book is started once from the first activation on, on the
// replacement of the member that ran it before.
func movesErr(recs []kernel.Record, w *world.World) error {
	start := kernel.Time(w.Scenario().Rollout.Start)
	names := map[kernel.NodeID]string{}
	for _, m := range w.Members() {
		names[m.Node.ID()] = m.Name
	}
	before := map[string]string{}  // by shard: the member that last started it before start
	after := map[string][]string{} // by shard: the members that started it from start on
	for _, r := range recs {
		if r.Kind != kindRun || attr(r, "op") != "start" || attr(r, "space") != "book" {
			continue
		}
		if s := attr(r, "shard"); r.At < start {
			before[s] = names[r.Node]
		} else {
			after[s] = append(after[s], names[r.Node])
		}
	}
	for shard := range 16 {
		s := strconv.Itoa(shard)
		if want := []string{before[s] + "-r"}; before[s] == "" || !slices.Equal(after[s], want) {
			return fmt.Errorf("book/%d ran on %q before the rollout and started on %q from it, want once, on %q",
				shard, before[s], after[s], want)
		}
	}
	return nil
}
