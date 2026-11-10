// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
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
)

// runWorld runs Build for sc in every run of a faultline test with opts, which checks
// determinism, then body with the world. A failure of body goes to fw.T().
func runWorld(t *testing.T, opts faultline.Options, sc Scenario, body func(fw *faultline.World, w *World)) {
	t.Helper()
	opts.CheckDeterminism = true
	if opts.Seeds == 0 {
		opts.Seeds = 3
	}
	faultline.Run(t, opts, func(fw *faultline.World) {
		body(fw, Build(fw, sc))
	})
}

// fullTrace returns options that keep every record, for a run of d.
func fullTrace(d time.Duration) faultline.Options {
	return faultline.Options{Duration: d, Trace: kernel.TraceConfig{Level: kernel.TraceFull}}
}

// records returns the records of the kind, in trace order.
func records(sim *kernel.Sim, kind string) []kernel.Record {
	var out []kernel.Record
	for _, r := range sim.Records() {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// attrs returns the attributes of r as "key=value" strings, in order.
func attrs(r kernel.Record) []string {
	out := make([]string, len(r.Attrs))
	for i, a := range r.Attrs {
		out[i] = a.Key + "=" + a.Value
	}
	return out
}

// attrValue returns the value of r's attribute key, or "" when it has none.
func attrValue(r kernel.Record, key string) string {
	for _, a := range r.Attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return ""
}

// stream returns the random stream label of a fresh simulation of seed: the stream the world
// drew from, from its first draw.
func stream(seed uint64, label string) *rand.Rand {
	return kernel.New(kernel.Config{Seed: seed}).Rand(label)
}

// at returns a time of the run.
func at(d time.Duration) kernel.Time { return kernel.Time(d) }

// rollout is a rollout that a test never activates.
var rollout = &Rollout{Start: 10 * time.Second, Gap: 5 * time.Second, Surge: 2 * time.Second}

// TestATSIM04NodeOrder checks the order of the nodes: the store (NodeID 1), the fence (2), the
// members m1 … mN from NodeID 3 and, with a Rollout, the replacements m1-r … mN-r after them;
// members and replacements are servers, the store and the fence are not; the roles store, fence
// and members; members start active and replacements on standby. The steps were written for
// this task from the issue and the design note.
func TestATSIM04NodeOrder(t *testing.T) {
	tests := []struct {
		name  string
		sc    Scenario
		nodes []string
	}{
		{"defaults", Scenario{}, []string{"store", "fence", "m1", "m2", "m3", "m4", "m5"}},
		{"rollout", Scenario{Members: 3, Slots: 3, Rollout: rollout},
			[]string{"store", "fence", "m1", "m2", "m3", "m1-r", "m2-r", "m3-r"}},
		{"one member", Scenario{Members: 1}, []string{"store", "fence", "m1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runWorld(t, faultline.Options{Duration: 120 * time.Second}, tt.sc, func(fw *faultline.World, w *World) {
				t := fw.T()
				var names []string
				for i, n := range fw.Sim.Nodes() {
					names = append(names, n.Name())
					if n.ID() != kernel.NodeID(i+1) { //nolint:gosec // a few nodes
						t.Errorf("node %s has ID %d, want %d", n.Name(), n.ID(), i+1)
					}
					if server := n.Name() != "store" && n.Name() != "fence"; n.HasTag("server") != server {
						t.Errorf("node %s: server tag %v, want %v", n.Name(), n.HasTag("server"), server)
					}
				}
				if !slices.Equal(names, tt.nodes) {
					t.Errorf("nodes %v, want %v", names, tt.nodes)
				}
				if w.Store().ID() != 1 || w.Fence().ID() != 2 {
					t.Errorf("store %d, fence %d, want 1 and 2", w.Store().ID(), w.Fence().ID())
				}
				var members, active []kernel.NodeID
				for i, m := range w.Members() {
					if m.Node.Name() != tt.nodes[i+2] || m.Name != m.Node.Name() {
						t.Errorf("Members()[%d] is %s on node %s, want %s", i, m.Name, m.Node.Name(), tt.nodes[i+2])
					}
					if w.memberOf(m.Node) != m {
						t.Errorf("memberOf(%s) is not the member", m.Name)
					}
					want := Active
					if m.Replacement {
						want = Standby
					} else {
						active = append(active, m.Node.ID())
					}
					name := "m" + strconv.Itoa(m.Index)
					if m.Replacement {
						name += "-r"
					}
					if m.Phase() != want || m.Name != name {
						t.Errorf("member %s: index %d, replacement %v, phase %v", m.Name, m.Index, m.Replacement, m.Phase())
					}
					members = append(members, m.Node.ID())
				}
				if w.memberOf(w.Store()) != nil || w.memberOf(w.Fence()) != nil {
					t.Errorf("memberOf gives a member for the store or the fence")
				}
				var servers []kernel.NodeID
				for _, n := range fw.Servers() {
					servers = append(servers, n.ID())
				}
				if !slices.Equal(servers, members) {
					t.Errorf("servers %v, want the members %v", servers, members)
				}
				if got := w.storeRole(); !slices.Equal(got, []kernel.NodeID{1}) {
					t.Errorf("role store %v, want [1]", got)
				}
				if got := w.fenceRole(); !slices.Equal(got, []kernel.NodeID{2}) {
					t.Errorf("role fence %v, want [2]", got)
				}
				if got := w.membersRole(); !slices.Equal(got, active) {
					t.Errorf("role members %v, want the active members %v", got, active)
				}
				if tt.sc.Rollout == nil {
					return
				}
				// The role follows the phases: an exited member leaves it, an activated
				// replacement joins it, in NodeID order.
				ms := w.Members()
				ms[1].phase, ms[3].phase = Exited, Active
				if got, want := w.membersRole(), []kernel.NodeID{3, 5, 6}; !slices.Equal(got, want) {
					t.Errorf("role members after m2 exited and m1-r became active: %v, want %v", got, want)
				}
			})
		})
	}
}

// holderPattern is the form of a holder string.
var holderPattern = regexp.MustCompile(`^m[0-9]+(-r)?/[0-9a-f]{32}$`)

// TestATSIM08Holders checks the incarnations: each boot of an active member draws a holder
// `<member>/<32 hex>` from two Uint64 draws of tenure/incarnation/<member> and emits
// tenure.boot cause=boot; a restart draws the next holder; another start of an incarnation, as
// a recovery, passes its own cause; a replacement on standby and an exited member start none
// at their boots. The steps were written for this task from the issue and the design note.
func TestATSIM08Holders(t *testing.T) {
	sc := Scenario{Members: 3, Slots: 3, Rollout: rollout}
	runWorld(t, fullTrace(120*time.Second), sc, func(fw *faultline.World, w *World) {
		t := fw.T()
		m := map[string]*Member{}
		for _, mb := range w.Members() {
			m[mb.Name] = mb
		}
		// The driver starts after the holder was drawn and tenure.boot emitted.
		var starts []string
		w.startMember = func(mb *Member) {
			recs := fw.Sim.Records()
			last := recs[len(recs)-1]
			if last.Kind != kindBoot || last.Node != mb.Node.ID() || attrValue(last, "holder") != mb.Holder() {
				t.Errorf("%s started before its tenure.boot: the last record is %s %v", mb.Name, last.Kind, attrs(last))
			}
			starts = append(starts, mb.Holder())
		}
		var firstM1 string
		crashAndRestart := func(name string, down, up time.Duration) {
			fw.Sim.At(at(down), "test/crash", func() { m[name].Node.Crash() })
			fw.Sim.At(at(up), "test/restart", func() { m[name].Node.Restart() })
		}
		crashAndRestart("m2", 5*time.Second, 6*time.Second)
		crashAndRestart("m1-r", 5*time.Second, 6*time.Second)
		fw.Sim.At(at(7*time.Second), "test/recover", func() { w.incarnate(m["m3"], "recover") })
		fw.Sim.At(at(8*time.Second), "test/exit", func() {
			firstM1 = m["m1"].Holder()
			m["m1"].phase = Exited
		})
		crashAndRestart("m1", 8*time.Second, 9*time.Second)
		fw.RunFor(10 * time.Second)

		type boot struct {
			node   string
			inc    uint32
			at     time.Duration
			holder string
			cause  string
		}
		var got []boot
		for _, r := range records(fw.Sim, kindBoot) {
			if len(r.Attrs) != 2 || r.Attrs[0].Key != "holder" || r.Attrs[1].Key != "cause" {
				t.Errorf("tenure.boot attributes %v, want holder and cause", attrs(r))
			}
			got = append(got, boot{fw.Sim.Node(r.Node).Name(), r.Inc, time.Duration(r.At), attrValue(r, "holder"), attrValue(r, "cause")})
		}
		draws := map[string]*rand.Rand{}
		holder := func(name string) string {
			if draws[name] == nil {
				draws[name] = stream(fw.Seed(), streamIncarnation+name)
			}
			r := draws[name]
			hi, lo := r.Uint64(), r.Uint64()
			return fmt.Sprintf("%s/%016x%016x", name, hi, lo)
		}
		// The boots at time 0 run in the scheduler's order, so both lists are compared sorted
		// by time and node; the holders of a member follow its stream.
		slices.SortStableFunc(got, func(a, b boot) int {
			if a.at != b.at {
				return cmp.Compare(a.at, b.at)
			}
			return strings.Compare(a.node, b.node)
		})
		want := []boot{
			{"m1", 1, 0, holder("m1"), "boot"},
			{"m2", 1, 0, holder("m2"), "boot"},
			{"m3", 1, 0, holder("m3"), "boot"},
			{"m2", 2, 6 * time.Second, holder("m2"), "boot"},
			{"m3", 1, 7 * time.Second, holder("m3"), "recover"},
		}
		if !slices.Equal(got, want) {
			t.Errorf("tenure.boot records:\n got %v\nwant %v", got, want)
		}
		if len(starts) != len(want) {
			t.Errorf("the driver started %d times, want once per incarnation, %d: %v", len(starts), len(want), starts)
		}
		seen := map[string]bool{}
		for _, b := range got {
			if !holderPattern.MatchString(b.holder) || !strings.HasPrefix(b.holder, b.node+"/") || seen[b.holder] {
				t.Errorf("holder %q of %s is malformed or not new", b.holder, b.node)
			}
			seen[b.holder] = true
		}
		for name, h := range map[string]string{"m1": firstM1, "m2": want[3].holder, "m3": want[4].holder, "m1-r": "", "m2-r": ""} {
			if m[name].Holder() != h {
				t.Errorf("%s.Holder() = %q, want %q", name, m[name].Holder(), h)
			}
		}
		if firstM1 != want[0].holder {
			t.Errorf("m1's holder %q, want %q", firstM1, want[0].holder)
		}
		if m["m1"].Phase() != Exited || m["m1-r"].Phase() != Standby || m["m1"].Node.State() != kernel.NodeUp {
			t.Errorf("m1 %v (%v), m1-r %v", m["m1"].Phase(), m["m1"].Node.State(), m["m1-r"].Phase())
		}
	})
}

// TestPhaseString checks the names of the phases.
func TestPhaseString(t *testing.T) {
	for p, want := range map[Phase]string{Standby: "standby", Active: "active", Exited: "exited", 7: "Phase(7)"} {
		if p.String() != want {
			t.Errorf("Phase(%d).String() = %q, want %q", p, p.String(), want)
		}
	}
}

// crashes is a fault plan that crashes members.
func crashes(quiet time.Duration) *fault.Random {
	return &fault.Random{Quiet: quiet, Rules: []fault.Rule{
		{Kind: fault.KindCrash, Every: 3 * time.Second, MaxFor: time.Second, Target: roleMembers},
	}}
}

// TestBuildRecovery checks R, the quiet window and the tenure.bound record: R is 0 without
// faults; with faults or wall steps it is the end minus the quiet window, which defaults to the
// larger of a quarter of the run and B rounded up to a whole second; faultline's recovery
// window starts at R; the plan is a copy, and the caller's scenario stays as it is.
func TestBuildRecovery(t *testing.T) {
	tests := []struct {
		name     string
		sc       Scenario
		d        time.Duration
		recovery time.Duration
		bound    time.Duration
		g2       bool
	}{
		{"no faults", Scenario{}, 60 * time.Second, 0, 21_045_000_000, true},
		{"faults, B rounded up", Scenario{Faults: crashes(0)}, 60 * time.Second, 38 * time.Second, 21_045_000_000, true},
		{"faults, a quarter of the run", Scenario{Faults: crashes(0)}, 120 * time.Second, 90 * time.Second, 21_045_000_000, true},
		{"faults, their Quiet", Scenario{Faults: crashes(25 * time.Second)}, 60 * time.Second, 35 * time.Second, 21_045_000_000, true},
		{"wall steps", Scenario{WallSteps: time.Second}, 60 * time.Second, 38 * time.Second, 21_045_000_000, true},
		{"observer, member pauses", Scenario{Takeover: Observer, Faults: random(pause(roleMembers))},
			60 * time.Second, 38 * time.Second, 21_090_000_000, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var before fault.Random
			if tt.sc.Faults != nil {
				before = *tt.sc.Faults
			}
			runWorld(t, fullTrace(tt.d), tt.sc, func(fw *faultline.World, w *World) {
				t := fw.T()
				if w.Recovery() != at(tt.recovery) || w.Bound() != tt.bound || w.AssertsG2() != tt.g2 {
					t.Errorf("Recovery %v, Bound %v, AssertsG2 %v; want %v, %v, %v",
						w.Recovery(), w.Bound(), w.AssertsG2(), at(tt.recovery), tt.bound, tt.g2)
				}
				if tt.sc.Faults != nil {
					if fw.RecoveryStart() != w.Recovery() {
						t.Errorf("faultline's RecoveryStart %v, want R %v", fw.RecoveryStart(), w.Recovery())
					}
					plan := w.Scenario().Faults
					if plan == tt.sc.Faults || plan.Quiet != tt.d-tt.recovery || !slices.Equal(plan.Rules, tt.sc.Faults.Rules) ||
						plan.MaxDown != tt.sc.Faults.MaxDown {
						t.Errorf("planned %+v, want a copy of %+v with Quiet %v", *plan, *tt.sc.Faults, tt.d-tt.recovery)
					}
				}
				bound := records(fw.Sim, kindBound)
				want := []string{
					"takeover=" + tt.sc.Takeover.String(),
					"r_ns=" + strconv.FormatInt(int64(tt.recovery), 10),
					"b_ns=" + strconv.FormatInt(int64(tt.bound), 10),
					"g2=" + strconv.FormatBool(tt.g2),
				}
				if len(bound) != 1 || bound[0].Node != 0 || bound[0].At != 0 || !slices.Equal(attrs(bound[0]), want) {
					t.Errorf("tenure.bound records %+v, want one global record at 0 with %v", bound, want)
				}
			})
			if tt.sc.Faults != nil && (tt.sc.Faults.Quiet != before.Quiet || !slices.Equal(tt.sc.Faults.Rules, before.Rules)) {
				t.Errorf("the scenario's Faults changed: %+v, was %+v", *tt.sc.Faults, before)
			}
		})
	}
}

// clockJumps is a scenario with a clock-jump rule.
var clockJumps = Scenario{Faults: random(fault.Rule{Kind: fault.KindClockJump, Every: time.Second, Magnitude: int64(time.Millisecond)})}

// childEnv is the variable that makes TestATSIM06Child run.
const childEnv = "TENURE_SIM06_CHILD"

// TestATSIM06ClockJump checks that clock jumps are refused: a clock-jump rule is a setup error
// with its exact text; Build fails the seed with it, which a child process shows, since a seed
// that fails fails its test; and the timer observer fails a run whose clock jumped all the same
// with the same text, while drift changes pass. The steps were written for this task from the
// issue and the design note.
func TestATSIM06ClockJump(t *testing.T) {
	if os.Getenv(childEnv) == "1" {
		t.Skip("in the child process")
	}
	t.Run("setup", func(t *testing.T) {
		err := SetupError(clockJumps, at(time.Minute))
		if err == nil || err.Error() != clockJumpText {
			t.Errorf("SetupError = %v, want %q", err, clockJumpText)
		}
		if want := "tenure: setup: Faults: clock-jump rules are not supported: a jump would move the monotonic clock; use WallSteps"; clockJumpText != want {
			t.Errorf("the text is %q, want %q", clockJumpText, want)
		}
	})
	t.Run("build", func(t *testing.T) {
		// The child's output is matched by substring: go test adds lines of its own. The child
		// runs one seed of its own, so none of faultline's variables of this process reaches
		// it: a seed list, a schedule to replay or a results file would change its run or have
		// it write where this process does, and an artifact directory may not be writable.
		cmd := exec.Command(os.Args[0], "-test.run=^TestATSIM06Child$", "-test.v") //nolint:gosec // the test binary itself
		cmd.Env = append(childEnviron(), childEnv+"=1", "FAULTLINE_ARTIFACTS=off")
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() == 0 {
			t.Fatalf("child exited with %v, want a failure; output:\n%s", err, out.String())
		}
		for _, want := range []string{clockJumpText, "--- FAIL: TestATSIM06Child/seed=0x"} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("child's output does not contain %q:\n%s", want, out.String())
			}
		}
		if strings.Contains(out.String(), "built a world") {
			t.Errorf("Build returned in the child:\n%s", out.String())
		}
	})
	t.Run("observer", func(t *testing.T) {
		for _, tt := range []struct {
			name  string
			event func(n *kernel.Node)
			fails bool
		}{
			{"jump forward", func(n *kernel.Node) { n.JumpClock(time.Millisecond) }, true},
			{"jump back", func(n *kernel.Node) { n.JumpClock(-time.Microsecond) }, true},
			{"jump forward 2ns", func(n *kernel.Node) { n.JumpClock(2) }, true},
			{"jump back 3ns", func(n *kernel.Node) { n.JumpClock(-3) }, true},
			{"drift change", func(n *kernel.Node) { n.SetDrift(-300_000) }, false},
		} {
			s, nodes, _ := rawSim(t, 77_777)
			n := nodes[1]
			s.At(at(time.Second+3), "test/clock", func() { tt.event(n) })
			s.RunFor(2 * time.Second)
			switch err := s.Err(); {
			case tt.fails && (err == nil || err.Error() != clockJumpText):
				t.Errorf("%s: the run failed with %v, want %q", tt.name, err, clockJumpText)
			case !tt.fails && err != nil:
				t.Errorf("%s: the run failed with %v", tt.name, err)
			}
		}
		// A drift changed between two events, by the body of a test, passes too: the look after
		// the next event sees a reading between what the two drifts predict.
		s, nodes, c := rawSim(t, 77_777)
		s.At(at(1500*time.Millisecond), "test/event", func() {})
		s.RunFor(1234567)
		nodes[0].SetDrift(400_000)
		s.RunFor(time.Second)
		if c.of(nodes[0]).ppm != 0 {
			t.Fatalf("the look ran before the event that should show the change")
		}
		s.RunFor(time.Second)
		if err := s.Err(); err != nil {
			t.Errorf("a drift changed between events failed the run: %v", err)
		}
		if nc := c.of(nodes[0]); nc.ppm != 400_000 || nc.at != at(1500*time.Millisecond) {
			t.Errorf("the last look was at %v with %d ppm, want at 1.5s with 400000", nc.at, nc.ppm)
		}
	})
}

// childEnviron returns the environment of this process without faultline's variables.
func childEnviron() []string {
	return slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "FAULTLINE_") })
}

// TestATSIM06Child is the child process of TestATSIM06ClockJump: it builds a world with a
// clock-jump rule. It runs only with TENURE_SIM06_CHILD=1.
func TestATSIM06Child(t *testing.T) {
	if os.Getenv(childEnv) != "1" {
		t.Skip("runs only as the child process of TestATSIM06ClockJump")
	}
	faultline.Run(t, faultline.Options{Seeds: 1, CheckDeterminism: true}, func(fw *faultline.World) {
		Build(fw, clockJumps)
		fw.T().Log("built a world")
	})
}

// TestOnlyRecordsEmits checks that records.go is the only file of the package that calls Emit.
func TestOnlyRecordsEmits(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		name := f.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "records.go" {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(src, []byte(".Emit(")) {
			t.Errorf("%s calls Emit; only records.go emits records", name)
		}
	}
}
